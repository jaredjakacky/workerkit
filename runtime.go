package workerkit

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	retrykit "github.com/jaredjakacky/workerkit-incubator/retry"
)

var (
	// Registration and lookup errors.
	ErrNilWorker                = errors.New("worker must not be nil")
	ErrWorkerAlreadyRegistered  = errors.New("worker already registered")
	ErrWorkerNotFound           = errors.New("worker not found")
	ErrCommandAlreadyRegistered = errors.New("command already registered")
	ErrCommandNotFound          = errors.New("command not found")

	// Lifecycle and admission errors.
	ErrInvalidWorkerState      = errors.New("invalid worker state")
	ErrRuntimeNotAcceptingWork = errors.New("runtime not accepting work")
	ErrWorkerNotAcceptingWork  = errors.New("worker not accepting work")

	// Capacity errors.
	ErrRuntimeSaturated = errors.New("runtime command concurrency limit reached")
	ErrWorkerSaturated  = errors.New("worker command concurrency limit reached")
)

const waitIdlePollInterval = 10 * time.Millisecond

// Identity describes the public identity of a workerkit runtime.
//
// A runtime represents one service boundary. Workerkit treats runtime names as
// operational identifiers, not display labels. The name is intentionally
// strict because it flows into runtime qualification, operations surfaces,
// configuration, and telemetry. A conservative naming rule keeps those
// cross-cutting surfaces predictable.
type Identity struct {
	// Name identifies the runtime boundary.
	//
	// Runtime names are flat identifiers. They must use lowercase ASCII
	// letters, digits, '-' and '_' only. Workerkit intentionally restricts
	// names to this small character set so they remain portable and predictable
	// across logs, metrics, traces, config, and operations APIs.
	Name string `json:"name"`
}

// Validate reports whether the runtime identity is structurally valid.
func (m Identity) Validate() error {
	return ValidateRuntimeName(m.Name)
}

// Runtime manages worker registration, lifecycle control, worker-owned
// commands, and operational state for one workerkit service boundary.
type Runtime struct {
	mu sync.RWMutex

	identity Identity
	config   runtimeConfig

	registrations map[string]WorkerSpec
	workerOrder   []string
	workerConfigs map[string]workerConfig
	// commands maps qualified worker names to worker-owned command handlers.
	commands     commandRegistry
	workerStates map[string]workerState

	commandDispatchSeq atomic.Uint64
	status             RuntimeStatus
}

// New constructs a runtime for one workerkit service boundary.
//
// The returned runtime starts in the registered lifecycle state with no workers
// registered. Runtime options define defaults for worker lifecycle, command
// handling, readiness, failure behavior, and telemetry.
func New(identity Identity, opts ...RuntimeOption) (*Runtime, error) {
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("invalid runtime identity: %w", err)
	}

	cfg := defaultRuntimeConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Runtime{
		identity:      identity,
		config:        cfg,
		registrations: make(map[string]WorkerSpec),
		workerOrder:   make([]string, 0),
		workerConfigs: make(map[string]workerConfig),
		commands:      make(commandRegistry),
		workerStates:  make(map[string]workerState),
		status: RuntimeStatus{
			Name:  identity.Name,
			State: StateRegistered,
		},
	}, nil
}

// Register adds a worker to the runtime in the registered lifecycle state.
//
// The worker name is local to the runtime and is qualified during registration.
// Per-worker options override runtime defaults for this worker and may attach
// worker-owned commands. Registration is closed once the runtime leaves the
// registered lifecycle state.
func (r *Runtime) Register(reg WorkerSpec, opts ...WorkerOption) error {
	if reg.Worker == nil {
		return ErrNilWorker
	}
	if err := validateIdentifier(reg.Name, "worker registration name"); err != nil {
		return err
	}

	qualifiedName := qualifyWorkerName(r.identity.Name, reg.Name)

	cfg := r.config.defaultWorker
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	commands, err := r.bindWorkerCommands(qualifiedName, cfg.commands)
	if err != nil {
		return err
	}

	// Registration checks and map writes share one lock so duplicate detection
	// and runtime state initialization observe the same snapshot.
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status.State != StateRegistered {
		return fmt.Errorf("%w: cannot register worker %q when runtime is in state %q", ErrInvalidWorkerState, qualifiedName, r.status.State)
	}
	if _, exists := r.registrations[qualifiedName]; exists {
		return fmt.Errorf("%w: %s", ErrWorkerAlreadyRegistered, qualifiedName)
	}

	// Registered workers are known to the runtime but inactive until lifecycle
	// control starts them.
	state := workerState{
		qualifiedName: qualifiedName,
		localName:     reg.Name,
		lifecycle:     StateRegistered,
	}

	// Install every per-worker runtime record under the qualified name. All worker
	// maps are kept in sync by Register, so later lookups can assume a registered
	// worker has config, command handlers, and runtime state.
	r.registrations[qualifiedName] = reg
	r.workerOrder = append(r.workerOrder, qualifiedName)
	r.workerConfigs[qualifiedName] = cfg
	r.commands[qualifiedName] = make(workerCommands)
	for _, command := range commands {
		r.commands[qualifiedName][command.Name] = command
	}
	r.workerStates[qualifiedName] = state

	r.recomputeStatusLocked()

	return nil
}

// -----------------------------------------------------------------------------
// Command dispatch.

// Dispatch routes one command request to a registered worker command handler.
//
// Dispatch normalizes the target, admits the command, reserves capacity, runs
// the handler under the worker's command timeout and retry policy, records
// command failures, applies panic policy, and emits command observations.
//
// The command timeout is a cooperative context deadline passed to the command
// handler. The handler must observe ctx.Done() and return. Workerkit cannot
// interrupt a blocked handler.
//
// Returned handler errors are command failures: they update LastCommandFailure
// and emit FailureEvent, but do not move the worker to StateFailed. Command
// panics are recovered outside the retry loop, are not retried, and currently
// fail the worker according to PanicPolicy. Worker code that decides a command
// failure means the worker is unhealthy should call
// WorkerRuntime.ReportFailure(err).
//
// Dispatch only invokes commands registered through worker registration options.
// It does not discover or infer commands.
func (r *Runtime) Dispatch(ctx context.Context, req CommandRequest) (res CommandResult, err error) {
	startedAt := time.Now()
	attempts := 0

	req, err = r.normalizeCommandRequest(req, startedAt)
	if err != nil {
		return CommandResult{}, err
	}

	dispatchID := r.nextCommandDispatchID()
	ctx, commandObservation := r.startCommandObservation(ctx, req.Worker, req.Name, dispatchID, startedAt)
	defer func() {
		r.endCommandObservation(ctx, commandObservation, req.Worker, req.Name, dispatchID, attempts, startedAt, err)
	}()

	cfg, reg, err := r.lookupCommandTarget(req)
	if err != nil {
		return CommandResult{}, err
	}

	if err := r.admitCommand(req.Worker); err != nil {
		return CommandResult{}, err
	}
	defer r.releaseCommandSlot(req.Worker)

	defer r.recoverCommandPanic(ctx, req, cfg, dispatchID, &attempts, &res, &err)

	res, err = r.executeCommand(ctx, req, reg, cfg, dispatchID, &attempts)
	if err != nil {
		return CommandResult{}, err
	}

	return res, nil
}

// -----------------------------------------------------------------------------
// Worker lifecycle.

// Start brings one registered worker into service.
//
// The worker must currently be registered, stopped, or failed. Start calls
// Worker.Start with a WorkerRuntime handle in context and applies the worker's
// start timeout, retry, panic, and failure policies.
//
// The start timeout is a cooperative context deadline. Worker.Start must observe
// ctx.Done() and return. Workerkit cannot preempt a blocked Start call.
//
// If Worker.Start fails after partial setup, Workerkit records the failure but
// does not call Worker.Stop from inside Start.
//
// If the worker sets readiness or accepting-work state during Worker.Start,
// those choices are preserved. Otherwise the worker receives its configured
// ready-on-start and accepting-work-on-start defaults.
func (r *Runtime) Start(ctx context.Context, name string) (err error) {
	name, worker, cfg, err := r.lookupWorker(name)
	if err != nil {
		return err
	}

	if err := r.beginWorkerStart(ctx, name); err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr := newRecoveredPanicError(
				fmt.Sprintf("starting worker %q", name),
				recovered,
			)
			r.failWorker(ctx, name, "", panicErr, true)
			if cfg.panicPolicy == PanicPolicyCrash {
				panic(recovered)
			}
			err = panicErr
		}
	}()

	err = runWithRetry(ctx, cfg.startTimeout, cfg.startRetryPolicy, func(attemptCtx context.Context, _ int) error {
		attemptCtx = withWorkerRuntime(attemptCtx, r.workerRuntimeHandle(attemptCtx, name))
		return worker.Start(attemptCtx)
	}, func(opErr error, _ int) {
		r.recordWorkerFailureStatus(name, opErr)
	})
	if err != nil {
		r.failWorker(ctx, name, "", err, false)
		return err
	}

	return r.completeWorkerStart(ctx, name, cfg.readyOnStart, cfg.acceptingWork)
}

// Stop takes one running, draining, or failed worker out of service.
//
// Stop immediately moves the worker to stopping, marks it unready, and stops
// accepting new work. It calls Worker.Stop with a WorkerRuntime handle in
// context and applies the worker's stop timeout, panic, and failure policies.
// Stop may be called after StateFailed so worker-owned resources can be cleaned
// up after failure reporting.
//
// The stop timeout is a cooperative context deadline. Worker.Stop must observe
// ctx.Done() and return. Workerkit cannot force resource cleanup if Stop blocks.
//
// Stop does not wait for in-flight commands to drain. Use Drain when the worker
// should stop accepting new work before a later Stop call. A successful stop
// preserves prior LastFailure evidence for operator inspection.
func (r *Runtime) Stop(ctx context.Context, name string) (err error) {
	name, worker, cfg, err := r.lookupWorker(name)
	if err != nil {
		return err
	}

	if err := r.beginWorkerStop(ctx, name); err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr := newRecoveredPanicError(
				fmt.Sprintf("stopping worker %q", name),
				recovered,
			)
			r.failWorker(ctx, name, "", panicErr, true)
			if cfg.panicPolicy == PanicPolicyCrash {
				panic(recovered)
			}
			err = panicErr
		}
	}()

	attemptCtx, cancel := withTimeout(ctx, cfg.stopTimeout)
	defer cancel()
	attemptCtx = withWorkerRuntime(attemptCtx, r.workerRuntimeHandle(attemptCtx, name))

	err = worker.Stop(attemptCtx)
	if err != nil {
		r.recordWorkerFailureStatus(name, err)
		r.failWorker(ctx, name, "", err, false)
		return err
	}

	r.completeWorkerTransition(ctx, name, StateStopped, false, false)
	return nil
}

// Drain marks one running worker as draining.
//
// Drain marks the worker unready and stops new Workerkit command dispatches. It
// does not call Worker.Stop, wait for in-flight commands, or control the
// worker's domain-specific business loop. Calling Drain on an already-draining
// worker is allowed.
func (r *Runtime) Drain(ctx context.Context, name string) error {
	name, err := r.resolveWorkerNameStrict(name)
	if err != nil {
		return fmt.Errorf("invalid worker name: %w", err)
	}
	return r.beginWorkerDrain(ctx, name)
}

// StartAll starts registered workers in registration order.
//
// StartAll is a convenience for service bootstrap. It starts registered,
// stopped, and failed workers, skips workers that are already running, and is
// fail-fast for workers in transitional states or workers that fail to start.
//
// StartAll does not roll back partial startup. If a later worker fails to
// start, workers already started by this call remain running. Callers that need
// startup cleanup should compose DrainAllBestEffort, WaitAllIdle, and StopAll,
// or use servekitservice.Run, which performs graceful worker shutdown after
// startup failure when configured.
//
// Call Start directly when the application needs custom ordering or error
// handling.
func (r *Runtime) StartAll(ctx context.Context) error {
	for _, worker := range r.registeredWorkerLifecycles() {
		switch worker.state {
		case StateRegistered, StateStopped, StateFailed:
			if err := r.Start(ctx, worker.name); err != nil {
				return err
			}
		case StateRunning:
			continue
		default:
			return fmt.Errorf("%w: cannot start worker %q in state %q", ErrInvalidWorkerState, worker.name, worker.state)
		}
	}
	return nil
}

// DrainAll drains registered workers in registration order.
//
// DrainAll drains running workers, allows already-draining workers, and skips
// workers that are not active. It is fail-fast for transitional states or drain
// failures.
//
// DrainAll applies worker-scoped drain sequentially. It is not an atomic
// runtime-wide command admission cutoff: workers later in registration order may
// still accept Workerkit command dispatches until their own drain step runs.
func (r *Runtime) DrainAll(ctx context.Context) error {
	for _, worker := range r.registeredWorkerLifecycles() {
		switch worker.state {
		case StateRunning:
			if err := r.Drain(ctx, worker.name); err != nil {
				return err
			}
		case StateDraining, StateRegistered, StateStopped, StateFailed:
			continue
		default:
			return fmt.Errorf("%w: cannot drain worker %q in state %q", ErrInvalidWorkerState, worker.name, worker.state)
		}
	}
	return nil
}

// DrainAllBestEffort drains registered workers in registration order and
// continues after individual drain failures.
//
// It drains running workers, allows already-draining workers, skips workers that
// are not active, and returns the combined error from any worker that could not
// be drained.
//
// DrainAllBestEffort applies worker-scoped drain sequentially. It is not an
// atomic runtime-wide command admission cutoff: workers later in registration
// order may still accept Workerkit command dispatches until their own drain step
// runs.
func (r *Runtime) DrainAllBestEffort(ctx context.Context) error {
	var errs []error
	for _, worker := range r.registeredWorkerLifecycles() {
		switch worker.state {
		case StateRunning:
			if err := r.Drain(ctx, worker.name); err != nil {
				errs = append(errs, err)
			}
		case StateDraining, StateRegistered, StateStopped, StateFailed:
			continue
		default:
			errs = append(errs, fmt.Errorf("%w: cannot drain worker %q in state %q", ErrInvalidWorkerState, worker.name, worker.state))
		}
	}
	return errors.Join(errs...)
}

// StopAll stops registered workers in reverse registration order.
//
// StopAll stops running, draining, and failed workers, skips workers that are
// already inactive, and continues after individual stop failures. If any worker
// fails to stop, StopAll returns the combined error.
func (r *Runtime) StopAll(ctx context.Context) error {
	workers := r.registeredWorkerLifecycles()
	var errs []error
	for i := len(workers) - 1; i >= 0; i-- {
		worker := workers[i]
		switch worker.state {
		case StateRunning, StateDraining, StateFailed:
			if err := r.Stop(ctx, worker.name); err != nil {
				errs = append(errs, err)
			}
		case StateRegistered, StateStopped, StateStopping:
			continue
		default:
			errs = append(errs, fmt.Errorf("%w: cannot stop worker %q in state %q", ErrInvalidWorkerState, worker.name, worker.state))
		}
	}
	return errors.Join(errs...)
}

// Shutdown gracefully takes the runtime out of service.
//
// Shutdown drains active workers best-effort, waits for in-flight Workerkit
// commands to finish, then stops workers in reverse registration order. It uses
// the caller's context directly for the entire sequence. Callers that need a
// shutdown budget should pass a context with a deadline.
func (r *Runtime) Shutdown(ctx context.Context) error {
	var errs []error
	if err := r.DrainAllBestEffort(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := r.WaitAllIdle(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := r.StopAll(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// WaitIdle waits until one registered worker has no in-flight commands.
//
// The name may be local to this runtime or fully qualified. If ctx is canceled
// before the worker becomes idle, WaitIdle returns the context error.
func (r *Runtime) WaitIdle(ctx context.Context, name string) error {
	name, err := r.resolveWorkerNameStrict(name)
	if err != nil {
		return fmt.Errorf("invalid worker name: %w", err)
	}

	for {
		idle, err := r.workerIdle(name)
		if err != nil {
			return err
		}
		if idle {
			return nil
		}
		if err := waitForIdlePoll(ctx); err != nil {
			return err
		}
	}
}

// WaitAllIdle waits until the runtime has no in-flight commands.
//
// If ctx is canceled before the runtime becomes idle, WaitAllIdle returns the
// context error.
func (r *Runtime) WaitAllIdle(ctx context.Context) error {
	for {
		if r.runtimeIdle() {
			return nil
		}
		if err := waitForIdlePoll(ctx); err != nil {
			return err
		}
	}
}

// -----------------------------------------------------------------------------
// Runtime and worker status/discovery.

// Status returns a point-in-time aggregate runtime status snapshot.
//
// The returned status does not include per-worker detail. Use Workers or Worker
// when callers need worker-level inspection snapshots.
func (r *Runtime) Status() RuntimeStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	status := r.status
	status.LastTransition = cloneLifecycleTransition(r.status.LastTransition)
	return status
}

// Identity returns the runtime identity used for worker qualification,
// telemetry, and operations surfaces.
func (r *Runtime) Identity() Identity {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.identity
}

// Workers returns point-in-time inspection snapshots for registered workers in
// registration order.
func (r *Runtime) Workers() []WorkerSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workers := make([]WorkerSnapshot, 0, len(r.workerOrder))
	for _, name := range r.workerOrder {
		reg := r.registrations[name]
		workers = append(workers, WorkerSnapshot{
			QualifiedName: name,
			Name:          reg.Name,
			Description:   reg.Description,
			Status:        r.workerStates[name].toWorkerStatus(),
		})
	}
	return workers
}

// Worker returns the current inspection snapshot for one registered worker.
//
// The name may be local to this runtime or fully qualified. The returned
// snapshot combines registration metadata with current operational status. The
// boolean is false when the worker is not registered.
func (r *Runtime) Worker(name string) (WorkerSnapshot, bool) {
	name = r.resolveWorkerName(name)

	r.mu.RLock()
	defer r.mu.RUnlock()

	reg, ok := r.registrations[name]
	if !ok {
		return WorkerSnapshot{}, false
	}
	return WorkerSnapshot{
		QualifiedName: name,
		Name:          reg.Name,
		Description:   reg.Description,
		Status:        r.workerStates[name].toWorkerStatus(),
	}, true
}

// Commands returns registered worker-owned commands for one worker in stable
// name order.
//
// The worker name may be local to this runtime or fully qualified. The returned
// commands are discovery metadata only. Use Dispatch to invoke a command. The
// boolean is false when the worker is not registered. A registered worker with
// no commands returns an empty slice and true.
func (r *Runtime) Commands(worker string) ([]CommandInfo, bool) {
	worker = r.resolveWorkerName(worker)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, ok := r.registrations[worker]; !ok {
		return nil, false
	}

	commands := make([]CommandInfo, 0, len(r.commands[worker]))

	names := make([]string, 0, len(r.commands[worker]))
	for name := range r.commands[worker] {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		reg := r.commands[worker][name]
		commands = append(commands, CommandInfo{
			Worker:      reg.Worker,
			Name:        reg.Name,
			Description: reg.Description,
		})
	}

	return commands, true
}

// -----------------------------------------------------------------------------
// Worker lifecycle internals.
//
// These helpers are the only code paths that mutate worker lifecycle,
// readiness, accepting-work state, and failure status. Public lifecycle methods
// orchestrate worker calls while these helpers own the locked state
// transitions and related observations.

type workerStateObservation struct {
	worker      string
	from        LifecycleState
	to          LifecycleState
	beforeReady bool
	afterReady  bool
	before      RuntimeStatus
	after       RuntimeStatus
	at          time.Time
}

func (r *Runtime) lookupWorker(name string) (string, Worker, workerConfig, error) {
	name, err := r.resolveWorkerNameStrict(name)
	if err != nil {
		return "", nil, workerConfig{}, fmt.Errorf("invalid worker name: %w", err)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	reg, exists := r.registrations[name]
	if !exists {
		return "", nil, workerConfig{}, fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}
	return name, reg.Worker, r.workerConfigs[name], nil
}

type workerLifecycleSnapshot struct {
	name  string
	state LifecycleState
}

func (r *Runtime) registeredWorkerLifecycles() []workerLifecycleSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workers := make([]workerLifecycleSnapshot, 0, len(r.workerOrder))
	for _, name := range r.workerOrder {
		workers = append(workers, workerLifecycleSnapshot{
			name:  name,
			state: r.workerStates[name].lifecycle,
		})
	}
	return workers
}

func (r *Runtime) workerStatus(name string) (WorkerStatus, bool) {
	name = r.resolveWorkerName(name)

	r.mu.RLock()
	defer r.mu.RUnlock()

	state, ok := r.workerStates[name]
	if !ok {
		return WorkerStatus{}, false
	}
	return state.toWorkerStatus(), true
}

func (r *Runtime) workerIdle(name string) (bool, error) {
	name, err := r.resolveWorkerNameStrict(name)
	if err != nil {
		return false, fmt.Errorf("invalid worker name: %w", err)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	state, ok := r.workerStates[name]
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}
	return state.inFlight == 0, nil
}

func (r *Runtime) runtimeIdle() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.status.InFlight == 0
}

func (r *Runtime) resolveWorkerName(name string) string {
	resolved, err := resolveWorkerName(r.identity.Name, name)
	if err != nil {
		return name
	}
	return resolved
}

func (r *Runtime) resolveWorkerNameStrict(name string) (string, error) {
	return resolveWorkerName(r.identity.Name, name)
}

func (r *Runtime) workerRuntimeHandle(ctx context.Context, name string) WorkerRuntime {
	return &workerControlHandle{
		runtime: r,
		name:    name,
		ctx:     observerContext(ctx),
	}
}

func observerContext(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	// WorkerRuntime uses this context only for observer correlation. Detach
	// cancellation so critical state mutations still record readiness, lifecycle,
	// and failure changes after the execution context has ended.
	return context.WithoutCancel(ctx)
}

func waitForIdlePoll(ctx context.Context) error {
	timer := time.NewTimer(waitIdlePollInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runtime) commitWorkerStateLocked(name string, before workerState, after workerState, at time.Time) workerStateObservation {
	prevRuntime := r.status
	r.workerStates[name] = after
	r.recomputeStatusLocked()
	return workerStateObservation{
		worker:      name,
		from:        before.lifecycle,
		to:          after.lifecycle,
		beforeReady: before.ready,
		afterReady:  after.ready,
		before:      prevRuntime,
		after:       r.status,
		at:          at,
	}
}

func (r *Runtime) observeWorkerStateChange(ctx context.Context, obs workerStateObservation) {
	if obs.from != obs.to {
		r.observeTransition(ctx, obs.worker, obs.from, obs.to, obs.at)
	}
	r.observeReadinessChange(ctx, obs.worker, obs.beforeReady, obs.afterReady, obs.at)
	r.observeRuntimeChanges(ctx, obs.before, obs.after)
}

func (r *Runtime) beginWorkerStart(ctx context.Context, name string) error {
	r.mu.Lock()

	state, ok := r.workerStates[name]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}
	switch state.lifecycle {
	case StateRegistered, StateStopped, StateFailed:
	default:
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot start worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}

	before := state
	now := time.Now()
	state.lastTransition = &LifecycleTransition{
		From: state.lifecycle,
		To:   StateStarting,
		At:   now,
	}
	state.lifecycle = StateStarting
	state.ready = false
	state.acceptingWork = false
	state.readySetDuringStart = false
	state.acceptingWorkSetDuringStart = false
	obs := r.commitWorkerStateLocked(name, before, state, now)
	r.mu.Unlock()

	r.observeWorkerStateChange(ctx, obs)
	return nil
}

func (r *Runtime) beginWorkerStop(ctx context.Context, name string) error {
	r.mu.Lock()

	state, ok := r.workerStates[name]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}
	switch state.lifecycle {
	case StateRunning, StateDraining, StateFailed:
	default:
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot stop worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}

	before := state
	now := time.Now()
	state.lastTransition = &LifecycleTransition{
		From: state.lifecycle,
		To:   StateStopping,
		At:   now,
	}
	state.lifecycle = StateStopping
	state.ready = false
	state.acceptingWork = false
	state.readySetDuringStart = false
	state.acceptingWorkSetDuringStart = false
	obs := r.commitWorkerStateLocked(name, before, state, now)
	r.mu.Unlock()

	r.observeWorkerStateChange(ctx, obs)
	return nil
}

func (r *Runtime) setWorkerReady(ctx context.Context, name string, ready bool) error {
	r.mu.Lock()

	state, ok := r.workerStates[name]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}
	if !lifecycleAllowsWorkerRuntimeMutation(state.lifecycle) {
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot set readiness for worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}
	if ready && !lifecycleAllowsWorkerRuntimePositiveSignal(state.lifecycle) {
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot set readiness for worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}

	before := state
	now := time.Now()
	state.ready = ready
	if state.lifecycle == StateStarting {
		state.readySetDuringStart = true
	}
	obs := r.commitWorkerStateLocked(name, before, state, now)
	r.mu.Unlock()

	r.observeWorkerStateChange(ctx, obs)
	return nil
}

func (r *Runtime) setWorkerAcceptingWork(name string, accepting bool) error {
	r.mu.Lock()

	state, ok := r.workerStates[name]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}
	if !lifecycleAllowsWorkerRuntimeMutation(state.lifecycle) {
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot set accepting-work for worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}
	if accepting && !lifecycleAllowsWorkerRuntimePositiveSignal(state.lifecycle) {
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot set accepting-work for worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}

	state.acceptingWork = accepting
	if state.lifecycle == StateStarting {
		state.acceptingWorkSetDuringStart = true
	}
	r.workerStates[name] = state
	r.recomputeStatusLocked()
	r.mu.Unlock()
	return nil
}

func (r *Runtime) beginWorkerDrain(ctx context.Context, name string) error {
	r.mu.Lock()

	state, ok := r.workerStates[name]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}

	switch state.lifecycle {
	case StateDraining:
		// Drain is idempotent for already-draining workers. Keep the forced
		// unready/not-accepting state in case worker code tried to re-enable
		// either signal during cleanup.
		state.ready = false
		state.acceptingWork = false
		r.workerStates[name] = state
		r.recomputeStatusLocked()
		r.mu.Unlock()
		return nil
	case StateRunning:
	default:
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot begin drain for worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}

	before := state
	now := time.Now()
	state.lastTransition = &LifecycleTransition{
		From: state.lifecycle,
		To:   StateDraining,
		At:   now,
	}
	state.lifecycle = StateDraining
	state.ready = false
	state.acceptingWork = false
	obs := r.commitWorkerStateLocked(name, before, state, now)
	r.mu.Unlock()

	r.observeWorkerStateChange(ctx, obs)
	return nil
}

func (r *Runtime) completeWorkerTransition(ctx context.Context, name string, to LifecycleState, ready bool, acceptingWork bool) {
	r.mu.Lock()

	state := r.workerStates[name]
	before := state
	now := time.Now()
	state.lastTransition = &LifecycleTransition{
		From: state.lifecycle,
		To:   to,
		At:   now,
	}
	state.lifecycle = to
	state.ready = ready
	state.acceptingWork = acceptingWork
	state.readySetDuringStart = false
	state.acceptingWorkSetDuringStart = false
	obs := r.commitWorkerStateLocked(name, before, state, now)
	r.mu.Unlock()

	r.observeWorkerStateChange(ctx, obs)
}

func (r *Runtime) completeWorkerStart(ctx context.Context, name string, defaultReady bool, defaultAcceptingWork bool) error {
	r.mu.Lock()

	state := r.workerStates[name]
	if state.lifecycle != StateStarting {
		r.mu.Unlock()
		return completeWorkerStartStateError(name, state)
	}
	before := state
	now := time.Now()
	state.lastTransition = &LifecycleTransition{
		From: state.lifecycle,
		To:   StateRunning,
		At:   now,
	}
	state.lifecycle = StateRunning
	if !state.readySetDuringStart {
		state.ready = defaultReady
	}
	if !state.acceptingWorkSetDuringStart {
		state.acceptingWork = defaultAcceptingWork
	}
	state.readySetDuringStart = false
	state.acceptingWorkSetDuringStart = false
	state.lastFailure = nil
	obs := r.commitWorkerStateLocked(name, before, state, now)
	r.mu.Unlock()

	r.observeWorkerStateChange(ctx, obs)
	return nil
}

func completeWorkerStartStateError(name string, state workerState) error {
	if state.lastFailure == nil {
		return fmt.Errorf("%w: cannot complete start for worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}
	return fmt.Errorf(
		"%w: cannot complete start for worker %q in state %q: last failure: %s",
		ErrInvalidWorkerState,
		name,
		state.lifecycle,
		state.lastFailure.Message,
	)
}

func (r *Runtime) failWorker(ctx context.Context, name, command string, err error, panicked bool) {
	r.failWorkerWithCommandAttempt(ctx, name, command, err, panicked, "", 0)
}

func (r *Runtime) failWorkerWithCommandAttempt(ctx context.Context, name, command string, err error, panicked bool, dispatchID string, attempt int) {
	r.mu.Lock()

	state := r.workerStates[name]
	before := state
	now := time.Now()
	state.lastTransition = &LifecycleTransition{
		From: state.lifecycle,
		To:   StateFailed,
		At:   now,
	}
	state.lifecycle = StateFailed
	state.ready = false
	state.acceptingWork = false
	state.readySetDuringStart = false
	state.acceptingWorkSetDuringStart = false
	state.lastFailure = &FailureInfo{
		Message: err.Error(),
		At:      now,
	}
	obs := r.commitWorkerStateLocked(name, before, state, now)
	r.mu.Unlock()

	if obs.from != obs.to {
		r.observeTransition(ctx, obs.worker, obs.from, obs.to, obs.at)
	}
	r.observeReadinessChange(ctx, obs.worker, obs.beforeReady, obs.afterReady, obs.at)
	r.observeFailure(ctx, name, command, err, panicked, now, dispatchID, attempt)
	r.observeRuntimeChanges(ctx, obs.before, obs.after)
}

func (r *Runtime) reportWorkerFailure(ctx context.Context, name string, err error) error {
	if err == nil {
		return nil
	}

	r.mu.Lock()

	state, ok := r.workerStates[name]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}
	switch state.lifecycle {
	case StateStarting, StateRunning, StateDraining:
	default:
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot report failure for worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}

	before := state
	now := time.Now()
	state.lastTransition = &LifecycleTransition{
		From: state.lifecycle,
		To:   StateFailed,
		At:   now,
	}
	state.lifecycle = StateFailed
	state.ready = false
	state.acceptingWork = false
	state.readySetDuringStart = false
	state.acceptingWorkSetDuringStart = false
	state.lastFailure = &FailureInfo{
		Message: err.Error(),
		At:      now,
	}
	obs := r.commitWorkerStateLocked(name, before, state, now)
	r.mu.Unlock()

	if obs.from != obs.to {
		r.observeTransition(ctx, obs.worker, obs.from, obs.to, obs.at)
	}
	r.observeReadinessChange(ctx, obs.worker, obs.beforeReady, obs.afterReady, obs.at)
	r.observeFailure(ctx, name, "", err, false, now, "", 0)
	r.observeRuntimeChanges(ctx, obs.before, obs.after)
	return nil
}

// recordWorkerFailureStatus records worker failure status without changing
// worker lifecycle or emitting an observer event. Lifecycle retry attempts use
// this path so only terminal lifecycle failure increments failure observations.
func (r *Runtime) recordWorkerFailureStatus(name string, err error) {
	r.mu.Lock()

	state := r.workerStates[name]
	now := time.Now()
	state.lastFailure = &FailureInfo{
		Message: err.Error(),
		At:      now,
	}
	r.workerStates[name] = state
	r.mu.Unlock()
}

// recordCommandFailure records a command failure without changing worker
// lifecycle or worker health failure status. Command handler returned errors
// use this path because domain command failures are not automatically worker
// health failures.
func (r *Runtime) recordCommandFailure(ctx context.Context, name, command string, err error, dispatchID string, attempt int) {
	r.mu.Lock()

	state := r.workerStates[name]
	now := time.Now()
	state.lastCommandFailure = &CommandFailureInfo{
		Command: command,
		Message: err.Error(),
		At:      now,
	}
	r.workerStates[name] = state
	r.mu.Unlock()

	r.observeFailure(ctx, name, command, err, false, now, dispatchID, attempt)
}

// -----------------------------------------------------------------------------
// Command dispatch internals.
//
// These helpers normalize command requests, enforce lifecycle admission,
// reserve command capacity, recover panics, and invoke worker-owned handlers.

type commandRegistration struct {
	Worker      string
	Name        string
	Description string
	Handler     CommandHandler
}

type workerCommands map[string]commandRegistration
type commandRegistry map[string]workerCommands

func (r *Runtime) bindWorkerCommands(worker string, commands []CommandSpec) ([]commandRegistration, error) {
	if len(commands) == 0 {
		return nil, nil
	}

	bound := make([]commandRegistration, 0, len(commands))
	seen := make(map[string]struct{}, len(commands))
	for _, command := range commands {
		if err := ValidateCommandName(command.Name); err != nil {
			return nil, fmt.Errorf("invalid command name: %w", err)
		}
		if command.Handler == nil {
			return nil, fmt.Errorf("command handler must not be nil")
		}
		if _, exists := seen[command.Name]; exists {
			return nil, fmt.Errorf("%w: %s/%s", ErrCommandAlreadyRegistered, worker, command.Name)
		}
		seen[command.Name] = struct{}{}

		bound = append(bound, commandRegistration{
			Worker:      worker,
			Name:        command.Name,
			Description: command.Description,
			Handler:     command.Handler,
		})
	}
	return bound, nil
}

func (r *Runtime) normalizeCommandRequest(req CommandRequest, requestedAt time.Time) (CommandRequest, error) {
	if req.RequestedAt.IsZero() {
		req.RequestedAt = requestedAt
	}

	worker, err := resolveWorkerName(r.identity.Name, req.Worker)
	if err != nil {
		return CommandRequest{}, fmt.Errorf("invalid command target: %w", err)
	}
	if err := ValidateCommandName(req.Name); err != nil {
		return CommandRequest{}, fmt.Errorf("invalid command name: %w", err)
	}

	req.Worker = worker
	return req, nil
}

// lookupCommandTarget verifies that the worker and command are registered and
// returns the worker command policy and bound command registration.
func (r *Runtime) lookupCommandTarget(req CommandRequest) (workerConfig, commandRegistration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, exists := r.registrations[req.Worker]; !exists {
		return workerConfig{}, commandRegistration{}, fmt.Errorf("%w: %s", ErrWorkerNotFound, req.Worker)
	}
	reg, exists := r.commands[req.Worker][req.Name]
	if !exists {
		return workerConfig{}, commandRegistration{}, fmt.Errorf("%w: %s/%s", ErrCommandNotFound, req.Worker, req.Name)
	}
	return r.workerConfigs[req.Worker], reg, nil
}

func (r *Runtime) admitCommand(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Runtime aggregate draining is status, not a global command admission gate.
	// Worker-level lifecycle and accepting-work state decide whether a command
	// can target a specific worker.
	switch r.status.State {
	case StateFailed, StateStopping, StateStopped, StateRegistered:
		return fmt.Errorf("%w: runtime %q is in state %q", ErrRuntimeNotAcceptingWork, r.identity.Name, r.status.State)
	}

	state, ok := r.workerStates[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, name)
	}
	if state.lifecycle != StateRunning {
		return fmt.Errorf("%w: cannot dispatch to worker %q in state %q", ErrInvalidWorkerState, name, state.lifecycle)
	}
	if !state.acceptingWork {
		return fmt.Errorf("%w: worker %q", ErrWorkerNotAcceptingWork, name)
	}

	// Runtime and worker command caps are separate gates. A command needs
	// capacity in both scopes before it can run.
	if r.config.commandConcurrency > 0 && r.status.InFlight >= r.config.commandConcurrency {
		return fmt.Errorf("%w: limit=%d", ErrRuntimeSaturated, r.config.commandConcurrency)
	}

	cfg := r.workerConfigs[name]
	if cfg.commandConcurrency > 0 && state.inFlight >= cfg.commandConcurrency {
		return fmt.Errorf("%w: worker=%s limit=%d", ErrWorkerSaturated, name, cfg.commandConcurrency)
	}

	state.inFlight++
	r.workerStates[name] = state
	r.status.InFlight++
	return nil
}

func (r *Runtime) releaseCommandSlot(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.workerStates[name]
	if !ok {
		return
	}
	// Be defensive against future call-site mistakes: release should never drive
	// counters negative, even if a command target disappeared or was released
	// twice.
	if state.inFlight > 0 {
		state.inFlight--
	}
	r.workerStates[name] = state

	if r.status.InFlight > 0 {
		r.status.InFlight--
	}
}

// recoverCommandPanic applies command panic policy after handler execution.
// Recovered command panics fail the worker and either become the returned
// command error or are re-thrown for crash policy.
func (r *Runtime) recoverCommandPanic(ctx context.Context, req CommandRequest, cfg workerConfig, dispatchID string, attempts *int, res *CommandResult, err *error) {
	if recovered := recover(); recovered != nil {
		panicErr := newRecoveredPanicError(
			fmt.Sprintf("handling command %q for worker %q", req.Name, req.Worker),
			recovered,
		)
		r.failWorkerWithCommandAttempt(ctx, req.Worker, req.Name, panicErr, true, dispatchID, *attempts)
		*err = panicErr
		*res = CommandResult{}
		if cfg.panicPolicy == PanicPolicyCrash {
			panic(recovered)
		}
	}
}

// executeCommand runs the registered handler under the worker command timeout
// and returned-error retry policy. Each attempt receives a WorkerRuntime handle
// in context. Returned handler errors are command failures: they are recorded in
// LastCommandFailure and emitted through observer events, but they do not
// automatically move the worker to StateFailed. Panics escape this retry loop
// and are handled by Dispatch-level panic recovery. Worker code can call
// WorkerRuntime.ReportFailure when a command error reflects worker health.
func (r *Runtime) executeCommand(ctx context.Context, req CommandRequest, reg commandRegistration, cfg workerConfig, dispatchID string, attempts *int) (CommandResult, error) {
	var res CommandResult

	err := runWithRetry(ctx, cfg.commandTimeout, cfg.commandRetryPolicy, func(attemptCtx context.Context, attempt int) error {
		*attempts = attempt
		attemptCtx = withWorkerRuntime(attemptCtx, r.workerRuntimeHandle(attemptCtx, req.Worker))
		attemptResult, attemptErr := reg.Handler.HandleCommand(attemptCtx, req)
		if attemptErr == nil {
			res = attemptResult
		}
		return attemptErr
	}, func(opErr error, attempt int) {
		r.recordCommandFailure(ctx, req.Worker, req.Name, opErr, dispatchID, attempt)
	})
	if err != nil {
		return CommandResult{}, err
	}
	return res, nil
}

// -----------------------------------------------------------------------------
// Shared runtime internals.

// runWithRetry runs operation once, then asks policy whether each failure should
// be retried. The timeout applies to the context passed to each attempt, not to
// the whole retry loop. It does not preempt operation. Retry can continue only
// after operation returns. onFailure is called after a failed attempt and before
// any retry delay is applied.
func runWithRetry(ctx context.Context, timeout time.Duration, policy retrykit.Policy, operation func(context.Context, int) error, onFailure func(error, int)) error {
	if policy == nil {
		policy = retrykit.Never()
	}

	for attempt := 1; ; attempt++ {
		// Each attempt gets its own derived context so one timed-out attempt does
		// not poison a later retry attempt.
		attemptCtx, cancel := withTimeout(ctx, timeout)
		var err error
		func() {
			defer cancel()
			err = operation(attemptCtx, attempt)
		}()
		if err == nil {
			return nil
		}
		if onFailure != nil {
			onFailure(err, attempt)
		}

		// Parent cancellation wins over retry policy. Once the caller has given
		// up, Workerkit should not keep issuing attempts.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		delay, ok := policy.NextDelay(attempt, err)
		if !ok {
			return err
		}
		if err := waitBeforeRetry(ctx, delay); err != nil {
			return err
		}
	}
}

func waitBeforeRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// workerState is the runtime-owned status record for one worker.
//
// It contains stable identity plus the mutable operational fields needed for
// lifecycle control, admission, readiness, capacity, and status reporting.
type workerState struct {
	qualifiedName string
	localName     string
	lifecycle     LifecycleState
	ready         bool
	acceptingWork bool
	inFlight      int

	readySetDuringStart         bool
	acceptingWorkSetDuringStart bool

	lastTransition     *LifecycleTransition
	lastFailure        *FailureInfo
	lastCommandFailure *CommandFailureInfo
}

func (s workerState) toWorkerStatus() WorkerStatus {
	return WorkerStatus{
		Name:               s.qualifiedName,
		LocalName:          s.localName,
		State:              s.lifecycle,
		Ready:              s.ready,
		AcceptingWork:      s.acceptingWork,
		InFlight:           s.inFlight,
		LastTransition:     cloneLifecycleTransition(s.lastTransition),
		LastFailure:        cloneFailureInfo(s.lastFailure),
		LastCommandFailure: cloneCommandFailureInfo(s.lastCommandFailure),
	}
}

func cloneLifecycleTransition(t *LifecycleTransition) *LifecycleTransition {
	if t == nil {
		return nil
	}
	copy := *t
	return &copy
}

func cloneFailureInfo(info *FailureInfo) *FailureInfo {
	if info == nil {
		return nil
	}
	copy := *info
	return &copy
}

func cloneCommandFailureInfo(info *CommandFailureInfo) *CommandFailureInfo {
	if info == nil {
		return nil
	}
	copy := *info
	return &copy
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (r *Runtime) observer() Observer {
	if r.config.observer == nil {
		return NopObserver{}
	}
	return r.config.observer
}

func (r *Runtime) observeTransition(ctx context.Context, worker string, from LifecycleState, to LifecycleState, at time.Time) {
	r.observer().ObserveTransition(ctx, TransitionEvent{
		Runtime: r.identity.Name,
		Worker:  worker,
		From:    from,
		To:      to,
		At:      at,
	})
}

func (r *Runtime) nextCommandDispatchID() string {
	seq := r.commandDispatchSeq.Add(1)
	return fmt.Sprintf("%s-%d", r.identity.Name, seq)
}

func (r *Runtime) startCommandObservation(ctx context.Context, worker string, command string, dispatchID string, startedAt time.Time) (context.Context, CommandObservation) {
	observedCtx, observation := r.observer().StartCommand(ctx, CommandStartEvent{
		Runtime:    r.identity.Name,
		Worker:     worker,
		Command:    command,
		DispatchID: dispatchID,
		StartedAt:  startedAt,
	})
	if observedCtx == nil {
		observedCtx = ctx
	}
	if observation == nil {
		observation = NopCommandObservation{}
	}
	return observedCtx, observation
}

func (r *Runtime) endCommandObservation(ctx context.Context, observation CommandObservation, worker string, command string, dispatchID string, attempts int, startedAt time.Time, err error) {
	endedAt := time.Now()
	message := ""
	if err != nil {
		message = err.Error()
	}
	observation.End(ctx, CommandEndEvent{
		Runtime:    r.identity.Name,
		Worker:     worker,
		Command:    command,
		DispatchID: dispatchID,
		Attempts:   attempts,
		StartedAt:  startedAt,
		EndedAt:    endedAt,
		Duration:   endedAt.Sub(startedAt),
		Success:    err == nil,
		Err:        err,
		Message:    message,
	})
}

func (r *Runtime) observeFailure(ctx context.Context, worker string, command string, err error, panicked bool, at time.Time, dispatchID string, attempt int) {
	r.observer().ObserveFailure(ctx, FailureEvent{
		Runtime:    r.identity.Name,
		Worker:     worker,
		Command:    command,
		DispatchID: dispatchID,
		Attempt:    attempt,
		At:         at,
		Err:        err,
		Message:    err.Error(),
		Panic:      panicked,
	})
}

func (r *Runtime) observeReadinessChange(ctx context.Context, worker string, before bool, after bool, at time.Time) {
	if before == after {
		return
	}
	r.observer().ObserveReadiness(ctx, ReadinessEvent{
		Runtime: r.identity.Name,
		Worker:  worker,
		Ready:   after,
		At:      at,
	})
}

func (r *Runtime) observeRuntimeChanges(ctx context.Context, before RuntimeStatus, after RuntimeStatus) {
	at := time.Now()
	if before.State != after.State {
		r.observeTransition(ctx, "", before.State, after.State, at)
	}
	r.observeReadinessChange(ctx, "", before.Ready, after.Ready, at)
}

func newRecoveredPanicError(operation string, recovered any) error {
	return fmt.Errorf("recovered panic during %s: %v", operation, recovered)
}
