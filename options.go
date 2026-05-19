package workerkit

import (
	"fmt"
	"time"

	retrykit "github.com/jaredjakacky/workerkit/retry"
)

const (
	defaultStartTimeout       = 30 * time.Second
	defaultStopTimeout        = 30 * time.Second
	defaultCommandTimeout     = 30 * time.Second
	defaultCommandConcurrency = 0
)

// PanicPolicy controls how the runtime treats panics inside managed Start,
// Stop, and command handling paths.
type PanicPolicy string

const (
	// PanicPolicyRecover means the runtime should recover the panic, record it
	// as a worker failure, and apply the configured failure policy.
	PanicPolicyRecover PanicPolicy = "recover"
	// PanicPolicyCrash means the runtime should surface the panic and allow the
	// process to crash after any best-effort telemetry hooks run.
	PanicPolicyCrash PanicPolicy = "crash"
)

// FailurePolicy controls how the runtime reacts when a worker enters the
// failed lifecycle state.
//
// Worker failure policy is intentionally separate from readiness policy.
// A worker can be isolated operationally while still affecting runtime
// readiness if it contributes to readiness.
type FailurePolicy string

const (
	// FailurePolicyIsolate means only the failing worker is moved into failed.
	// The runtime stays up and readiness is derived separately from readiness
	// policy and worker status.
	FailurePolicyIsolate FailurePolicy = "isolate"
	// FailurePolicyMarkRuntimeUnready means a worker failure should keep the
	// runtime process alive but force runtime readiness down.
	FailurePolicyMarkRuntimeUnready FailurePolicy = "mark_runtime_unready"
	// FailurePolicyFailRuntime means a worker failure should force aggregate
	// runtime state to failed and readiness to false. A failed runtime does not
	// accept new command dispatches.
	FailurePolicyFailRuntime FailurePolicy = "fail_runtime"
)

// ReadinessPolicy controls when the runtime reports ready.
type ReadinessPolicy string

const (
	// ReadyWhenContributingWorkersReady means all readiness-contributing workers
	// must be running and ready. Workers contribute by default. Use
	// WithWorkerReadinessContribution(false) for optional workers that should
	// not block runtime readiness.
	//
	// If no registered workers contribute to readiness, runtime readiness follows
	// aggregate lifecycle: at least one worker must be running, and no worker may
	// be starting, draining, or stopping.
	ReadyWhenContributingWorkersReady ReadinessPolicy = "contributing_workers"

	// ReadyWhenAllWorkersReady means every registered worker must be running and
	// ready for the runtime to be considered ready. Registered, stopped,
	// starting, draining, stopping, failed, or unready workers make the runtime
	// unready.
	ReadyWhenAllWorkersReady ReadinessPolicy = "all_workers"
)

// RuntimeOption configures runtime-wide policy or default worker policy.
//
// Runtime-wide policy always applies. Default worker policy is copied into each
// worker when it is registered, then WorkerOption values may override that
// worker's copy. Layered limits, such as runtime and worker command
// concurrency, are cumulative: a command must pass both gates to run.
type RuntimeOption func(*runtimeConfig)

// WorkerOption overrides the operational policy for one worker at registration
// time.
type WorkerOption func(*workerConfig)

// runtimeConfig holds runtime-owned policy plus the worker policy copied into
// each worker at registration time.
type runtimeConfig struct {
	// Runtime-owned policy.
	commandConcurrency int
	readinessPolicy    ReadinessPolicy
	observer           Observer

	// Worker policy copied at registration before WorkerOption values are
	// applied.
	defaultWorker workerConfig
}

// workerConfig holds the operational policy for one registered worker. Runtime
// defaults seed this value, and WorkerOption values can override that seed for a
// specific worker.
type workerConfig struct {
	// commands are user-defined mutating operations exposed by this worker.
	// Workerkit lifecycle operations such as Start and Stop are runtime control,
	// not entries in this list.
	commands []CommandSpec

	// Timeout values apply per attempt by adding a deadline to the context passed
	// to worker or command code. They are cooperative deadlines, not hard
	// preemption. The worker or command must observe ctx.Done() and return for the
	// timeout to take effect. Zero or negative disables the timeout for that path.
	startTimeout   time.Duration
	stopTimeout    time.Duration
	commandTimeout time.Duration

	// Retry policies decide whether a failed Start or command attempt should be
	// tried again. Nil is normalized to retrykit.Never by the option helpers and
	// retry runner.
	startRetryPolicy   retrykit.Policy
	commandRetryPolicy retrykit.Policy

	// commandConcurrency caps command executions for this worker only. Runtime
	// command concurrency is checked separately, and both gates must have
	// capacity before a command can run.
	commandConcurrency int

	// Panic and failure policies decide how runtime-managed worker lifecycle
	// paths affect worker and runtime state after panics or returned Start/Stop
	// errors. Command handler returned errors are command failures: Workerkit
	// records LastCommandFailure and emits observer events, but does not
	// automatically move the worker to failed. Command panics are recovered
	// according to panic policy and currently fail the worker.
	panicPolicy   PanicPolicy
	failurePolicy FailurePolicy

	// readyOnStart and acceptingWork are the operational state assigned after
	// successful Start. acceptingWork controls Workerkit command dispatch
	// admission, not the worker's domain-specific business loop. Workers may
	// later change readiness and accepting-work state through WorkerRuntime.
	readyOnStart  bool
	acceptingWork bool

	// contributesToReadiness decides whether this worker participates in the
	// default runtime readiness policy.
	contributesToReadiness bool
}

func defaultRuntimeConfig() runtimeConfig {
	return runtimeConfig{
		commandConcurrency: 0,
		readinessPolicy:    ReadyWhenContributingWorkersReady,
		observer:           NopObserver{},
		defaultWorker:      defaultWorkerConfig(),
	}
}

func defaultWorkerConfig() workerConfig {
	return workerConfig{
		startTimeout:           defaultStartTimeout,
		stopTimeout:            defaultStopTimeout,
		commandTimeout:         defaultCommandTimeout,
		startRetryPolicy:       retrykit.Never(),
		commandRetryPolicy:     retrykit.Never(),
		commandConcurrency:     defaultCommandConcurrency,
		panicPolicy:            PanicPolicyRecover,
		failurePolicy:          FailurePolicyIsolate,
		readyOnStart:           true,
		acceptingWork:          true,
		contributesToReadiness: true,
	}
}

func (cfg runtimeConfig) validate() error {
	if !validReadinessPolicy(cfg.readinessPolicy) {
		return fmt.Errorf("invalid readiness policy: %q", cfg.readinessPolicy)
	}
	if err := cfg.defaultWorker.validate(); err != nil {
		return fmt.Errorf("invalid default worker config: %w", err)
	}
	return nil
}

func (cfg workerConfig) validate() error {
	if !validPanicPolicy(cfg.panicPolicy) {
		return fmt.Errorf("invalid panic policy: %q", cfg.panicPolicy)
	}
	if !validFailurePolicy(cfg.failurePolicy) {
		return fmt.Errorf("invalid failure policy: %q", cfg.failurePolicy)
	}
	if cfg.startRetryPolicy == nil {
		return fmt.Errorf("start retry policy must not be nil")
	}
	if cfg.commandRetryPolicy == nil {
		return fmt.Errorf("command retry policy must not be nil")
	}
	return nil
}

func validReadinessPolicy(policy ReadinessPolicy) bool {
	switch policy {
	case ReadyWhenContributingWorkersReady, ReadyWhenAllWorkersReady:
		return true
	default:
		return false
	}
}

func validPanicPolicy(policy PanicPolicy) bool {
	switch policy {
	case PanicPolicyRecover, PanicPolicyCrash:
		return true
	default:
		return false
	}
}

func validFailurePolicy(policy FailurePolicy) bool {
	switch policy {
	case FailurePolicyIsolate, FailurePolicyMarkRuntimeUnready, FailurePolicyFailRuntime:
		return true
	default:
		return false
	}
}

// -----------------------------------------------------------------------------
// Runtime-wide options.

// WithRuntimeCommandConcurrency caps total concurrent command executions across
// the runtime. Zero or negative leaves the runtime-wide cap unbounded. Worker
// command concurrency caps are still enforced, and a command must acquire both a
// runtime slot and a worker slot before it can run.
func WithRuntimeCommandConcurrency(limit int) RuntimeOption {
	return func(cfg *runtimeConfig) {
		if limit < 0 {
			limit = 0
		}
		cfg.commandConcurrency = limit
	}
}

// WithReadinessPolicy sets how runtime readiness is derived from worker
// readiness and lifecycle state.
func WithReadinessPolicy(policy ReadinessPolicy) RuntimeOption {
	return func(cfg *runtimeConfig) {
		if policy != "" {
			cfg.readinessPolicy = policy
		}
	}
}

// -----------------------------------------------------------------------------
// Default worker options.
//
// Default worker options configure policy copied into each worker at
// registration time. They do not affect workers that have already been
// registered. Use the matching WorkerOption to override one Register call.
//
// Timeout options configure cooperative context deadlines. Workerkit cannot
// interrupt worker or command code that ignores ctx.Done().

// WithDefaultStartTimeout sets the Start timeout copied into each worker when
// it is registered. Zero or negative disables the per-attempt timeout. Use
// WithWorkerStartTimeout to override one worker.
func WithDefaultStartTimeout(timeout time.Duration) RuntimeOption {
	return func(cfg *runtimeConfig) { cfg.defaultWorker.startTimeout = timeout }
}

// WithDefaultStopTimeout sets the Stop timeout copied into each worker when it
// is registered. Zero or negative disables the per-attempt timeout. Use
// WithWorkerStopTimeout to override one worker.
func WithDefaultStopTimeout(timeout time.Duration) RuntimeOption {
	return func(cfg *runtimeConfig) { cfg.defaultWorker.stopTimeout = timeout }
}

// WithDefaultCommandTimeout sets the command timeout copied into each worker
// when it is registered. Zero or negative disables the per-attempt timeout. Use
// WithWorkerCommandTimeout to override one worker.
func WithDefaultCommandTimeout(timeout time.Duration) RuntimeOption {
	return func(cfg *runtimeConfig) { cfg.defaultWorker.commandTimeout = timeout }
}

// WithDefaultStartRetry sets the Start retry policy copied into each worker
// when it is registered. Nil means no retry. Use WithWorkerStartRetry to
// override one worker.
//
// Configure Start retry only when Worker.Start is safe to call again after a
// failed attempt.
func WithDefaultStartRetry(policy retrykit.Policy) RuntimeOption {
	return func(cfg *runtimeConfig) {
		if policy == nil {
			cfg.defaultWorker.startRetryPolicy = retrykit.Never()
			return
		}
		cfg.defaultWorker.startRetryPolicy = policy
	}
}

// WithDefaultCommandRetry sets the retry policy for command handler returned
// errors. The policy is copied into each worker when it is registered. Nil
// means no retry. Use WithWorkerCommandRetry to override one worker.
//
// Command retries can amplify side effects unless handlers are explicitly
// written to be idempotent. Command panics are handled by PanicPolicy and are
// not retried.
func WithDefaultCommandRetry(policy retrykit.Policy) RuntimeOption {
	return func(cfg *runtimeConfig) {
		if policy == nil {
			cfg.defaultWorker.commandRetryPolicy = retrykit.Never()
			return
		}
		cfg.defaultWorker.commandRetryPolicy = policy
	}
}

// WithDefaultWorkerCommandConcurrency sets the per-worker command concurrency
// limit copied into each worker when it is registered. Zero or negative leaves
// the per-worker cap unbounded. Runtime command concurrency is still enforced,
// and a command must pass both gates to run.
func WithDefaultWorkerCommandConcurrency(limit int) RuntimeOption {
	return func(cfg *runtimeConfig) {
		if limit < 0 {
			limit = 0
		}
		cfg.defaultWorker.commandConcurrency = limit
	}
}

// WithDefaultPanicPolicy sets the panic policy copied into each worker when it
// is registered. Empty policy values leave the default unchanged. Use
// WithWorkerPanicPolicy to override one worker.
func WithDefaultPanicPolicy(policy PanicPolicy) RuntimeOption {
	return func(cfg *runtimeConfig) {
		if policy != "" {
			cfg.defaultWorker.panicPolicy = policy
		}
	}
}

// WithDefaultFailurePolicy sets the failure policy copied into each worker when
// it is registered. Empty policy values leave the default unchanged. Use
// WithWorkerFailurePolicy to override one worker.
func WithDefaultFailurePolicy(policy FailurePolicy) RuntimeOption {
	return func(cfg *runtimeConfig) {
		if policy != "" {
			cfg.defaultWorker.failurePolicy = policy
		}
	}
}

// WithDefaultReadyOnStart sets the ready state assigned after each worker starts
// successfully. Use WithWorkerReadyOnStart to override one worker.
func WithDefaultReadyOnStart(ready bool) RuntimeOption {
	return func(cfg *runtimeConfig) { cfg.defaultWorker.readyOnStart = ready }
}

// WithDefaultAcceptingWorkOnStart sets whether workers accept Workerkit command
// dispatches after Start succeeds. It does not control the worker's
// domain-specific business loop. Use WithWorkerAcceptingWorkOnStart to override
// one worker.
func WithDefaultAcceptingWorkOnStart(accepting bool) RuntimeOption {
	return func(cfg *runtimeConfig) { cfg.defaultWorker.acceptingWork = accepting }
}

// WithDefaultWorkerReadinessContribution sets whether workers contribute to
// aggregate runtime readiness by default. Use WithWorkerReadinessContribution
// to override one worker.
func WithDefaultWorkerReadinessContribution(contributes bool) RuntimeOption {
	return func(cfg *runtimeConfig) { cfg.defaultWorker.contributesToReadiness = contributes }
}

// -----------------------------------------------------------------------------
// Telemetry options.

// WithObserver sets the runtime's transport-neutral observer hook. Nil installs
// a no-op observer. Non-nil observers are wrapped with SafeObserver so telemetry
// panics do not escape runtime lifecycle or command dispatch paths.
func WithObserver(observer Observer) RuntimeOption {
	return func(cfg *runtimeConfig) {
		if observer == nil {
			cfg.observer = NopObserver{}
			return
		}
		cfg.observer = SafeObserver(observer)
	}
}

// -----------------------------------------------------------------------------
// Per-worker options.
//
// Per-worker options override the copied default policy for one Register call.
// Timeout options configure cooperative context deadlines. Workerkit cannot
// interrupt worker or command code that ignores ctx.Done().

// WithWorkerStartTimeout overrides the Start timeout for one worker. Zero or
// negative disables the per-attempt timeout.
func WithWorkerStartTimeout(timeout time.Duration) WorkerOption {
	return func(cfg *workerConfig) { cfg.startTimeout = timeout }
}

// WithWorkerStopTimeout overrides the Stop timeout for one worker. Zero or
// negative disables the per-attempt timeout.
func WithWorkerStopTimeout(timeout time.Duration) WorkerOption {
	return func(cfg *workerConfig) { cfg.stopTimeout = timeout }
}

// WithWorkerCommandTimeout overrides the command timeout for one worker. Zero
// or negative disables the per-attempt timeout.
func WithWorkerCommandTimeout(timeout time.Duration) WorkerOption {
	return func(cfg *workerConfig) { cfg.commandTimeout = timeout }
}

// WithWorkerStartRetry overrides the Start retry policy for one worker.
// Nil means no retry.
//
// Configure Start retry only when Worker.Start is safe to call again after a
// failed attempt.
func WithWorkerStartRetry(policy retrykit.Policy) WorkerOption {
	return func(cfg *workerConfig) {
		if policy == nil {
			cfg.startRetryPolicy = retrykit.Never()
			return
		}
		cfg.startRetryPolicy = policy
	}
}

// WithWorkerCommandRetry overrides the retry policy for command handler
// returned errors on one worker. Nil means no retry. Command panics are handled
// by PanicPolicy and are not retried.
func WithWorkerCommandRetry(policy retrykit.Policy) WorkerOption {
	return func(cfg *workerConfig) {
		if policy == nil {
			cfg.commandRetryPolicy = retrykit.Never()
			return
		}
		cfg.commandRetryPolicy = policy
	}
}

// WithWorkerCommandConcurrency caps concurrent command executions for one
// worker. Zero or negative leaves the per-worker cap unbounded. Runtime command
// concurrency is still enforced, and a command must pass both gates to run.
func WithWorkerCommandConcurrency(limit int) WorkerOption {
	return func(cfg *workerConfig) {
		if limit < 0 {
			limit = 0
		}
		cfg.commandConcurrency = limit
	}
}

// WithWorkerPanicPolicy overrides the panic policy for one worker. Empty policy
// values leave the inherited default unchanged.
func WithWorkerPanicPolicy(policy PanicPolicy) WorkerOption {
	return func(cfg *workerConfig) {
		if policy != "" {
			cfg.panicPolicy = policy
		}
	}
}

// WithWorkerFailurePolicy overrides the failure policy for one worker. Empty
// policy values leave the inherited default unchanged.
func WithWorkerFailurePolicy(policy FailurePolicy) WorkerOption {
	return func(cfg *workerConfig) {
		if policy != "" {
			cfg.failurePolicy = policy
		}
	}
}

// WithWorkerReadyOnStart overrides the ready state assigned after one worker
// starts successfully.
func WithWorkerReadyOnStart(ready bool) WorkerOption {
	return func(cfg *workerConfig) { cfg.readyOnStart = ready }
}

// WithWorkerAcceptingWorkOnStart overrides whether one worker accepts Workerkit
// command dispatches after Start succeeds. It does not control the worker's
// domain-specific business loop.
func WithWorkerAcceptingWorkOnStart(accepting bool) WorkerOption {
	return func(cfg *workerConfig) { cfg.acceptingWork = accepting }
}

// WithWorkerReadinessContribution controls whether one worker contributes to
// aggregate runtime readiness.
func WithWorkerReadinessContribution(contributes bool) WorkerOption {
	return func(cfg *workerConfig) { cfg.contributesToReadiness = contributes }
}

// WithCommand registers one worker-owned command on this worker.
//
// Workerkit lifecycle operations such as Start and Stop are runtime control,
// not commands. Use worker commands for domain control operations such as
// pause, resume, refresh, or snapshot-like actions that the worker chooses to
// expose.
func WithCommand(name string, handler CommandHandler) WorkerOption {
	return WithCommandSpec(CommandSpec{
		Name:    name,
		Handler: handler,
	})
}

// WithCommandSpec registers one worker-owned command spec on this worker.
func WithCommandSpec(command CommandSpec) WorkerOption {
	return func(cfg *workerConfig) {
		cfg.commands = append(cfg.commands, command)
	}
}
