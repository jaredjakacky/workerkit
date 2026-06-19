package workerkit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const loopFailureStopHookTimeout = 5 * time.Second

var (
	// ErrLoopExitedUnexpectedly reports that a LoopWorker loop returned nil before
	// Stop canceled it.
	ErrLoopExitedUnexpectedly = errors.New("loop worker exited unexpectedly")
	// ErrLoopWorkerActive reports that Start found an existing loop lifecycle in
	// progress instead of launching a new loop.
	ErrLoopWorkerActive = errors.New("loop worker already active")
)

// LoopFunc is the long-running background function managed by LoopWorker.
type LoopFunc func(context.Context, WorkerRuntime) error

// LoopWorkerOption configures a LoopWorker.
type LoopWorkerOption func(*loopWorkerConfig)

type loopWorkerConfig struct {
	onStart   func(context.Context, WorkerRuntime) error
	onStop    func(context.Context, WorkerRuntime) error
	autoReady bool
}

type loopWorkerState int

const (
	loopIdle loopWorkerState = iota
	loopStarting
	loopRunning
	loopStopping
	loopStopped
)

func (s loopWorkerState) String() string {
	switch s {
	case loopIdle:
		return "idle"
	case loopStarting:
		return "starting"
	case loopRunning:
		return "running"
	case loopStopping:
		return "stopping"
	case loopStopped:
		return "stopped"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// WithLoopStart sets an optional hook run before the loop goroutine starts.
func WithLoopStart(fn func(context.Context, WorkerRuntime) error) LoopWorkerOption {
	return func(cfg *loopWorkerConfig) {
		cfg.onStart = fn
	}
}

// WithLoopStop sets an optional cleanup hook run after the loop goroutine
// stops. The hook runs at most once, including after an unexpected loop failure.
// Stop waits for an in-progress cleanup hook before returning. If Stop times
// out before the loop exits, a later Stop resumes waiting on the same loop and
// cleanup; Start remains blocked until both complete.
func WithLoopStop(fn func(context.Context, WorkerRuntime) error) LoopWorkerOption {
	return func(cfg *loopWorkerConfig) {
		cfg.onStop = fn
	}
}

// WithLoopAutoReady controls whether Start marks the worker ready after
// launching the loop goroutine.
//
// Auto-ready is a convenience for loops whose successful launch is enough to
// consider the worker operational. It does not prove that the loop completed
// domain warmup, acquired leases, connected to brokers, completed a first poll,
// or validated external dependencies. Disable auto-ready when readiness depends
// on work performed inside the loop, and call WorkerRuntime.SetReady(true) from
// the loop after that condition is met.
func WithLoopAutoReady(enabled bool) LoopWorkerOption {
	return func(cfg *loopWorkerConfig) {
		cfg.autoReady = enabled
	}
}

// LoopWorker is a Worker implementation for long-running background loops.
//
// Use NewLoopWorker to construct one with production-oriented lifecycle
// behavior: Start launches the loop in a goroutine, Stop cancels the loop and
// waits for it to exit, and unexpected loop exits are reported through
// WorkerRuntime.ReportFailure before stop completion is published. Cancellation
// errors caused by Stop are treated as normal exits, while independent errors
// racing with Stop remain failures. By default, Start marks the worker ready
// after the loop goroutine starts. This is only a launch-readiness signal. Use
// WithLoopAutoReady(false) when readiness depends on domain warmup inside the
// loop, such as acquiring a lease, connecting to a broker, loading initial
// state, completing a first poll, or validating external dependencies. In that
// mode, the loop should call WorkerRuntime.SetReady(true) when it is actually
// ready.
//
// The loop context preserves values from the Start call for telemetry and
// correlation, but is detached from Start cancellation because Start returns
// after launching the background loop. Stop owns loop cancellation through the
// LoopWorker's internal cancel function. Loop functions should still observe
// ctx.Done() so Stop can shut them down cleanly.
type LoopWorker struct {
	loop      LoopFunc
	onStart   func(context.Context, WorkerRuntime) error
	onStop    func(context.Context, WorkerRuntime) error
	autoReady bool

	mu      sync.Mutex
	state   loopWorkerState
	cancel  context.CancelFunc
	done    chan struct{}
	runtime WorkerRuntime

	stopHookRunning  bool
	stopHookComplete bool
	stopHookDone     chan struct{}
	stopHookErr      error
}

// NewLoopWorker constructs a LoopWorker for a long-running background loop.
// It enables auto-ready by default. Use WithLoopAutoReady(false) for
// domain-gated readiness.
func NewLoopWorker(loop LoopFunc, opts ...LoopWorkerOption) *LoopWorker {
	cfg := loopWorkerConfig{
		autoReady: true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &LoopWorker{
		loop:      loop,
		onStart:   cfg.onStart,
		onStop:    cfg.onStop,
		autoReady: cfg.autoReady,
	}
}

// Start implements Worker.
func (w *LoopWorker) Start(ctx context.Context) error {
	if w.loop == nil {
		return errors.New("loop worker loop must not be nil")
	}

	runtime, ok := WorkerRuntimeFromContext(ctx)
	if !ok {
		return errors.New("worker runtime handle unavailable")
	}

	if err := w.beginStart(); err != nil {
		return err
	}
	started := false
	defer func() {
		if !started {
			w.finishFailedStart()
		}
	}()

	if w.onStart != nil {
		if err := w.onStart(ctx, runtime); err != nil {
			return err
		}
	}
	if !w.autoReady {
		if err := runtime.SetReady(false); err != nil {
			return err
		}
	}

	// Preserve Start context values for telemetry/correlation, but detach from
	// Start cancellation because the loop outlives the Start call. Stop uses this
	// cancel function to shut the loop down.
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	w.finishStart(runtime, cancel, done)
	started = true

	go w.runLoop(loopCtx, runtime, done)

	if !w.autoReady {
		return nil
	}
	if err := runtime.SetReady(true); err != nil {
		w.markStopping(done)
		cancel()
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("mark loop worker ready: %w", err)
		}
		if stopErr := w.runStopHook(ctx, runtime); stopErr != nil {
			return errors.Join(err, stopErr)
		}
		return err
	}
	return nil
}

// Stop implements Worker.
func (w *LoopWorker) Stop(ctx context.Context) error {
	cancel, done, runtime, ok := w.beginStop()
	if ok {
		cancel()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if runtime == nil {
		var ok bool
		runtime, ok = WorkerRuntimeFromContext(ctx)
		if !ok {
			return errors.New("worker runtime handle unavailable")
		}
	}
	return w.runStopHook(ctx, runtime)
}

func (w *LoopWorker) beginStart() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state == loopStarting || w.state == loopRunning || w.state == loopStopping {
		return fmt.Errorf("%w: state=%s", ErrLoopWorkerActive, w.state)
	}
	if w.state == loopStopped && !w.stopHookComplete {
		return fmt.Errorf("%w: state=%s cleanup=pending", ErrLoopWorkerActive, w.state)
	}
	w.state = loopStarting
	w.stopHookRunning = false
	w.stopHookComplete = false
	w.stopHookDone = make(chan struct{})
	w.stopHookErr = nil
	return nil
}

func (w *LoopWorker) finishStart(runtime WorkerRuntime, cancel context.CancelFunc, done chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.runtime = runtime
	w.cancel = cancel
	w.done = done
	w.state = loopRunning
}

func (w *LoopWorker) finishFailedStart() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.state = loopIdle
}

func (w *LoopWorker) beginStop() (context.CancelFunc, chan struct{}, WorkerRuntime, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.cancel == nil || w.done == nil {
		return nil, nil, w.runtime, false
	}
	switch w.state {
	case loopRunning:
		w.state = loopStopping
	case loopStopping:
	default:
		return nil, nil, w.runtime, false
	}
	return w.cancel, w.done, w.runtime, true
}

func (w *LoopWorker) markStopping(done chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.done == done {
		w.state = loopStopping
	}
}

func (w *LoopWorker) runLoop(ctx context.Context, runtime WorkerRuntime, done chan struct{}) {
	err := w.loop(ctx, runtime)

	w.mu.Lock()
	stopRequested := w.state == loopStopping && ctx.Err() != nil
	if w.done == done && !stopRequested {
		w.state = loopStopping
	}
	w.mu.Unlock()

	if stopRequested && intentionalLoopStop(err, ctx.Err()) {
		w.finishLoop(done)
		return
	}
	if err == nil {
		err = ErrLoopExitedUnexpectedly
	}
	if err != nil {
		_ = runtime.ReportFailure(err)
		// Run failure cleanup here once because the loop has already exited and
		// Stop can no longer wait on it. Preserve loop context values for
		// telemetry, but bound cleanup because this path is best-effort and
		// errors are ignored after the primary loop failure has already been
		// reported.
		stopCtx := context.WithoutCancel(ctx)
		stopCtx, cancel := context.WithTimeout(stopCtx, loopFailureStopHookTimeout)
		defer cancel()
		_ = w.runStopHook(stopCtx, runtime)
	}
	w.finishLoop(done)
}

func intentionalLoopStop(loopErr, contextErr error) bool {
	return contextErr != nil && (loopErr == nil || errors.Is(loopErr, contextErr))
}

func (w *LoopWorker) finishLoop(done chan struct{}) {
	w.mu.Lock()
	if w.done == done {
		w.cancel = nil
		w.done = nil
		w.runtime = nil
		w.state = loopStopped
	}
	w.mu.Unlock()
	close(done)
}

func (w *LoopWorker) runStopHook(ctx context.Context, runtime WorkerRuntime) error {
	done, run, err := w.beginStopHook()
	if err != nil || !run {
		if err != nil {
			return err
		}
		select {
		case <-done:
			return w.completedStopHookError()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	defer func() {
		w.finishStopHook(done, err)
	}()
	if w.onStop != nil {
		err = w.onStop(ctx, runtime)
	}
	return err
}

func (w *LoopWorker) beginStopHook() (chan struct{}, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopHookComplete {
		return w.stopHookDone, false, nil
	}
	if w.stopHookDone == nil {
		w.stopHookDone = make(chan struct{})
	}
	if w.stopHookRunning {
		return w.stopHookDone, false, nil
	}
	w.stopHookRunning = true
	return w.stopHookDone, true, nil
}

func (w *LoopWorker) finishStopHook(done chan struct{}, err error) {
	w.mu.Lock()
	if w.stopHookDone == done && !w.stopHookComplete {
		w.stopHookErr = err
		w.stopHookRunning = false
		w.stopHookComplete = true
		close(done)
	}
	w.mu.Unlock()
}

func (w *LoopWorker) completedStopHookError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopHookErr
}
