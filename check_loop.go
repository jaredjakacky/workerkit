package workerkit

import (
	"context"
	"errors"
	"fmt"
	"time"

	opskit "github.com/jaredjakacky/opskit"
)

const defaultCheckLoopInterval = 30 * time.Second

var (
	// ErrNilChecker reports that a check loop was constructed without a checker.
	ErrNilChecker = errors.New("opskit checker must not be nil")
	// ErrNilCheckGroup reports that a check group loop was constructed without a group.
	ErrNilCheckGroup = errors.New("opskit check group must not be nil")
	// ErrCheckLoopPanicked reports that a check loop recovered a panic from an
	// Opskit check execution path.
	ErrCheckLoopPanicked = errors.New("opskit check loop panicked")
)

// CheckResultObserver observes one completed Opskit check result. Use it for
// rich result payloads; bounded core execution telemetry is emitted through an
// optional CheckExecutionObserver attached to the Runtime.
type CheckResultObserver func(context.Context, opskit.CheckResult)

// CheckSummaryObserver observes one completed Opskit check group summary. Use
// it for child result detail; bounded core execution telemetry is emitted
// through an optional CheckExecutionObserver attached to the Runtime.
type CheckSummaryObserver func(context.Context, opskit.CheckSummary)

// CheckLoopOption configures a Workerkit loop that periodically executes
// Opskit active checks.
type CheckLoopOption func(*checkLoopConfig)

type checkLoopConfig struct {
	kind                    CheckKind
	interval                time.Duration
	initialDelay            time.Duration
	runImmediately          bool
	timeout                 time.Duration
	panicErr                error
	jitter                  func(time.Duration) time.Duration
	readyOnSuccess          bool
	reportFailureOnNotReady bool
	resultObserver          CheckResultObserver
	summaryObserver         CheckSummaryObserver
}

type checkLoopOutcome struct {
	ready   bool
	state   opskit.State
	message string
}

type checkLoopObservationRuntime interface {
	startCheckObservation(context.Context, CheckKind, time.Time) (context.Context, CheckObservation)
	endCheckObservation(context.Context, CheckObservation, CheckKind, CheckOutcome, bool, time.Time)
}

func defaultCheckLoopConfig() checkLoopConfig {
	return checkLoopConfig{
		interval:       defaultCheckLoopInterval,
		runImmediately: true,
		readyOnSuccess: true,
	}
}

// WithCheckInterval sets the steady-state interval between check executions.
// Non-positive values keep the default interval.
func WithCheckInterval(interval time.Duration) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		if interval > 0 {
			cfg.interval = interval
		}
	}
}

// WithCheckInitialDelay delays the first check loop action after Start.
// Non-positive values disable the initial delay.
func WithCheckInitialDelay(delay time.Duration) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		if delay > 0 {
			cfg.initialDelay = delay
		}
	}
}

// WithCheckRunImmediately controls whether the loop executes once before
// waiting for the first interval. An initial delay, when configured, is still
// honored before that first execution.
func WithCheckRunImmediately(enabled bool) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.runImmediately = enabled
	}
}

// WithCheckTimeout sets a per-execution timeout. The deadline is cooperative:
// Workerkit cannot interrupt a checker that ignores ctx.Done(). A result
// returned after the deadline is not applied to readiness. A non-positive
// timeout means executions use only the loop context cancellation.
func WithCheckTimeout(timeout time.Duration) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.timeout = timeout
	}
}

// WithCheckJitter sets an optional function that adjusts each interval wait.
// Returned non-positive durations fall back to the configured interval.
func WithCheckJitter(fn func(time.Duration) time.Duration) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.jitter = fn
	}
}

// WithCheckReadyOnSuccess controls whether ready check results mark the worker
// ready and not-ready results mark it unready. Enabled by default.
func WithCheckReadyOnSuccess(enabled bool) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.readyOnSuccess = enabled
	}
}

// WithCheckReportFailureOnNotReady controls whether not-ready check results and
// per-execution timeouts are also reported as Workerkit worker failures and
// stop the check loop. Disabled by default.
func WithCheckReportFailureOnNotReady(enabled bool) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.reportFailureOnNotReady = enabled
	}
}

// WithCheckResultObserver observes completed single-check results, including
// late results that Workerkit does not apply after a check timeout.
func WithCheckResultObserver(observer CheckResultObserver) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.resultObserver = observer
	}
}

// WithCheckSummaryObserver observes completed check-group summaries, including
// late summaries that Workerkit does not apply after a check timeout.
func WithCheckSummaryObserver(observer CheckSummaryObserver) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.summaryObserver = observer
	}
}

// NewCheckLoop constructs a Worker that periodically executes one Opskit
// Checker. Workerkit owns the background execution policy, including timeout,
// cancellation, panic recovery, bounded execution observation, and Workerkit
// failure reporting. The checked component remains responsible for any cached
// dependency health state.
func NewCheckLoop(checker opskit.Checker, opts ...CheckLoopOption) Worker {
	cfg := newCheckLoopConfig(opts)
	cfg.kind = CheckKindChecker
	cfg.panicErr = WithOperationalFailure(ErrCheckLoopPanicked, opskit.Failure{
		Code:    FailureCodePanic,
		Message: "opskit checker panicked: opskit check loop panicked",
	})
	return NewLoopWorker(
		func(ctx context.Context, runtime WorkerRuntime) error {
			return runCheckLoop(ctx, runtime, cfg, func(ctx context.Context) checkLoopOutcome {
				result := checker.Check(ctx)
				if cfg.resultObserver != nil {
					cfg.resultObserver(ctx, result)
				}
				return checkLoopOutcome{
					ready:   result.Ready,
					state:   result.State,
					message: result.Message,
				}
			})
		},
		WithLoopAutoReady(false),
		WithLoopStart(func(context.Context, WorkerRuntime) error {
			if checker == nil {
				return ErrNilChecker
			}
			return nil
		}),
	)
}

// NewCheckGroupLoop constructs a Worker that periodically executes one Opskit
// CheckGroup. Workerkit owns the background execution policy, including timeout,
// cancellation, panic recovery, bounded execution observation, and Workerkit
// failure reporting. The checked component remains responsible for any cached
// dependency health state.
func NewCheckGroupLoop(group opskit.CheckGroup, opts ...CheckLoopOption) Worker {
	cfg := newCheckLoopConfig(opts)
	cfg.kind = CheckKindGroup
	cfg.panicErr = WithOperationalFailure(ErrCheckLoopPanicked, opskit.Failure{
		Code:    FailureCodePanic,
		Message: "opskit check group panicked: opskit check loop panicked",
	})
	return NewLoopWorker(
		func(ctx context.Context, runtime WorkerRuntime) error {
			return runCheckLoop(ctx, runtime, cfg, func(ctx context.Context) checkLoopOutcome {
				summary := group.CheckAll(ctx)
				if cfg.summaryObserver != nil {
					cfg.summaryObserver(ctx, summary)
				}
				return checkLoopOutcome{
					ready:   summary.Ready,
					state:   summary.State,
					message: summary.Message,
				}
			})
		},
		WithLoopAutoReady(false),
		WithLoopStart(func(context.Context, WorkerRuntime) error {
			if group == nil {
				return ErrNilCheckGroup
			}
			return nil
		}),
	)
}

func newCheckLoopConfig(opts []CheckLoopOption) checkLoopConfig {
	cfg := defaultCheckLoopConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func runCheckLoop(ctx context.Context, runtime WorkerRuntime, cfg checkLoopConfig, run func(context.Context) checkLoopOutcome) error {
	if cfg.initialDelay > 0 {
		if err := waitCheckLoopDelay(ctx, cfg.initialDelay); err != nil {
			return nil
		}
	}

	if cfg.runImmediately {
		if err := runCheckLoopOnce(ctx, runtime, cfg, run); err != nil {
			return err
		}
	}

	for {
		if err := waitCheckLoopDelay(ctx, nextCheckLoopDelay(cfg)); err != nil {
			return nil
		}
		if err := runCheckLoopOnce(ctx, runtime, cfg, run); err != nil {
			return err
		}
	}
}

func runCheckLoopOnce(ctx context.Context, runtime WorkerRuntime, cfg checkLoopConfig, run func(context.Context) checkLoopOutcome) (err error) {
	loopCtx := ctx
	startedAt := time.Now()
	observedCtx, observation := startCheckLoopObservation(ctx, runtime, cfg.kind, startedAt)
	ctx = observedCtx
	executionOutcome := CheckOutcomeError
	loopContinues := false
	defer func() {
		if recover() != nil {
			executionOutcome = CheckOutcomePanic
			if cfg.panicErr != nil {
				err = cfg.panicErr
			} else {
				err = ErrCheckLoopPanicked
			}
		}
		endCheckLoopObservation(ctx, runtime, observation, cfg.kind, executionOutcome, loopContinues, startedAt)
	}()

	checkCtx := ctx
	cancel := func() {}
	if cfg.timeout > 0 {
		checkCtx, cancel = context.WithTimeout(ctx, cfg.timeout)
	}
	defer cancel()

	checkOutcome := run(checkCtx)
	checkErr := checkCtx.Err()
	if parentErr := loopCtx.Err(); parentErr != nil {
		executionOutcome = CheckOutcomeCanceled
		return parentErr
	}
	if checkErr != nil {
		if errors.Is(checkErr, context.DeadlineExceeded) {
			executionOutcome = CheckOutcomeTimeout
		} else {
			executionOutcome = CheckOutcomeCanceled
		}
		if cfg.readyOnSuccess {
			if err := runtime.SetReady(false); err != nil {
				executionOutcome = CheckOutcomeError
				return err
			}
		}
		if cfg.reportFailureOnNotReady {
			return checkErr
		}
		loopContinues = true
		return nil
	}

	if checkOutcome.ready {
		executionOutcome = CheckOutcomeReady
	} else {
		executionOutcome = CheckOutcomeNotReady
	}
	if cfg.readyOnSuccess {
		if err := runtime.SetReady(checkOutcome.ready); err != nil {
			executionOutcome = CheckOutcomeError
			return err
		}
	}
	if !checkOutcome.ready && cfg.reportFailureOnNotReady {
		return checkLoopNotReadyError(checkOutcome)
	}
	loopContinues = true
	return nil
}

func startCheckLoopObservation(ctx context.Context, runtime WorkerRuntime, kind CheckKind, startedAt time.Time) (context.Context, CheckObservation) {
	observedRuntime, ok := runtime.(checkLoopObservationRuntime)
	if !ok {
		return ctx, NopCheckObservation{}
	}
	observedCtx, observation := observedRuntime.startCheckObservation(ctx, kind, startedAt)
	if observedCtx == nil {
		observedCtx = ctx
	}
	if observation == nil {
		observation = NopCheckObservation{}
	}
	return observedCtx, observation
}

func endCheckLoopObservation(ctx context.Context, runtime WorkerRuntime, observation CheckObservation, kind CheckKind, outcome CheckOutcome, loopContinues bool, startedAt time.Time) {
	observedRuntime, ok := runtime.(checkLoopObservationRuntime)
	if !ok {
		return
	}
	observedRuntime.endCheckObservation(ctx, observation, kind, outcome, loopContinues, startedAt)
}

func waitCheckLoopDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nextCheckLoopDelay(cfg checkLoopConfig) time.Duration {
	if cfg.jitter == nil {
		return cfg.interval
	}
	delay := cfg.jitter(cfg.interval)
	if delay <= 0 {
		return cfg.interval
	}
	return delay
}

func checkLoopNotReadyError(outcome checkLoopOutcome) error {
	if outcome.message != "" {
		return fmt.Errorf("opskit check not ready: state=%s message=%s", outcome.state, outcome.message)
	}
	return fmt.Errorf("opskit check not ready: state=%s", outcome.state)
}
