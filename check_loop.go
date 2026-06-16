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
)

// CheckResultObserver observes one completed Opskit check result.
type CheckResultObserver func(context.Context, opskit.CheckResult)

// CheckSummaryObserver observes one completed Opskit check group summary.
type CheckSummaryObserver func(context.Context, opskit.CheckSummary)

// CheckLoopOption configures a Workerkit loop that periodically executes
// Opskit active checks.
type CheckLoopOption func(*checkLoopConfig)

type checkLoopConfig struct {
	interval                time.Duration
	initialDelay            time.Duration
	runImmediately          bool
	timeout                 time.Duration
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

// WithCheckTimeout sets a per-execution timeout. A non-positive timeout means
// executions use only the loop context cancellation.
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

// WithCheckReportFailureOnNotReady controls whether not-ready check results are
// also reported as Workerkit worker failures. Disabled by default.
func WithCheckReportFailureOnNotReady(enabled bool) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.reportFailureOnNotReady = enabled
	}
}

// WithCheckResultObserver observes completed single-check results.
func WithCheckResultObserver(observer CheckResultObserver) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.resultObserver = observer
	}
}

// WithCheckSummaryObserver observes completed check-group summaries.
func WithCheckSummaryObserver(observer CheckSummaryObserver) CheckLoopOption {
	return func(cfg *checkLoopConfig) {
		cfg.summaryObserver = observer
	}
}

// NewCheckLoop constructs a Worker that periodically executes one Opskit
// Checker. Workerkit owns the background execution policy; the checked
// component remains responsible for any cached dependency health state.
func NewCheckLoop(checker opskit.Checker, opts ...CheckLoopOption) Worker {
	cfg := newCheckLoopConfig(opts)
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
// CheckGroup. Workerkit owns the background execution policy; the checked
// component remains responsible for any cached dependency health state.
func NewCheckGroupLoop(group opskit.CheckGroup, opts ...CheckLoopOption) Worker {
	cfg := newCheckLoopConfig(opts)
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
			return nil
		}
	}

	for {
		if err := waitCheckLoopDelay(ctx, nextCheckLoopDelay(cfg)); err != nil {
			return nil
		}
		if err := runCheckLoopOnce(ctx, runtime, cfg, run); err != nil {
			return nil
		}
	}
}

func runCheckLoopOnce(ctx context.Context, runtime WorkerRuntime, cfg checkLoopConfig, run func(context.Context) checkLoopOutcome) error {
	checkCtx := ctx
	cancel := func() {}
	if cfg.timeout > 0 {
		checkCtx, cancel = context.WithTimeout(ctx, cfg.timeout)
	}
	defer cancel()

	outcome := run(checkCtx)
	if checkCtx.Err() != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if cfg.readyOnSuccess {
		if err := runtime.SetReady(outcome.ready); err != nil {
			return err
		}
	}
	if !outcome.ready && cfg.reportFailureOnNotReady {
		if err := runtime.ReportFailure(checkLoopNotReadyError(outcome)); err != nil {
			return err
		}
	}
	return nil
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
