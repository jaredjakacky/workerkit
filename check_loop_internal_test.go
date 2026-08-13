package workerkit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	opskit "github.com/jaredjakacky/opskit"
)

func TestRunCheckLoopWaitsDefaultIntervalAfterExecutionCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := defaultCheckLoopConfig()
		if cfg.interval != 30*time.Second || !cfg.runImmediately || !cfg.readyOnSuccess {
			t.Fatalf("defaultCheckLoopConfig() = %#v, want 30s immediate ready-managed loop", cfg)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		var firstStarted time.Time
		var firstCompleted time.Time
		var secondStarted time.Time
		calls := 0

		go func() {
			done <- runCheckLoop(ctx, &checkLoopRuntime{}, cfg, func(context.Context) checkLoopOutcome {
				calls++
				switch calls {
				case 1:
					firstStarted = time.Now()
					time.Sleep(5 * time.Second)
					firstCompleted = time.Now()
				case 2:
					secondStarted = time.Now()
					cancel()
				default:
					panic("unexpected extra check execution")
				}
				return checkLoopOutcome{ready: true, state: opskit.StateReady}
			})
		}()

		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("runCheckLoop error = %v, want context.Canceled", err)
		}
		if calls != 2 {
			t.Fatalf("check executions = %d, want 2", calls)
		}
		if got := firstCompleted.Sub(firstStarted); got != 5*time.Second {
			t.Fatalf("first execution duration = %v, want 5s", got)
		}
		if got := secondStarted.Sub(firstCompleted); got != 30*time.Second {
			t.Fatalf("completion-to-next-start wait = %v, want 30s", got)
		}
		if got := secondStarted.Sub(firstStarted); got != 35*time.Second {
			t.Fatalf("start-to-start cadence = %v, want execution plus interval (35s)", got)
		}
	})
}

func TestRunCheckLoopReturnsSetReadyError(t *testing.T) {
	t.Parallel()

	want := errors.New("set ready failed")
	runtime := &checkLoopRuntime{setReadyErr: want}

	err := runCheckLoop(context.Background(), runtime, checkLoopConfig{
		interval:       time.Hour,
		runImmediately: true,
		readyOnSuccess: true,
	}, func(context.Context) checkLoopOutcome {
		return checkLoopOutcome{ready: true, state: opskit.StateReady}
	})
	if !errors.Is(err, want) {
		t.Fatalf("runCheckLoop error = %v, want %v", err, want)
	}
}

func TestRunCheckLoopReturnsNotReadyWithoutReportingFailure(t *testing.T) {
	t.Parallel()

	runtime := &checkLoopRuntime{}

	err := runCheckLoop(context.Background(), runtime, checkLoopConfig{
		interval:                time.Hour,
		runImmediately:          true,
		readyOnSuccess:          false,
		reportFailureOnNotReady: true,
	}, func(context.Context) checkLoopOutcome {
		return checkLoopOutcome{ready: false, state: opskit.StateNotReady}
	})
	if err == nil {
		t.Fatal("runCheckLoop error = nil, want not-ready error")
	}
	if runtime.reportFailure != nil {
		t.Fatalf("ReportFailure error = %v, want nil", runtime.reportFailure)
	}
}

func TestRunCheckLoopReportFailureOnNotReadyStopsLoop(t *testing.T) {
	t.Parallel()

	runtime := &checkLoopRuntime{}
	calls := 0

	err := runCheckLoop(context.Background(), runtime, checkLoopConfig{
		interval:                time.Millisecond,
		runImmediately:          true,
		readyOnSuccess:          true,
		reportFailureOnNotReady: true,
	}, func(context.Context) checkLoopOutcome {
		calls++
		return checkLoopOutcome{ready: false, state: opskit.StateNotReady, message: "dependency down"}
	})
	if err == nil {
		t.Fatal("runCheckLoop error = nil, want not-ready error")
	}
	if calls != 1 {
		t.Fatalf("check calls = %d, want 1", calls)
	}
	if runtime.reportFailure != nil {
		t.Fatalf("ReportFailure error = %v, want nil", runtime.reportFailure)
	}
}

func TestRunCheckLoopOnceRecoversPanics(t *testing.T) {
	t.Parallel()

	err := runCheckLoopOnce(context.Background(), &checkLoopRuntime{}, checkLoopConfig{}, func(context.Context) checkLoopOutcome {
		panic("secret panic payload")
	})
	if !errors.Is(err, ErrCheckLoopPanicked) {
		t.Fatalf("runCheckLoopOnce error = %v, want ErrCheckLoopPanicked", err)
	}
}

func TestRunCheckLoopOnceUsesConfiguredPanicContext(t *testing.T) {
	t.Parallel()

	err := runCheckLoopOnce(
		context.Background(),
		&checkLoopRuntime{},
		checkLoopConfig{panicErr: fmt.Errorf("opskit checker panicked: %w", ErrCheckLoopPanicked)},
		func(context.Context) checkLoopOutcome {
			panic("secret panic payload")
		},
	)
	if !errors.Is(err, ErrCheckLoopPanicked) {
		t.Fatalf("runCheckLoopOnce error = %v, want ErrCheckLoopPanicked", err)
	}
	if !strings.Contains(err.Error(), "opskit checker panicked") {
		t.Fatalf("runCheckLoopOnce error = %v, want checker panic context", err)
	}
	if strings.Contains(err.Error(), "secret panic payload") {
		t.Fatalf("runCheckLoopOnce error exposed panic payload: %v", err)
	}
}

func TestRunCheckLoopOnceRejectsReadyResultAfterTimeout(t *testing.T) {
	t.Parallel()

	runtime := &checkLoopRuntime{ready: true}
	err := runCheckLoopOnce(
		context.Background(),
		runtime,
		checkLoopConfig{timeout: time.Millisecond, readyOnSuccess: true},
		func(ctx context.Context) checkLoopOutcome {
			<-ctx.Done()
			return checkLoopOutcome{ready: true, state: opskit.StateReady}
		},
	)
	if err != nil {
		t.Fatalf("runCheckLoopOnce error = %v", err)
	}
	if runtime.ready {
		t.Fatal("ready = true, want false after timed-out check")
	}
	if runtime.reportFailure != nil {
		t.Fatalf("ReportFailure error = %v, want nil", runtime.reportFailure)
	}
}

func TestRunCheckLoopOnceReturnsTimeoutWithoutReportingFailure(t *testing.T) {
	t.Parallel()

	runtime := &checkLoopRuntime{ready: true}
	err := runCheckLoopOnce(
		context.Background(),
		runtime,
		checkLoopConfig{
			timeout:                 time.Millisecond,
			readyOnSuccess:          true,
			reportFailureOnNotReady: true,
		},
		func(ctx context.Context) checkLoopOutcome {
			<-ctx.Done()
			return checkLoopOutcome{ready: true, state: opskit.StateReady}
		},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runCheckLoopOnce error = %v, want context.DeadlineExceeded", err)
	}
	if runtime.ready {
		t.Fatal("ready = true, want false after timed-out check")
	}
	if runtime.reportFailure != nil {
		t.Fatalf("ReportFailure error = %v, want nil", runtime.reportFailure)
	}
}

func TestRunCheckLoopOnceObservesBoundedOutcomeAndContinuation(t *testing.T) {
	tests := []struct {
		name          string
		ctx           func() context.Context
		cfg           checkLoopConfig
		run           func(context.Context) checkLoopOutcome
		setReadyErr   error
		wantErr       bool
		wantIs        error
		wantOutcome   CheckOutcome
		wantContinues bool
	}{
		{
			name:          "ready",
			cfg:           checkLoopConfig{kind: CheckKindChecker, readyOnSuccess: true},
			run:           func(context.Context) checkLoopOutcome { return checkLoopOutcome{ready: true} },
			wantOutcome:   CheckOutcomeReady,
			wantContinues: true,
		},
		{
			name:          "not ready continues",
			cfg:           checkLoopConfig{kind: CheckKindChecker, readyOnSuccess: true},
			run:           func(context.Context) checkLoopOutcome { return checkLoopOutcome{ready: false} },
			wantOutcome:   CheckOutcomeNotReady,
			wantContinues: true,
		},
		{
			name:        "not ready terminates",
			cfg:         checkLoopConfig{kind: CheckKindChecker, reportFailureOnNotReady: true},
			run:         func(context.Context) checkLoopOutcome { return checkLoopOutcome{ready: false} },
			wantErr:     true,
			wantOutcome: CheckOutcomeNotReady,
		},
		{
			name: "timeout continues",
			cfg:  checkLoopConfig{kind: CheckKindChecker, timeout: time.Millisecond},
			run: func(ctx context.Context) checkLoopOutcome {
				<-ctx.Done()
				return checkLoopOutcome{ready: true}
			},
			wantOutcome:   CheckOutcomeTimeout,
			wantContinues: true,
		},
		{
			name: "timeout terminates",
			cfg: checkLoopConfig{
				kind:                    CheckKindChecker,
				timeout:                 time.Millisecond,
				reportFailureOnNotReady: true,
			},
			run: func(ctx context.Context) checkLoopOutcome {
				<-ctx.Done()
				return checkLoopOutcome{ready: true}
			},
			wantErr:     true,
			wantIs:      context.DeadlineExceeded,
			wantOutcome: CheckOutcomeTimeout,
		},
		{
			name: "canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			cfg:         checkLoopConfig{kind: CheckKindChecker},
			run:         func(context.Context) checkLoopOutcome { return checkLoopOutcome{ready: true} },
			wantErr:     true,
			wantIs:      context.Canceled,
			wantOutcome: CheckOutcomeCanceled,
		},
		{
			name:        "panic",
			cfg:         checkLoopConfig{kind: CheckKindChecker},
			run:         func(context.Context) checkLoopOutcome { panic("private panic") },
			wantErr:     true,
			wantIs:      ErrCheckLoopPanicked,
			wantOutcome: CheckOutcomePanic,
		},
		{
			name:          "runtime integration error",
			cfg:           checkLoopConfig{kind: CheckKindChecker, readyOnSuccess: true},
			run:           func(context.Context) checkLoopOutcome { return checkLoopOutcome{ready: true} },
			setReadyErr:   errors.New("set ready failed"),
			wantErr:       true,
			wantOutcome:   CheckOutcomeError,
			wantContinues: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.ctx != nil {
				ctx = tt.ctx()
			}
			runtime := &observedCheckLoopRuntime{}
			runtime.setReadyErr = tt.setReadyErr
			err := runCheckLoopOnce(ctx, runtime, tt.cfg, tt.run)
			if !tt.wantErr && err != nil {
				t.Fatalf("runCheckLoopOnce returned error: %v", err)
			}
			if tt.wantErr && err == nil {
				t.Fatal("runCheckLoopOnce error = nil, want error")
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Fatalf("runCheckLoopOnce error = %v, want %v", err, tt.wantIs)
			}
			if tt.setReadyErr != nil && !errors.Is(err, tt.setReadyErr) {
				t.Fatalf("runCheckLoopOnce error = %v, want %v", err, tt.setReadyErr)
			}
			if len(runtime.starts) != 1 || len(runtime.ends) != 1 {
				t.Fatalf("check observations = starts:%d ends:%d, want 1 each", len(runtime.starts), len(runtime.ends))
			}
			end := runtime.ends[0]
			if end.Outcome != tt.wantOutcome || end.LoopContinues != tt.wantContinues {
				t.Fatalf("CheckEndEvent = %#v, want outcome=%s continues=%t", end, tt.wantOutcome, tt.wantContinues)
			}
			if end.Kind != CheckKindChecker || end.StartedAt.IsZero() || end.EndedAt.Before(end.StartedAt) || end.Duration < 0 {
				t.Fatalf("CheckEndEvent timing or kind invalid: %#v", end)
			}
		})
	}
}

func TestCheckLoopImmediateTerminalFailurePublishesOnceDuringStart(t *testing.T) {
	tests := []struct {
		name   string
		worker func() Worker
	}{
		{
			name: "checker",
			worker: func() Worker {
				return NewCheckLoop(
					opskit.CheckFunc(func(context.Context) opskit.CheckResult {
						return opskit.NotReadyCheck("dependency down", 0)
					}),
					WithCheckInterval(time.Hour),
					WithCheckReportFailureOnNotReady(true),
				)
			},
		},
		{
			name: "check group",
			worker: func() Worker {
				return NewCheckGroupLoop(
					opskit.CheckGroupFunc(func(context.Context) opskit.CheckSummary {
						return opskit.SummarizeChecks("", time.Now(), []opskit.NamedCheck{
							{Name: "dependency", Result: opskit.NotReadyCheck("dependency down", 0)},
						})
					}),
					WithCheckInterval(time.Hour),
					WithCheckReportFailureOnNotReady(true),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observer := &checkLoopFailureObserver{}
			inner := tt.worker().(*LoopWorker)
			startReturned := make(chan struct{})
			releaseStart := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(releaseStart) }) })
			worker := &heldStartWorker{
				worker:        inner,
				startReturned: startReturned,
				release:       releaseStart,
			}
			rt, err := New(Identity{Name: "test-runtime"}, WithObserver(observer))
			if err != nil {
				t.Fatalf("New returned error: %v", err)
			}
			if err := rt.Register(WorkerSpec{Name: "checks", Worker: worker}); err != nil {
				t.Fatalf("Register returned error: %v", err)
			}

			startDone := make(chan error, 1)
			go func() { startDone <- rt.Start(context.Background(), "checks") }()
			select {
			case <-startReturned:
			case <-time.After(time.Second):
				t.Fatal("LoopWorker.Start did not return to wrapper")
			}
			waitForInternalLoopWorkerState(t, inner, loopStopped)
			waitForInternalRuntimeState(t, rt, StateStarting)
			if failures := observer.snapshot(); len(failures) != 1 {
				t.Fatalf("failure events during Start = %#v, want exactly one", failures)
			}

			releaseOnce.Do(func() { close(releaseStart) })
			if err := <-startDone; err != nil {
				t.Fatalf("Start returned error: %v", err)
			}
			waitForInternalRuntimeState(t, rt, StateFailed)
			if err := rt.Stop(context.Background(), "checks"); err != nil {
				t.Fatalf("Stop returned error: %v", err)
			}
		})
	}
}

func TestCheckLoopTerminalFailurePublishesOnceWhenStopBeginsDuringObservation(t *testing.T) {
	checkEntered := make(chan struct{})
	releaseCheck := make(chan struct{})
	failureEntered := make(chan struct{})
	releaseFailure := make(chan struct{})
	var releaseCheckOnce sync.Once
	var releaseFailureOnce sync.Once
	t.Cleanup(func() {
		releaseCheckOnce.Do(func() { close(releaseCheck) })
		releaseFailureOnce.Do(func() { close(releaseFailure) })
	})
	observer := &checkLoopFailureObserver{
		firstEntered: failureEntered,
		firstRelease: releaseFailure,
	}
	worker := NewCheckLoop(
		opskit.CheckFunc(func(context.Context) opskit.CheckResult {
			close(checkEntered)
			<-releaseCheck
			return opskit.NotReadyCheck("dependency down", 0)
		}),
		WithCheckInterval(time.Hour),
		WithCheckReportFailureOnNotReady(true),
	)
	rt, err := New(Identity{Name: "test-runtime"}, WithObserver(observer))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	select {
	case <-checkEntered:
	case <-time.After(time.Second):
		t.Fatal("check did not start")
	}

	releaseCheckOnce.Do(func() { close(releaseCheck) })
	select {
	case <-failureEntered:
	case <-time.After(time.Second):
		t.Fatal("failure observation did not start")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- rt.Stop(context.Background(), "checks") }()
	waitForInternalRuntimeState(t, rt, StateStopping)
	releaseFailureOnce.Do(func() { close(releaseFailure) })
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if failures := observer.snapshot(); len(failures) != 1 {
		t.Fatalf("failure events = %#v, want exactly one", failures)
	}
}

type heldStartWorker struct {
	worker        Worker
	startReturned chan<- struct{}
	release       <-chan struct{}
}

func (w *heldStartWorker) Start(ctx context.Context) error {
	err := w.worker.Start(ctx)
	close(w.startReturned)
	<-w.release
	return err
}

func (w *heldStartWorker) Stop(ctx context.Context) error {
	return w.worker.Stop(ctx)
}

type checkLoopFailureObserver struct {
	NopObserver

	mu           sync.Mutex
	failures     []FailureEvent
	firstEntered chan<- struct{}
	firstRelease <-chan struct{}
}

func (o *checkLoopFailureObserver) ObserveFailure(_ context.Context, event FailureEvent) {
	o.mu.Lock()
	o.failures = append(o.failures, event)
	first := len(o.failures) == 1
	o.mu.Unlock()

	if first && o.firstEntered != nil {
		close(o.firstEntered)
		<-o.firstRelease
	}
}

func (o *checkLoopFailureObserver) snapshot() []FailureEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]FailureEvent(nil), o.failures...)
}

func waitForInternalLoopWorkerState(t *testing.T, worker *LoopWorker, want loopWorkerState) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		worker.mu.Lock()
		state := worker.state
		worker.mu.Unlock()
		if state == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("LoopWorker state = %s, want %s", state, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func waitForInternalRuntimeState(t *testing.T, runtime *Runtime, want LifecycleState) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		snapshot, ok := runtime.Worker("checks")
		if !ok {
			t.Fatal("worker missing")
		}
		if snapshot.Status.State == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("worker state = %s, want %s", snapshot.Status.State, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

type checkLoopRuntime struct {
	setReadyErr   error
	ready         bool
	reportFailure error
}

type observedCheckLoopRuntime struct {
	checkLoopRuntime
	starts []CheckStartEvent
	ends   []CheckEndEvent
}

func (r *observedCheckLoopRuntime) startCheckObservation(ctx context.Context, kind CheckKind, startedAt time.Time) (context.Context, CheckObservation) {
	r.starts = append(r.starts, CheckStartEvent{Kind: kind, StartedAt: startedAt})
	return ctx, CheckObservationFunc(func(_ context.Context, event CheckEndEvent) {
		r.ends = append(r.ends, event)
	})
}

func (r *observedCheckLoopRuntime) endCheckObservation(ctx context.Context, observation CheckObservation, kind CheckKind, outcome CheckOutcome, loopContinues bool, startedAt time.Time) {
	endedAt := time.Now()
	observation.End(ctx, CheckEndEvent{
		Kind:          kind,
		Outcome:       outcome,
		LoopContinues: loopContinues,
		StartedAt:     startedAt,
		EndedAt:       endedAt,
		Duration:      endedAt.Sub(startedAt),
	})
}

func (r *checkLoopRuntime) Name() string {
	return "runtime/checks"
}

func (r *checkLoopRuntime) Status() WorkerStatus {
	return WorkerStatus{Name: r.Name(), State: StateRunning, Ready: r.ready}
}

func (r *checkLoopRuntime) SetReady(ready bool) error {
	if r.setReadyErr != nil {
		return r.setReadyErr
	}
	r.ready = ready
	return nil
}

func (r *checkLoopRuntime) SetAcceptingWork(bool) error {
	return nil
}

func (r *checkLoopRuntime) ReportFailure(err error) error {
	r.reportFailure = err
	return nil
}
