package workerkit_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opskit "github.com/jaredjakacky/opskit"
	. "github.com/jaredjakacky/workerkit"
)

func TestCheckLoopStartRejectsNilChecker(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(nil)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	err := rt.Start(context.Background(), "checks")
	if !errors.Is(err, ErrNilChecker) {
		t.Fatalf("Start error = %v, want ErrNilChecker", err)
	}
}

func TestCheckGroupLoopStartRejectsNilGroup(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckGroupLoop(nil)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	err := rt.Start(context.Background(), "checks")
	if !errors.Is(err, ErrNilCheckGroup) {
		t.Fatalf("Start error = %v, want ErrNilCheckGroup", err)
	}
}

func TestCheckLoopRunsImmediatelyAndMarksReady(t *testing.T) {
	calls := make(chan struct{}, 1)
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		calls <- struct{}{}
		return opskit.ReadyCheck("ready", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(checker, WithCheckInterval(time.Hour))}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	readCheckLoopCall(t, calls)
	waitForCheckWorkerReady(t, rt, true)
}

func TestCheckLoopCanDelayFirstRun(t *testing.T) {
	calls := make(chan struct{}, 1)
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		calls <- struct{}{}
		return opskit.ReadyCheck("ready", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(
		checker,
		WithCheckInitialDelay(40*time.Millisecond),
		WithCheckInterval(time.Hour),
	)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	select {
	case <-calls:
		t.Fatal("check ran before initial delay")
	case <-time.After(10 * time.Millisecond):
	}
	readCheckLoopCall(t, calls)
}

func TestCheckLoopCanWaitForIntervalBeforeFirstRun(t *testing.T) {
	calls := make(chan struct{}, 1)
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		calls <- struct{}{}
		return opskit.ReadyCheck("ready", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(
		checker,
		WithCheckRunImmediately(false),
		WithCheckInterval(30*time.Millisecond),
	)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	select {
	case <-calls:
		t.Fatal("check ran before first interval")
	case <-time.After(10 * time.Millisecond):
	}
	readCheckLoopCall(t, calls)
}

func TestCheckLoopRepeatsAtIntervalWithJitter(t *testing.T) {
	calls := make(chan struct{}, 2)
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		calls <- struct{}{}
		return opskit.ReadyCheck("ready", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(
		checker,
		WithCheckInterval(time.Hour),
		WithCheckJitter(func(time.Duration) time.Duration { return 10 * time.Millisecond }),
	)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	readCheckLoopCall(t, calls)
	readCheckLoopCall(t, calls)
}

func TestCheckLoopPassesTimeoutContext(t *testing.T) {
	done := make(chan error, 1)
	checker := opskit.CheckFunc(func(ctx context.Context) opskit.CheckResult {
		<-ctx.Done()
		done <- ctx.Err()
		return opskit.NotReadyCheck("timeout", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(
		checker,
		WithCheckInterval(time.Hour),
		WithCheckTimeout(10*time.Millisecond),
	)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("check context error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for check timeout")
	}
}

func TestCheckLoopStopCancelsInFlightCheck(t *testing.T) {
	started := make(chan struct{})
	done := make(chan error, 1)
	observer := &checkExecutionRecorder{
		starts: make(chan CheckStartEvent, 1),
		ends:   make(chan CheckEndEvent, 1),
	}
	checker := opskit.CheckFunc(func(ctx context.Context) opskit.CheckResult {
		close(started)
		<-ctx.Done()
		done <- ctx.Err()
		return opskit.NotReadyCheck("stopped", 0)
	})
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(checker, WithCheckInterval(time.Hour))}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for check start")
	}
	if err := rt.Stop(context.Background(), "checks"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("check context error = %v, want canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for check cancellation")
	}
	end := readCheckEndEvent(t, observer.ends)
	if end.Outcome != CheckOutcomeCanceled || end.LoopContinues {
		t.Fatalf("CheckEndEvent = %#v, want canceled terminal execution", end)
	}
}

func TestCheckLoopNotReadyMarksWorkerUnreadyWithoutFailureByDefault(t *testing.T) {
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		return opskit.NotReadyCheck("not ready", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(checker, WithCheckInterval(time.Hour))}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	waitForCheckWorkerReady(t, rt, false)
	snapshot, ok := rt.Worker("checks")
	if !ok {
		t.Fatal("worker missing")
	}
	if snapshot.Status.State != StateRunning {
		t.Fatalf("worker state = %s, want running", snapshot.Status.State)
	}
	if snapshot.Status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", snapshot.Status.LastFailure)
	}
}

func TestCheckLoopCanReportFailureOnNotReady(t *testing.T) {
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		return opskit.NotReadyCheck("not ready", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(
		checker,
		WithCheckInterval(time.Hour),
		WithCheckReportFailureOnNotReady(true),
	)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	waitForCheckWorkerState(t, rt, StateFailed)
}

func TestCheckLoopPanicMarksWorkerFailed(t *testing.T) {
	const secret = "secret checker panic payload"
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		panic(secret)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(checker, WithCheckInterval(time.Hour))}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	waitForCheckWorkerState(t, rt, StateFailed)
	assertCheckLoopPanicFailure(t, rt, "opskit checker panicked", secret)
}

func TestCheckGroupLoopPanicMarksWorkerFailed(t *testing.T) {
	const secret = "secret check group panic payload"
	group := opskit.CheckGroupFunc(func(context.Context) opskit.CheckSummary {
		panic(secret)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckGroupLoop(group, WithCheckInterval(time.Hour))}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	waitForCheckWorkerState(t, rt, StateFailed)
	assertCheckLoopPanicFailure(t, rt, "opskit check group panicked", secret)
}

func TestCheckLoopCanLeaveReadinessUnmanaged(t *testing.T) {
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		return opskit.ReadyCheck("ready", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(
		checker,
		WithCheckInterval(time.Hour),
		WithCheckReadyOnSuccess(false),
	)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	waitForCheckWorkerReady(t, rt, false)
}

func TestCheckLoopObserverReceivesResult(t *testing.T) {
	observed := make(chan opskit.CheckResult, 1)
	checker := opskit.CheckFunc(func(context.Context) opskit.CheckResult {
		return opskit.ReadyCheck("ready", 0)
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckLoop(
		checker,
		WithCheckInterval(time.Hour),
		WithCheckResultObserver(func(_ context.Context, result opskit.CheckResult) {
			observed <- result
		}),
	)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	select {
	case result := <-observed:
		if !result.Ready {
			t.Fatalf("observed result ready = false, want true: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observed result")
	}
}

func TestCheckGroupLoopExecutesGroupAndObservesSummary(t *testing.T) {
	var calls atomic.Int32
	observed := make(chan opskit.CheckSummary, 1)
	group := opskit.CheckGroupFunc(func(context.Context) opskit.CheckSummary {
		calls.Add(1)
		return opskit.SummarizeChecks("", time.Now(), []opskit.NamedCheck{
			{Name: "cache", Result: opskit.ReadyCheck("ready", 0)},
		})
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "checks", Worker: NewCheckGroupLoop(
		group,
		WithCheckInterval(time.Hour),
		WithCheckSummaryObserver(func(_ context.Context, summary opskit.CheckSummary) {
			observed <- summary
		}),
	)}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "checks"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "checks")
	})

	select {
	case summary := <-observed:
		if !summary.Ready {
			t.Fatalf("observed summary ready = false, want true: %+v", summary)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observed summary")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("CheckAll calls = %d, want 1", got)
	}
	waitForCheckWorkerReady(t, rt, true)
}

func TestCheckLoopExecutionObservationIncludesIdentityKindAndDerivedContext(t *testing.T) {
	tests := []struct {
		name   string
		kind   CheckKind
		worker func(chan<- bool, chan<- bool) Worker
	}{
		{
			name: "checker",
			kind: CheckKindChecker,
			worker: func(executionContext chan<- bool, resultContext chan<- bool) Worker {
				return NewCheckLoop(
					opskit.CheckFunc(func(ctx context.Context) opskit.CheckResult {
						executionContext <- ctx.Value(checkExecutionContextKey{}) == "observed"
						return opskit.ReadyCheck("ready", 0)
					}),
					WithCheckInterval(time.Hour),
					WithCheckResultObserver(func(ctx context.Context, _ opskit.CheckResult) {
						resultContext <- ctx.Value(checkExecutionContextKey{}) == "observed"
					}),
				)
			},
		},
		{
			name: "check group",
			kind: CheckKindGroup,
			worker: func(executionContext chan<- bool, resultContext chan<- bool) Worker {
				return NewCheckGroupLoop(
					opskit.CheckGroupFunc(func(ctx context.Context) opskit.CheckSummary {
						executionContext <- ctx.Value(checkExecutionContextKey{}) == "observed"
						return opskit.SummarizeChecks("", time.Now(), []opskit.NamedCheck{
							{Name: "dependency", Result: opskit.ReadyCheck("ready", 0)},
						})
					}),
					WithCheckInterval(time.Hour),
					WithCheckSummaryObserver(func(ctx context.Context, _ opskit.CheckSummary) {
						resultContext <- ctx.Value(checkExecutionContextKey{}) == "observed"
					}),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionContext := make(chan bool, 1)
			resultContext := make(chan bool, 1)
			observer := &checkExecutionRecorder{
				starts: make(chan CheckStartEvent, 1),
				ends:   make(chan CheckEndEvent, 1),
			}
			rt, err := New(Identity{Name: "test-runtime"}, WithObserver(observer))
			if err != nil {
				t.Fatalf("New returned error: %v", err)
			}
			if err := rt.Register(WorkerSpec{Name: "checks", Worker: tt.worker(executionContext, resultContext)}); err != nil {
				t.Fatalf("Register returned error: %v", err)
			}
			if err := rt.Start(context.Background(), "checks"); err != nil {
				t.Fatalf("Start returned error: %v", err)
			}
			t.Cleanup(func() { _ = rt.Stop(context.Background(), "checks") })

			start := readCheckStartEvent(t, observer.starts)
			end := readCheckEndEvent(t, observer.ends)
			if start.Runtime != "test-runtime" || start.Worker != "test-runtime/checks" || start.Kind != tt.kind {
				t.Fatalf("CheckStartEvent = %#v, want runtime and worker identity with kind %s", start, tt.kind)
			}
			if end.Runtime != start.Runtime || end.Worker != start.Worker || end.Kind != tt.kind || end.Outcome != CheckOutcomeReady || !end.LoopContinues {
				t.Fatalf("CheckEndEvent = %#v, want matching ready continuing execution", end)
			}
			if !readCheckContextObserved(t, executionContext) || !readCheckContextObserved(t, resultContext) {
				t.Fatal("derived check observation context did not reach execution and result callback")
			}
		})
	}
}

func TestCheckLoopExecutionObserverPanicsDoNotStopLoop(t *testing.T) {
	for _, tt := range []struct {
		name     string
		observer Observer
	}{
		{name: "start", observer: panicObserver{panicCheckStart: true}},
		{name: "end", observer: panicObserver{panicCheckEnd: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := make(chan struct{}, 2)
			worker := NewCheckLoop(
				opskit.CheckFunc(func(context.Context) opskit.CheckResult {
					calls <- struct{}{}
					return opskit.ReadyCheck("ready", 0)
				}),
				WithCheckInterval(time.Millisecond),
			)
			rt := newTestRuntime(t, WithObserver(tt.observer))
			if err := rt.Register(WorkerSpec{Name: "checks", Worker: worker}); err != nil {
				t.Fatalf("Register returned error: %v", err)
			}
			if err := rt.Start(context.Background(), "checks"); err != nil {
				t.Fatalf("Start returned error: %v", err)
			}
			t.Cleanup(func() { _ = rt.Stop(context.Background(), "checks") })

			readCheckLoopCall(t, calls)
			readCheckLoopCall(t, calls)
			snapshot, ok := rt.Worker("checks")
			if !ok || snapshot.Status.State != StateRunning {
				t.Fatalf("worker snapshot = %#v, want running after observer panic", snapshot)
			}
		})
	}
}

type checkExecutionContextKey struct{}

type checkExecutionRecorder struct {
	NopObserver
	starts chan CheckStartEvent
	ends   chan CheckEndEvent
}

func (o *checkExecutionRecorder) StartCheck(ctx context.Context, event CheckStartEvent) (context.Context, CheckObservation) {
	o.starts <- event
	ctx = context.WithValue(ctx, checkExecutionContextKey{}, "observed")
	return ctx, CheckObservationFunc(func(_ context.Context, event CheckEndEvent) {
		o.ends <- event
	})
}

func readCheckStartEvent(t *testing.T, events <-chan CheckStartEvent) CheckStartEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CheckStartEvent")
		return CheckStartEvent{}
	}
}

func readCheckEndEvent(t *testing.T, events <-chan CheckEndEvent) CheckEndEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CheckEndEvent")
		return CheckEndEvent{}
	}
}

func readCheckContextObserved(t *testing.T, observed <-chan bool) bool {
	t.Helper()
	select {
	case value := <-observed:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for derived check context")
		return false
	}
}

func readCheckLoopCall(t *testing.T, calls <-chan struct{}) {
	t.Helper()

	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for check call")
	}
}

func waitForCheckWorkerReady(t *testing.T, rt *Runtime, want bool) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		snapshot, ok := rt.Worker("checks")
		if !ok {
			t.Fatal("worker missing")
		}
		if snapshot.Status.Ready == want {
			return
		}

		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("worker ready = %t, want %t", snapshot.Status.Ready, want)
		}
	}
}

func waitForCheckWorkerState(t *testing.T, rt *Runtime, want LifecycleState) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		snapshot, ok := rt.Worker("checks")
		if !ok {
			t.Fatal("worker missing")
		}
		if snapshot.Status.State == want {
			return
		}

		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("worker state = %s, want %s", snapshot.Status.State, want)
		}
	}
}

func assertCheckLoopPanicFailure(t *testing.T, rt *Runtime, want string, secret string) {
	t.Helper()

	snapshot, ok := rt.Worker("checks")
	if !ok {
		t.Fatal("worker missing")
	}
	if snapshot.Status.LastFailure == nil {
		t.Fatal("LastFailure = nil, want panic failure")
	}
	message := snapshot.Status.LastFailure.Message
	if !strings.Contains(message, want) {
		t.Fatalf("LastFailure.Message = %q, want %q", message, want)
	}
	if !strings.Contains(message, ErrCheckLoopPanicked.Error()) {
		t.Fatalf("LastFailure.Message = %q, want %q", message, ErrCheckLoopPanicked)
	}
	if strings.Contains(message, secret) {
		t.Fatalf("LastFailure.Message exposed panic payload: %q", message)
	}
}
