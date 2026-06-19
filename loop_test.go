package workerkit_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoopWorkerStartRejectsNilLoop(t *testing.T) {
	worker := NewLoopWorker(nil)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	err := rt.Start(context.Background(), "loop")
	if err == nil || !strings.Contains(err.Error(), "loop worker loop must not be nil") {
		t.Fatalf("Start error = %v, want nil loop error", err)
	}
}

func TestLoopWorkerStartRequiresWorkerRuntime(t *testing.T) {
	worker := NewLoopWorker(func(context.Context, WorkerRuntime) error { return nil })
	err := worker.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "worker runtime handle unavailable") {
		t.Fatalf("Start error = %v, want missing runtime error", err)
	}
}

func TestLoopWorkerStopRequiresWorkerRuntimeWhenNeverStarted(t *testing.T) {
	worker := NewLoopWorker(func(context.Context, WorkerRuntime) error { return nil })
	err := worker.Stop(context.Background())
	if err == nil || !strings.Contains(err.Error(), "worker runtime handle unavailable") {
		t.Fatalf("Stop error = %v, want missing runtime error", err)
	}
}

func TestLoopWorkerStartReturnsErrorWhenStartHookReportsFailure(t *testing.T) {
	startFailure := errors.New("loop start hook failed")
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopStart(func(_ context.Context, runtime WorkerRuntime) error {
			if err := runtime.ReportFailure(startFailure); err != nil {
				t.Fatalf("ReportFailure returned error: %v", err)
			}
			return startFailure
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	err := rt.Start(context.Background(), "loop")
	if err == nil {
		t.Fatal("Start returned nil, want error")
	}
	if !errors.Is(err, startFailure) {
		t.Fatalf("Start error = %v, want %v", err, startFailure)
	}
}

func TestLoopWorkerStartHookRunsBeforeLoop(t *testing.T) {
	events := make(chan string, 2)
	worker := NewLoopWorker(
		func(ctx context.Context, runtime WorkerRuntime) error {
			events <- "loop"
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopStart(func(context.Context, WorkerRuntime) error {
			events <- "start"
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "loop")
	})

	got := []string{
		readLoopEvent(t, events),
		readLoopEvent(t, events),
	}
	if strings.Join(got, ",") != "start,loop" {
		t.Fatalf("events = %#v, want start then loop", got)
	}
}

func TestLoopWorkerFailedRestartCanRunStopHook(t *testing.T) {
	startErr := errors.New("restart failed")
	var starts atomic.Int32
	var stops atomic.Int32
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopStart(func(context.Context, WorkerRuntime) error {
			if starts.Add(1) == 2 {
				return startErr
			}
			return nil
		}),
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			stops.Add(1)
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("first Start returned error: %v", err)
	}
	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("first Stop returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); !errors.Is(err, startErr) {
		t.Fatalf("second Start error = %v, want %v", err, startErr)
	}
	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("second Stop returned error: %v", err)
	}
	if got := stops.Load(); got != 2 {
		t.Fatalf("stop hook calls = %d, want 2", got)
	}
}

func TestLoopWorkerAutoReadyCanBeDisabled(t *testing.T) {
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopAutoReady(false),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "loop")
	})

	snapshot, ok := rt.Worker("loop")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	status := snapshot.Status
	if status.Ready {
		t.Fatal("worker ready = true, want false")
	}
}

func TestLoopWorkerAutoReadyMarksWorkerReadyByDefault(t *testing.T) {
	worker := NewLoopWorker(func(ctx context.Context, _ WorkerRuntime) error {
		<-ctx.Done()
		return ctx.Err()
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "loop")
	})

	snapshot, ok := rt.Worker("loop")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if !snapshot.Status.Ready {
		t.Fatal("worker ready = false, want true")
	}
}

func TestLoopWorkerLoopCanMarkReadyWhenAutoReadyDisabled(t *testing.T) {
	readySet := make(chan struct{})
	worker := NewLoopWorker(
		func(ctx context.Context, runtime WorkerRuntime) error {
			if err := runtime.SetReady(true); err != nil {
				return err
			}
			close(readySet)
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopAutoReady(false),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "loop")
	})

	select {
	case <-readySet:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loop to mark ready")
	}

	snapshot, ok := rt.Worker("loop")
	if !ok {
		t.Fatal("Worker missing loop")
	}
	if !snapshot.Status.Ready {
		t.Fatal("worker ready = false, want true")
	}
}

func TestLoopWorkerStopCancelsLoopAndRunsStopHookOnce(t *testing.T) {
	loopDone := make(chan struct{})
	var stopCalls atomic.Int32
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			<-ctx.Done()
			close(loopDone)
			return ctx.Err()
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			stopCalls.Add(1)
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	select {
	case <-loopDone:
	default:
		t.Fatal("loop was not canceled")
	}
	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("stop calls = %d, want 1", got)
	}

	err := rt.Stop(context.Background(), "loop")
	if !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("second Stop error = %v, want ErrInvalidWorkerState", err)
	}
	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("stop calls after second stop = %d, want 1", got)
	}
}

func TestLoopWorkerStopReturnsContextErrorWhenLoopDoesNotExit(t *testing.T) {
	worker := NewLoopWorker(func(context.Context, WorkerRuntime) error {
		select {}
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := rt.Stop(ctx, "loop"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop error = %v, want DeadlineExceeded", err)
	}
}

func TestLoopWorkerStopTimeoutCannotCreateDuplicateLoop(t *testing.T) {
	firstRelease := make(chan struct{})
	started := make(chan int32, 2)
	var starts atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	var stopCalls atomic.Int32
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			n := starts.Add(1)
			current := active.Add(1)
			defer active.Add(-1)
			for {
				max := maxActive.Load()
				if current <= max || maxActive.CompareAndSwap(max, current) {
					break
				}
			}
			started <- n
			if n == 1 {
				<-firstRelease
				return ctx.Err()
			}
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			stopCalls.Add(1)
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if got := <-started; got != 1 {
		t.Fatalf("first loop number = %d, want 1", got)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := rt.Stop(stopCtx, "loop"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Stop error = %v, want DeadlineExceeded", err)
	}
	if err := rt.Start(context.Background(), "loop"); !errors.Is(err, ErrLoopWorkerActive) {
		t.Fatalf("Start while first loop active error = %v, want ErrLoopWorkerActive", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("loop starts = %d, want 1", got)
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- rt.Stop(context.Background(), "loop")
	}()
	select {
	case err := <-stopDone:
		t.Fatalf("second Stop returned before first loop exited: %v", err)
	case <-time.After(testNoSignalTimeout):
	}
	close(firstRelease)
	if err := <-stopDone; err != nil {
		t.Fatalf("second Stop returned error: %v", err)
	}
	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("stop hook calls = %d, want 1", got)
	}

	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("restart returned error: %v", err)
	}
	if got := <-started; got != 2 {
		t.Fatalf("second loop number = %d, want 2", got)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("maximum active loops = %d, want 1", got)
	}
	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("final Stop returned error: %v", err)
	}
}

func TestLoopWorkerRestartWaitsForCleanupAfterTimedOutStop(t *testing.T) {
	release := make(chan struct{})
	loopExited := make(chan struct{})
	cleanupDone := make(chan struct{})
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			<-release
			close(loopExited)
			return ctx.Err()
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			close(cleanupDone)
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := rt.Stop(stopCtx, "loop"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Stop error = %v, want DeadlineExceeded", err)
	}
	close(release)
	<-loopExited

	if err := rt.Start(context.Background(), "loop"); !errors.Is(err, ErrLoopWorkerActive) {
		t.Fatalf("Start before cleanup error = %v, want ErrLoopWorkerActive", err)
	}
	select {
	case <-cleanupDone:
		t.Fatal("cleanup ran without a successful Stop")
	default:
	}

	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("cleanup Stop returned error: %v", err)
	}
	select {
	case <-cleanupDone:
	default:
		t.Fatal("cleanup did not run")
	}
}

func TestLoopWorkerUnexpectedNilExitReportsFailureAndRunsStopHook(t *testing.T) {
	var stopCalls atomic.Int32
	release := make(chan struct{})
	cleanupDone := make(chan struct{})
	worker := NewLoopWorker(
		func(context.Context, WorkerRuntime) error {
			<-release
			return nil
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			stopCalls.Add(1)
			close(cleanupDone)
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	close(release)

	snapshot := waitForLoopState(t, rt, StateFailed)
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != ErrLoopExitedUnexpectedly.Error() {
		t.Fatalf("LastFailure = %#v, want %q", snapshot.Status.LastFailure, ErrLoopExitedUnexpectedly.Error())
	}
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("stop hook did not complete")
	}
	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("stop calls = %d, want 1", got)
	}
}

func TestLoopWorkerStopWaitsForFailureCleanup(t *testing.T) {
	loopRelease := make(chan struct{})
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	worker := NewLoopWorker(
		func(context.Context, WorkerRuntime) error {
			<-loopRelease
			return errors.New("loop failed")
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			close(cleanupEntered)
			<-cleanupRelease
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	close(loopRelease)
	<-cleanupEntered
	waitForLoopState(t, rt, StateFailed)

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- rt.Stop(context.Background(), "loop")
	}()
	waitForLoopState(t, rt, StateStopping)

	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before cleanup completed: %v", err)
	case <-time.After(testNoSignalTimeout):
	}
	startCtx, cancelStart := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelStart()
	if err := rt.Start(startCtx, "loop"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start during cleanup error = %v, want DeadlineExceeded", err)
	}

	close(cleanupRelease)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if snapshot := waitForLoopState(t, rt, StateStopped); snapshot.Status.State != StateStopped {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateStopped)
	}
}

func TestLoopWorkerReportsGenuineErrorRacingWithStop(t *testing.T) {
	loopErr := errors.New("loop failed while stopping")
	aboutToReturn := make(chan struct{})
	allowReturn := make(chan struct{})
	worker := NewLoopWorker(func(context.Context, WorkerRuntime) error {
		close(aboutToReturn)
		<-allowReturn
		return loopErr
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	<-aboutToReturn

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- rt.Stop(context.Background(), "loop")
	}()
	waitForLoopState(t, rt, StateStopping)
	close(allowReturn)

	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	snapshot := requireWorker(t, rt, "loop")
	if snapshot.Status.State != StateStopped {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateStopped)
	}
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != loopErr.Error() {
		t.Fatalf("LastFailure = %#v, want %q", snapshot.Status.LastFailure, loopErr.Error())
	}
}

func TestLoopWorkerPublishesFailureBeforeStopCompletes(t *testing.T) {
	loopErr := errors.New("loop failed")
	releaseLoop := make(chan struct{})
	releaseObserver := make(chan struct{})
	observer := &blockingFailureObserver{
		entered: make(chan FailureEvent, 1),
		release: releaseObserver,
	}
	worker := NewLoopWorker(func(context.Context, WorkerRuntime) error {
		<-releaseLoop
		return loopErr
	})
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	close(releaseLoop)

	event := <-observer.entered
	if !errors.Is(event.Err, loopErr) {
		t.Fatalf("failure event error = %v, want %v", event.Err, loopErr)
	}
	stopDone := make(chan error, 1)
	go func() {
		stopDone <- rt.Stop(context.Background(), "loop")
	}()
	waitForLoopState(t, rt, StateStopping)

	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before failure publication completed: %v", err)
	case <-time.After(testNoSignalTimeout):
	}
	close(releaseObserver)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}

	snapshot := requireWorker(t, rt, "loop")
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != loopErr.Error() {
		t.Fatalf("LastFailure = %#v, want %q", snapshot.Status.LastFailure, loopErr.Error())
	}
}

func TestLoopWorkerUnexpectedErrorExitReportsFailure(t *testing.T) {
	loopErr := errors.New("loop failed")
	release := make(chan struct{})
	worker := NewLoopWorker(func(context.Context, WorkerRuntime) error {
		<-release
		return loopErr
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	close(release)

	snapshot := waitForLoopState(t, rt, StateFailed)
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != loopErr.Error() {
		t.Fatalf("LastFailure = %#v, want %q", snapshot.Status.LastFailure, loopErr.Error())
	}
}

type blockingFailureObserver struct {
	entered chan FailureEvent
	release <-chan struct{}
}

func (*blockingFailureObserver) ObserveTransition(context.Context, TransitionEvent) {}

func (*blockingFailureObserver) StartCommand(ctx context.Context, _ CommandStartEvent) (context.Context, CommandObservation) {
	return ctx, NopCommandObservation{}
}

func (o *blockingFailureObserver) ObserveFailure(_ context.Context, event FailureEvent) {
	o.entered <- event
	<-o.release
}

func (*blockingFailureObserver) ObserveReadiness(context.Context, ReadinessEvent) {}

func TestLoopWorkerStartWhileRunningReturnsActiveError(t *testing.T) {
	worker := NewLoopWorker(func(ctx context.Context, _ WorkerRuntime) error {
		<-ctx.Done()
		return ctx.Err()
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "loop")
	})

	err := rt.Start(context.Background(), "loop")
	if !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("second Start error = %v, want ErrInvalidWorkerState", err)
	}
}

func TestLoopWorkerCanRestartAfterStop(t *testing.T) {
	started := make(chan struct{}, 2)
	worker := NewLoopWorker(func(ctx context.Context, _ WorkerRuntime) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("first Start returned error: %v", err)
	}
	<-started
	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("second Start returned error: %v", err)
	}
	<-started
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "loop")
	})
}

func waitForLoopState(t *testing.T, rt *Runtime, want LifecycleState) WorkerSnapshot {
	t.Helper()

	deadline := time.After(200 * time.Millisecond)
	for {
		snapshot, ok := rt.Worker("loop")
		if !ok {
			t.Fatal("Worker missing loop")
		}
		if snapshot.Status.State == want {
			return snapshot
		}
		select {
		case <-deadline:
			t.Fatalf("worker state did not become %s, last snapshot %#v", want, snapshot)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func readLoopEvent(t *testing.T, events <-chan string) string {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for loop event")
		return ""
	}
}
