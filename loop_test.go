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
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != "worker operation failed" {
		t.Fatalf("LastFailure = %#v, want generic public failure", snapshot.Status.LastFailure)
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
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != "worker operation failed" {
		t.Fatalf("LastFailure = %#v, want generic public failure", snapshot.Status.LastFailure)
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
	if !errors.Is(event.Cause, loopErr) {
		t.Fatalf("failure event cause = %v, want %v", event.Cause, loopErr)
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
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != "worker operation failed" {
		t.Fatalf("LastFailure = %#v, want generic public failure", snapshot.Status.LastFailure)
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
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != "worker operation failed" {
		t.Fatalf("LastFailure = %#v, want generic public failure", snapshot.Status.LastFailure)
	}
}

func TestLoopWorkerFailureCleanupErrorBlocksRestartUntilStopRetrySucceeds(t *testing.T) {
	const privateCleanupDetail = "broker unsubscribe token=private"
	loopErr := errors.New("loop failed")
	cleanupErr := errors.New(privateCleanupDetail)
	loopRelease := make(chan struct{})
	started := make(chan int32, 2)
	cleanupAttempts := make(chan int32, 3)
	var starts atomic.Int32
	var stops atomic.Int32
	observer := &recordingObserver{}
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			n := starts.Add(1)
			started <- n
			if n == 1 {
				<-loopRelease
				return loopErr
			}
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			n := stops.Add(1)
			cleanupAttempts <- n
			if n == 1 {
				return cleanupErr
			}
			return nil
		}),
	)
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if got := readLoopAttempt(t, started); got != 1 {
		t.Fatalf("first loop generation = %d, want 1", got)
	}

	close(loopRelease)
	if got := readLoopAttempt(t, cleanupAttempts); got != 1 {
		t.Fatalf("automatic cleanup attempt = %d, want 1", got)
	}
	failed := waitForLoopState(t, rt, StateFailed)
	if failed.Status.LastFailure == nil || failed.Status.LastFailure.Code != FailureCodeWorkerFailed {
		t.Fatalf("LastFailure = %#v, want original loop failure", failed.Status.LastFailure)
	}
	event := waitForLoopFailureEvent(t, observer, cleanupErr)
	if event.Code != FailureCodeLoopCleanupFailed || event.Message != "loop worker cleanup failed" {
		t.Fatalf("cleanup FailureEvent = %#v, want safe cleanup presentation", event)
	}
	if strings.Contains(event.Message, privateCleanupDetail) {
		t.Fatalf("cleanup FailureEvent exposed private cause: %#v", event)
	}
	failures := observer.snapshot().failures
	if len(failures) < 2 || !errors.Is(failures[0].Cause, loopErr) || !errors.Is(failures[1].Cause, cleanupErr) {
		t.Fatalf("failure events = %#v, want loop failure followed by cleanup failure", failures)
	}

	if err := rt.Start(context.Background(), "loop"); !errors.Is(err, ErrLoopWorkerActive) {
		t.Fatalf("Start with pending cleanup error = %v, want ErrLoopWorkerActive", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("loop starts = %d, want 1 before cleanup succeeds", got)
	}
	preserved := waitForLoopState(t, rt, StateFailed)
	if preserved.Status.LastFailure == nil || preserved.Status.LastFailure.Code != FailureCodeWorkerFailed {
		t.Fatalf("LastFailure after rejected restart = %#v, want original loop failure", preserved.Status.LastFailure)
	}

	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("cleanup retry Stop returned error: %v", err)
	}
	if got := readLoopAttempt(t, cleanupAttempts); got != 2 {
		t.Fatalf("cleanup retry attempt = %d, want 2", got)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("restart after successful cleanup returned error: %v", err)
	}
	if got := readLoopAttempt(t, started); got != 2 {
		t.Fatalf("second loop generation = %d, want 2", got)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "loop")
	})
}

func TestLoopWorkerStopRetriesCleanupAfterHookTimeout(t *testing.T) {
	var attempts atomic.Int32
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopStop(func(ctx context.Context, _ WorkerRuntime) error {
			if attempts.Add(1) == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "loop", Worker: worker},
		WithWorkerStopTimeout(5*time.Millisecond),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if err := rt.Stop(context.Background(), "loop"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Stop error = %v, want DeadlineExceeded", err)
	}
	failed := waitForLoopState(t, rt, StateFailed)
	if failed.Status.LastFailure == nil || failed.Status.LastFailure.Code != FailureCodeLoopCleanupFailed {
		t.Fatalf("LastFailure = %#v, want safe cleanup failure", failed.Status.LastFailure)
	}
	if err := rt.Start(context.Background(), "loop"); !errors.Is(err, ErrLoopWorkerActive) {
		t.Fatalf("Start after timed-out cleanup error = %v, want ErrLoopWorkerActive", err)
	}
	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("cleanup retry Stop returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("cleanup attempts = %d, want 2", got)
	}
}

func TestLoopWorkerFailedStartCleanupErrorBlocksRestartUntilStopRetrySucceeds(t *testing.T) {
	startErr := errors.New("partial start failed")
	cleanupErr := errors.New("partial start cleanup failed")
	var starts atomic.Int32
	var stops atomic.Int32
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopStart(func(context.Context, WorkerRuntime) error {
			if starts.Add(1) == 1 {
				return startErr
			}
			return nil
		}),
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			if stops.Add(1) == 1 {
				return cleanupErr
			}
			return nil
		}),
	)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "loop", Worker: worker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); !errors.Is(err, startErr) {
		t.Fatalf("first Start error = %v, want %v", err, startErr)
	}
	if err := rt.Stop(context.Background(), "loop"); !errors.Is(err, cleanupErr) {
		t.Fatalf("first cleanup Stop error = %v, want %v", err, cleanupErr)
	}
	if err := rt.Start(context.Background(), "loop"); !errors.Is(err, ErrLoopWorkerActive) {
		t.Fatalf("Start with pending partial-start cleanup error = %v, want ErrLoopWorkerActive", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("start hook calls = %d, want 1 before cleanup succeeds", got)
	}
	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("cleanup retry Stop returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "loop"); err != nil {
		t.Fatalf("Start after successful cleanup returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "loop")
	})
}

func TestLoopWorkerStopJoinsFailedFailureCleanupBeforeRetrying(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	loopRelease := make(chan struct{})
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	retryEntered := make(chan struct{})
	var attempts atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	worker := NewLoopWorker(
		func(context.Context, WorkerRuntime) error {
			<-loopRelease
			return errors.New("loop failed")
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				max := maxActive.Load()
				if current <= max || maxActive.CompareAndSwap(max, current) {
					break
				}
			}
			switch attempts.Add(1) {
			case 1:
				close(firstEntered)
				<-firstRelease
				return cleanupErr
			case 2:
				close(retryEntered)
				return nil
			default:
				return errors.New("unexpected cleanup attempt")
			}
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
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("automatic cleanup did not start")
	}
	waitForLoopState(t, rt, StateFailed)

	stopDone := make(chan error, 1)
	go func() { stopDone <- rt.Stop(context.Background(), "loop") }()
	waitForLoopState(t, rt, StateStopping)
	close(firstRelease)
	select {
	case <-retryEntered:
	case <-time.After(time.Second):
		t.Fatal("Stop did not retry failed automatic cleanup")
	}
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("cleanup attempts = %d, want 2", got)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent cleanup attempts = %d, want 1", got)
	}
}

func TestLoopWorkerCleanupPanicLeavesCleanupPending(t *testing.T) {
	var attempts atomic.Int32
	worker := NewLoopWorker(
		func(ctx context.Context, _ WorkerRuntime) error {
			<-ctx.Done()
			return ctx.Err()
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			if attempts.Add(1) == 1 {
				panic("cleanup panic")
			}
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

	if err := rt.Stop(context.Background(), "loop"); err == nil {
		t.Fatal("Stop returned nil after cleanup panic")
	}
	if err := rt.Start(context.Background(), "loop"); !errors.Is(err, ErrLoopWorkerActive) {
		t.Fatalf("Start after cleanup panic error = %v, want ErrLoopWorkerActive", err)
	}
	if err := rt.Stop(context.Background(), "loop"); err != nil {
		t.Fatalf("cleanup retry Stop returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("cleanup attempts = %d, want 2", got)
	}
}

func TestLoopWorkerStopAllRetriesPendingFailureCleanup(t *testing.T) {
	loopRelease := make(chan struct{})
	cleanupAttempted := make(chan int32, 2)
	var attempts atomic.Int32
	worker := NewLoopWorker(
		func(context.Context, WorkerRuntime) error {
			<-loopRelease
			return errors.New("loop failed")
		},
		WithLoopStop(func(context.Context, WorkerRuntime) error {
			n := attempts.Add(1)
			cleanupAttempted <- n
			if n == 1 {
				return errors.New("cleanup failed")
			}
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
	if got := readLoopAttempt(t, cleanupAttempted); got != 1 {
		t.Fatalf("automatic cleanup attempt = %d, want 1", got)
	}
	waitForLoopState(t, rt, StateFailed)

	if err := rt.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll returned error: %v", err)
	}
	if got := readLoopAttempt(t, cleanupAttempted); got != 2 {
		t.Fatalf("StopAll cleanup attempt = %d, want 2", got)
	}
	waitForLoopState(t, rt, StateStopped)
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

func readLoopAttempt(t *testing.T, attempts <-chan int32) int32 {
	t.Helper()

	select {
	case attempt := <-attempts:
		return attempt
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loop attempt")
		return 0
	}
}

func waitForLoopFailureEvent(t *testing.T, observer *recordingObserver, cause error) FailureEvent {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		for _, event := range observer.snapshot().failures {
			if errors.Is(event.Cause, cause) {
				return event
			}
		}
		select {
		case <-deadline:
			t.Fatalf("failure event for %v was not observed", cause)
			return FailureEvent{}
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
