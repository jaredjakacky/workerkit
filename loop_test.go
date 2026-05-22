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

type loopReadyFailRuntime struct {
	fakeWorkerRuntime
	readyFailure error
}

func (r loopReadyFailRuntime) SetReady(ready bool) error {
	if !ready {
		return r.readyFailure
	}
	return nil
}

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

	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("stop calls after stop = %d, want 1", got)
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

func TestLoopWorkerUnexpectedNilExitReportsFailureAndRunsStopHook(t *testing.T) {
	var stopCalls atomic.Int32
	release := make(chan struct{})
	worker := NewLoopWorker(
		func(context.Context, WorkerRuntime) error {
			<-release
			return nil
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
	close(release)

	snapshot := waitForLoopState(t, rt, StateFailed)
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != ErrLoopExitedUnexpectedly.Error() {
		t.Fatalf("LastFailure = %#v, want %q", snapshot.Status.LastFailure, ErrLoopExitedUnexpectedly.Error())
	}
	if got := stopCalls.Load(); got != 1 {
		t.Fatalf("stop calls = %d, want 1", got)
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
