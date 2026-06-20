package workerkit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	opskit "github.com/jaredjakacky/opskit"
)

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

func TestRunCheckLoopReturnsReportFailureError(t *testing.T) {
	t.Parallel()

	want := errors.New("report failure failed")
	runtime := &checkLoopRuntime{reportFailureErr: want}

	err := runCheckLoop(context.Background(), runtime, checkLoopConfig{
		interval:                time.Hour,
		runImmediately:          true,
		readyOnSuccess:          false,
		reportFailureOnNotReady: true,
	}, func(context.Context) checkLoopOutcome {
		return checkLoopOutcome{ready: false, state: opskit.StateNotReady}
	})
	if !errors.Is(err, want) {
		t.Fatalf("runCheckLoop error = %v, want %v", err, want)
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
	if runtime.reportFailure == nil {
		t.Fatal("ReportFailure was not called")
	}
	if !errors.Is(err, runtime.reportFailure) {
		t.Fatalf("runCheckLoop error = %v, want reported failure %v", err, runtime.reportFailure)
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

func TestRunCheckLoopOnceReportsTimeoutWhenConfigured(t *testing.T) {
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
	if !errors.Is(runtime.reportFailure, context.DeadlineExceeded) {
		t.Fatalf("ReportFailure error = %v, want context.DeadlineExceeded", runtime.reportFailure)
	}
}

type checkLoopRuntime struct {
	setReadyErr      error
	reportFailureErr error
	ready            bool
	reportFailure    error
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
	if r.reportFailureErr != nil {
		return r.reportFailureErr
	}
	r.reportFailure = err
	return nil
}
