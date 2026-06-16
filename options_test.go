package workerkit_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit"
	"strings"
	"testing"
	"time"

	retrykit "github.com/jaredjakacky/workerkit/retry"
)

func TestRuntimeOptionDefaultsApplyAtRegistration(t *testing.T) {
	defaultRuntime := newTestRuntime(t)
	if err := defaultRuntime.Register(WorkerSpec{Name: "default-worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register default worker returned error: %v", err)
	}
	if err := defaultRuntime.Start(context.Background(), "default-worker"); err != nil {
		t.Fatalf("Start default worker returned error: %v", err)
	}
	defaultSnapshot, ok := defaultRuntime.Worker("default-worker")
	if !ok {
		t.Fatal("default worker missing")
	}
	if !defaultSnapshot.Status.Ready {
		t.Fatal("default worker ready = false, want true")
	}
	if !defaultSnapshot.Status.AcceptingWork {
		t.Fatal("default worker accepting work = false, want true")
	}
	if !defaultRuntime.RuntimeStatus().Ready {
		t.Fatal("default runtime ready = false, want true")
	}

	rt := newTestRuntime(
		t,
		WithDefaultReadyOnStart(false),
		WithDefaultAcceptingWorkOnStart(false),
		WithDefaultWorkerReadinessContribution(false),
	)
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("worker missing")
	}
	if snapshot.Status.Ready {
		t.Fatal("worker ready = true, want default false")
	}
	if snapshot.Status.AcceptingWork {
		t.Fatal("worker accepting work = true, want default false")
	}
	if !rt.RuntimeStatus().Ready {
		t.Fatal("runtime ready = false, want fallback ready with no contributing workers")
	}
}

func TestInvalidPolicyOptionsAreRejected(t *testing.T) {
	if _, err := New(Identity{Name: "test-runtime"}, WithReadinessPolicy(ReadinessPolicy("bad"))); err == nil {
		t.Fatal("New invalid readiness policy returned nil, want error")
	}

	if _, err := New(Identity{Name: "test-runtime"}, WithDefaultPanicPolicy(PanicPolicy("bad"))); err == nil {
		t.Fatal("New invalid default panic policy returned nil, want error")
	}

	if _, err := New(Identity{Name: "test-runtime"}, WithDefaultFailurePolicy(FailurePolicy("bad"))); err == nil {
		t.Fatal("New invalid default failure policy returned nil, want error")
	}

	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithWorkerPanicPolicy(PanicPolicy("bad")),
	); err == nil {
		t.Fatal("Register invalid worker panic policy returned nil, want error")
	}

	rt = newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithWorkerFailurePolicy(FailurePolicy("bad")),
	); err == nil {
		t.Fatal("Register invalid worker failure policy returned nil, want error")
	}
}

func TestWorkerOptionsOverrideRuntimeDefaults(t *testing.T) {
	rt := newTestRuntime(
		t,
		WithDefaultReadyOnStart(false),
		WithDefaultAcceptingWorkOnStart(false),
	)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithWorkerReadyOnStart(true),
		WithWorkerAcceptingWorkOnStart(true),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if !snapshot.Status.Ready {
		t.Fatal("worker ready = false, want override true")
	}
	if !snapshot.Status.AcceptingWork {
		t.Fatal("worker accepting work = false, want override true")
	}
}

func TestTimeoutOptionsSetAttemptDeadlines(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		rt := newTestRuntime(t, WithDefaultStartTimeout(time.Millisecond))
		if err := rt.Register(WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				start: func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				},
			},
		}); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Start error = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("worker start override", func(t *testing.T) {
		rt := newTestRuntime(t, WithDefaultStartTimeout(0))
		if err := rt.Register(
			WorkerSpec{
				Name: "worker",
				Worker: testWorker{
					start: func(ctx context.Context) error {
						<-ctx.Done()
						return ctx.Err()
					},
				},
			},
			WithWorkerStartTimeout(time.Millisecond),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Start error = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("command", func(t *testing.T) {
		rt := newTestRuntime(t, WithDefaultCommandTimeout(time.Millisecond))
		if err := rt.Register(
			WorkerSpec{Name: "worker", Worker: testWorker{}},
			WithCommand("block", CommandHandlerFunc(func(ctx context.Context, _ CommandRequest) (CommandResult, error) {
				<-ctx.Done()
				return CommandResult{}, ctx.Err()
			})),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "block"})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Dispatch error = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("worker command override", func(t *testing.T) {
		rt := newTestRuntime(t, WithDefaultCommandTimeout(0))
		if err := rt.Register(
			WorkerSpec{Name: "worker", Worker: testWorker{}},
			WithWorkerCommandTimeout(time.Millisecond),
			WithCommand("block", CommandHandlerFunc(func(ctx context.Context, _ CommandRequest) (CommandResult, error) {
				<-ctx.Done()
				return CommandResult{}, ctx.Err()
			})),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "block"})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Dispatch error = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("stop", func(t *testing.T) {
		rt := newTestRuntime(t, WithDefaultStopTimeout(time.Millisecond))
		if err := rt.Register(WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				stop: func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				},
			},
		}); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if err := rt.Stop(context.Background(), "worker"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop error = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("worker stop override", func(t *testing.T) {
		rt := newTestRuntime(t, WithDefaultStopTimeout(0))
		if err := rt.Register(
			WorkerSpec{
				Name: "worker",
				Worker: testWorker{
					stop: func(ctx context.Context) error {
						<-ctx.Done()
						return ctx.Err()
					},
				},
			},
			WithWorkerStopTimeout(time.Millisecond),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if err := rt.Stop(context.Background(), "worker"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop error = %v, want DeadlineExceeded", err)
		}
	})
}

func TestRetryOptions(t *testing.T) {
	t.Run("default start retry", func(t *testing.T) {
		var attempts int
		rt := newTestRuntime(t, WithDefaultStartRetry(retrykit.Attempts(2, nil, nil)))
		if err := rt.Register(WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				start: func(context.Context) error {
					attempts++
					if attempts == 1 {
						return errors.New("try again")
					}
					return nil
				},
			},
		}); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if attempts != 2 {
			t.Fatalf("attempts = %d, want 2", attempts)
		}
	})

	t.Run("worker start retry override", func(t *testing.T) {
		var attempts int
		rt := newTestRuntime(t, WithDefaultStartRetry(nil))
		if err := rt.Register(
			WorkerSpec{
				Name: "worker",
				Worker: testWorker{
					start: func(context.Context) error {
						attempts++
						if attempts == 1 {
							return errors.New("try again")
						}
						return nil
					},
				},
			},
			WithWorkerStartRetry(retrykit.Attempts(2, nil, nil)),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if attempts != 2 {
			t.Fatalf("attempts = %d, want 2", attempts)
		}
	})

	t.Run("default command retry", func(t *testing.T) {
		var attempts int
		rt := newTestRuntime(t, WithDefaultCommandRetry(retrykit.Attempts(2, nil, nil)))
		if err := rt.Register(
			WorkerSpec{Name: "worker", Worker: testWorker{}},
			WithCommand("retry", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
				attempts++
				if attempts == 1 {
					return CommandResult{}, errors.New("try again")
				}
				return CommandResult{}, nil
			})),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "retry"}); err != nil {
			t.Fatalf("Dispatch returned error: %v", err)
		}
		if attempts != 2 {
			t.Fatalf("attempts = %d, want 2", attempts)
		}
	})

	t.Run("worker command retry override", func(t *testing.T) {
		var attempts int
		rt := newTestRuntime(t, WithDefaultCommandRetry(nil))
		if err := rt.Register(
			WorkerSpec{Name: "worker", Worker: testWorker{}},
			WithWorkerCommandRetry(retrykit.Attempts(2, nil, nil)),
			WithCommand("retry", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
				attempts++
				if attempts == 1 {
					return CommandResult{}, errors.New("try again")
				}
				return CommandResult{}, nil
			})),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "retry"}); err != nil {
			t.Fatalf("Dispatch returned error: %v", err)
		}
		if attempts != 2 {
			t.Fatalf("attempts = %d, want 2", attempts)
		}
	})
}

func TestPanicPolicyOptions(t *testing.T) {
	t.Run("default crash", func(t *testing.T) {
		rt := newTestRuntime(t, WithDefaultPanicPolicy(PanicPolicyCrash))
		if err := rt.Register(WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				start: func(context.Context) error {
					panic("boom")
				},
			},
		}); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("Start did not panic, want crash policy panic")
			}
		}()
		_ = rt.Start(context.Background(), "worker")
	})

	t.Run("worker recover override", func(t *testing.T) {
		rt := newTestRuntime(t, WithDefaultPanicPolicy(PanicPolicyCrash))
		if err := rt.Register(
			WorkerSpec{
				Name: "worker",
				Worker: testWorker{
					start: func(context.Context) error {
						panic("boom")
					},
				},
			},
			WithWorkerPanicPolicy(PanicPolicyRecover),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		err := rt.Start(context.Background(), "worker")
		if err == nil || !strings.Contains(err.Error(), "recovered panic") {
			t.Fatalf("Start error = %v, want recovered panic error", err)
		}
	})
}

func TestFailurePolicyOptions(t *testing.T) {
	t.Run("default failure policy", func(t *testing.T) {
		var workerRuntime WorkerRuntime
		rt := newTestRuntime(t, WithDefaultFailurePolicy(FailurePolicyMarkRuntimeUnready))
		if err := rt.Register(WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				start: func(ctx context.Context) error {
					var ok bool
					workerRuntime, ok = WorkerRuntimeFromContext(ctx)
					if !ok {
						t.Fatal("missing WorkerRuntime")
					}
					return nil
				},
			},
		}); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if err := workerRuntime.ReportFailure(errors.New("failed")); err != nil {
			t.Fatalf("ReportFailure returned error: %v", err)
		}
		if rt.RuntimeStatus().Ready {
			t.Fatal("runtime ready = true, want false")
		}
	})

	t.Run("worker failure policy override", func(t *testing.T) {
		var workerRuntime WorkerRuntime
		rt := newTestRuntime(t, WithDefaultFailurePolicy(FailurePolicyIsolate))
		if err := rt.Register(
			WorkerSpec{
				Name: "worker",
				Worker: testWorker{
					start: func(ctx context.Context) error {
						var ok bool
						workerRuntime, ok = WorkerRuntimeFromContext(ctx)
						if !ok {
							t.Fatal("missing WorkerRuntime")
						}
						return nil
					},
				},
			},
			WithWorkerFailurePolicy(FailurePolicyFailRuntime),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if err := workerRuntime.ReportFailure(errors.New("failed")); err != nil {
			t.Fatalf("ReportFailure returned error: %v", err)
		}
		if rt.RuntimeStatus().State != StateFailed {
			t.Fatalf("runtime state = %s, want %s", rt.RuntimeStatus().State, StateFailed)
		}
	})
}

func TestWithObserverNilAndSafeObserverWrapping(t *testing.T) {
	rt := newTestRuntime(t, WithObserver(nil))
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start with nil observer returned error: %v", err)
	}

	observer := &panicObserver{panicStart: true}
	rt = newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("work", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{Message: "ok"}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work"}); err != nil {
		t.Fatalf("Dispatch returned error despite observer panic: %v", err)
	}
}

func TestWithCommandSpecValidation(t *testing.T) {
	rt := newTestRuntime(t)
	err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("bad command", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{}, nil
		})),
	)
	if err == nil {
		t.Fatal("Register invalid command returned nil, want error")
	}

	rt = newTestRuntime(t)
	err = rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("duplicate", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{}, nil
		})),
		WithCommand("duplicate", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{}, nil
		})),
	)
	if !errors.Is(err, ErrCommandAlreadyRegistered) {
		t.Fatalf("Register duplicate command error = %v, want ErrCommandAlreadyRegistered", err)
	}

	rt = newTestRuntime(t)
	err = rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommandSpec(CommandSpec{Name: "nil-handler"}),
	)
	if err == nil {
		t.Fatal("Register nil command handler returned nil, want error")
	}
}
