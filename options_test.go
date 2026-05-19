package workerkit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	retrykit "github.com/jaredjakacky/workerkit/retry"
)

func TestDefaultRuntimeAndWorkerConfig(t *testing.T) {
	cfg := defaultRuntimeConfig()
	if cfg.commandConcurrency != 0 {
		t.Fatalf("commandConcurrency = %d, want 0", cfg.commandConcurrency)
	}
	if cfg.readinessPolicy != ReadyWhenContributingWorkersReady {
		t.Fatalf("readinessPolicy = %s, want %s", cfg.readinessPolicy, ReadyWhenContributingWorkersReady)
	}
	if _, ok := cfg.observer.(NopObserver); !ok {
		t.Fatalf("observer = %T, want NopObserver", cfg.observer)
	}

	worker := cfg.defaultWorker
	if worker.startTimeout != defaultStartTimeout {
		t.Fatalf("startTimeout = %s, want %s", worker.startTimeout, defaultStartTimeout)
	}
	if worker.stopTimeout != defaultStopTimeout {
		t.Fatalf("stopTimeout = %s, want %s", worker.stopTimeout, defaultStopTimeout)
	}
	if worker.commandTimeout != defaultCommandTimeout {
		t.Fatalf("commandTimeout = %s, want %s", worker.commandTimeout, defaultCommandTimeout)
	}
	if worker.commandConcurrency != defaultCommandConcurrency {
		t.Fatalf("worker commandConcurrency = %d, want %d", worker.commandConcurrency, defaultCommandConcurrency)
	}
	if worker.panicPolicy != PanicPolicyRecover {
		t.Fatalf("panicPolicy = %s, want %s", worker.panicPolicy, PanicPolicyRecover)
	}
	if worker.failurePolicy != FailurePolicyIsolate {
		t.Fatalf("failurePolicy = %s, want %s", worker.failurePolicy, FailurePolicyIsolate)
	}
	if !worker.readyOnStart {
		t.Fatal("readyOnStart = false, want true")
	}
	if !worker.acceptingWork {
		t.Fatal("acceptingWork = false, want true")
	}
	if !worker.contributesToReadiness {
		t.Fatal("contributesToReadiness = false, want true")
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("default config validate returned error: %v", err)
	}
}

func TestConfigValidationRejectsInvalidPoliciesAndNilRetries(t *testing.T) {
	cfg := defaultRuntimeConfig()
	cfg.readinessPolicy = ReadinessPolicy("bad")
	if err := cfg.validate(); err == nil {
		t.Fatal("runtimeConfig.validate invalid readiness returned nil, want error")
	}

	worker := defaultWorkerConfig()
	worker.panicPolicy = PanicPolicy("bad")
	if err := worker.validate(); err == nil {
		t.Fatal("workerConfig.validate invalid panic returned nil, want error")
	}

	worker = defaultWorkerConfig()
	worker.failurePolicy = FailurePolicy("bad")
	if err := worker.validate(); err == nil {
		t.Fatal("workerConfig.validate invalid failure returned nil, want error")
	}

	worker = defaultWorkerConfig()
	worker.startRetryPolicy = nil
	if err := worker.validate(); err == nil {
		t.Fatal("workerConfig.validate nil start retry returned nil, want error")
	}

	worker = defaultWorkerConfig()
	worker.commandRetryPolicy = nil
	if err := worker.validate(); err == nil {
		t.Fatal("workerConfig.validate nil command retry returned nil, want error")
	}
}

func TestRuntimeOptionDefaultsAreCopiedAtRegistration(t *testing.T) {
	rt := newTestRuntime(
		t,
		WithDefaultReadyOnStart(false),
		WithDefaultAcceptingWorkOnStart(false),
		WithDefaultWorkerCommandConcurrency(7),
		WithDefaultWorkerReadinessContribution(false),
	)
	if err := rt.Register(WorkerSpec{Name: "first", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}

	WithDefaultReadyOnStart(true)(&rt.config)
	WithDefaultAcceptingWorkOnStart(true)(&rt.config)
	WithDefaultWorkerCommandConcurrency(3)(&rt.config)
	WithDefaultWorkerReadinessContribution(true)(&rt.config)
	if err := rt.Register(WorkerSpec{Name: "second", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register second returned error: %v", err)
	}
	if err := rt.StartAll(context.Background()); err != nil {
		t.Fatalf("StartAll returned error: %v", err)
	}

	first, ok := rt.Worker("first")
	if !ok {
		t.Fatal("first worker missing")
	}
	if first.Status.Ready {
		t.Fatal("first ready = true, want copied false")
	}
	if first.Status.AcceptingWork {
		t.Fatal("first accepting work = true, want copied false")
	}
	if rt.workerConfigs[first.QualifiedName].commandConcurrency != 7 {
		t.Fatalf("first command concurrency = %d, want copied 7", rt.workerConfigs[first.QualifiedName].commandConcurrency)
	}
	if rt.workerConfigs[first.QualifiedName].contributesToReadiness {
		t.Fatal("first contributesToReadiness = true, want copied false")
	}

	second, ok := rt.Worker("second")
	if !ok {
		t.Fatal("second worker missing")
	}
	if !second.Status.Ready {
		t.Fatal("second ready = false, want copied true")
	}
	if !second.Status.AcceptingWork {
		t.Fatal("second accepting work = false, want copied true")
	}
	if rt.workerConfigs[second.QualifiedName].commandConcurrency != 3 {
		t.Fatalf("second command concurrency = %d, want copied 3", rt.workerConfigs[second.QualifiedName].commandConcurrency)
	}
	if !rt.workerConfigs[second.QualifiedName].contributesToReadiness {
		t.Fatal("second contributesToReadiness = false, want copied true")
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
		if rt.Status().Ready {
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
		if rt.Status().State != StateFailed {
			t.Fatalf("runtime state = %s, want %s", rt.Status().State, StateFailed)
		}
	})
}

func TestWithObserverNilAndSafeObserverWrapping(t *testing.T) {
	rt := newTestRuntime(t, WithObserver(nil))
	if _, ok := rt.config.observer.(NopObserver); !ok {
		t.Fatalf("observer = %T, want NopObserver", rt.config.observer)
	}

	observer := &panicObserver{panicStart: true}
	rt = newTestRuntime(t, WithObserver(observer))
	if _, ok := rt.config.observer.(safeObserver); !ok {
		t.Fatalf("observer = %T, want safeObserver", rt.config.observer)
	}
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
