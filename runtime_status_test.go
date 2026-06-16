package workerkit_test

import (
	"context"
	. "github.com/jaredjakacky/workerkit"
	"testing"
)

func TestRuntimeStatusFailurePoliciesWithActiveWorkers(t *testing.T) {
	t.Parallel()

	t.Run("mark runtime unready keeps active lifecycle running", func(t *testing.T) {
		var failingRuntime WorkerRuntime
		rt := newTestRuntime(t)
		if err := rt.Register(WorkerSpec{Name: "healthy", Worker: testWorker{}}); err != nil {
			t.Fatalf("Register healthy returned error: %v", err)
		}
		if err := rt.Register(
			WorkerSpec{
				Name: "failing",
				Worker: testWorker{
					start: func(ctx context.Context) error {
						var ok bool
						failingRuntime, ok = WorkerRuntimeFromContext(ctx)
						if !ok {
							t.Fatal("missing WorkerRuntime")
						}
						return nil
					},
				},
			},
			WithWorkerFailurePolicy(FailurePolicyMarkRuntimeUnready),
			WithWorkerReadinessContribution(false),
		); err != nil {
			t.Fatalf("Register failing returned error: %v", err)
		}
		if err := rt.StartAll(context.Background()); err != nil {
			t.Fatalf("StartAll returned error: %v", err)
		}
		if err := failingRuntime.ReportFailure(assertionError("background failed")); err != nil {
			t.Fatalf("ReportFailure returned error: %v", err)
		}

		status := rt.RuntimeStatus()
		if status.State != StateRunning {
			t.Fatalf("runtime state = %s, want %s", status.State, StateRunning)
		}
		if status.Ready {
			t.Fatal("runtime ready = true, want false")
		}
	})

	t.Run("fail runtime dominates active lifecycle", func(t *testing.T) {
		var failingRuntime WorkerRuntime
		rt := newTestRuntime(t)
		if err := rt.Register(WorkerSpec{Name: "healthy", Worker: testWorker{}}); err != nil {
			t.Fatalf("Register healthy returned error: %v", err)
		}
		if err := rt.Register(
			WorkerSpec{
				Name: "failing",
				Worker: testWorker{
					start: func(ctx context.Context) error {
						var ok bool
						failingRuntime, ok = WorkerRuntimeFromContext(ctx)
						if !ok {
							t.Fatal("missing WorkerRuntime")
						}
						return nil
					},
				},
			},
			WithWorkerFailurePolicy(FailurePolicyFailRuntime),
			WithWorkerReadinessContribution(false),
		); err != nil {
			t.Fatalf("Register failing returned error: %v", err)
		}
		if err := rt.StartAll(context.Background()); err != nil {
			t.Fatalf("StartAll returned error: %v", err)
		}
		if err := failingRuntime.ReportFailure(assertionError("background failed")); err != nil {
			t.Fatalf("ReportFailure returned error: %v", err)
		}

		status := rt.RuntimeStatus()
		if status.State != StateFailed {
			t.Fatalf("runtime state = %s, want %s", status.State, StateFailed)
		}
		if status.Ready {
			t.Fatal("runtime ready = true, want false")
		}
	})

	t.Run("non-contributing isolated failure does not block ready running worker", func(t *testing.T) {
		var failingRuntime WorkerRuntime
		rt := newTestRuntime(t)
		if err := rt.Register(WorkerSpec{Name: "healthy", Worker: testWorker{}}); err != nil {
			t.Fatalf("Register healthy returned error: %v", err)
		}
		if err := rt.Register(
			WorkerSpec{
				Name: "optional",
				Worker: testWorker{
					start: func(ctx context.Context) error {
						var ok bool
						failingRuntime, ok = WorkerRuntimeFromContext(ctx)
						if !ok {
							t.Fatal("missing WorkerRuntime")
						}
						return nil
					},
				},
			},
			WithWorkerFailurePolicy(FailurePolicyIsolate),
			WithWorkerReadinessContribution(false),
		); err != nil {
			t.Fatalf("Register optional returned error: %v", err)
		}
		if err := rt.StartAll(context.Background()); err != nil {
			t.Fatalf("StartAll returned error: %v", err)
		}
		if err := failingRuntime.ReportFailure(assertionError("background failed")); err != nil {
			t.Fatalf("ReportFailure returned error: %v", err)
		}

		status := rt.RuntimeStatus()
		if status.State != StateRunning {
			t.Fatalf("runtime state = %s, want %s", status.State, StateRunning)
		}
		if !status.Ready {
			t.Fatal("runtime ready = false, want true")
		}
	})
}

func TestRuntimeReadinessFallsBackWhenNoWorkersContribute(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "optional", Worker: testWorker{}},
		WithWorkerReadinessContribution(false),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if rt.RuntimeStatus().Ready {
		t.Fatal("registered runtime ready = true, want false")
	}
	if err := rt.Start(context.Background(), "optional"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if !rt.RuntimeStatus().Ready {
		t.Fatal("running runtime ready = false, want true")
	}
	if err := rt.Drain(context.Background(), "optional"); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	if rt.RuntimeStatus().Ready {
		t.Fatal("draining runtime ready = true, want false")
	}
}

type assertionError string

func (e assertionError) Error() string {
	return string(e)
}
