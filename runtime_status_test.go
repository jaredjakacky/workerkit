package workerkit

import (
	"context"
	"testing"
	"time"
)

func TestDeriveRuntimeLifecycleStatePrecedence(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(t)
	rt.status.State = StateStopped

	tests := []struct {
		name  string
		facts statusFacts
		want  LifecycleState
	}{
		{
			name:  "no workers",
			facts: statusFacts{},
			want:  StateRegistered,
		},
		{
			name: "runtime failure dominates active workers",
			facts: statusFacts{
				hasWorkers:                  true,
				hasRuntimeFailedWorker:      true,
				anyStarting:                 true,
				anyRunning:                  true,
				allContributingWorkersReady: true,
				allWorkersReady:             true,
			},
			want: StateFailed,
		},
		{
			name: "starting dominates draining",
			facts: statusFacts{
				hasWorkers:  true,
				anyStarting: true,
				anyDraining: true,
			},
			want: StateStarting,
		},
		{
			name: "draining dominates stopping",
			facts: statusFacts{
				hasWorkers:  true,
				anyDraining: true,
				anyStopping: true,
			},
			want: StateDraining,
		},
		{
			name: "stopping dominates running",
			facts: statusFacts{
				hasWorkers:  true,
				anyStopping: true,
				anyRunning:  true,
			},
			want: StateStopping,
		},
		{
			name: "running dominates isolated failure",
			facts: statusFacts{
				hasWorkers: true,
				anyRunning: true,
				anyFailed:  true,
			},
			want: StateRunning,
		},
		{
			name: "isolated failure dominates stopped",
			facts: statusFacts{
				hasWorkers: true,
				anyFailed:  true,
				allStopped: true,
			},
			want: StateFailed,
		},
		{
			name: "all stopped",
			facts: statusFacts{
				hasWorkers: true,
				allStopped: true,
			},
			want: StateStopped,
		},
		{
			name: "all registered",
			facts: statusFacts{
				hasWorkers:    true,
				allRegistered: true,
			},
			want: StateRegistered,
		},
		{
			name: "fallback keeps previous state",
			facts: statusFacts{
				hasWorkers: true,
			},
			want: StateStopped,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := rt.deriveRuntimeLifecycleState(tc.facts); got != tc.want {
				t.Fatalf("deriveRuntimeLifecycleState() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestDeriveRuntimeReadinessPolicies(t *testing.T) {
	t.Parallel()

	t.Run("all workers policy", func(t *testing.T) {
		rt := newTestRuntime(t, WithReadinessPolicy(ReadyWhenAllWorkersReady))
		readyFacts := statusFacts{
			hasWorkers:                  true,
			allWorkersReady:             true,
			allContributingWorkersReady: true,
		}
		if !rt.deriveRuntimeReadiness(readyFacts) {
			t.Fatal("deriveRuntimeReadiness ready facts = false, want true")
		}

		notReadyCases := []struct {
			name  string
			facts statusFacts
		}{
			{name: "no workers", facts: statusFacts{allWorkersReady: true}},
			{name: "starting", facts: statusFacts{hasWorkers: true, anyStarting: true, allWorkersReady: true}},
			{name: "draining", facts: statusFacts{hasWorkers: true, anyDraining: true, allWorkersReady: true}},
			{name: "stopping", facts: statusFacts{hasWorkers: true, anyStopping: true, allWorkersReady: true}},
			{name: "failed", facts: statusFacts{hasWorkers: true, anyFailed: true, allWorkersReady: true}},
			{name: "unready worker", facts: statusFacts{hasWorkers: true}},
		}
		for _, tc := range notReadyCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				if rt.deriveRuntimeReadiness(tc.facts) {
					t.Fatalf("deriveRuntimeReadiness(%s) = true, want false", tc.name)
				}
			})
		}
	})

	t.Run("contributing workers policy", func(t *testing.T) {
		rt := newTestRuntime(t, WithReadinessPolicy(ReadyWhenContributingWorkersReady))
		if !rt.deriveRuntimeReadiness(statusFacts{
			hasWorkers:                  true,
			readinessWorkers:            1,
			allContributingWorkersReady: true,
		}) {
			t.Fatal("contributing ready facts = false, want true")
		}
		if rt.deriveRuntimeReadiness(statusFacts{
			hasWorkers:                  true,
			readinessWorkers:            1,
			allContributingWorkersReady: false,
		}) {
			t.Fatal("contributing unready facts = true, want false")
		}
		if !rt.deriveRuntimeReadiness(statusFacts{
			hasWorkers: true,
			anyRunning: true,
		}) {
			t.Fatal("no contributing workers with running worker = false, want true")
		}
		if rt.deriveRuntimeReadiness(statusFacts{
			hasWorkers:  true,
			anyRunning:  true,
			anyDraining: true,
		}) {
			t.Fatal("no contributing workers while draining = true, want false")
		}
	})

	t.Run("failure policies force unready", func(t *testing.T) {
		rt := newTestRuntime(t)
		facts := statusFacts{
			hasWorkers:                  true,
			anyRunning:                  true,
			allWorkersReady:             true,
			allContributingWorkersReady: true,
			hasRuntimeUnreadyFailure:    true,
		}
		if rt.deriveRuntimeReadiness(facts) {
			t.Fatal("runtime unready failure readiness = true, want false")
		}
		facts.hasRuntimeUnreadyFailure = false
		facts.hasRuntimeFailedWorker = true
		if rt.deriveRuntimeReadiness(facts) {
			t.Fatal("runtime failed worker readiness = true, want false")
		}
	})
}

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

		status := rt.Status()
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

		status := rt.Status()
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

		status := rt.Status()
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
	if rt.Status().Ready {
		t.Fatal("registered runtime ready = true, want false")
	}
	if err := rt.Start(context.Background(), "optional"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if !rt.Status().Ready {
		t.Fatal("running runtime ready = false, want true")
	}
	if err := rt.Drain(context.Background(), "optional"); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	if rt.Status().Ready {
		t.Fatal("draining runtime ready = true, want false")
	}
}

func TestDeriveRuntimeLifecycleTransition(t *testing.T) {
	t.Parallel()

	last := &LifecycleTransition{
		From: StateRegistered,
		To:   StateRunning,
		At:   time.Unix(1, 0).UTC(),
	}
	same := deriveRuntimeLifecycleTransition(StateRunning, StateRunning, last, time.Unix(2, 0).UTC())
	if same == nil {
		t.Fatal("same-state transition = nil, want clone")
	}
	if same == last {
		t.Fatal("same-state transition returned original pointer, want clone")
	}
	if same.From != last.From || same.To != last.To || !same.At.Equal(last.At) {
		t.Fatalf("same-state transition = %#v, want clone of %#v", same, last)
	}

	now := time.Unix(3, 0).UTC()
	next := deriveRuntimeLifecycleTransition(StateRunning, StateStopped, last, now)
	if next == nil {
		t.Fatal("state-change transition = nil, want transition")
	}
	if next.From != StateRunning || next.To != StateStopped || !next.At.Equal(now) {
		t.Fatalf("state-change transition = %#v, want running -> stopped at %s", next, now)
	}
}

type assertionError string

func (e assertionError) Error() string {
	return string(e)
}
