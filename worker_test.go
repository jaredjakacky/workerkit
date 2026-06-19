package workerkit_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit"
	"testing"
)

func TestWorkerRuntimeFromContext(t *testing.T) {
	var nilCtx context.Context
	if got, ok := WorkerRuntimeFromContext(nilCtx); ok || got != nil {
		t.Fatalf("nil context returned runtime=%#v ok=%v, want nil false", got, ok)
	}
	if got, ok := WorkerRuntimeFromContext(context.Background()); ok || got != nil {
		t.Fatalf("background context returned runtime=%#v ok=%v, want nil false", got, ok)
	}

	var got WorkerRuntime
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(ctx context.Context) error {
				var ok bool
				got, ok = WorkerRuntimeFromContext(ctx)
				if !ok {
					t.Fatal("WorkerRuntimeFromContext ok = false, want true")
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
	if got == nil {
		t.Fatal("WorkerRuntimeFromContext returned nil")
	}
}

func TestWorkerRuntimeControlsWorkerState(t *testing.T) {
	var workerRuntime WorkerRuntime
	rt := newTestRuntime(t)
	err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(ctx context.Context) error {
				var ok bool
				workerRuntime, ok = WorkerRuntimeFromContext(ctx)
				if !ok {
					t.Fatal("missing WorkerRuntime")
				}
				if workerRuntime.Name() != "test-runtime/worker" {
					t.Fatalf("runtime name = %q, want test-runtime/worker", workerRuntime.Name())
				}
				status := workerRuntime.Status()
				if status.Name != "test-runtime/worker" {
					t.Fatalf("status name = %q, want test-runtime/worker", status.Name)
				}
				if status.LocalName != "worker" {
					t.Fatalf("status local name = %q, want worker", status.LocalName)
				}
				if status.State != StateStarting {
					t.Fatalf("status state = %s, want %s", status.State, StateStarting)
				}
				if err := workerRuntime.SetReady(false); err != nil {
					t.Fatalf("SetReady returned error: %v", err)
				}
				if err := workerRuntime.SetAcceptingWork(false); err != nil {
					t.Fatalf("SetAcceptingWork returned error: %v", err)
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.Status.Ready {
		t.Fatal("worker ready = true, want false")
	}
	if snapshot.Status.AcceptingWork {
		t.Fatal("worker accepting work = true, want false")
	}

	if err := workerRuntime.SetReady(true); err != nil {
		t.Fatalf("SetReady after start returned error: %v", err)
	}
	if err := workerRuntime.SetAcceptingWork(true); err != nil {
		t.Fatalf("SetAcceptingWork after start returned error: %v", err)
	}
	snapshot, ok = rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.Status.State != StateRunning {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateRunning)
	}
	if !snapshot.Status.Ready {
		t.Fatal("worker ready = false, want true after SetReady")
	}
	if !snapshot.Status.AcceptingWork {
		t.Fatal("worker accepting work = false, want true after SetAcceptingWork")
	}
}

func TestWorkerRuntimeSignalsWhileDraining(t *testing.T) {
	var workerRuntime WorkerRuntime
	rt := newTestRuntime(t)
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
	if err := rt.Drain(context.Background(), "worker"); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}

	if err := workerRuntime.SetReady(false); err != nil {
		t.Fatalf("SetReady(false) while draining returned error: %v", err)
	}
	if err := workerRuntime.SetAcceptingWork(false); err != nil {
		t.Fatalf("SetAcceptingWork(false) while draining returned error: %v", err)
	}
	if err := workerRuntime.SetReady(true); !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("SetReady(true) while draining error = %v, want ErrInvalidWorkerState", err)
	}
	if err := workerRuntime.SetAcceptingWork(true); !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("SetAcceptingWork(true) while draining error = %v, want ErrInvalidWorkerState", err)
	}

	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.Status.Ready {
		t.Fatal("worker ready = true, want false")
	}
	if snapshot.Status.AcceptingWork {
		t.Fatal("worker accepting work = true, want false")
	}
}

func TestWorkerRuntimeReportFailureNilIsIgnored(t *testing.T) {
	var workerRuntime WorkerRuntime
	rt := newTestRuntime(t)
	err := rt.Register(WorkerSpec{
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
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if err := workerRuntime.ReportFailure(nil); err != nil {
		t.Fatalf("ReportFailure(nil) returned error: %v", err)
	}

	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.Status.State != StateRunning {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateRunning)
	}
	if snapshot.Status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", snapshot.Status.LastFailure)
	}
}

func TestWorkerSnapshotIncludesRegistrationMetadata(t *testing.T) {
	rt := newTestRuntime(t)
	err := rt.Register(WorkerSpec{
		Name:        "worker",
		Description: "background projector",
		Worker:      testWorker{},
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.QualifiedName != "test-runtime/worker" {
		t.Fatalf("qualified name = %q, want test-runtime/worker", snapshot.QualifiedName)
	}
	if snapshot.Name != "worker" {
		t.Fatalf("name = %q, want worker", snapshot.Name)
	}
	if snapshot.Description != "background projector" {
		t.Fatalf("description = %q, want background projector", snapshot.Description)
	}
	if snapshot.Status.Name != "test-runtime/worker" {
		t.Fatalf("status name = %q, want test-runtime/worker", snapshot.Status.Name)
	}
	if snapshot.Status.LocalName != "worker" {
		t.Fatalf("status local name = %q, want worker", snapshot.Status.LocalName)
	}

	workers := rt.Workers()
	if len(workers) != 1 {
		t.Fatalf("workers length = %d, want 1", len(workers))
	}
	if workers[0].QualifiedName != snapshot.QualifiedName ||
		workers[0].Name != snapshot.Name ||
		workers[0].Description != snapshot.Description {
		t.Fatalf("Workers()[0] = %#v, want metadata from Worker()", workers[0])
	}
}

func TestWorkerStatusReturnsClonedFailureInfo(t *testing.T) {
	var workerRuntime WorkerRuntime
	rt := newTestRuntime(t)
	err := rt.Register(WorkerSpec{
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
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	failure := errors.New("background failed")
	if err := workerRuntime.ReportFailure(failure); err != nil {
		t.Fatalf("ReportFailure returned error: %v", err)
	}

	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.Status.LastFailure == nil {
		t.Fatal("LastFailure = nil, want failure")
	}
	snapshot.Status.LastFailure.Message = "mutated"

	fresh, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if fresh.Status.LastFailure == nil || fresh.Status.LastFailure.Message != failure.Error() {
		t.Fatalf("fresh LastFailure = %#v, want %q", fresh.Status.LastFailure, failure.Error())
	}
}

func TestStaleWorkerRuntimeCannotMutateRestartedWorker(t *testing.T) {
	var handles []WorkerRuntime
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(ctx context.Context) error {
				runtime, ok := WorkerRuntimeFromContext(ctx)
				if !ok {
					t.Fatal("missing WorkerRuntime")
				}
				handles = append(handles, runtime)
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("first Start returned error: %v", err)
	}
	if err := rt.Stop(context.Background(), "worker"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("second Start returned error: %v", err)
	}

	staleFailure := errors.New("stale worker failure")
	if err := handles[0].ReportFailure(staleFailure); !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("stale ReportFailure error = %v, want ErrInvalidWorkerState", err)
	}
	if err := handles[0].SetReady(false); !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("stale SetReady error = %v, want ErrInvalidWorkerState", err)
	}

	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateRunning {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateRunning)
	}
	if snapshot.Status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", snapshot.Status.LastFailure)
	}

	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})
}

func TestValidateWorkerLocalName(t *testing.T) {
	valid := []string{
		"worker",
		"worker-1",
		"worker_1",
	}
	for _, name := range valid {
		t.Run("valid "+name, func(t *testing.T) {
			if err := ValidateWorkerLocalName(name); err != nil {
				t.Fatalf("ValidateWorkerLocalName(%q) returned error: %v", name, err)
			}
		})
	}

	invalid := []string{
		"",
		"runtime/worker",
		"Worker",
		"worker.name",
		"worker name",
	}
	for _, name := range invalid {
		t.Run("invalid "+testName(name), func(t *testing.T) {
			if err := ValidateWorkerLocalName(name); err == nil {
				t.Fatalf("ValidateWorkerLocalName(%q) returned nil, want error", name)
			}
		})
	}
}

func TestValidateWorkerName(t *testing.T) {
	valid := []string{
		"worker",
		"worker-1",
		"runtime/worker",
		"runtime_1/worker-1",
	}
	for _, name := range valid {
		t.Run("valid "+name, func(t *testing.T) {
			if err := ValidateWorkerName(name); err != nil {
				t.Fatalf("ValidateWorkerName(%q) returned error: %v", name, err)
			}
		})
	}

	invalid := []string{
		"",
		"runtime/",
		"/worker",
		"runtime/worker/extra",
		"Runtime/worker",
		"runtime/Worker",
		"worker name",
	}
	for _, name := range invalid {
		t.Run("invalid "+testName(name), func(t *testing.T) {
			if err := ValidateWorkerName(name); err == nil {
				t.Fatalf("ValidateWorkerName(%q) returned nil, want error", name)
			}
		})
	}
}
