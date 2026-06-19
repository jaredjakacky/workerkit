package testing_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"

	workerkit "github.com/jaredjakacky/workerkit"
)

func TestWorkerkitRuntimeDirectly(t *testing.T) {
	ctx := context.Background()
	observer := &recordingObserver{}

	// This example demonstrates that Workerkit's core runtime can be tested
	// directly in Go without an HTTP operations plane or external infrastructure.
	runtime, err := workerkit.New(
		workerkit.Identity{Name: "test_service"},
		workerkit.WithObserver(observer),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	worker := &fakeWorker{}
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "worker",
		Description: "Fake worker used by the test example.",
		Worker:      worker,
	}, workerkit.WithCommand("worker/echo", workerkit.CommandHandlerFunc(echoCommand))); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		t.Fatalf("StartAll returned error: %v", err)
	}

	status := runtime.RuntimeStatus()
	if status.State != workerkit.StateRunning {
		t.Fatalf("runtime state = %s, want %s", status.State, workerkit.StateRunning)
	}
	if !status.Ready {
		t.Fatal("runtime ready = false, want true")
	}

	snapshot, ok := runtime.Worker("worker")
	if !ok {
		t.Fatal("Worker returned ok=false")
	}
	if snapshot.Status.State != workerkit.StateRunning {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, workerkit.StateRunning)
	}
	if !snapshot.Status.Ready {
		t.Fatal("worker ready = false, want true")
	}

	result, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker:  "worker",
		Name:    "worker/echo",
		Payload: mustJSON(t, map[string]string{"message": "hello"}),
	})
	if err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if result.Message != "echo" {
		t.Fatalf("result message = %q, want echo", result.Message)
	}

	var payload map[string]string
	if err := json.Unmarshal(result.Payload, &payload); err != nil {
		t.Fatalf("result payload is not JSON: %v", err)
	}
	if payload["message"] != "hello" {
		t.Fatalf("result payload message = %q, want hello", payload["message"])
	}

	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	status = runtime.RuntimeStatus()
	if status.State != workerkit.StateStopped {
		t.Fatalf("runtime state after shutdown = %s, want %s", status.State, workerkit.StateStopped)
	}
	if status.Ready {
		t.Fatal("runtime ready after shutdown = true, want false")
	}

	events := observer.snapshot()
	if !slices.Contains(events.transitions, "test_service/worker:registered->starting") {
		t.Fatalf("observer transitions = %#v, want worker start transition", events.transitions)
	}
	if !slices.Contains(events.commands, "test_service/worker:worker/echo:true") {
		t.Fatalf("observer commands = %#v, want successful command observation", events.commands)
	}
	if !slices.Contains(events.readiness, "test_service/worker:true") {
		t.Fatalf("observer readiness = %#v, want worker ready event", events.readiness)
	}
}

type fakeWorker struct{}

func (w *fakeWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return fmt.Errorf("worker runtime handle unavailable")
	}
	return runtime.SetReady(true)
}

func (w *fakeWorker) Stop(ctx context.Context) error {
	return nil
}

func echoCommand(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	return workerkit.CommandResult{
		Message: "echo",
		Payload: req.Payload,
	}, nil
}

type recordingObserver struct {
	mu sync.Mutex

	transitions []string
	commands    []string
	readiness   []string
}

func (o *recordingObserver) ObserveTransition(ctx context.Context, event workerkit.TransitionEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.transitions = append(o.transitions, fmt.Sprintf("%s:%s->%s", event.Worker, event.From, event.To))
}

func (o *recordingObserver) StartCommand(ctx context.Context, event workerkit.CommandStartEvent) (context.Context, workerkit.CommandObservation) {
	return ctx, workerkit.CommandObservationFunc(func(ctx context.Context, end workerkit.CommandEndEvent) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.commands = append(o.commands, fmt.Sprintf("%s:%s:%t", end.Worker, end.Command, end.Success))
	})
}

func (o *recordingObserver) ObserveFailure(ctx context.Context, event workerkit.FailureEvent) {}

func (o *recordingObserver) ObserveReadiness(ctx context.Context, event workerkit.ReadinessEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.readiness = append(o.readiness, fmt.Sprintf("%s:%t", event.Worker, event.Ready))
}

type recordingObserverSnapshot struct {
	transitions []string
	commands    []string
	readiness   []string
}

func (o *recordingObserver) snapshot() recordingObserverSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	return recordingObserverSnapshot{
		transitions: append([]string(nil), o.transitions...),
		commands:    append([]string(nil), o.commands...),
		readiness:   append([]string(nil), o.readiness...),
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()

	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	return data
}
