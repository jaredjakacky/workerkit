package workerkit_test

import (
	"context"
	"encoding/json"
	"errors"
	. "github.com/jaredjakacky/workerkit"
	"testing"
	"time"
)

func TestWorkerStatusJSONShape(t *testing.T) {
	status := WorkerStatus{
		Name:          "test-runtime/worker",
		State:         StateRegistered,
		Ready:         false,
		AcceptingWork: false,
		InFlight:      0,
	}

	data, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	assertJSONField(t, body, "name", "test-runtime/worker")
	assertJSONField(t, body, "state", string(StateRegistered))
	assertJSONField(t, body, "ready", false)
	assertJSONField(t, body, "acceptingWork", false)
	assertJSONField(t, body, "inFlight", float64(0))
	assertJSONOmitted(t, body, "localName")
	assertJSONOmitted(t, body, "lastTransition")
	assertJSONOmitted(t, body, "lastFailure")
	assertJSONOmitted(t, body, "lastCommandFailure")
}

func TestRuntimeStatusJSONShape(t *testing.T) {
	status := RuntimeStatus{
		Name:     "test-runtime",
		State:    StateRunning,
		Ready:    true,
		InFlight: 2,
		Workers:  3,
		LastTransition: &LifecycleTransition{
			From: StateRegistered,
			To:   StateRunning,
			At:   time.Unix(10, 0).UTC(),
		},
	}

	data, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	assertJSONField(t, body, "name", "test-runtime")
	assertJSONField(t, body, "state", string(StateRunning))
	assertJSONField(t, body, "ready", true)
	assertJSONField(t, body, "inFlight", float64(2))
	assertJSONField(t, body, "workers", float64(3))
	if _, ok := body["lastTransition"]; !ok {
		t.Fatal("lastTransition omitted, want present")
	}
}

func TestRuntimeStatusTracksRegisteredWorkerCount(t *testing.T) {
	rt := newTestRuntime(t)
	if got := rt.Status().Workers; got != 0 {
		t.Fatalf("initial workers = %d, want 0", got)
	}
	if err := rt.Register(WorkerSpec{Name: "first", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{Name: "second", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register second returned error: %v", err)
	}

	if got := rt.Status().Workers; got != 2 {
		t.Fatalf("workers = %d, want 2", got)
	}
}

func TestRuntimeStatusReturnsClonedLastTransition(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	status := rt.Status()
	if status.LastTransition == nil {
		t.Fatal("LastTransition = nil, want transition")
	}
	status.LastTransition.From = StateFailed

	fresh := rt.Status()
	if fresh.LastTransition == nil {
		t.Fatal("fresh LastTransition = nil, want transition")
	}
	if fresh.LastTransition.From == StateFailed {
		t.Fatalf("fresh LastTransition.From = %s, want unmutated value", fresh.LastTransition.From)
	}
}

func TestWorkerStatusReturnsClonedTransitionAndCommandFailure(t *testing.T) {
	commandErr := errors.New("command failed")
	rt := newTestRuntime(t)
	err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("fail", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{}, commandErr
		})),
	)
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "fail"}); !errors.Is(err, commandErr) {
		t.Fatalf("Dispatch error = %v, want %v", err, commandErr)
	}

	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.Status.LastTransition == nil {
		t.Fatal("LastTransition = nil, want transition")
	}
	if snapshot.Status.LastCommandFailure == nil {
		t.Fatal("LastCommandFailure = nil, want command failure")
	}
	snapshot.Status.LastTransition.To = StateFailed
	snapshot.Status.LastCommandFailure.Message = "mutated"

	fresh, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if fresh.Status.LastTransition == nil || fresh.Status.LastTransition.To != StateRunning {
		t.Fatalf("fresh LastTransition = %#v, want running transition", fresh.Status.LastTransition)
	}
	if fresh.Status.LastCommandFailure == nil || fresh.Status.LastCommandFailure.Message != commandErr.Error() {
		t.Fatalf("fresh LastCommandFailure = %#v, want %q", fresh.Status.LastCommandFailure, commandErr.Error())
	}
}

func TestStatusInFlightTracksRunningCommand(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	rt := newTestRuntime(t)
	err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("block", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			return CommandResult{Message: "ok"}, nil
		})),
	)
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "block"})
		done <- err
	}()
	<-entered

	runtimeStatus := rt.Status()
	if runtimeStatus.InFlight != 1 {
		t.Fatalf("runtime InFlight = %d, want 1", runtimeStatus.InFlight)
	}
	snapshot, ok := rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.Status.InFlight != 1 {
		t.Fatalf("worker InFlight = %d, want 1", snapshot.Status.InFlight)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if got := rt.Status().InFlight; got != 0 {
		t.Fatalf("runtime InFlight after dispatch = %d, want 0", got)
	}
	snapshot, ok = rt.Worker("worker")
	if !ok {
		t.Fatal("Worker missing worker")
	}
	if snapshot.Status.InFlight != 0 {
		t.Fatalf("worker InFlight after dispatch = %d, want 0", snapshot.Status.InFlight)
	}
}

func assertJSONField(t *testing.T, body map[string]any, key string, want any) {
	t.Helper()

	got, ok := body[key]
	if !ok {
		t.Fatalf("%s omitted, want %v", key, want)
	}
	if got != want {
		t.Fatalf("%s = %#v, want %#v", key, got, want)
	}
}

func assertJSONOmitted(t *testing.T, body map[string]any, key string) {
	t.Helper()

	if _, ok := body[key]; ok {
		t.Fatalf("%s present, want omitted", key)
	}
}
