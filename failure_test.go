package workerkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	opskit "github.com/jaredjakacky/opskit"
	. "github.com/jaredjakacky/workerkit"
)

func TestWithOperationalFailureKeepsCausePrivateAndDiscoverable(t *testing.T) {
	t.Parallel()

	cause := errors.New("postgres://user:pass@internal/database")
	err := WithOperationalFailure(cause, opskit.Failure{
		Code:    "database_unavailable",
		Message: "database unavailable",
	})

	if err.Error() != "database unavailable" {
		t.Fatalf("Error() = %q, want safe public message", err.Error())
	}
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is(%v, cause) = false", err)
	}
}

func TestWorkerFailureStatusDoesNotFormatArbitraryCause(t *testing.T) {
	t.Parallel()

	cause := panicOnWorkerErrorString{}
	rt, err := New(Identity{Name: "runtime"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: failureTestWorker{start: func(context.Context) error {
			return cause
		}},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	got := rt.Start(context.Background(), "worker")
	if _, ok := got.(panicOnWorkerErrorString); !ok {
		t.Fatalf("Start error type = %T, want original private cause", got)
	}
	snapshot, ok := rt.Worker("worker")
	if !ok || snapshot.Status.LastFailure == nil {
		t.Fatalf("Worker = %#v, want recorded failure", snapshot)
	}
	if snapshot.Status.LastFailure.Code != FailureCodeWorkerFailed || snapshot.Status.LastFailure.Message != "worker operation failed" {
		t.Fatalf("LastFailure = %#v, want generic safe failure", snapshot.Status.LastFailure)
	}
}

func TestWorkerFailureStatusUsesExplicitOperationalFailure(t *testing.T) {
	t.Parallel()

	cause := errors.New("secret database diagnostic")
	rt, err := New(Identity{Name: "runtime"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: failureTestWorker{start: func(context.Context) error {
			return WithOperationalFailure(cause, opskit.Failure{
				Code:    "database_unavailable",
				Message: "database unavailable",
			})
		}},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	got := rt.Start(context.Background(), "worker")
	if !errors.Is(got, cause) || got.Error() != "database unavailable" {
		t.Fatalf("Start error = %v, want safe wrapper retaining cause", got)
	}
	snapshot, _ := rt.Worker("worker")
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Code != "database_unavailable" || snapshot.Status.LastFailure.Message != "database unavailable" {
		t.Fatalf("LastFailure = %#v, want explicit operational failure", snapshot.Status.LastFailure)
	}
}

func TestWorkerFailureProjectionRecoversBrokenErrorChain(t *testing.T) {
	t.Parallel()

	rt, err := New(Identity{Name: "runtime"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: failureTestWorker{start: func(context.Context) error {
			return panickingUnwrapFailure{}
		}},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	if err := rt.Start(context.Background(), "worker"); err == nil {
		t.Fatal("Start returned nil, want original failure")
	}
	snapshot, _ := rt.Worker("worker")
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Code != FailureCodeWorkerFailed || snapshot.Status.LastFailure.Message != "worker operation failed" {
		t.Fatalf("LastFailure = %#v, want panic-safe generic failure", snapshot.Status.LastFailure)
	}
}

func TestWorkerFailureProjectionDoesNotRunErrorCodeUnderRuntimeLock(t *testing.T) {
	t.Parallel()

	rt, err := New(Identity{Name: "runtime"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: failureTestWorker{start: func(context.Context) error {
			return reentrantUnwrapFailure{runtime: rt}
		}},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- rt.Start(context.Background(), "worker") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start returned nil, want failure")
		}
	case <-time.After(time.Second):
		t.Fatal("Start deadlocked while projecting failure")
	}
}

func TestCommandFailureKeepsArbitraryCauseOutOfOperationalData(t *testing.T) {
	t.Parallel()

	const secret = "postgres://user:pass@internal/commands"
	cause := errors.New(secret)
	observer := &failureTestObserver{}
	rt, err := New(Identity{Name: "runtime"}, WithObserver(observer))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: failureTestWorker{}},
		WithCommand("fail", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{}, cause
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "fail"}); !errors.Is(err, cause) {
		t.Fatalf("Dispatch error = %v, want original cause", err)
	}
	snapshot, _ := rt.Worker("worker")
	if snapshot.Status.LastCommandFailure == nil || snapshot.Status.LastCommandFailure.Code != FailureCodeCommandFailed || snapshot.Status.LastCommandFailure.Message != "command failed" {
		t.Fatalf("LastCommandFailure = %#v, want generic safe failure", snapshot.Status.LastCommandFailure)
	}
	if len(observer.failures) != 1 || observer.failures[0].Code != FailureCodeCommandFailed || observer.failures[0].Message != "command failed" || !errors.Is(observer.failures[0].Cause, cause) {
		t.Fatalf("FailureEvent = %#v, want safe presentation and private cause", observer.failures)
	}
	if len(observer.ends) != 1 || observer.ends[0].Code != FailureCodeCommandFailed || observer.ends[0].Message != "command failed" || !errors.Is(observer.ends[0].Cause, cause) {
		t.Fatalf("CommandEndEvent = %#v, want safe presentation and private cause", observer.ends)
	}
	body, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("Marshal snapshot returned error: %v", err)
	}
	if strings.Contains(string(body), secret) {
		t.Fatalf("Worker snapshot exposed private cause: %s", body)
	}
}

func TestFailureEventJSONOmitsPrivateCause(t *testing.T) {
	t.Parallel()

	const secret = "postgres://user:pass@internal/database"
	body, err := json.Marshal(FailureEvent{
		Code:    FailureCodeWorkerFailed,
		Message: "worker operation failed",
		Cause:   errors.New(secret),
	})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if strings.Contains(string(body), secret) || strings.Contains(string(body), "Cause") {
		t.Fatalf("FailureEvent JSON exposed private cause: %s", body)
	}
}

type failureTestWorker struct {
	start func(context.Context) error
}

func (w failureTestWorker) Start(ctx context.Context) error {
	if w.start == nil {
		return nil
	}
	return w.start(ctx)
}

func (failureTestWorker) Stop(context.Context) error { return nil }

type failureTestObserver struct {
	failures []FailureEvent
	ends     []CommandEndEvent
}

func (*failureTestObserver) ObserveTransition(context.Context, TransitionEvent) {}

func (o *failureTestObserver) StartCommand(ctx context.Context, _ CommandStartEvent) (context.Context, CommandObservation) {
	return ctx, CommandObservationFunc(func(_ context.Context, event CommandEndEvent) {
		o.ends = append(o.ends, event)
	})
}

func (o *failureTestObserver) ObserveFailure(_ context.Context, event FailureEvent) {
	o.failures = append(o.failures, event)
}

func (*failureTestObserver) ObserveReadiness(context.Context, ReadinessEvent) {}

type panicOnWorkerErrorString struct{}

func (panicOnWorkerErrorString) Error() string {
	panic("arbitrary cause was formatted")
}

type panickingUnwrapFailure struct{}

func (panickingUnwrapFailure) Error() string { return "private failure" }

func (panickingUnwrapFailure) Unwrap() error {
	panic("private failure unwrap panicked")
}

type reentrantUnwrapFailure struct {
	runtime *Runtime
}

func (reentrantUnwrapFailure) Error() string { return "private failure" }

func (e reentrantUnwrapFailure) Unwrap() error {
	_ = e.runtime.RuntimeStatus()
	return nil
}
