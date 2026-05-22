package workerkit_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit"
	"slices"
	"testing"
)

type panicObserver struct {
	panicTransition bool
	panicStart      bool
	panicEnd        bool
	panicFailure    bool
	panicReadiness  bool
}

func (o panicObserver) ObserveTransition(context.Context, TransitionEvent) {
	if o.panicTransition {
		panic("transition panic")
	}
}

func (o panicObserver) StartCommand(ctx context.Context, _ CommandStartEvent) (context.Context, CommandObservation) {
	if o.panicStart {
		panic("start panic")
	}
	return ctx, CommandObservationFunc(func(context.Context, CommandEndEvent) {
		if o.panicEnd {
			panic("end panic")
		}
	})
}

func (o panicObserver) ObserveFailure(context.Context, FailureEvent) {
	if o.panicFailure {
		panic("failure panic")
	}
}

func (o panicObserver) ObserveReadiness(context.Context, ReadinessEvent) {
	if o.panicReadiness {
		panic("readiness panic")
	}
}

type commandObservationRecorder struct {
	starts int
	ends   int
}

func (o *commandObservationRecorder) ObserveTransition(context.Context, TransitionEvent) {}

func (o *commandObservationRecorder) StartCommand(ctx context.Context, _ CommandStartEvent) (context.Context, CommandObservation) {
	o.starts++
	return ctx, CommandObservationFunc(func(context.Context, CommandEndEvent) {
		o.ends++
	})
}

func (o *commandObservationRecorder) ObserveFailure(context.Context, FailureEvent) {}

func (o *commandObservationRecorder) ObserveReadiness(context.Context, ReadinessEvent) {}

type observerRecorder struct {
	name         string
	events       *[]string
	derivedKey   any
	derivedValue any
	nilCtx       bool
	nilObs       bool

	transitions []TransitionEvent
	starts      []CommandStartEvent
	ends        []CommandEndEvent
	failures    []FailureEvent
	readiness   []ReadinessEvent
}

func (o *observerRecorder) ObserveTransition(_ context.Context, event TransitionEvent) {
	o.transitions = append(o.transitions, event)
	if o.events != nil {
		*o.events = append(*o.events, o.name+":transition")
	}
}

func (o *observerRecorder) StartCommand(ctx context.Context, event CommandStartEvent) (context.Context, CommandObservation) {
	o.starts = append(o.starts, event)
	if o.events != nil {
		*o.events = append(*o.events, o.name+":start")
	}
	if o.derivedKey != nil {
		ctx = context.WithValue(ctx, o.derivedKey, o.derivedValue)
	}
	var observedCtx context.Context = ctx
	if o.nilCtx {
		observedCtx = nil
	}
	if o.nilObs {
		return observedCtx, nil
	}
	return observedCtx, CommandObservationFunc(func(_ context.Context, end CommandEndEvent) {
		o.ends = append(o.ends, end)
		if o.events != nil {
			*o.events = append(*o.events, o.name+":end")
		}
	})
}

func (o *observerRecorder) ObserveFailure(_ context.Context, event FailureEvent) {
	o.failures = append(o.failures, event)
	if o.events != nil {
		*o.events = append(*o.events, o.name+":failure")
	}
}

func (o *observerRecorder) ObserveReadiness(_ context.Context, event ReadinessEvent) {
	o.readiness = append(o.readiness, event)
	if o.events != nil {
		*o.events = append(*o.events, o.name+":readiness")
	}
}

type contextKey string

func TestCommandObservationFunc(t *testing.T) {
	t.Parallel()

	var nilFunc CommandObservationFunc
	nilFunc.End(context.Background(), CommandEndEvent{})

	called := false
	want := CommandEndEvent{
		Runtime: "runtime",
		Worker:  "runtime/worker",
		Command: "command",
		Success: true,
	}
	fn := CommandObservationFunc(func(_ context.Context, event CommandEndEvent) {
		called = true
		if event != want {
			t.Fatalf("event = %#v, want %#v", event, want)
		}
	})
	fn.End(context.Background(), want)
	if !called {
		t.Fatal("CommandObservationFunc was not called")
	}
}

func TestNopObserver(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), contextKey("trace"), "value")
	observer := NopObserver{}

	gotCtx, observation := observer.StartCommand(ctx, CommandStartEvent{})
	if gotCtx != ctx {
		t.Fatal("NopObserver changed context")
	}
	if _, ok := observation.(NopCommandObservation); !ok {
		t.Fatalf("observation = %T, want NopCommandObservation", observation)
	}
	observer.ObserveTransition(ctx, TransitionEvent{})
	observer.ObserveFailure(ctx, FailureEvent{})
	observer.ObserveReadiness(ctx, ReadinessEvent{})
	observation.End(ctx, CommandEndEvent{})
}

func TestMultiObserverNoObserversReturnsNopObserver(t *testing.T) {
	t.Parallel()

	if _, ok := MultiObserver(nil, nil).(NopObserver); !ok {
		t.Fatalf("MultiObserver(nil, nil) = %T, want NopObserver", MultiObserver(nil, nil))
	}
}

func TestMultiObserverContinuesAfterObserverPanic(t *testing.T) {
	t.Parallel()

	first := &commandObservationRecorder{}
	second := &commandObservationRecorder{}
	observer := MultiObserver(
		first,
		panicObserver{panicStart: true},
		second,
	)

	_, observation := observer.StartCommand(context.Background(), CommandStartEvent{
		Runtime: "runtime",
		Worker:  "runtime/worker",
		Command: "command",
	})
	observation.End(context.Background(), CommandEndEvent{})

	if first.starts != 1 || second.starts != 1 {
		t.Fatalf("starts = first:%d second:%d, want 1 each", first.starts, second.starts)
	}
	if first.ends != 1 || second.ends != 1 {
		t.Fatalf("ends = first:%d second:%d, want 1 each", first.ends, second.ends)
	}
}

func TestMultiObserverFansOutEventsAndRecoversPanics(t *testing.T) {
	t.Parallel()

	first := &observerRecorder{name: "first"}
	second := &observerRecorder{name: "second"}
	observer := MultiObserver(
		first,
		panicObserver{
			panicTransition: true,
			panicFailure:    true,
			panicReadiness:  true,
		},
		second,
	)

	transition := TransitionEvent{Runtime: "runtime", Worker: "runtime/worker", From: StateRegistered, To: StateRunning}
	failureErr := errors.New("failed")
	failure := FailureEvent{Runtime: "runtime", Worker: "runtime/worker", Err: failureErr, Message: failureErr.Error()}
	readiness := ReadinessEvent{Runtime: "runtime", Worker: "runtime/worker", Ready: true}

	observer.ObserveTransition(context.Background(), transition)
	observer.ObserveFailure(context.Background(), failure)
	observer.ObserveReadiness(context.Background(), readiness)

	if len(first.transitions) != 1 || len(second.transitions) != 1 {
		t.Fatalf("transition fanout = first:%d second:%d, want 1 each", len(first.transitions), len(second.transitions))
	}
	if len(first.failures) != 1 || len(second.failures) != 1 {
		t.Fatalf("failure fanout = first:%d second:%d, want 1 each", len(first.failures), len(second.failures))
	}
	if len(first.readiness) != 1 || len(second.readiness) != 1 {
		t.Fatalf("readiness fanout = first:%d second:%d, want 1 each", len(first.readiness), len(second.readiness))
	}
}

func TestMultiObserverStartCommandPropagatesDerivedContextAndEndsReverseOrder(t *testing.T) {
	t.Parallel()

	var events []string
	firstKey := contextKey("first")
	secondKey := contextKey("second")
	first := &observerRecorder{name: "first", events: &events, derivedKey: firstKey, derivedValue: "first-value"}
	second := &observerRecorder{name: "second", events: &events, derivedKey: secondKey, derivedValue: "second-value"}

	ctx, observation := MultiObserver(first, second).StartCommand(context.Background(), CommandStartEvent{
		Runtime: "runtime",
		Worker:  "runtime/worker",
		Command: "command",
	})
	if ctx.Value(firstKey) != "first-value" {
		t.Fatalf("first context value = %#v, want first-value", ctx.Value(firstKey))
	}
	if ctx.Value(secondKey) != "second-value" {
		t.Fatalf("second context value = %#v, want second-value", ctx.Value(secondKey))
	}
	observation.End(ctx, CommandEndEvent{Runtime: "runtime", Worker: "runtime/worker", Command: "command"})

	want := []string{"first:start", "second:start", "second:end", "first:end"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestMultiObserverStartCommandHandlesNilContextAndObservation(t *testing.T) {
	t.Parallel()

	firstKey := contextKey("first")
	first := &observerRecorder{name: "first", derivedKey: firstKey, derivedValue: "first-value"}
	second := &observerRecorder{name: "second", nilCtx: true, nilObs: true}

	ctx, observation := MultiObserver(first, second).StartCommand(context.Background(), CommandStartEvent{})
	if ctx.Value(firstKey) != "first-value" {
		t.Fatalf("context value = %#v, want first-value", ctx.Value(firstKey))
	}
	observation.End(ctx, CommandEndEvent{})
	if len(first.ends) != 1 {
		t.Fatalf("first ends = %d, want 1", len(first.ends))
	}
	if len(second.ends) != 0 {
		t.Fatalf("second ends = %d, want 0", len(second.ends))
	}
}

func TestMultiObserverEndsSuccessfullyStartedCommandObservationsAfterPanic(t *testing.T) {
	t.Parallel()

	first := &commandObservationRecorder{}
	second := &commandObservationRecorder{}
	observer := MultiObserver(
		first,
		panicObserver{panicEnd: true},
		second,
	)

	_, observation := observer.StartCommand(context.Background(), CommandStartEvent{
		Runtime: "runtime",
		Worker:  "runtime/worker",
		Command: "command",
	})
	observation.End(context.Background(), CommandEndEvent{})

	if first.starts != 1 || second.starts != 1 {
		t.Fatalf("starts = first:%d second:%d, want 1 each", first.starts, second.starts)
	}
	if first.ends != 1 || second.ends != 1 {
		t.Fatalf("ends = first:%d second:%d, want 1 each", first.ends, second.ends)
	}
}

func TestSafeObserverNilReturnsNopObserver(t *testing.T) {
	t.Parallel()

	if _, ok := SafeObserver(nil).(NopObserver); !ok {
		t.Fatalf("SafeObserver(nil) = %T, want NopObserver", SafeObserver(nil))
	}
}

func TestSafeObserverRecoversCallbackPanics(t *testing.T) {
	t.Parallel()

	observer := SafeObserver(panicObserver{
		panicTransition: true,
		panicStart:      true,
		panicFailure:    true,
		panicReadiness:  true,
	})

	ctx := context.WithValue(context.Background(), contextKey("base"), "value")
	observer.ObserveTransition(ctx, TransitionEvent{})
	gotCtx, observation := observer.StartCommand(ctx, CommandStartEvent{})
	observer.ObserveFailure(ctx, FailureEvent{})
	observer.ObserveReadiness(ctx, ReadinessEvent{})
	if gotCtx != ctx {
		t.Fatal("SafeObserver changed context after StartCommand panic")
	}
	if _, ok := observation.(NopCommandObservation); !ok {
		t.Fatalf("observation = %T, want NopCommandObservation", observation)
	}
	observation.End(gotCtx, CommandEndEvent{})
}

func TestSafeObserverWrapsCommandObservationAndRecoversEndPanic(t *testing.T) {
	t.Parallel()

	observer := SafeObserver(panicObserver{panicEnd: true})

	ctx, observation := observer.StartCommand(context.Background(), CommandStartEvent{})
	if ctx == nil {
		t.Fatal("StartCommand returned nil context")
	}
	observation.End(ctx, CommandEndEvent{})
}

func TestSafeObserverStartCommandUsesFallbacksForNilReturns(t *testing.T) {
	t.Parallel()

	base := context.Background()
	recorder := &observerRecorder{nilCtx: true, nilObs: true}

	ctx, observation := SafeObserver(recorder).StartCommand(base, CommandStartEvent{})
	if ctx != base {
		t.Fatal("SafeObserver did not keep original context when child returned nil")
	}
	if _, ok := observation.(NopCommandObservation); !ok {
		t.Fatalf("observation = %T, want NopCommandObservation", observation)
	}
}
