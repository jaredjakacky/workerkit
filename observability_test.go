package workerkit_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit"
	"slices"
	"sync"
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
	mu     sync.Mutex
	starts int
	ends   int
}

func (o *commandObservationRecorder) ObserveTransition(context.Context, TransitionEvent) {}

func (o *commandObservationRecorder) StartCommand(ctx context.Context, _ CommandStartEvent) (context.Context, CommandObservation) {
	o.mu.Lock()
	o.starts++
	o.mu.Unlock()
	return ctx, CommandObservationFunc(func(context.Context, CommandEndEvent) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.ends++
	})
}

func (o *commandObservationRecorder) ObserveFailure(context.Context, FailureEvent) {}

func (o *commandObservationRecorder) ObserveReadiness(context.Context, ReadinessEvent) {}

func (o *commandObservationRecorder) counts() (starts int, ends int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.starts, o.ends
}

type recordingEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *recordingEventLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *recordingEventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type observerRecorder struct {
	mu sync.Mutex

	name         string
	events       *recordingEventLog
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
	o.mu.Lock()
	o.transitions = append(o.transitions, event)
	o.mu.Unlock()
	if o.events != nil {
		o.events.add(o.name + ":transition")
	}
}

func (o *observerRecorder) StartCommand(ctx context.Context, event CommandStartEvent) (context.Context, CommandObservation) {
	o.mu.Lock()
	o.starts = append(o.starts, event)
	o.mu.Unlock()
	if o.events != nil {
		o.events.add(o.name + ":start")
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
		o.mu.Lock()
		o.ends = append(o.ends, end)
		o.mu.Unlock()
		if o.events != nil {
			o.events.add(o.name + ":end")
		}
	})
}

func (o *observerRecorder) ObserveFailure(_ context.Context, event FailureEvent) {
	o.mu.Lock()
	o.failures = append(o.failures, event)
	o.mu.Unlock()
	if o.events != nil {
		o.events.add(o.name + ":failure")
	}
}

func (o *observerRecorder) ObserveReadiness(_ context.Context, event ReadinessEvent) {
	o.mu.Lock()
	o.readiness = append(o.readiness, event)
	o.mu.Unlock()
	if o.events != nil {
		o.events.add(o.name + ":readiness")
	}
}

type observerRecorderSnapshot struct {
	transitions []TransitionEvent
	starts      []CommandStartEvent
	ends        []CommandEndEvent
	failures    []FailureEvent
	readiness   []ReadinessEvent
}

func (o *observerRecorder) snapshot() observerRecorderSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	return observerRecorderSnapshot{
		transitions: append([]TransitionEvent(nil), o.transitions...),
		starts:      append([]CommandStartEvent(nil), o.starts...),
		ends:        append([]CommandEndEvent(nil), o.ends...),
		failures:    append([]FailureEvent(nil), o.failures...),
		readiness:   append([]ReadinessEvent(nil), o.readiness...),
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

	firstStarts, firstEnds := first.counts()
	secondStarts, secondEnds := second.counts()
	if firstStarts != 1 || secondStarts != 1 {
		t.Fatalf("starts = first:%d second:%d, want 1 each", firstStarts, secondStarts)
	}
	if firstEnds != 1 || secondEnds != 1 {
		t.Fatalf("ends = first:%d second:%d, want 1 each", firstEnds, secondEnds)
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

	firstEvents := first.snapshot()
	secondEvents := second.snapshot()
	if len(firstEvents.transitions) != 1 || len(secondEvents.transitions) != 1 {
		t.Fatalf("transition fanout = first:%d second:%d, want 1 each", len(firstEvents.transitions), len(secondEvents.transitions))
	}
	if len(firstEvents.failures) != 1 || len(secondEvents.failures) != 1 {
		t.Fatalf("failure fanout = first:%d second:%d, want 1 each", len(firstEvents.failures), len(secondEvents.failures))
	}
	if len(firstEvents.readiness) != 1 || len(secondEvents.readiness) != 1 {
		t.Fatalf("readiness fanout = first:%d second:%d, want 1 each", len(firstEvents.readiness), len(secondEvents.readiness))
	}
}

func TestMultiObserverStartCommandPropagatesDerivedContextAndEndsReverseOrder(t *testing.T) {
	t.Parallel()

	events := &recordingEventLog{}
	firstKey := contextKey("first")
	secondKey := contextKey("second")
	first := &observerRecorder{name: "first", events: events, derivedKey: firstKey, derivedValue: "first-value"}
	second := &observerRecorder{name: "second", events: events, derivedKey: secondKey, derivedValue: "second-value"}

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
	gotEvents := events.snapshot()
	if !slices.Equal(gotEvents, want) {
		t.Fatalf("events = %#v, want %#v", gotEvents, want)
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
	firstEvents := first.snapshot()
	secondEvents := second.snapshot()
	if len(firstEvents.ends) != 1 {
		t.Fatalf("first ends = %d, want 1", len(firstEvents.ends))
	}
	if len(secondEvents.ends) != 0 {
		t.Fatalf("second ends = %d, want 0", len(secondEvents.ends))
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

	firstStarts, firstEnds := first.counts()
	secondStarts, secondEnds := second.counts()
	if firstStarts != 1 || secondStarts != 1 {
		t.Fatalf("starts = first:%d second:%d, want 1 each", firstStarts, secondStarts)
	}
	if firstEnds != 1 || secondEnds != 1 {
		t.Fatalf("ends = first:%d second:%d, want 1 each", firstEnds, secondEnds)
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

func TestMultiObserverSupportsConcurrentCallbacks(t *testing.T) {
	t.Parallel()

	const calls = 32
	first := &observerRecorder{name: "first"}
	second := &observerRecorder{name: "second"}
	observer := MultiObserver(first, second)

	var wg sync.WaitGroup
	for range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, observation := observer.StartCommand(context.Background(), CommandStartEvent{})
			observer.ObserveTransition(ctx, TransitionEvent{})
			observer.ObserveFailure(ctx, FailureEvent{})
			observer.ObserveReadiness(ctx, ReadinessEvent{})
			observation.End(ctx, CommandEndEvent{})
		}()
	}
	wg.Wait()

	for name, events := range map[string]observerRecorderSnapshot{
		"first":  first.snapshot(),
		"second": second.snapshot(),
	} {
		if len(events.starts) != calls || len(events.ends) != calls ||
			len(events.transitions) != calls || len(events.failures) != calls ||
			len(events.readiness) != calls {
			t.Fatalf("%s events = %#v, want %d of each callback", name, events, calls)
		}
	}
}
