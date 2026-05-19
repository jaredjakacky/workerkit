package workerkit

import (
	"context"
	"time"
)

// Observer receives runtime telemetry events emitted by Workerkit.
//
// The core runtime does not depend on a concrete telemetry backend. Adapters can
// turn these events into logs, metrics, traces, or test assertions without
// leaking backend-specific types into workers, commands, or status models.
//
// Observer implementations must be safe for concurrent use. Workerkit calls
// observers synchronously on runtime execution paths, so implementations should
// avoid blocking work. StartCommand may return a derived context that Workerkit
// passes to the command handler.
//
// The Observer method set is intended to stay stable within a major version.
// New telemetry details should generally be added as fields on existing event
// structs instead of as new Observer methods, because adding interface methods
// would break custom observer implementations.
type Observer interface {
	ObserveTransition(context.Context, TransitionEvent)
	StartCommand(context.Context, CommandStartEvent) (context.Context, CommandObservation)
	ObserveFailure(context.Context, FailureEvent)
	ObserveReadiness(context.Context, ReadinessEvent)
}

// CommandObservation receives the end of a command dispatch observation.
//
// The runtime calls End exactly once for each started command observation.
type CommandObservation interface {
	End(context.Context, CommandEndEvent)
}

// CommandObservationFunc adapts a function into a CommandObservation.
type CommandObservationFunc func(context.Context, CommandEndEvent)

// End implements CommandObservation.
func (f CommandObservationFunc) End(ctx context.Context, event CommandEndEvent) {
	if f == nil {
		return
	}
	f(ctx, event)
}

// NopCommandObservation discards the end of a command observation.
type NopCommandObservation struct{}

// End implements CommandObservation.
func (NopCommandObservation) End(context.Context, CommandEndEvent) {}

// NopObserver discards all telemetry callbacks.
type NopObserver struct{}

// ObserveTransition implements Observer.
func (NopObserver) ObserveTransition(context.Context, TransitionEvent) {}

// StartCommand implements Observer.
func (NopObserver) StartCommand(ctx context.Context, _ CommandStartEvent) (context.Context, CommandObservation) {
	return ctx, NopCommandObservation{}
}

// ObserveFailure implements Observer.
func (NopObserver) ObserveFailure(context.Context, FailureEvent) {}

// ObserveReadiness implements Observer.
func (NopObserver) ObserveReadiness(context.Context, ReadinessEvent) {}

// MultiObserver fans telemetry events out to multiple observers.
//
// Nil observers are ignored. Command observations end in reverse observer order
// to mirror stacked span or cleanup semantics. Panics from one child observer
// are recovered so later observers still receive the event. Command
// observations that were successfully started are always attempted during End.
func MultiObserver(observers ...Observer) Observer {
	kept := make([]Observer, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			kept = append(kept, observer)
		}
	}
	if len(kept) == 0 {
		return NopObserver{}
	}
	return multiObserver{observers: kept}
}

type multiObserver struct {
	observers []Observer
}

func (o multiObserver) ObserveTransition(ctx context.Context, event TransitionEvent) {
	for _, observer := range o.observers {
		observeTransitionSafely(observer, ctx, event)
	}
}

func (o multiObserver) StartCommand(ctx context.Context, event CommandStartEvent) (context.Context, CommandObservation) {
	observedCtx := ctx
	observations := make([]CommandObservation, 0, len(o.observers))
	for _, observer := range o.observers {
		nextCtx, observation := startCommandSafely(observer, observedCtx, event)
		if nextCtx != nil {
			observedCtx = nextCtx
		}
		if observation != nil {
			observations = append(observations, observation)
		}
	}
	if len(observations) == 0 {
		return observedCtx, NopCommandObservation{}
	}
	return observedCtx, multiCommandObservation{observations: observations}
}

func (o multiObserver) ObserveFailure(ctx context.Context, event FailureEvent) {
	for _, observer := range o.observers {
		observeFailureSafely(observer, ctx, event)
	}
}

func (o multiObserver) ObserveReadiness(ctx context.Context, event ReadinessEvent) {
	for _, observer := range o.observers {
		observeReadinessSafely(observer, ctx, event)
	}
}

type multiCommandObservation struct {
	observations []CommandObservation
}

func (o multiCommandObservation) End(ctx context.Context, event CommandEndEvent) {
	for i := len(o.observations) - 1; i >= 0; i-- {
		endCommandObservationSafely(o.observations[i], ctx, event)
	}
}

func observeTransitionSafely(observer Observer, ctx context.Context, event TransitionEvent) {
	defer recoverObserverPanic()
	observer.ObserveTransition(ctx, event)
}

func startCommandSafely(observer Observer, ctx context.Context, event CommandStartEvent) (observedCtx context.Context, observation CommandObservation) {
	observedCtx = ctx
	defer func() {
		if recover() != nil {
			observedCtx = ctx
			observation = nil
		}
	}()
	return observer.StartCommand(ctx, event)
}

func observeFailureSafely(observer Observer, ctx context.Context, event FailureEvent) {
	defer recoverObserverPanic()
	observer.ObserveFailure(ctx, event)
}

func observeReadinessSafely(observer Observer, ctx context.Context, event ReadinessEvent) {
	defer recoverObserverPanic()
	observer.ObserveReadiness(ctx, event)
}

func endCommandObservationSafely(observation CommandObservation, ctx context.Context, event CommandEndEvent) {
	if observation == nil {
		return
	}
	defer recoverObserverPanic()
	observation.End(ctx, event)
}

// SafeObserver wraps an observer so telemetry panics do not escape runtime
// lifecycle or command dispatch paths.
//
// If observer is nil, SafeObserver returns NopObserver{}.
func SafeObserver(observer Observer) Observer {
	if observer == nil {
		return NopObserver{}
	}
	return safeObserver{observer: observer}
}

type safeObserver struct {
	observer Observer
}

func (o safeObserver) ObserveTransition(ctx context.Context, event TransitionEvent) {
	defer recoverObserverPanic()
	o.observer.ObserveTransition(ctx, event)
}

func (o safeObserver) StartCommand(ctx context.Context, event CommandStartEvent) (observedCtx context.Context, observation CommandObservation) {
	observedCtx = ctx
	observation = NopCommandObservation{}
	defer func() {
		if recover() != nil {
			observedCtx = ctx
			observation = NopCommandObservation{}
		}
	}()

	nextCtx, nextObservation := o.observer.StartCommand(ctx, event)
	if nextCtx != nil {
		observedCtx = nextCtx
	}
	if nextObservation != nil {
		observation = safeCommandObservation{observation: nextObservation}
	}
	return observedCtx, observation
}

func (o safeObserver) ObserveFailure(ctx context.Context, event FailureEvent) {
	defer recoverObserverPanic()
	o.observer.ObserveFailure(ctx, event)
}

func (o safeObserver) ObserveReadiness(ctx context.Context, event ReadinessEvent) {
	defer recoverObserverPanic()
	o.observer.ObserveReadiness(ctx, event)
}

type safeCommandObservation struct {
	observation CommandObservation
}

func (o safeCommandObservation) End(ctx context.Context, event CommandEndEvent) {
	defer recoverObserverPanic()
	o.observation.End(ctx, event)
}

func recoverObserverPanic() {
	_ = recover()
}

// TransitionEvent reports one worker or runtime lifecycle transition.
//
// Worker is empty when the event describes the runtime itself.
type TransitionEvent struct {
	// Runtime is the runtime name that emitted the event.
	Runtime string
	// Worker is the fully qualified worker name, or empty for runtime-level
	// transitions.
	Worker string
	// From is the lifecycle state before the transition.
	From LifecycleState
	// To is the lifecycle state after the transition.
	To LifecycleState
	// At is when the transition was observed.
	At time.Time
}

// CommandStartEvent reports the start of one command dispatch.
//
// Observers may use this event to create command-scoped telemetry and return a
// derived context from Observer.StartCommand.
type CommandStartEvent struct {
	// Runtime is the runtime name that emitted the event.
	Runtime string
	// Worker is the fully qualified worker name targeted by the command.
	Worker string
	// Command is the registered command name.
	Command string
	// DispatchID correlates command start, retry attempt failures, and command
	// end for one dispatch. It is unique within the runtime process.
	DispatchID string
	// StartedAt is when dispatch handling began.
	StartedAt time.Time
}

// CommandEndEvent reports the end of one command dispatch.
//
// CommandEndEvent is emitted for normalized command attempts that reached
// command observation. It includes lookup, admission, execution, and panic
// failures that happen after the command request has been normalized.
type CommandEndEvent struct {
	// Runtime is the runtime name that emitted the event.
	Runtime string
	// Worker is the fully qualified worker name targeted by the command.
	Worker string
	// Command is the registered command name.
	Command string
	// DispatchID correlates command start, retry attempt failures, and command
	// end for one dispatch. It is unique within the runtime process.
	DispatchID string
	// Attempts is the number of command handler attempts actually executed.
	// Lookup, admission, and other pre-handler failures report 0.
	Attempts int
	// StartedAt is when dispatch handling began.
	StartedAt time.Time
	// EndedAt is when dispatch handling completed.
	EndedAt time.Time
	// Duration is EndedAt minus StartedAt.
	Duration time.Duration
	// Success reports whether dispatch completed without an error.
	Success bool
	// Err is the original command dispatch failure when dispatch failed.
	Err error
	// Message is a stable string representation of Err for simple observers.
	Message string
}

// FailureEvent reports one worker lifecycle, background, or command failure
// observed by the runtime.
//
// Command is empty for worker lifecycle failures.
// Background failures reported through WorkerRuntime.ReportFailure also have an
// empty Command.
// Lifecycle retry attempt failures update worker status but do not emit
// FailureEvent until the worker enters StateFailed. Command handler returned
// errors emit FailureEvent per failed attempt, including attempts that are later
// retried successfully.
// Command failures may also be reflected in CommandEndEvent so command observers
// can emit complete command spans or metrics. CommandEndEvent reports the final
// command dispatch outcome. Command handler returned errors do not automatically
// mean the worker lifecycle moved to failed.
// Panic reports whether the failure came from a panic recovered by one of the
// runtime's managed execution boundaries.
type FailureEvent struct {
	// Runtime is the runtime name that emitted the event.
	Runtime string
	// Worker is the fully qualified worker name associated with the failure.
	Worker string
	// Command is the command name associated with the failure, or empty for
	// worker lifecycle failures.
	Command string
	// DispatchID correlates command retry attempt failures with the command
	// dispatch that produced them. It is empty for non-command failures.
	DispatchID string
	// Attempt is the 1-based command handler attempt that failed. It is 0 when
	// the failure is not tied to a command handler attempt.
	Attempt int
	// At is when the failure was observed.
	At time.Time
	// Err is the original failure value when available.
	Err error
	// Message is a stable string representation of Err for simple observers.
	Message string
	// Panic reports whether the failure came from a recovered panic.
	Panic bool
}

// ReadinessEvent reports one worker or runtime readiness change.
//
// Worker is empty when the event describes the runtime itself.
type ReadinessEvent struct {
	// Runtime is the runtime name that emitted the event.
	Runtime string
	// Worker is the fully qualified worker name, or empty for runtime-level
	// readiness changes.
	Worker string
	// Ready is the readiness value after the change.
	Ready bool
	// At is when the readiness change was observed.
	At time.Time
}
