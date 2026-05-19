package workerkit

import "time"

// LifecycleTransition describes the most recent lifecycle transition recorded
// in a status snapshot.
//
// LifecycleTransition is part of Workerkit's public JSON status contract. JSON
// field names and meanings are stable within a major version. Minor versions
// may add fields, so clients should ignore unknown fields.
type LifecycleTransition struct {
	// From is the lifecycle state before the transition.
	From LifecycleState `json:"from"`
	// To is the lifecycle state after the transition.
	To LifecycleState `json:"to"`
	// At is when the transition was observed.
	At time.Time `json:"at"`
}

// FailureInfo describes the most recent worker lifecycle or background failure
// recorded in a worker status snapshot.
//
// FailureInfo is part of Workerkit's public JSON status contract. JSON field
// names and meanings are stable within a major version. Minor versions may add
// fields, so clients should ignore unknown fields.
type FailureInfo struct {
	// Message is the failure message shown in status surfaces.
	Message string `json:"message"`
	// At is when the failure was recorded.
	At time.Time `json:"at"`
}

// CommandFailureInfo describes the most recent command handler returned error
// recorded in a worker status snapshot.
//
// Command failures are kept separate from worker failures because returned
// command errors do not automatically move a worker lifecycle to failed.
//
// CommandFailureInfo is part of Workerkit's public JSON status contract. JSON
// field names and meanings are stable within a major version. Minor versions
// may add fields, so clients should ignore unknown fields.
type CommandFailureInfo struct {
	// Command is the worker-owned command name that failed.
	Command string `json:"command"`
	// Message is the failure message shown in status surfaces.
	Message string `json:"message"`
	// At is when the failure was recorded.
	At time.Time `json:"at"`
}

// WorkerStatus is a point-in-time operational snapshot for one worker.
//
// Readiness, admission, and activity are separate on purpose. A worker can be
// running without being ready, ready without accepting command dispatches, and
// running without currently processing command work.
//
// WorkerStatus is part of Workerkit's public JSON status contract. JSON field
// names and meanings are stable within a major version. Minor versions may add
// fields, so clients should ignore unknown fields.
type WorkerStatus struct {
	// Name is the worker's fully qualified runtime/worker name.
	Name string `json:"name"`
	// LocalName is the worker's name inside the runtime boundary.
	LocalName string `json:"localName,omitempty"`
	// State is the worker's current lifecycle state.
	State LifecycleState `json:"state"`

	// Ready reports whether the worker currently considers itself ready.
	Ready bool `json:"ready"`
	// AcceptingWork reports whether the worker may accept command dispatches.
	AcceptingWork bool `json:"acceptingWork"`
	// InFlight is the number of commands currently running on the worker.
	InFlight int `json:"inFlight"`

	// LastTransition is the most recent lifecycle transition, if any.
	LastTransition *LifecycleTransition `json:"lastTransition,omitempty"`
	// LastFailure is the most recent worker lifecycle or background failure, if
	// any. Successful stop and drain cleanup preserve this evidence. A successful
	// start clears it.
	LastFailure *FailureInfo `json:"lastFailure,omitempty"`
	// LastCommandFailure is the most recent command handler returned error, if any.
	LastCommandFailure *CommandFailureInfo `json:"lastCommandFailure,omitempty"`
}

// RuntimeStatus is a point-in-time aggregate operational snapshot for the
// runtime.
//
// RuntimeStatus is part of Workerkit's public JSON status contract. JSON field
// names and meanings are stable within a major version. Minor versions may add
// fields, so clients should ignore unknown fields.
type RuntimeStatus struct {
	// Name is the runtime name.
	Name string `json:"name"`
	// State is the runtime's current aggregate lifecycle state.
	State LifecycleState `json:"state"`
	// Ready reports whether the runtime is ready according to its readiness policy.
	Ready bool `json:"ready"`
	// InFlight is the total number of commands currently running across the runtime.
	InFlight int `json:"inFlight"`
	// Workers is the number of workers registered in the runtime.
	Workers int `json:"workers"`

	// LastTransition is the most recent aggregate runtime lifecycle transition, if any.
	LastTransition *LifecycleTransition `json:"lastTransition,omitempty"`
}
