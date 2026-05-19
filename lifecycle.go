package workerkit

// LifecycleState describes where a worker or runtime is in its managed
// lifecycle.
//
// Worker lifecycle state is owned by the runtime and changes through lifecycle
// operations such as start, drain, stop, and failure handling. Runtime lifecycle
// state is derived from the registered workers rather than set independently.
type LifecycleState string

// Lifecycle states are intentionally shared by workers and the runtime. Worker
// state records direct lifecycle transitions. Runtime state is an aggregate view
// derived from worker states.
const (
	// StateRegistered means the component is known to the runtime but has not
	// started.
	StateRegistered LifecycleState = "registered"

	// StateStarting means the runtime is transitioning the component into service.
	StateStarting LifecycleState = "starting"

	// StateRunning means the component is active.
	StateRunning LifecycleState = "running"

	// StateDraining means the component is alive but draining existing work.
	// For workers, this means refusing new Workerkit command dispatches. For
	// runtime aggregate status, this reports that at least one worker is draining.
	// It is not by itself a global command admission cutoff.
	StateDraining LifecycleState = "draining"

	// StateStopping means the component is actively shutting down.
	StateStopping LifecycleState = "stopping"

	// StateStopped means the component intentionally completed shutdown.
	StateStopped LifecycleState = "stopped"

	// StateFailed means normal execution cannot continue without intervention.
	// Failed workers are not ready and do not accept new work, but they may
	// still need Stop to release worker-owned resources.
	StateFailed LifecycleState = "failed"
)

func lifecycleAllowsWorkerRuntimeMutation(state LifecycleState) bool {
	switch state {
	case StateStarting, StateRunning, StateDraining:
		return true
	default:
		return false
	}
}

func lifecycleAllowsWorkerRuntimePositiveSignal(state LifecycleState) bool {
	switch state {
	case StateStarting, StateRunning:
		return true
	default:
		return false
	}
}
