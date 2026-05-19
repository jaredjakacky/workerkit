package workerkit

import "time"

// recomputeStatusLocked derives the runtime's aggregate status from the
// current worker records. It intentionally separates observation collection from
// lifecycle/readiness policy so the runtime's user-visible behavior is easy to
// review. The caller must hold r.mu.
func (r *Runtime) recomputeStatusLocked() {
	facts := r.statusFactsLocked()

	next := r.status
	next.Name = r.identity.Name
	next.State = r.deriveRuntimeLifecycleState(facts)
	next.Ready = r.deriveRuntimeReadiness(facts)
	next.Workers = len(r.workerStates)
	next.LastTransition = deriveRuntimeLifecycleTransition(
		r.status.State,
		next.State,
		r.status.LastTransition,
		time.Now(),
	)

	r.status = next
}

type statusFacts struct {
	hasWorkers bool

	anyStarting bool
	anyDraining bool
	anyStopping bool
	anyRunning  bool
	anyFailed   bool

	allRegistered bool
	allStopped    bool

	hasRuntimeFailedWorker   bool
	hasRuntimeUnreadyFailure bool

	readinessWorkers            int
	allContributingWorkersReady bool
	allWorkersReady             bool
}

// statusFactsLocked summarizes worker state without deciding runtime
// lifecycle or readiness policy. The caller must hold r.mu.
func (r *Runtime) statusFactsLocked() statusFacts {
	facts := statusFacts{
		hasWorkers:                  len(r.workerStates) > 0,
		allRegistered:               len(r.workerStates) > 0,
		allStopped:                  len(r.workerStates) > 0,
		allContributingWorkersReady: true,
		allWorkersReady:             true,
	}

	for name, state := range r.workerStates {
		// Active and failed phases affect aggregate runtime lifecycle directly.
		// Registered and stopped workers are handled by the allRegistered and
		// allStopped checks below.
		switch state.lifecycle {
		case StateStarting:
			facts.anyStarting = true
		case StateDraining:
			facts.anyDraining = true
		case StateStopping:
			facts.anyStopping = true
		case StateRunning:
			facts.anyRunning = true
		case StateFailed:
			facts.anyFailed = true
			// Failure policy decides whether a worker failure is isolated,
			// marks runtime readiness down, or fails the runtime.
			switch r.workerConfigs[name].failurePolicy {
			case FailurePolicyFailRuntime:
				facts.hasRuntimeFailedWorker = true
			case FailurePolicyMarkRuntimeUnready:
				facts.hasRuntimeUnreadyFailure = true
			}
		}

		if state.lifecycle != StateRegistered {
			facts.allRegistered = false
		}
		if state.lifecycle != StateStopped {
			facts.allStopped = false
		}
		if state.lifecycle != StateRunning || !state.ready {
			facts.allWorkersReady = false
		}

		if r.workerConfigs[name].contributesToReadiness {
			facts.readinessWorkers++
			if state.lifecycle != StateRunning || !state.ready {
				facts.allContributingWorkersReady = false
			}
		}
	}

	return facts
}

// deriveRuntimeLifecycleState applies workerkit's aggregate lifecycle precedence.
// FailurePolicyFailRuntime is the only per-worker failure mode that immediately
// dominates active lifecycle states.
func (r *Runtime) deriveRuntimeLifecycleState(facts statusFacts) LifecycleState {
	switch {
	case !facts.hasWorkers:
		return StateRegistered
	case facts.hasRuntimeFailedWorker:
		return StateFailed
	// Transitional states dominate normal running status so operators can see
	// lifecycle work in progress at the runtime level.
	case facts.anyStarting:
		return StateStarting
	case facts.anyDraining:
		return StateDraining
	case facts.anyStopping:
		return StateStopping
	case facts.anyRunning:
		return StateRunning
	// Isolated failures only dominate when no active state above is present.
	case facts.anyFailed:
		return StateFailed
	case facts.allStopped:
		return StateStopped
	case facts.allRegistered:
		return StateRegistered
	default:
		return r.status.State
	}
}

// deriveRuntimeReadiness applies the configured readiness policy to the current
// aggregate worker facts. Unknown policies fail closed.
func (r *Runtime) deriveRuntimeReadiness(facts statusFacts) bool {
	if facts.hasRuntimeFailedWorker || facts.hasRuntimeUnreadyFailure {
		return false
	}

	switch r.config.readinessPolicy {
	case ReadyWhenAllWorkersReady:
		return facts.hasWorkers &&
			!facts.anyStarting &&
			!facts.anyDraining &&
			!facts.anyStopping &&
			!facts.anyFailed &&
			facts.allWorkersReady
	case ReadyWhenContributingWorkersReady:
		if facts.readinessWorkers == 0 {
			// With no readiness-contributing workers, readiness falls back to
			// aggregate lifecycle so the runtime can still become ready when it
			// has active running workers and no transition is in progress.
			return facts.anyRunning &&
				!facts.anyStarting &&
				!facts.anyDraining &&
				!facts.anyStopping
		}
		return facts.allContributingWorkersReady
	default:
		return false
	}
}

func deriveRuntimeLifecycleTransition(previous LifecycleState, next LifecycleState, last *LifecycleTransition, now time.Time) *LifecycleTransition {
	if previous == next {
		return cloneLifecycleTransition(last)
	}
	return &LifecycleTransition{
		From: previous,
		To:   next,
		At:   now,
	}
}
