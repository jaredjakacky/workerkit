package workerkit

import (
	"context"
	"strconv"
	"time"

	opskit "github.com/jaredjakacky/opskit"
)

const opskitRuntimeKind = "worker_runtime"

// Compile-time checks for Workerkit's Opskit component surface.
var (
	_ opskit.Component            = (*Runtime)(nil)
	_ opskit.ReadinessContributor = (*Runtime)(nil)
	_ opskit.Inspector            = (*Runtime)(nil)
)

// ComponentInfo returns the Opskit identity for this Workerkit runtime.
func (r *Runtime) ComponentInfo() opskit.ComponentInfo {
	identity := r.Identity()
	return opskit.ComponentInfo{
		Name:        identity.Name,
		Kind:        opskitRuntimeKind,
		Description: "Workerkit runtime",
		Labels: []opskit.Attribute{
			opskit.Attr("kit", "workerkit"),
		},
	}
}

// Status returns the runtime's cached aggregate state as an Opskit component
// status. It does not run active checks or call worker code.
func (r *Runtime) Status(context.Context) opskit.Status {
	status := r.RuntimeStatus()
	return opskit.Status{
		State:      opskitStateFromRuntimeStatus(status),
		Ready:      status.Ready,
		Message:    opskitRuntimeStatusMessage(status),
		UpdatedAt:  opskitRuntimeUpdatedAt(status),
		Attributes: opskitRuntimeStatusAttributes(status, r.readinessPolicy()),
	}
}

// Readiness returns the runtime's cached aggregate readiness for Opskit.
func (r *Runtime) Readiness(context.Context) opskit.Readiness {
	status := r.RuntimeStatus()
	reason := "runtime ready"
	if !status.Ready {
		reason = "runtime not ready: state=" + string(status.State)
	}
	return opskit.Readiness{
		Ready:  status.Ready,
		Reason: reason,
		Components: []opskit.ReadinessItem{
			{
				Name:    r.ComponentInfo().Name,
				Kind:    opskitRuntimeKind,
				Policy:  opskit.ReadinessRequired,
				Ready:   status.Ready,
				State:   opskitStateFromRuntimeStatus(status),
				Message: opskitRuntimeStatusMessage(status),
			},
		},
	}
}

// Inspect returns a safe local inspection snapshot for this runtime.
func (r *Runtime) Inspect(context.Context) (opskit.Inspection, error) {
	status := r.RuntimeStatus()
	workers := r.Workers()
	commandsByWorker := make(map[string][]CommandInfo, len(workers))
	inspectionWorkers := make([]runtimeInspectionWorker, 0, len(workers))
	for _, worker := range workers {
		inspectionWorkers = append(inspectionWorkers, runtimeInspectionWorkerFromSnapshot(worker))
		commands, _ := r.Commands(worker.QualifiedName)
		commandsByWorker[worker.QualifiedName] = commands
	}

	return opskit.Inspection{
		Summary: runtimeInspectionSummary{
			Identity: r.Identity(),
			Status:   status,
			Policy: runtimeInspectionPolicy{
				ReadinessPolicy: r.readinessPolicy(),
			},
		},
		Details: runtimeInspectionDetails{
			Workers:  inspectionWorkers,
			Commands: commandsByWorker,
		},
		Attributes: opskitRuntimeStatusAttributes(status, r.readinessPolicy()),
	}, nil
}

type runtimeInspectionSummary struct {
	Identity Identity                `json:"identity"`
	Status   RuntimeStatus           `json:"status"`
	Policy   runtimeInspectionPolicy `json:"policy,omitempty"`
}

type runtimeInspectionPolicy struct {
	ReadinessPolicy ReadinessPolicy `json:"readinessPolicy"`
}

type runtimeInspectionDetails struct {
	Workers  []runtimeInspectionWorker `json:"workers,omitempty"`
	Commands map[string][]CommandInfo  `json:"commands,omitempty"`
}

type runtimeInspectionWorker struct {
	QualifiedName string                        `json:"qualifiedName"`
	Name          string                        `json:"name"`
	Description   string                        `json:"description,omitempty"`
	Status        runtimeInspectionWorkerStatus `json:"status"`
}

type runtimeInspectionWorkerStatus struct {
	State                     LifecycleState       `json:"state"`
	Ready                     bool                 `json:"ready"`
	AcceptingWork             bool                 `json:"acceptingWork"`
	InFlight                  int                  `json:"inFlight"`
	LastTransition            *LifecycleTransition `json:"lastTransition,omitempty"`
	LastFailureAt             *time.Time           `json:"lastFailureAt,omitempty"`
	LastCommandFailureAt      *time.Time           `json:"lastCommandFailureAt,omitempty"`
	LastCommandFailureCommand string               `json:"lastCommandFailureCommand,omitempty"`
}

func opskitStateFromRuntimeStatus(status RuntimeStatus) opskit.State {
	switch status.State {
	case StateRegistered:
		return opskit.StateStopped
	case StateStarting:
		return opskit.StateInitializing
	case StateRunning:
		if status.Ready {
			return opskit.StateReady
		}
		return opskit.StateNotReady
	case StateDraining, StateStopping:
		return opskit.StateNotReady
	case StateStopped:
		return opskit.StateStopped
	case StateFailed:
		return opskit.StateFailed
	default:
		return opskit.StateUnknown
	}
}

func (r *Runtime) readinessPolicy() ReadinessPolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config.readinessPolicy
}

func opskitRuntimeStatusMessage(status RuntimeStatus) string {
	switch status.State {
	case StateFailed:
		return "runtime failed"
	case StateStopped:
		return "runtime stopped"
	}
	if status.Ready {
		return "runtime ready: state=" + string(status.State)
	}
	return "runtime not ready: state=" + string(status.State)
}

func opskitRuntimeUpdatedAt(status RuntimeStatus) *time.Time {
	if status.LastTransition == nil {
		return nil
	}
	updatedAt := status.LastTransition.At
	return &updatedAt
}

func opskitRuntimeStatusAttributes(status RuntimeStatus, readinessPolicy ReadinessPolicy) []opskit.Attribute {
	return []opskit.Attribute{
		opskit.Attr("runtime_state", string(status.State)),
		opskit.Attr("worker_count", strconv.Itoa(status.Workers)),
		opskit.Attr("in_flight", strconv.Itoa(status.InFlight)),
		opskit.Attr("readiness_policy", string(readinessPolicy)),
	}
}

func runtimeInspectionWorkerFromSnapshot(snapshot WorkerSnapshot) runtimeInspectionWorker {
	return runtimeInspectionWorker{
		QualifiedName: snapshot.QualifiedName,
		Name:          snapshot.Name,
		Description:   snapshot.Description,
		Status:        runtimeInspectionWorkerStatusFromStatus(snapshot.Status),
	}
}

func runtimeInspectionWorkerStatusFromStatus(status WorkerStatus) runtimeInspectionWorkerStatus {
	inspectionStatus := runtimeInspectionWorkerStatus{
		State:          status.State,
		Ready:          status.Ready,
		AcceptingWork:  status.AcceptingWork,
		InFlight:       status.InFlight,
		LastTransition: status.LastTransition,
	}
	if status.LastFailure != nil {
		failedAt := status.LastFailure.At
		inspectionStatus.LastFailureAt = &failedAt
	}
	if status.LastCommandFailure != nil {
		failedAt := status.LastCommandFailure.At
		inspectionStatus.LastCommandFailureAt = &failedAt
		inspectionStatus.LastCommandFailureCommand = status.LastCommandFailure.Command
	}
	return inspectionStatus
}
