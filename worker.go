package workerkit

import "context"

// Worker is the lifecycle contract managed by a Runtime.
//
// Start is called when the runtime brings the worker into service. It should
// acquire resources, start worker-owned loops, open subscriptions, or perform
// whatever setup the worker needs before it can run. Start may return after
// launching background work. Trigger-driven workers can use Start only to open
// resources.
//
// Start should be written so a failed attempt leaves the worker safe for a later
// Stop or Start call. If Start returns an error after partially acquiring
// resources, the worker remains responsible for cleaning up those resources from
// a later Stop call or a later successful Start attempt. Workerkit records the
// failure but does not automatically call Stop from inside Start.
//
// Start does not need to be idempotent for already-running workers. Runtime
// lifecycle state prevents normal duplicate starts. Start may be called again
// after the runtime has moved the worker back through stopped or failed states.
// Configure Start retry only when Start is safe to call again after a failed
// attempt.
//
// Stop is called when the runtime takes the worker out of service. It should
// stop accepting worker-owned work, cancel background work, close resources, and
// release anything acquired by Start or by worker-owned goroutines. Stop may be
// called after the worker has entered StateFailed, because failure reporting
// does not imply worker-owned resources have already been released.
//
// Stop should tolerate partial startup and prior failure. It should be safe to
// call when some resources were never acquired, were already released, or when
// background work has already exited.
//
// If Stop returns because its context expired before cleanup completed, a later
// Stop may be called to resume waiting and finish cleanup. Start must not launch
// duplicate background work while the previous stop remains active.
//
// Start and Stop must honor context cancellation. Runtime timeouts are delivered
// through ctx.Done(). Workerkit cannot interrupt implementations that block
// without observing the context.
//
// Runtime lifecycle operations are serialized and the lifecycle gate is not
// reentrant. Start and Stop implementations must not call public Runtime
// lifecycle methods. Use WorkerRuntime for worker-scoped status, readiness,
// command admission, and failure reporting.
//
// NewLoopWorker provides a ready-made implementation for workers backed by a
// long-running background loop.
//
// Worker-owned commands can be attached during registration with WorkerOption
// values such as WithCommand.
type Worker interface {
	Start(context.Context) error
	Stop(context.Context) error
}

// WorkerRuntime is the worker-scoped runtime handle available during managed
// worker execution.
//
// It lets worker code report readiness, inspect its own status, and control
// Workerkit command admission without receiving access to the full Runtime.
// These controls affect Workerkit's operational state. They do not start, stop,
// or otherwise own the worker's domain-specific business loop.
type WorkerRuntime interface {
	// Name returns the worker's fully qualified name inside the runtime
	// boundary.
	Name() string
	// Status returns the worker's current operational snapshot.
	Status() WorkerStatus
	// SetReady updates the worker's readiness signal.
	//
	// Runtime readiness may or may not depend on this worker's readiness
	// depending on the worker's readiness contribution setting and the runtime
	// readiness policy.
	SetReady(bool) error
	// SetAcceptingWork controls whether Workerkit may dispatch new commands to
	// this worker.
	//
	// This does not start or stop the worker's own business loop. Domain
	// behavior such as pausing trading or replication should be implemented by
	// the worker or by a worker-owned command.
	SetAcceptingWork(bool) error
	// ReportFailure records an asynchronous worker-owned failure.
	//
	// Use ReportFailure for background loops, watchers, pollers, or other
	// worker-owned goroutines that fail outside a direct Start, Stop, or command
	// handler return path. ReportFailure applies the worker failure policy. It
	// does not call Stop or clean up worker-owned resources. A nil error is
	// ignored. Worker.Start should return setup errors directly; ReportFailure
	// records worker health independently of the Start return path. If a current
	// generation reports failure while Start is running, Start may still return
	// nil while the worker finishes in StateFailed.
	ReportFailure(error) error
}

type workerRuntimeContextKey struct{}

// WorkerRuntimeFromContext returns the runtime-owned worker handle from contexts
// passed to Worker.Start, Worker.Stop, and command handlers by Workerkit. A
// handle is scoped to the worker lifecycle generation that created it; mutation
// calls from a stale handle return ErrInvalidWorkerState after restart.
func WorkerRuntimeFromContext(ctx context.Context) (WorkerRuntime, bool) {
	if ctx == nil {
		return nil, false
	}
	runtime, ok := ctx.Value(workerRuntimeContextKey{}).(WorkerRuntime)
	return runtime, ok
}

// workerControlHandle is the runtime-backed implementation of WorkerRuntime.
//
// The handle intentionally exposes only worker-scoped control operations to
// code running inside a worker lifecycle or command context. It lets a worker
// update its own readiness, command admission, and failure state without
// receiving access to the full Runtime.
type workerControlHandle struct {
	runtime    *Runtime
	name       string
	generation uint64
	ctx        context.Context
}

func (h *workerControlHandle) Name() string {
	return h.name
}

func (h *workerControlHandle) Status() WorkerStatus {
	status, _ := h.runtime.workerStatus(h.name)
	return status
}

func (h *workerControlHandle) SetReady(ready bool) error {
	return h.runtime.setWorkerReady(h.context(), h.name, h.generation, ready)
}

func (h *workerControlHandle) SetAcceptingWork(accepting bool) error {
	return h.runtime.setWorkerAcceptingWork(h.name, h.generation, accepting)
}

func (h *workerControlHandle) ReportFailure(err error) error {
	return h.runtime.reportWorkerFailure(h.context(), h.name, h.generation, err)
}

func (h *workerControlHandle) context() context.Context {
	if h.ctx == nil {
		return context.Background()
	}
	return h.ctx
}

// WorkerSpec binds worker behavior and discovery metadata to one runtime-owned
// worker identity.
type WorkerSpec struct {
	// Name is the worker's local name inside the runtime boundary.
	Name string
	// Description is optional discovery text for admin and inspection surfaces.
	Description string
	// Worker is the behavior managed by the runtime.
	Worker Worker
}

// WorkerSnapshot is a point-in-time inspection view of one registered worker.
//
// It combines stable registration metadata with the worker's current
// operational status so admin and inspection surfaces can render workers
// without stitching together separate metadata and status lookups.
//
// WorkerSnapshot is part of Workerkit's public JSON status contract. JSON field
// names and meanings are stable within a major version. Minor versions may add
// fields, so clients should ignore unknown fields.
type WorkerSnapshot struct {
	// QualifiedName is the runtime-qualified worker name.
	QualifiedName string `json:"qualifiedName"`
	// Name is the worker's local registration name inside the runtime boundary.
	Name string `json:"name"`
	// Description is optional discovery text for admin and inspection surfaces.
	Description string `json:"description,omitempty"`
	// Status is the worker's current operational status.
	Status WorkerStatus `json:"status"`
}

// ValidateWorkerLocalName validates a worker capability name relative to its
// runtime boundary.
//
// Local worker names are flat operational identifiers. Workerkit constructs
// qualified worker names as runtime/worker after registration.
func ValidateWorkerLocalName(name string) error {
	return validateIdentifier(name, "worker local name")
}

// ValidateWorkerName validates a worker identifier for status lookup and
// command targeting.
//
// Worker identifiers may be either runtime-local names such as `order-router`
// or fully qualified names such as `trading-core/order-router`, depending on
// the calling surface.
func ValidateWorkerName(name string) error {
	return validateWorkerReferenceName(name)
}

func withWorkerRuntime(ctx context.Context, runtime WorkerRuntime) context.Context {
	return context.WithValue(ctx, workerRuntimeContextKey{}, runtime)
}
