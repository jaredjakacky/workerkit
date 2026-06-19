# Usage Guide

This guide covers the normal Workerkit path: create a runtime, register
workers, start them, inspect status, expose commands when useful, and shut down
cleanly.

For symbol-level details, see [`api.md`](api.md).

## The Normal Path

Most Workerkit programs follow this shape:

1. Create one `Runtime` for a service boundary.
2. Register one or more workers.
3. Start the runtime.
4. Inspect status or dispatch worker-owned commands.
5. Drain and shut down gracefully.

```go
runtime, err := workerkit.New(workerkit.Identity{Name: "search"})
if err != nil {
	return err
}

err = runtime.Register(workerkit.WorkerSpec{
	Name:        "index",
	Description: "maintains the search index",
	Worker:      indexWorker,
})
if err != nil {
	return err
}

if err := runtime.StartAll(ctx); err != nil {
	return err
}

defer func() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = runtime.Shutdown(shutdownCtx)
}()
```

## Runtime

A runtime represents one service boundary. It is the aggregate operational
object for workers that should share lifecycle, status, readiness, command
admission, retry policy, concurrency limits, failure policy, and observers.

Use multiple runtimes only when you truly want separate operational boundaries.
Most services should start with one runtime.

## Workers

A Workerkit worker is ordinary Go code behind a small lifecycle interface:

```go
type Worker interface {
	Start(context.Context) error
	Stop(context.Context) error
}
```

`Start` should begin the worker's domain work or initialize the resources it
owns. `Stop` should release those resources and stop the worker's activity.

Workerkit does not own the domain model, storage, queue, broker, scheduler, or
business rules. It owns the runtime envelope around the worker.

## Registration

Workers are registered before startup:

```go
err := runtime.Register(workerkit.WorkerSpec{
	Name:        "ingest",
	Description: "consumes source records",
	Worker:      ingestWorker,
})
```

Worker names are local inside the runtime. Workerkit qualifies them as
`runtime/worker` in status, telemetry, and operations surfaces.

Registration order matters. `StartAll` starts workers in registration order.
`StopAll` stops workers in reverse registration order.

## Lifecycle

The common lifecycle methods are:

- `Start` starts one worker.
- `StartAll` starts registered workers in registration order.
- `Drain` marks one worker unready and not accepting new Workerkit commands.
- `DrainAll` drains running workers in registration order.
- `DrainAllBestEffort` attempts to drain all running workers and joins errors.
- `WaitIdle` waits for one worker to have no in-flight commands.
- `WaitAllIdle` waits for runtime-wide command idleness.
- `Stop` stops one worker.
- `StopAll` stops workers in reverse registration order.
- `Shutdown` drains all workers best-effort, waits for runtime idle, then stops.

Lifecycle mutations are serialized per runtime. Concurrent calls wait for the
active lifecycle operation, and that wait counts against their context
deadline. Command dispatch and status reads remain concurrent with lifecycle
operations and are gated by current runtime state.

For the full lifecycle model, read [`lifecycle.md`](lifecycle.md).

## Readiness

Running does not imply ready.

Workers can start, warm up, and then call `WorkerRuntime.SetReady(true)` through
the worker-scoped runtime handle:

```go
func (w *worker) Start(ctx context.Context) error {
	workerRuntime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return errors.New("worker runtime missing")
	}

	workerRuntime.SetReady(false)
	// warm up local state
	workerRuntime.SetReady(true)
	return nil
}
```

Aggregate runtime readiness is derived from readiness-contributing workers.
Workers can opt out with `WithWorkerReadinessContribution(false)`.

## Commands

Workers can register domain commands:

```go
err := runtime.Register(spec,
	workerkit.WithCommand("refresh", workerkit.CommandHandlerFunc(refresh)),
)
```

Commands are not lifecycle controls. They are worker-owned domain operations
routed by Workerkit.

Use `Runtime.Commands` for discovery and `Runtime.Dispatch` for direct
execution. The command payload and result payload are raw bytes, so the worker
owns the contract.

Read [`commands.md`](commands.md) for the full command model.

## Loop Workers

Use `NewLoopWorker` when the domain work is a long-running loop:

```go
worker := workerkit.NewLoopWorker(func(ctx context.Context, runtime workerkit.WorkerRuntime) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// do domain work
		}
	}
})
```

This gives the loop managed lifecycle and cancellation instead of leaving it as
an unmanaged goroutine.

## Status

Use `Status` for aggregate runtime status:

```go
status := runtime.RuntimeStatus()
fmt.Println(status.State, status.Ready, status.InFlight)
```

Use `Workers` or `Worker` for worker inspection:

```go
for _, worker := range runtime.Workers() {
	fmt.Println(worker.QualifiedName, worker.Status.State, worker.Status.Ready)
}
```

Status snapshots separate lifecycle, readiness, command admission, in-flight
work, last lifecycle transition, last worker failure, and last command failure.

## Common Options

Runtime options set defaults:

- `WithReadinessPolicy`
- `WithRuntimeCommandConcurrency`
- `WithDefaultStartTimeout`
- `WithDefaultStopTimeout`
- `WithDefaultCommandTimeout`
- `WithDefaultStartRetry`
- `WithDefaultCommandRetry`
- `WithDefaultWorkerCommandConcurrency`
- `WithDefaultPanicPolicy`
- `WithDefaultFailurePolicy`
- `WithDefaultReadyOnStart`
- `WithDefaultAcceptingWorkOnStart`
- `WithDefaultWorkerReadinessContribution`
- `WithObserver`

Worker options override runtime defaults for one registered worker:

- `WithWorkerStartTimeout`
- `WithWorkerStopTimeout`
- `WithWorkerCommandTimeout`
- `WithWorkerStartRetry`
- `WithWorkerCommandRetry`
- `WithWorkerCommandConcurrency`
- `WithWorkerPanicPolicy`
- `WithWorkerFailurePolicy`
- `WithWorkerReadyOnStart`
- `WithWorkerAcceptingWorkOnStart`
- `WithWorkerReadinessContribution`
- `WithCommand`
- `WithCommandSpec`

## Recommended Adoption Path

Start with direct runtime usage and tests. Add worker commands only when the
worker has real domain operations worth exposing. Add retry and concurrency
policy where repeated work is safe and overload needs backpressure.

Use Servekit through `servekitservice.NewManaged` when Workerkit is part of an
HTTP service. In composed Kit Series services, register the runtime with Opskit
and pass that registry to Servekit for `/readyz` and generic admin inspection.
Use `opshttp` when you need Workerkit-specific HTTP command dispatch or
privileged lifecycle controls.

## Related Guides

- [`getting-started.md`](getting-started.md)
- [`lifecycle.md`](lifecycle.md)
- [`commands.md`](commands.md)
- [`policy.md`](policy.md)
- [`composition.md`](composition.md)
