# Lifecycle and Readiness

Workerkit separates lifecycle, readiness, command admission, and shutdown. A
worker can be running before it is ready, draining while it finishes in-flight
work, or failed while the runtime remains isolated depending on policy.

## Core States

Workerkit uses `LifecycleState` values to describe runtime and worker state:

- `registered`
- `starting`
- `running`
- `draining`
- `stopping`
- `stopped`
- `failed`

Runtime status is aggregate state derived from registered workers. Worker
status is the direct state of one managed worker.

## Start

`Start` starts one worker. `StartAll` starts workers in registration order.

During startup, Workerkit applies:

- start timeout
- start retry policy
- panic policy
- ready-on-start default
- accepting-work-on-start default
- observer events

`StartAll` is fail-fast. It does not roll back workers that already started.

Start timeouts are cooperative. Workerkit passes a deadline through the start
context; `Worker.Start` must observe `ctx.Done()` and return.

## Running Versus Ready

Running does not imply ready.

Readiness is a production signal. A worker may need to warm caches, establish
subscriptions, load model state, build indexes, or wait for downstream
dependencies before it should serve production work.

Workers can change readiness through `WorkerRuntime`:

```go
workerRuntime, ok := workerkit.WorkerRuntimeFromContext(ctx)
if !ok {
	return errors.New("worker runtime missing")
}

workerRuntime.SetReady(false)
// warm up
workerRuntime.SetReady(true)
```

`WithWorkerReadinessContribution(false)` excludes an optional worker from
aggregate runtime readiness.

## Command Admission

Readiness and command admission are related but separate.

`WorkerRuntime.SetAcceptingWork(false)` stops new Workerkit command dispatches
to that worker. `Drain` also marks the worker unready and not accepting new
commands.

This only controls Workerkit-managed command dispatch. It does not magically
stop external queues, sockets, goroutines, or domain input sources owned by the
worker. The worker still owns that domain behavior.

## Drain

`Drain` marks one worker as draining, unready, and not accepting new Workerkit
commands.

`DrainAll` drains running workers in registration order and returns on the
first error.

`DrainAllBestEffort` attempts to drain every running worker and returns the
combined error when any drain fails.

Drain is the beginning of graceful shutdown. It prevents new Workerkit command
admission before waiting for in-flight commands.

## Idle Wait

`WaitIdle` waits for one worker to have no in-flight commands.

`WaitAllIdle` waits for the runtime to have no in-flight commands.

These methods are useful when composing an explicit graceful path:

```go
if err := runtime.Drain(ctx, "index"); err != nil {
	return err
}
if err := runtime.WaitIdle(ctx, "index"); err != nil {
	return err
}
return runtime.Stop(ctx, "index")
```

## Stop

`Stop` stops one running, draining, or failed worker.

Stop timeouts are cooperative. Workerkit passes a deadline through the stop
context; `Worker.Stop` must observe `ctx.Done()` and return.

`StopAll` stops workers in reverse registration order and continues after
individual stop failures.

Stop does not wait for in-flight commands by itself. Use `Drain`, `WaitIdle`,
and `Stop` when you need a custom graceful sequence.

## Shutdown

`Shutdown` is the direct runtime convenience path for non-HTTP callers:

1. drain all workers best-effort
2. wait for runtime idle
3. stop all workers

Use `Shutdown` for CLIs, tests, and non-Servekit programs. Use
`servekitservice.NewManaged` when Servekit owns the process HTTP lifecycle.

HTTP lifecycle controls are not Kubernetes Deployment lifecycle controls. They
mutate one Workerkit runtime in one process. Kubernetes rollout, termination,
and multi-replica coordination remain Kubernetes and application concerns.

## Failure Reporting

Worker startup and command failures are observed by Workerkit automatically.

Background workers can report asynchronous failures through `WorkerRuntime`:

```go
workerRuntime.ReportFailure(err)
```

Failure policy determines how that worker failure affects aggregate runtime
status:

- `FailurePolicyIsolate` records the worker failure without forcing the whole runtime down.
- `FailurePolicyMarkRuntimeUnready` records the worker failure and forces aggregate readiness down.
- `FailurePolicyFailRuntime` records the worker failure and moves the runtime into failed state.

Read [`policy.md`](policy.md) for policy guidance.

## Servekit Readiness

Workerkit readiness is transport-neutral. In the composed Kit Series path,
register the Workerkit runtime in an Opskit registry and pass that registry to
Servekit with `servekit.WithOps(...)`. Servekit then includes Workerkit runtime
readiness in `/readyz` through Opskit.

`servekitservice.NewManaged` and `servekitservice.ReadinessOptions` use that
Opskit path for the common service shell. `opshttp.ReadinessCheck` remains as a
standalone Servekit readiness adapter for users who are not using an Opskit
registry.

That keeps the boundary clear:

- Workerkit owns runtime readiness semantics.
- Opskit carries the component/readiness contract.
- Servekit owns HTTP readiness endpoints such as `/readyz`.

## Examples

- [`examples/readiness`](../examples/readiness)
- [`examples/failure-policy`](../examples/failure-policy)
- [`examples/managed-service`](../examples/managed-service)
