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

## Lifecycle Serialization

`Register`, `Start`, `Drain`, `Stop`, their runtime-wide variants, and
`Shutdown` are serialized per runtime. One lifecycle operation completes before
the next begins, so bulk-operation snapshots cannot race another lifecycle
mutation. Time spent waiting for the lifecycle operation gate counts against
the caller's context deadline.

Status reads, readiness updates, failure reporting, idle waits, and command
dispatch remain concurrent. Command admission is still determined from the
worker and runtime state at dispatch time.

Worker lifecycle methods and observer callbacks run inside the active lifecycle
operation. They must not call public `Runtime` lifecycle methods recursively;
the lifecycle gate is intentionally non-reentrant. Worker code should use the
scoped `WorkerRuntime` handle for readiness, command admission, status, and
failure reporting.

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

A lifecycle generation spans the whole Start operation, including retries.
Start retry does not isolate `WorkerRuntime` handles retained by failed
attempts. Before returning an error, a failed attempt must stop any goroutines
or callbacks that could continue using its handle.

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

Stop does not wait for in-flight commands or cancel their contexts. A stopped
worker may temporarily report a positive `InFlight` count while previously
admitted commands finish. `Worker.Stop` may run concurrently with those command
handlers, so it must not release resources they still need unless the caller
first composes `Drain`, `WaitIdle`, and `Stop`.

Because command slots are released only when admitted handlers exit, a
restarted worker may temporarily report positive `InFlight` for commands
admitted by an older generation. Those commands continue to count toward worker
and runtime concurrency limits and idle waits.

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

`Worker.Start` should return setup failures directly. `ReportFailure` is an
asynchronous health signal: if the current worker generation reports failure
while `Worker.Start` is still running, the lifecycle remains `starting` until
the call finishes. A nil Start result then resolves the worker to `failed` while
Start itself returns nil. This keeps concurrent Start and Stop calls from
overlapping the active startup operation.

Worker runtime handles are scoped to one lifecycle generation. A loop, command,
or callback retained from an older generation cannot change readiness,
admission, or failure state after the worker restarts.

`ReportFailure` accepted while a worker is stopping records `LastFailure` and
emits failure observation without replacing the stopping lifecycle. A successful
Stop can still complete to `stopped`, preserving that failure for inspection.
Reports made after Stop completes, or through a stale generation handle after
restart, return `ErrInvalidWorkerState` without mutating current worker status.

When `LoopWorker.Stop` times out before its loop exits, the original stop remains
active. A later Stop waits on that same loop and runs the configured cleanup hook
at most once. Restart remains blocked until the loop has exited and cleanup has
completed, preventing overlapping loop generations.

Stop cancellation suppresses only nil or cancellation-related loop results. If
a loop returns an independent error while Stop races with it, Workerkit records
that failure before publishing loop completion. Stop can still finish as
`stopped`, while `LastFailure` preserves the unexpected exit.

Direct Stop does not wait for or cancel in-flight commands. Successful late
completion only releases command capacity. A late returned error or panic from
the current generation remains visible through `LastCommandFailure` and failure
observation, but it does not move a stopping or stopped worker back to `failed`.
After restart, returned errors and panics from stale commands are still observed
but cannot mutate the new generation's status. Use Drain, WaitIdle, and Stop
when shutdown must wait for all admitted command work.

Failure policy determines how that worker failure affects aggregate runtime
status:

- `FailurePolicyIsolate` records the worker failure without forcing the whole runtime down.
- `FailurePolicyMarkRuntimeUnready` records the worker failure and forces aggregate readiness down.
- `FailurePolicyFailRuntime` records the worker failure and moves the runtime into failed state.

Read [`policy.md`](policy.md) for policy guidance.

## Opskit Check Workers

`NewCheckLoop` and `NewCheckGroupLoop` adapt Opskit execution hooks into normal
Workerkit workers. Starting the worker starts periodic execution; draining
closes Workerkit command admission but does not stop the loop; stopping the
worker cancels the loop context and waits for the active check to return.

Workerkit owns interval timing, initial delay, jitter, cooperative per-check
timeouts, cancellation, panic recovery, readiness updates, and optional failure
reporting. Opskit defines the check contracts but does not schedule them. The
checked component owns check meaning and any cached component health exposed
through Opskit status or readiness.

Ready and not-ready results update the check worker's readiness by default. A
not-ready result does not stop the loop unless
`WithCheckReportFailureOnNotReady(true)` is configured. Panics fail the loop
through Workerkit's normal failure path. A checker that ignores `ctx.Done()`
cannot be forcibly interrupted by either timeout or Stop.

If both the checked component and its check worker participate in aggregate
readiness, choose their policies deliberately to avoid counting one dependency
twice. `WithCheckResultObserver` and `WithCheckSummaryObserver` can retain
result detail that is not represented in Workerkit's boolean worker readiness.

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
- [`examples/opskit-checks`](../examples/opskit-checks)
- [`examples/failure-policy`](../examples/failure-policy)
- [`examples/managed-service`](../examples/managed-service)
