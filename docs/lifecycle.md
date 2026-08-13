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
target worker state and any explicit runtime-wide cutoff at dispatch time.

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

Aggregate transitional lifecycle state is operational status, not command
admission policy. If one worker is draining or stopping, another worker that is
still running and accepting work remains eligible for command dispatch.

This only controls Workerkit-managed command dispatch. It does not magically
stop external queues, sockets, goroutines, or domain input sources owned by the
worker. The worker still owns that domain behavior.

## Drain

`Drain` marks one worker as draining, unready, and not accepting new Workerkit
commands. It does not close command admission for unrelated running workers.

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

`Stop` closes command admission for the target worker only. Aggregate runtime
state may report `stopping` while its `Worker.Stop` method runs, but unrelated
workers that remain running and accepting work can still receive commands.

Stop timeouts are cooperative. Workerkit passes a deadline through the stop
context; `Worker.Stop` must observe `ctx.Done()` and return.

`StopAll` stops workers in reverse registration order and continues after
individual stop failures. It establishes a runtime-wide command-admission
cutoff before stopping the first worker, so workers later in the sequence cannot
accept new commands while an earlier `Worker.Stop` call is running.

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

1. close runtime-wide command admission
2. drain all workers best-effort
3. wait for runtime idle
4. stop all workers

Use `Shutdown` for CLIs, tests, and non-Servekit programs. When useful,
`servekitservice.New` coordinates Workerkit lifecycle around an
application-owned Servekit server. That coordinator keeps workers available
while active HTTP handlers drain, then gives Workerkit the time remaining in
the shared service shutdown budget.

HTTP lifecycle controls are not Kubernetes Deployment lifecycle controls. They
mutate one Workerkit runtime in one process. Kubernetes rollout, termination,
and multi-replica coordination remain Kubernetes and application concerns.

## Failure Reporting

Worker startup and command failures are observed by Workerkit automatically.

Background workers can report asynchronous failures through `WorkerRuntime`:

```go
workerRuntime.ReportFailure(err)
```

The returned error remains available to direct callers and explicit observers
as a private cause, but Workerkit does not copy arbitrary error text into status,
logs, or telemetry. Status receives a generic safe failure by default. When the
worker owns an explicitly safe explanation, wrap the cause before returning or
reporting it:

```go
workerRuntime.ReportFailure(workerkit.WithOperationalFailure(err, opskit.Failure{
	Code:    "subscription_failed",
	Message: "subscription unavailable",
}))
```

See [`operational-safety.md`](operational-safety.md) for the publication
boundary.

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
active. A later Stop waits on that same loop. Only one configured cleanup-hook
attempt runs at a time. If an attempt returns an error or its context expires,
cleanup remains pending and a later Stop retries it. Restart remains blocked
until the loop has exited and one cleanup attempt succeeds, preventing
overlapping loop generations and resources.

An unexpected loop exit reports the loop failure first, then makes one
best-effort cleanup attempt with a five-second cooperative timeout. A failed
automatic attempt does not replace the primary loop failure. It emits a separate
sanitized cleanup-failure observation, using `loop_cleanup_failed` by default,
and leaves cleanup pending for a later Stop. The automatic timeout is
intentionally fixed; ordinary Stop retries use the worker's configured stop
timeout.

Automatic cleanup-hook panics follow the worker's panic policy. The default
recovery policy discards the panic value, emits one sanitized panic-marked
cleanup-failure observation, finalizes the exited loop, and leaves cleanup
pending for a later Stop retry. Crash policy emits best-effort sanitized
observation before surfacing the original panic.

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

Check loops execute serially and never overlap themselves. After one execution
returns, Workerkit applies the configured jitter function to the interval and
waits the complete resulting delay before starting the next execution. The
interval is therefore a post-completion wait, not a fixed start-to-start period;
the start-to-start cadence also includes execution time.

Components that expose time-limited cached health should set their stale-after
window greater than the maximum expected completion-to-completion refresh gap.
For one checker, budget the maximum jittered interval wait, maximum execution
duration, and scheduler margin. For a check-group member, also budget its queue
position and the work after its previous completion and before its next
completion. A second interval may be added deliberately as missed-cycle grace,
but it is a policy allowance rather than part of the ordinary timing equation.

Ready and not-ready results update the check worker's readiness by default. A
not-ready result does not stop the loop unless
`WithCheckReportFailureOnNotReady(true)` is configured. Panics fail the loop
through Workerkit's normal failure path. A checker that ignores `ctx.Done()`
cannot be forcibly interrupted by either timeout or Stop. Once it returns,
Workerkit rejects a result produced after the check deadline and marks the
worker unready when readiness management is enabled. The loop continues with
later checks by default. When
`WithCheckReportFailureOnNotReady(true)` is enabled, the deadline error is
reported and stops the loop instead.

If both the checked component and its check worker participate in aggregate
readiness, choose their policies deliberately to avoid counting one dependency
twice. `WithCheckResultObserver` and `WithCheckSummaryObserver` can retain
result detail that is not represented in Workerkit's boolean worker readiness.
Observers receive every completed result or summary, including a late value
that Workerkit rejects for readiness after its deadline.

Observers implementing `CheckExecutionObserver` also receive one start/end
observation for every managed iteration. The bounded end event reports
Workerkit-measured duration, checker versus check-group kind, ready/not-ready,
timeout, cancellation, panic, or integration-error outcome, and whether policy
continues the loop. These events drive the first-party slog and OpenTelemetry
adapters; result and summary observers remain the application-controlled path
for rich Opskit payloads.

## Servekit Readiness

Workerkit readiness is transport-neutral. In the composed Kit Series path,
register the Workerkit runtime in an Opskit registry and pass that registry to
Servekit with `servekit.WithOps(...)`. Servekit then includes Workerkit runtime
readiness in `/readyz` through Opskit.

Applications build that registry and server explicitly. The optional
`servekitservice.New` coordinator only manages startup and graceful shutdown;
it does not create a private registry or readiness adapter.

That keeps the boundary clear:

- Workerkit owns runtime readiness semantics.
- Opskit carries the component/readiness contract.
- Servekit owns HTTP readiness endpoints such as `/readyz`.

## Examples

- [`examples/readiness`](../examples/readiness)
- [`examples/opskit-checks`](../examples/opskit-checks)
- [`examples/failure-policy`](../examples/failure-policy)
- [`examples/production-composition`](../examples/production-composition)
