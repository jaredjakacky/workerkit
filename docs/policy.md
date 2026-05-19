# Policy Guide

Workerkit policy controls what happens around domain work: timeouts, retries,
panic handling, failure propagation, readiness contribution, and command
backpressure.

Policies should be explicit where behavior matters. Do not hide permanent
failures behind broad retry or expose unbounded command execution in production
services.

## Runtime Defaults and Worker Overrides

Runtime options set defaults copied into each worker at registration time.
Worker options override those defaults for one worker.

Use runtime defaults for service-wide posture. Use worker overrides when one
worker has different operational behavior.

## Timeouts

Workerkit supports separate timeouts for:

- start attempts
- stop attempts
- command attempts

Runtime defaults:

- `WithDefaultStartTimeout`
- `WithDefaultStopTimeout`
- `WithDefaultCommandTimeout`

Worker overrides:

- `WithWorkerStartTimeout`
- `WithWorkerStopTimeout`
- `WithWorkerCommandTimeout`

Timeouts bound one attempt. If retry is enabled, each attempt gets its own
timeout.

Workerkit timeouts are cooperative. They are delivered as context deadlines to
`Start`, `Stop`, and command handlers; worker code must observe `ctx.Done()` and
return. Workerkit cannot preempt a blocked goroutine or force resource cleanup.

## Retry

Retries are opt-in.

Use retry only when the operation is safe to repeat and the failure is likely
temporary.

Workerkit's retry package gives you:

- `retry.Attempts`
- `retry.AttemptsIf`
- `retry.Never`
- `retry.Constant`
- `retry.Linear`
- `retry.Exponential`
- `retry.None`
- `retry.Full`
- `retry.Symmetric`

`retry.Never` is the no-retry policy. `retry.None` is the no-jitter option.

Prefer bounded, predicate-gated retry:

```go
policy := retry.AttemptsIf(3,
	retry.Exponential(100*time.Millisecond, 2, 2*time.Second),
	retry.Full(),
	isTemporary,
)
```

Predicates prevent retrying permanent failures. Jitter prevents synchronized
retry waves when many processes fail at the same time.

## Start Retry

Start retry can be configured globally:

```go
runtime, err := workerkit.New(identity,
	workerkit.WithDefaultStartRetry(policy),
)
```

Or per worker:

```go
runtime.Register(spec,
	workerkit.WithWorkerStartRetry(policy),
)
```

Use start retry for transient dependency availability, not for hiding broken
configuration or invalid credentials.

## Command Retry

Command retry can be configured globally:

```go
runtime, err := workerkit.New(identity,
	workerkit.WithDefaultCommandRetry(policy),
)
```

Or per worker:

```go
runtime.Register(spec,
	workerkit.WithWorkerCommandRetry(policy),
)
```

Only retry commands that are idempotent or otherwise safe to repeat.

## Command Concurrency

Workerkit supports layered command backpressure:

- `WithRuntimeCommandConcurrency` protects the whole service boundary.
- `WithWorkerCommandConcurrency` protects one worker.

Runtime default worker concurrency can be configured with
`WithDefaultWorkerCommandConcurrency`.

A dispatch must pass both gates. Runtime saturation returns
`ErrRuntimeSaturated`. Worker saturation returns `ErrWorkerSaturated`.

## Panic Policy

Panic policy controls how Workerkit treats panics in managed lifecycle and
command execution.

Runtime default:

- `WithDefaultPanicPolicy`

Worker override:

- `WithWorkerPanicPolicy`

Use the default recovery posture unless you have a clear reason to let panics
escape.

## Failure Policy

Failure policy controls how worker failure affects aggregate runtime status.

Runtime default:

- `WithDefaultFailurePolicy`

Worker override:

- `WithWorkerFailurePolicy`

Available policies:

- `FailurePolicyIsolate`
- `FailurePolicyMarkRuntimeUnready`
- `FailurePolicyFailRuntime`

Use `FailurePolicyIsolate` for optional workers whose failure should be visible
but should not force the entire service down.

Use `FailurePolicyMarkRuntimeUnready` when the worker is required for
production readiness but the process can remain alive for inspection or
recovery.

Use `FailurePolicyFailRuntime` when the worker failure means the service
boundary itself has failed.

## Readiness Policy

Runtime readiness policy controls aggregate readiness behavior:

- `ReadyWhenContributingWorkersReady`
- `ReadyWhenAllWorkersReady`

Workers can opt out of readiness aggregation:

```go
runtime.Register(spec,
	workerkit.WithWorkerReadinessContribution(false),
)
```

This is useful for maintenance workers, optional observers, or workers that
should be inspectable but should not decide service readiness.

## Ready and Accepting Work Defaults

Worker defaults:

- `WithDefaultReadyOnStart`
- `WithDefaultAcceptingWorkOnStart`

Worker overrides:

- `WithWorkerReadyOnStart`
- `WithWorkerAcceptingWorkOnStart`

Use `WithWorkerReadyOnStart(false)` when a worker needs warmup before it should
contribute to readiness. Use `WithDefaultReadyOnStart(false)` to make that the
runtime default for newly registered workers.

## Recommended Production Posture

For production services:

- keep retries bounded
- use retry predicates
- add jitter to retry policies
- set command concurrency limits where commands can be expensive
- make readiness contribution intentional
- choose failure policy per worker importance
- use observer output as part of diagnostics

## Examples

- [`examples/retry-policy`](../examples/retry-policy)
- [`examples/concurrency-limits`](../examples/concurrency-limits)
- [`examples/failure-policy`](../examples/failure-policy)
- [`examples/production-composition`](../examples/production-composition)
