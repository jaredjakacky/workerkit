# Observability

Workerkit emits backend-neutral observer events from the core runtime. HTTP is
not required for lifecycle, readiness, command, check execution, retry, or
failure telemetry.

## Observer Model

Attach an observer with `WithObserver`:

```go
runtime, err := workerkit.New(identity,
	workerkit.WithObserver(observer),
)
```

The observer receives events for:

- lifecycle transitions
- command starts
- command ends
- managed Opskit check execution starts and ends, when the observer implements
  `CheckExecutionObserver`
- worker failures
- readiness changes

Workerkit wraps observers defensively so observer failures do not take down the
runtime. `CheckExecutionObserver` is an optional extension; existing `Observer`
implementations do not need to implement it. `NopObserver`, `SafeObserver`,
`MultiObserver`, and the first-party slog and OpenTelemetry adapters preserve
the extension.

Observer callbacks are synchronous. Lifecycle observations run inside the
runtime's serialized lifecycle operation, so callbacks must not recursively
call public `Runtime` lifecycle methods. Status reads and worker-scoped
`WorkerRuntime` operations remain available.

## Events

`TransitionEvent` describes worker lifecycle transitions.

`CommandStartEvent` describes one command dispatch beginning. It includes
runtime name, worker name, command name, dispatch ID, and start time.

`CommandEndEvent` describes one command dispatch completing. It includes
duration, final success or failure, total handler attempts, safe public failure
code/message, the private original cause, and the same dispatch identity.

`CheckStartEvent` and `CheckEndEvent` describe one Workerkit-managed execution
of an Opskit `Checker` or `CheckGroup`. The end event contains Workerkit-measured
wall-clock duration, a bounded outcome (`ready`, `not_ready`, `timeout`,
`canceled`, `panic`, or `error`), and whether that outcome permits the loop to
continue. Independent lifecycle cancellation may still stop it before another
iteration. A context returned by `StartCheck` is passed to the check hook and
its configured result or summary callback.

`WithCheckResultObserver` and `WithCheckSummaryObserver` remain the hooks for
rich Opskit payloads and per-check group detail. Core check execution events are
deliberately bounded and do not copy result messages, attributes, child results,
arbitrary errors, or panic payloads.

`FailureEvent` describes worker or command failure. Command handler failures
emit attempt data per failed attempt, including attempts that are later retried
successfully. `Code` and `Message` are safe operational presentation;
`Cause` is private diagnostic data and must not be published without explicit
application policy.

`ReadinessEvent` describes readiness changes for a worker.

## Structured Logs

The `slogobserver` package maps Workerkit observer events to structured
`log/slog` records:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

runtime, err := workerkit.New(identity,
	workerkit.WithObserver(slogobserver.New(logger)),
)
```

This is the lowest-setup production-friendly observer. It works without
Servekit and without an OpenTelemetry collector. It deliberately logs only
safe failure code/message fields and never formats an event's private `Cause`.
Managed check loops emit one structured completion record per iteration with
runtime, worker, kind, outcome, continuation, and duration fields.

## OpenTelemetry

The `otel` package maps Workerkit observer events into OpenTelemetry spans,
events, counters, and histograms:

```go
observer, err := otel.New(
	otel.WithAttributes(attribute.String("service.name", "search")),
)
if err != nil {
	return err
}

runtime, err := workerkit.New(identity,
	workerkit.WithObserver(observer),
)
```

Dispatch IDs are high-cardinality values. They belong on spans and events, not
metric labels.

Managed check executions create `workerkit.check` spans and record:

- `workerkit.check.executions`
- `workerkit.check.duration`

Default check metric attributes are limited to runtime name, qualified worker
name, check kind, bounded outcome, and the loop-continuation boolean. Result and
summary messages, Opskit attributes, group child names/results, errors, panic
payloads, execution identifiers, URLs, tenant or user identifiers, and other
unbounded values must not become metric attributes by default.

The OpenTelemetry adapter records only safe failure code/message fields. It
does not record the private cause as an exception.

## Multiple Observers

Use `MultiObserver` when you want to fan out to more than one backend:

```go
observer := workerkit.MultiObserver(
	slogobserver.New(logger),
	otelObserver,
)
```

Check observations are sent only to children implementing
`CheckExecutionObserver`. Derived contexts flow through children in order and
started observations end in reverse order, matching command observation
semantics.

Use `NopObserver` in tests when an explicit observer is useful but no output is
desired.

## Testing Observability

Because observers are ordinary Go interfaces, tests can attach a small custom
observer and assert emitted events directly.

This keeps runtime behavior testable without HTTP, Servekit, Kubernetes, or an
external telemetry backend.

## Servekit Boundary

Servekit has its own HTTP observability concerns: request IDs, access logs,
middleware, route timing, panic recovery, and HTTP spans.

Workerkit observability is separate. It describes worker runtime behavior,
command dispatch, managed check execution, readiness, and failure. When both kits are used together,
their telemetry should complement each other without mixing responsibilities.

## Examples

- [`examples/observability-slog`](../examples/observability-slog)
- [`examples/observability-otel`](../examples/observability-otel)
- [`examples/testing`](../examples/testing)
