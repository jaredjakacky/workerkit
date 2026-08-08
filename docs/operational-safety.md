# Operational Safety

Workerkit status, observer events, logs, traces, diagnostics, support tools,
tests, and HTTP responses are operational publication surfaces. Arbitrary
worker and command errors may contain connection strings, credentials, request
data, filesystem paths, query text, or other sensitive diagnostics, so
Workerkit does not copy their `err.Error()` text into those surfaces.

## Safe By Default

When `Worker.Start`, `Worker.Stop`, `WorkerRuntime.ReportFailure`, or a command
handler returns an ordinary error, Workerkit:

- returns the original error to the direct caller;
- retains it as the private `Cause` on observer events;
- records a stable generic code and message in public status and event fields;
- emits only the public code and message through the built-in `slog` and
  OpenTelemetry observers.

The default public presentations are `worker_failed` / `worker operation
failed` and `command_failed` / `command failed`. Context cancellation and
deadline expiry receive their own stable public codes. Recovered panic payloads
are discarded; only a bounded panic description is published.

## Explicit Public Detail

Use `WithOperationalFailure` when a worker owns a bounded, redacted explanation
that is genuinely safe on every operational surface:

```go
return workerkit.WithOperationalFailure(err, opskit.Failure{
    Code:    "broker_unavailable",
    Message: "broker unavailable",
})
```

The wrapper formats only the public message and unwraps to the private cause, so
`errors.Is` and `errors.As` continue to work. Both `Code` and `Message` are
publication data. Never populate them from `err.Error()` unless the application
has already applied an explicit sanitization policy.

Workerkit keeps failure projection internal rather than defining a general
presentation plugin interface. If an arbitrary error has a broken wrapping
implementation that panics during classification, Workerkit falls back to
generic public failure detail.

Opskit command results already carry explicit public `opskit.Failure` detail.
`CommandFromOpskit` preserves that presentation while keeping Workerkit-owned
adaptation causes private.

## Observer Boundary

`CommandEndEvent.Cause` and `FailureEvent.Cause` exist for application-owned
private diagnostics and correlation. Built-in observers deliberately ignore
them. A custom observer must not log, serialize, record, or format `Cause`
without an explicit application policy. Use `Code` and `Message` for normal
operational output.

Status records never retain a private cause.

The optional `opshttp` adapter also maps lifecycle and command sentinels to
stable public messages. Extra text wrapped around a matching sentinel remains
private. Explicit Opskit rejection messages retain their public semantics.
