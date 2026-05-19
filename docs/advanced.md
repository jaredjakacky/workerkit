# Advanced Guide

Most Workerkit users should start with [`getting-started.md`](getting-started.md),
[`usage.md`](usage.md), and [`lifecycle.md`](lifecycle.md). This guide covers
composition and customization points that are useful after the normal path is
clear.

## When You Are in Advanced Territory

You are probably in advanced Workerkit territory when:

- one service has several workers with different failure policies
- commands need different retry or concurrency posture per worker
- the service has both normal application routes and an operations plane
- mutating operations routes need endpoint-specific policy
- observers need to feed more than one backend
- startup and shutdown must coordinate with external resources

## Prefer Worker Options First

Before adding application-level control code, check whether the runtime or
worker option already expresses the behavior.

Use worker options for per-worker differences:

- timeout overrides
- retry overrides
- concurrency limits
- failure policy
- readiness contribution
- command registration

This keeps operational policy visible at registration time.

## Custom Workers

Implement `Worker` directly when the worker owns resources that need explicit
startup and shutdown:

```go
type consumer struct {
	client *Client
}

func (c *consumer) Start(ctx context.Context) error {
	return c.client.Connect(ctx)
}

func (c *consumer) Stop(ctx context.Context) error {
	return c.client.Close(ctx)
}
```

Use `NewLoopWorker` when the worker is primarily a long-running loop. The loop
should return when its context is canceled.

## Worker-Scoped Runtime Access

Use `WorkerRuntimeFromContext` inside managed worker methods and command
handlers when worker code needs to report runtime signals:

- `SetReady`
- `SetAcceptingWork`
- `ReportFailure`
- `Status`

Do not pass the full `Runtime` into workers unless the worker genuinely needs
runtime-wide authority. The worker-scoped handle is intentionally narrower.

## Custom Observers

Observers are ordinary interfaces. Implement `Observer` when the first-party
`slogobserver` or `otel` packages do not match your backend.

Use `SafeObserver` if you are composing an observer implementation that might
panic. Use `MultiObserver` when several observers should receive the same
runtime events.

## Explicit Graceful Shutdown

`Shutdown` is the convenience path. Compose the pieces manually when shutdown
needs service-specific sequencing:

```go
if err := runtime.DrainAllBestEffort(ctx); err != nil {
	logger.Warn("drain failed", "error", err)
}

if err := runtime.WaitAllIdle(ctx); err != nil {
	logger.Warn("idle wait failed", "error", err)
}

if err := runtime.StopAll(ctx); err != nil {
	return err
}
```

Use this when external systems need to be paused, flushed, or closed in a
specific order around Workerkit shutdown.

## Selective Ops HTTP Exposure

`opshttp.Mount` exposes read-only routes by default. Mutating routes are
opt-in.

Use read-only routes for inspection:

- runtime status
- workers
- one worker
- commands

Use command dispatch or lifecycle controls only on protected operations
surfaces. Servekit endpoint options are the right place for HTTP policy.

## Managed Service Versus Manual Service

Use `servekitservice.NewManaged` for the normal service shell. It constructs a
Servekit server, wires readiness, starts workers, and coordinates graceful
shutdown. Servekit owns the HTTP service lifecycle; Workerkit owns worker
runtime semantics.

Use `servekitservice.New` when you already have a Servekit server and want to
bind a Workerkit runtime into it.

Use direct `Runtime` methods when there is no HTTP service.

## Out of Scope

Workerkit intentionally does not provide:

- durable workflow state
- queue storage
- distributed leasing
- task assignment
- cross-service orchestration
- built-in persistence
- business-domain validation

Those systems can exist around Workerkit. Workerkit manages workers inside one
service boundary.

## Recommended Advanced Sequence

1. Build the service with direct runtime usage.
2. Add worker-specific readiness and failure policy.
3. Add commands only for real domain operations.
4. Add bounded retry and concurrency limits.
5. Add `slogobserver` or `otel`.
6. Use `servekitservice.NewManaged` for the service shell.
7. Add read-only `opshttp` routes if operators need HTTP inspection.
8. Add mutating ops routes only with real endpoint policy.

## Related Material

- [`policy.md`](policy.md)
- [`observability.md`](observability.md)
- [`composition.md`](composition.md)
- [`examples/production-composition`](../examples/production-composition)
