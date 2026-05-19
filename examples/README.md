# Workerkit Examples

This page is the directory index for Workerkit's runnable examples.

These examples are part of the public documentation, not just smoke-test
programs.

Use this page when you want the short version: what examples exist, what each
one demonstrates, and what to run next.

If you want the fuller narrative walkthrough, start with the [Examples Guide](../docs/examples.md).

Read the examples as a progression from the core transport-agnostic Workerkit
runtime outward into lifecycle, readiness, commands, policy, observability, and
Servekit-backed service integration.

## Read Order

1. [basic](basic)
2. [loop-worker](loop-worker)
3. [readiness](readiness)
4. [commands](commands)
5. [retry-policy](retry-policy)
6. [concurrency-limits](concurrency-limits)
7. [failure-policy](failure-policy)
8. [multi-worker](multi-worker)
9. [testing](testing)
10. [observability-slog](observability-slog)
11. [observability-otel](observability-otel)
12. [managed-service](managed-service)
13. [opshttp-basic](opshttp-basic)
14. [opshttp-commands](opshttp-commands)
15. [admin-lifecycle](admin-lifecycle)
16. [production-composition](production-composition)

## What Each Example Shows

- [basic](basic)
  The smallest credible Workerkit program: one custom worker, one runtime,
  registration, startup, status, and graceful shutdown without Servekit.
- [loop-worker](loop-worker)
  The managed long-running loop story: `NewLoopWorker`, ticker-based work,
  context cancellation, status, and clean shutdown.
- [readiness](readiness)
  The focused readiness story: running does not imply ready, and aggregate
  runtime readiness depends on readiness-contributing workers.
- [commands](commands)
  Worker-owned domain commands without HTTP: discovery, direct dispatch, JSON
  payload bytes, result payload bytes, and command failure status.
- [retry-policy](retry-policy)
  Production-grade retry policy: bounded attempts, exponential backoff, full
  jitter, and retry predicates for temporary failures.
- [concurrency-limits](concurrency-limits)
  Layered command backpressure with runtime-wide and worker-local concurrency
  limits.
- [failure-policy](failure-policy)
  How isolated worker failure, runtime-unready failure, and runtime-failed
  policy affect aggregate status.
- [multi-worker](multi-worker)
  One service boundary with `ingest`, `index`, and `maintenance` workers,
  shared lifecycle, independent inspection, command routing, and readiness.
- [testing](testing)
  The test-first story: Workerkit core can be tested directly with fake workers,
  direct dispatch, status assertions, readiness assertions, and a custom
  observer.
- [observability-slog](observability-slog)
  Backend-neutral Workerkit observer events mapped into production-friendly
  structured `slog` records.
- [observability-otel](observability-otel)
  Workerkit observer events flowing into OpenTelemetry spans, events, counters,
  and histograms while the runtime remains transport-agnostic.
- [managed-service](managed-service)
  The preferred microservice shell path with `servekitservice.NewManaged`,
  normal application routes, readiness, signal handling, and graceful shutdown.
- [opshttp-basic](opshttp-basic)
  The first Kit-series HTTP composition: Servekit exposes read-only Workerkit
  runtime status, worker inspection, command discovery, and readiness.
- [opshttp-commands](opshttp-commands)
  Opt-in HTTP command dispatch through `opshttp`, including success, invalid
  requests, missing targets, saturation mapping, and placeholder dispatch
  endpoint policy.
- [admin-lifecycle](admin-lifecycle)
  Privileged worker and runtime lifecycle controls over HTTP with placeholder
  auth, audit, body limit, timeout policy, and workers intentionally left
  registered but not running until started through lifecycle routes.
- [production-composition](production-composition)
  The flagship composition: multiple workers, readiness, commands, retry,
  concurrency, failure policy, slog observability, Servekit service lifecycle,
  protected read-only and mutating ops, and graceful shutdown.

## Why This Structure Exists

The examples are intentionally organized to answer five reader questions:

- "What is the shortest useful Workerkit runtime?"
- "How do lifecycle, readiness, commands, retry, limits, and failure policy work
  before HTTP enters the picture?"
- "How do I test and observe Workerkit as a transport-agnostic runtime?"
- "How does Servekit expose Workerkit safely as an HTTP/service shell?"
- "What does the full production-style composition look like?"

That is why the examples move from core runtime behavior outward instead of
starting with HTTP.

## Run Them

Run examples from the repository root:

```bash
go run ./examples/<name>

# for example
go run ./examples/basic
go run ./examples/commands
go run ./examples/opshttp-basic
go run ./examples/production-composition

# test-only example
go test ./examples/testing
```

Each runnable example prints useful commands or output on startup. The source
comments in each `main.go` explain the purpose of the example and the behavior
worth noticing.
