# Workerkit Examples

Workerkit is a transport-agnostic worker runtime for Go. These examples are the fastest way to understand what that means in practice — starting from a single worker with graceful shutdown, ending with a production-grade composition that includes retries, concurrency limits, failure policy, structured observability, HTTP ops, and Kubernetes-ready lifecycle management.

Every example is runnable. Every example is part of the public documentation. None of them are smoke tests.

If you want the fuller narrative walkthrough, start with the [Examples Guide](../docs/examples.md).

## The Progression

Read the examples in the order listed below. Each one builds on the last. By the end you will have a complete mental model of how a production Workerkit service is assembled — layer by layer, from the core runtime outward.

### Core Runtime

**[basic](basic)** — The smallest useful Workerkit program. One worker, one runtime, start, status, graceful shutdown. No HTTP, no framework. This is what Workerkit actually is before anything else is layered on.

**[loop-worker](loop-worker)** — The long-running worker loop story. `NewLoopWorker` with a ticker, context cancellation on drain, and clean shutdown. This is the common shape for long-running worker loops.

**[readiness](readiness)** — Running is not the same as ready. This example shows how individual worker readiness rolls up to aggregate runtime readiness, and why that distinction matters for traffic management and health checks.

### Commands, Policy, and Backpressure

**[commands](commands)** — Workers own their own command surface. No HTTP required. Direct dispatch, JSON payload bytes in and out, result handling, and command failure status. This shows that commands are part of the worker runtime, not an HTTP feature.

**[retry-policy](retry-policy)** — Bounded attempts, exponential backoff, full jitter, and retry predicates. This shows the retry posture Workerkit expects: bounded, jittered, and predicate-gated.

**[concurrency-limits](concurrency-limits)** — Layered backpressure. Runtime-wide limits and worker-local limits composing together. This is how you protect a service from being its own thundering herd.

**[failure-policy](failure-policy)** — What happens when a worker fails. Isolated failure, runtime-unready semantics, and runtime-failed policy. This shows how Workerkit makes failure behavior explicit instead of implicit.

### Multi-Worker Composition

**[multi-worker](multi-worker)** — One service boundary, three workers: `ingest`, `index`, `maintenance`. Shared lifecycle, independent inspection, command routing by worker, and aggregate readiness. This is the shape most real services take.

### Testing and Observability

**[testing](testing)** — Workerkit is testable without mocks, HTTP servers, or test harnesses. Fake workers, direct dispatch, status assertions, readiness assertions, and a custom observer in plain Go test code.

**[observability-slog](observability-slog)** — Workerkit emits structured observer events. This example maps them into production-friendly `slog` records: lifecycle transitions, readiness changes, command completions, and failures.

**[observability-otel](observability-otel)** — The same observer events flowing into OpenTelemetry. Command spans with timing and attributes, lifecycle events on parent spans, dispatch counters, duration histograms. The runtime never touches OTel directly — the adapter does.

### Servekit Integration

**[managed-service](managed-service)** — The preferred microservice shell. `servekitservice.NewManaged` wires Workerkit readiness into `/readyz` automatically, starts workers before serving, and drains and stops them gracefully on shutdown. Add application routes through `Service.Server()` while Workerkit owns worker lifecycle.

**[opshttp-basic](opshttp-basic)** — One `opshttp.Mount` call exposes runtime status, worker inspection, command discovery, and readiness over HTTP. Read-only by default. No lifecycle mutation without explicit opt-in.

**[opshttp-commands](opshttp-commands)** — Opt-in HTTP command dispatch. Includes success paths, invalid request handling, missing target mapping, saturation responses, and how to apply endpoint policy to the dispatch route specifically.

**[admin-lifecycle](admin-lifecycle)** — Privileged lifecycle controls over HTTP. Workers registered but not started — started, drained, and stopped through lifecycle routes. Includes placeholder auth, audit, and timeout policy to show where your own policy plugs in.

### The Full Picture

**[production-composition](production-composition)** — The full composition. Multiple workers, readiness, commands, retry, concurrency limits, failure policy, slog observability, Servekit service lifecycle, protected read-only ops, opt-in mutating ops, and graceful shutdown. This is the target shape for a production Workerkit service.

---

## Why This Order

The examples move from the core runtime outward deliberately. HTTP is not where Workerkit starts — it is where it ends up. Workerkit does not require a framework to be useful. The core runtime stands on its own; Servekit integration is an optional service-shell layer.

The progression answers five questions in order:

- "What is the shortest useful Workerkit runtime?"
- "How do lifecycle, readiness, commands, retry, limits, and failure policy work before HTTP enters the picture?"
- "How do I test and observe Workerkit as a transport-agnostic runtime?"
- "How does Servekit expose Workerkit safely as an HTTP service shell?"
- "What does the full production composition look like?"

---

## Run Them

Run examples from the repository root:

```bash
go run ./examples/<name>

# for example
go run ./examples/basic
go run ./examples/commands
go run ./examples/multi-worker
go run ./examples/opshttp-basic
go run ./examples/production-composition

# test-only example
go test ./examples/testing
```

Each example prints its own output and startup notes. The source comments in each `main.go` explain what to look for while it runs.
