# Examples Guide

Workerkit's examples are a guided product tour, not a random pile of demos.
Start with the smallest useful runtime, then move outward into managed loops,
readiness, Opskit execution hooks, commands, execution policy, observability,
Servekit presentation, and optional Workerkit-specific HTTP controls.

If you want the short directory index instead, use
[Workerkit Examples](../examples/README.md).

Run examples from the repository root. Most examples are runnable programs:

```bash
go run ./examples/<name>
```

The test example is intentionally test-only:

```bash
go test ./examples/testing
```

## Start here

### [`examples/basic`](../examples/basic)

**What it demonstrates:** The smallest credible Workerkit program: `New`,
`Register`, `StartAll`, runtime and worker status, and `Shutdown` with one
custom `Worker`.

**Run it:**

```bash
go run ./examples/basic
```

**Expected output:** The worker starts, the runtime reports `running` and
`ready=true`, the worker reports `running`, then shutdown stops the worker and
the runtime reports `stopped`.

**What to notice:** The worker owns only domain behavior. The runtime owns
lifecycle, status, readiness aggregation, and graceful shutdown.

**What this example intentionally does not show:** Servekit, HTTP, commands,
retry, observability, readiness warmup, or long-running loop cancellation.

### [`examples/loop-worker`](../examples/loop-worker)

**What it demonstrates:** `NewLoopWorker` as the managed alternative to an
unmanaged background goroutine.

**Run it:**

```bash
go run ./examples/loop-worker
```

**Expected output:** The worker processes a few ticker batches, prints status
while running, observes cancellation during shutdown, and exits cleanly.

**What to notice:** Workerkit owns lifecycle and cancellation. The loop owns
domain work and must observe `ctx.Done()`.

**What this example intentionally does not show:** Manual goroutine ownership,
commands, retry, Servekit, or HTTP.

### [`examples/readiness`](../examples/readiness)

**What it demonstrates:** Lifecycle and readiness are separate. A worker can be
running before it is ready.

**Run it:**

```bash
go run ./examples/readiness
```

**Expected output:** Initial status shows workers running but not all ready.
After warmup, the readiness-contributing worker reports ready and aggregate
runtime readiness changes.

**What to notice:** `WorkerRuntime.SetReady(false)` and
`WorkerRuntime.SetReady(true)` are worker-owned readiness signals.
`WithWorkerReadinessContribution(false)` keeps optional workers from deciding
aggregate readiness.

**What this example intentionally does not show:** Servekit `/readyz`, command
dispatch, retry policy, or HTTP operations routes.

## Core runtime operations

### [`examples/commands`](../examples/commands)

**What it demonstrates:** Worker-owned domain commands without HTTP.

**Run it:**

```bash
go run ./examples/commands
```

**Expected output:** The example lists registered commands, dispatches JSON
payload bytes directly through `Runtime.Dispatch`, prints JSON result payload
bytes, then shows a command error recorded in worker status without failing the
worker lifecycle.

**What to notice:** Commands are transport-neutral. Workerkit routes and
observes commands, but the worker owns payloads, result payloads, validation,
and domain meaning.

**What this example intentionally does not show:** HTTP command dispatch,
Servekit, retry, or lifecycle controls.

### [`examples/opskit-checks`](../examples/opskit-checks)

**What it demonstrates:** Workerkit periodically executing generic Opskit
`Checker` and `CheckGroup` hooks as managed workers.

**Run it:**

```bash
go run ./examples/opskit-checks
```

**Expected output:** The single check and grouped checks run immediately, their
observers print results, and the passive Opskit registry reports component and
runtime state.

**What to notice:** Opskit defines the contracts and stores passive component
inventory. Workerkit owns scheduling, timeout, cancellation, panic recovery,
and readiness integration. Checked components are informational in the Opskit
registry, and the group worker does not contribute to Workerkit readiness, so
the database is not counted as a readiness gate multiple times.

**What this example intentionally does not show:** Servekit presentation,
external dependencies, retry, or checks that ignore context cancellation.

### [`examples/opskit-command`](../examples/opskit-command)

**What it demonstrates:** Executing an `opskit.CommandHandler` through the
generic `workerkit.CommandFromOpskit` adapter.

**Run it:**

```bash
go run ./examples/opskit-command
```

**Expected output:** The example prints Opskit descriptor metadata discovered
through Workerkit, then dispatches an Opskit command and prints its structured
result as a JSON Workerkit payload.

**What to notice:** Opskit defines the command descriptor and handler hook but
does not invoke it. Workerkit owns registration, admission, execution policy,
and observation. No domain-specific Workerkit adapter is required.

**What this example intentionally does not show:** HTTP presentation, retries,
or asynchronous accepted work.

### [`examples/retry-policy`](../examples/retry-policy)

**What it demonstrates:** Predicate-gated command retry with bounded attempts,
exponential backoff, and full jitter.

**Run it:**

```bash
go run ./examples/retry-policy
```

**Expected output:** A temporary command failure retries before succeeding. A
permanent command failure stops without retry.

**What to notice:** `retry.AttemptsIf` receives max attempts, backoff, jitter,
and a retry predicate. The predicate decides which failures are safe to repeat.

**Why this matters in production:** Retries should be opt-in, bounded, jittered,
and limited to failures that are actually temporary. Broad retry can amplify
incidents or repeat unsafe side effects.

**What this example intentionally does not show:** HTTP, Servekit, lifecycle
retry, or durable retries across process restarts.

### [`examples/concurrency-limits`](../examples/concurrency-limits)

**What it demonstrates:** Layered command backpressure with runtime-wide and
worker-local command concurrency limits.

**Run it:**

```bash
go run ./examples/concurrency-limits
```

**Expected output:** Slow commands occupy command slots. Additional dispatches
are rejected with `ErrRuntimeSaturated` or `ErrWorkerSaturated`, and status
shows `InFlight` while work is active.

**What to notice:** A dispatch must pass both the runtime-wide gate and the
worker-local gate.

**Why this matters in production:** Runtime limits protect the service boundary.
Worker-local limits protect one worker from being overwhelmed by expensive
commands.

**What this example intentionally does not show:** HTTP saturation mapping,
Servekit, retry, or lifecycle controls.

### [`examples/failure-policy`](../examples/failure-policy)

**What it demonstrates:** How worker failures affect aggregate runtime status.

**Run it:**

```bash
go run ./examples/failure-policy
```

**Expected output:** Three labeled scenarios show
`FailurePolicyIsolate`, `FailurePolicyMarkRuntimeUnready`, and
`FailurePolicyFailRuntime`, followed by runtime and worker status.

**What to notice:** A worker failure can be isolated, force readiness down, or
move aggregate runtime state to failed depending on policy.

**Why this matters in production:** Not every worker has the same importance.
Failure policy lets optional and critical workers communicate different
service-level impact.

**What this example intentionally does not show:** Servekit readiness, HTTP,
retry, or command routing.

### [`examples/multi-worker`](../examples/multi-worker)

**What it demonstrates:** One runtime as a service boundary containing
`ingest`, `index`, and `maintenance` workers.

**Run it:**

```bash
go run ./examples/multi-worker
```

**Expected output:** Workers start in registration order, status lists each
worker independently, commands route to different workers, and shutdown stops
the runtime.

**What to notice:** A runtime gives multiple workers shared lifecycle and
aggregate readiness while preserving independent worker inspection and command
routing.

**What this example intentionally does not show:** Servekit, HTTP operations,
retry policy, or observability adapters.

## Testing and observability

### [`examples/testing`](../examples/testing)

**What it demonstrates:** Workerkit core can be tested directly without HTTP,
Servekit, Kubernetes, or external infrastructure.

**Run it:**

```bash
go test ./examples/testing
```

**Expected output:** Go test output should pass for `examples/testing`.

**What to notice:** Tests can create a runtime, register a fake worker, dispatch
commands directly, assert status and readiness, and attach a custom observer.

**What this example intentionally does not show:** HTTP boundary tests,
Servekit integration, or external telemetry infrastructure.

### [`examples/observability-slog`](../examples/observability-slog)

**What it demonstrates:** Backend-neutral Workerkit observer events mapped into
structured `slog` records.

**Run it:**

```bash
go run ./examples/observability-slog
```

**Expected output:** JSON-style log records for lifecycle transitions, readiness
changes, command success, command failure, and background worker failure.

**What to notice:** Observability is core-runtime behavior. Servekit is not
required for worker lifecycle, command, readiness, or failure events.

**What this example intentionally does not show:** HTTP request logs,
OpenTelemetry, or an external log backend.

### [`examples/observability-otel`](../examples/observability-otel)

**What it demonstrates:** Workerkit observer events flowing into the first-party
OpenTelemetry adapter.

**Run it:**

```bash
go run ./examples/observability-otel
```

**Expected output:** Local stdout-style provider output for command spans, span
events, command retry failure, counters, and histograms.

**What to notice:** Dispatch IDs belong on spans and events, not metric labels.
The example stays copyable without requiring an external OpenTelemetry
collector or telemetry backend.

**What this example intentionally does not show:** Servekit, HTTP tracing, a
real collector, or production OpenTelemetry exporter setup.

## Service shell examples

### [`examples/managed-service`](../examples/managed-service)

**What it demonstrates:** The preferred microservice shell path with
`servekitservice.NewManaged`.

**Run it:**

```bash
go run ./examples/managed-service
```

**Routes:**

- `GET /app/status`
- `GET /readyz`
- `GET /admin/runtime` should return `404` because ops HTTP is disabled

**Curl commands:**

```bash
curl -i http://localhost:8080/app/status
curl -i http://localhost:8080/readyz
curl -i http://localhost:8080/admin/runtime
```

**Expected output:** The service starts the worker through the managed service
lifecycle, serves the app route and readiness route, and reports 404 for ops
HTTP.

**What to notice:** `NewManaged` wires Workerkit lifecycle into Servekit's
service shell and exposes Workerkit readiness to `/readyz` through Opskit. It
does not make Workerkit own HTTP serving.

**Security note:** Ops HTTP is disabled by default. Enable it intentionally
only on an appropriate operations surface.

**What this example intentionally does not show:** Read-only ops routes,
command dispatch, lifecycle controls, or custom ops endpoint policy.

## Optional Workerkit HTTP controls

The following examples are intentionally outside the primary composition
progression. Use them when operators need Workerkit-specific inspection,
command dispatch, or privileged lifecycle controls. Generic readiness still
flows from the Opskit registry into Servekit.

### [`examples/opshttp-basic`](../examples/opshttp-basic)

**What it demonstrates:** Workerkit-specific read-only operations routes through
`opshttp`. In composed Kit Series services, prefer Opskit admin component routes
for generic read-only inspection.

**Run it:**

```bash
go run ./examples/opshttp-basic
```

**Routes:**

- `GET /admin/runtime`
- `GET /admin/workers`
- `GET /admin/worker?name=ops_demo/indexer`
- `GET /admin/commands?worker=ops_demo/indexer`
- `GET /readyz`

**Curl commands:**

```bash
curl -s http://localhost:8080/admin/runtime
curl -s http://localhost:8080/admin/workers
curl -s 'http://localhost:8080/admin/worker?name=ops_demo/indexer'
curl -s 'http://localhost:8080/admin/commands?worker=ops_demo/indexer'
curl -i http://localhost:8080/readyz
```

**Expected output:** The program prints the same curl commands at startup.
Read-only routes return runtime status, worker inspection, and command
discovery. `/readyz` reflects Workerkit runtime readiness through the
Opskit registry.

**What to notice:** Workerkit owns runtime status and readiness semantics.
Servekit owns HTTP routing, readiness endpoints, and endpoint policy. Opskit is
the preferred registry path when you want generic Kit Series admin routes.

**Security note:** Read-only operations routes still expose operational state.
Mount them only on an appropriate operations surface.

**What this example intentionally does not show:** HTTP command dispatch,
admin lifecycle controls, app routes, or mutating operations.

### [`examples/opshttp-commands`](../examples/opshttp-commands)

**What it demonstrates:** Opt-in HTTP command dispatch through `opshttp`.

**Run it:**

```bash
go run ./examples/opshttp-commands
```

**Routes:**

- `POST /admin/commands/dispatch`
- read-only `opshttp` routes from `opshttp.Mount`
- `GET /readyz`

**Curl commands:**

```bash
# 200: successful dispatch
curl -i -X POST http://localhost:8080/admin/commands/dispatch \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Token: dev-secret' \
  -d '{"worker":"cache","name":"cache/put","payload":{"key":"homepage","value":"warm"}}'

# 400: invalid request
curl -i -X POST http://localhost:8080/admin/commands/dispatch \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Token: dev-secret' \
  -d '{"worker":"","name":"cache/put"}'

# 404: missing worker
curl -i -X POST http://localhost:8080/admin/commands/dispatch \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Token: dev-secret' \
  -d '{"worker":"missing","name":"cache/put"}'

# 404: missing command
curl -i -X POST http://localhost:8080/admin/commands/dispatch \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Token: dev-secret' \
  -d '{"worker":"cache","name":"cache/missing"}'

# 429: saturation; run the slow command in one terminal,
# then run the second command in another terminal before it finishes
curl -i -X POST http://localhost:8080/admin/commands/dispatch \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Token: dev-secret' \
  -d '{"worker":"cache","name":"cache/slow","payload":{"sleepMillis":5000}}'

curl -i -X POST http://localhost:8080/admin/commands/dispatch \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Token: dev-secret' \
  -d '{"worker":"cache","name":"cache/put","payload":{"key":"homepage","value":"warm"}}'
```

**Expected output:** Successful dispatch returns `200` with the command result.
Invalid requests return `400`, missing workers and commands return `404`, and
worker saturation returns `429` through `opshttp`.

**What to notice:** Core Workerkit commands stay transport-neutral. `opshttp`
adapts command dispatch to HTTP and `WithDispatchOptions` applies dispatch-only
endpoint policy.

**Security note:** Command dispatch is mutating and intentionally opt-in. This
example uses a development-only `X-Ops-Token` gate and timeout to show where
real authentication, authorization, and audit policy belong. The read-only
`opshttp` routes are still exposed by default and should be mounted only on an
appropriate operations surface.

**What this example intentionally does not show:** Admin lifecycle controls or
production-grade authz.

### [`examples/admin-lifecycle`](../examples/admin-lifecycle)

**What it demonstrates:** Privileged Workerkit lifecycle controls exposed
through Servekit-backed `opshttp`.

**Run it:**

```bash
go run ./examples/admin-lifecycle
```

**Routes:**

- read-only `opshttp` routes from `opshttp.Mount`
- `POST /admin/workers/start`
- `POST /admin/workers/drain`
- `POST /admin/workers/stop`
- `POST /admin/runtime/start`
- `POST /admin/runtime/drain`
- `POST /admin/runtime/stop`
- `GET /readyz`

**Curl commands:**

```bash
curl -i http://localhost:8080/readyz
curl -i -X POST http://localhost:8080/admin/workers/start \
  -H 'Content-Type: application/json' \
  -H 'X-Admin-Token: dev-secret' \
  -d '{"name":"ingest"}'
curl -i -X POST http://localhost:8080/admin/runtime/start \
  -H 'X-Admin-Token: dev-secret'
curl -i -X POST http://localhost:8080/admin/runtime/drain \
  -H 'X-Admin-Token: dev-secret'
```

**Expected output:** Startup output explains that workers are registered but not
running. `/readyz` may fail until workers are started through lifecycle routes.
Authorized admin operations requests return `200`; missing or wrong tokens
return `401`. Lifecycle requests print audit logs and worker start/stop
messages.

**What to notice:** This example intentionally does not call
`runtime.StartAll` before serving. The lifecycle routes are the point of the
example.

**Security note:** The `/admin` operations surface exposes operational state and
privileged lifecycle controls. Do not expose it publicly. Protect it with real
authentication, authorization, audit logging, request limits, and route-specific
timeouts.

**What this example intentionally does not show:** Normal application routes,
command dispatch, or production authz.

## Full Kit Series composition

### [`examples/production-composition`](../examples/production-composition)

**What it demonstrates:** The flagship Kit Series composition in one copyable
service skeleton.

**Run it:**

```bash
go run ./examples/production-composition
```

**Architecture overview:** One Workerkit runtime contains `ingest`, `index`,
and `maintenance` workers. Workerkit owns worker lifecycle, readiness,
commands, retry, concurrency, failure policy, and `slog` observer events.
Opskit carries the component/readiness/inspection contract. Servekit owns HTTP
serving, app routes, readiness endpoints, endpoint policy, and shutdown.

**Routes:**

- `GET /app/status`
- `GET /app/search?q=...`
- `GET /readyz`
- `GET /admin/components`
- `GET /admin/components/catalog_service`
- `POST /admin/commands/dispatch`

**Curl commands:**

```bash
curl -i http://localhost:8080/app/status
curl -i http://localhost:8080/readyz
curl -s -H 'X-Ops-Token: dev-secret' http://localhost:8080/admin/components
curl -s -H 'X-Ops-Token: dev-secret' http://localhost:8080/admin/components/catalog_service
curl -i -X POST http://localhost:8080/admin/commands/dispatch \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Token: dev-secret' \
  -d '{"worker":"ingest","name":"ingest/enqueue","payload":{"documentID":"doc-123","title":"Workerkit"}}'
curl -i -X POST http://localhost:8080/admin/commands/dispatch \
  -H 'Content-Type: application/json' \
  -H 'X-Ops-Token: dev-secret' \
  -d '{"worker":"index","name":"index/rebuild"}'
```

**Expected output:** Workers warm up, structured logs describe runtime and
worker events, app routes return service state, and read-only ops routes expose
Workerkit inspection through Opskit. Authorized admin operations return success
responses; missing or wrong `X-Ops-Token` values return `401`.

**What to notice:** This is the full boundary story. Workerkit owns worker
semantics; Opskit owns the shared operational registry contract; Servekit owns
HTTP and service semantics. `opshttp` is used only for explicitly enabled
command dispatch; privileged lifecycle controls remain disabled.

In Kubernetes, these routes are pod-local. `/admin/components/catalog_service`
shows the runtime in the pod that handled the request, and command dispatch
affects only that pod unless you route directly to a specific replica or build a
separate control plane.

**Security note:** The token gate and audit middleware are placeholders.
Production deployments must use real authentication, authorization, audit
logging, request limits, route-specific timeouts, and network exposure policy
for the `/admin` operations surface.

**What this example intentionally does not show:** External infrastructure,
durable queues, durable workflow state, a real auth system, or a production
OpenTelemetry backend.
