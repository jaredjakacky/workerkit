# Composition with Opskit and Servekit

Workerkit, Opskit, and Servekit are separate kits with separate responsibilities.

Workerkit owns worker runtime semantics. Opskit owns the common operational
component registry and read models. Servekit owns HTTP and service semantics.

## Responsibility Boundary

Workerkit owns:

- worker lifecycle
- readiness aggregation
- status snapshots
- worker-owned commands
- command retry
- command concurrency limits
- failure policy
- observer events
- scheduled execution of Opskit check and check-group hooks
- execution of active Opskit command handlers through `CommandFromOpskit`

Servekit owns:

- HTTP server construction
- route registration
- endpoint policy
- readiness endpoints
- middleware
- auth gates
- request limits
- response encoding
- HTTP shutdown

Opskit is the primary composed Kit Series bridge for read-only operational
state. `Runtime` implements Opskit component, readiness, and inspection
contracts directly. Register the runtime as one Opskit component and pass the
registry to Servekit:

```go
ops := opskit.NewRegistry()

runtime, err := workerkit.New(workerkit.Identity{Name: "workers"})
if err != nil {
	return err
}

ops.MustRegister(runtime, opskit.Required())

server := servekit.New(
	servekit.WithOps(ops, servekit.WithOpsAdmin()),
)
```

Servekit then uses the registry for `/readyz` and generic admin component
routes such as:

- `GET /admin/components`
- `GET /admin/components/{name}`

This is pod-local/runtime-local state. It describes the Workerkit runtime in
this process; it is not a distributed worker registry.

## Active Opskit Execution

Opskit defines component metadata and read models together with explicit
`Checker`, `CheckGroup`, and `CommandHandler` execution hooks. Its registry can
discover those capabilities, but it does not schedule or invoke them.

Workerkit owns that active execution:

- `NewCheckLoop` turns one `opskit.Checker` into a periodically managed worker.
- `NewCheckGroupLoop` turns one `opskit.CheckGroup` into a periodically managed worker.
- `CommandFromOpskit` turns one descriptor and `opskit.CommandHandler` into a normal Workerkit command spec.

The check constructors preserve Workerkit lifecycle, cancellation, cooperative
timeouts, jitter, panic recovery, readiness, and failure reporting. The command
adapter preserves Workerkit dispatch admission, retry, concurrency, timeout,
panic, observation, and lifecycle behavior. Domain kits therefore implement
Opskit contracts instead of maintaining pairwise Workerkit adapters.

A checked component remains responsible for caching and exposing its component
status. Decide explicitly how readiness should be represented when both that
component and its Workerkit check-loop worker are registered. Usually one is the
required readiness signal and the other is optional or informational; making
both required can duplicate the same dependency gate.

Check timeouts are cooperative and cannot interrupt implementations that ignore
context cancellation, but results returned after the deadline are not applied
to readiness. When readiness management is enabled, the worker becomes unready
and the loop continues unless failure-on-not-ready is enabled. Result and
summary observers still receive the completed late value. Group-worker
readiness contains only the aggregate
summary; retain per-check detail in the checked component or through
`WithCheckSummaryObserver`.

## Kubernetes and Multiple Replicas

Workerkit runtime state is process-local. In Kubernetes, that means it is
pod-local.

`/readyz`, Opskit admin component views, Workerkit inspection, command dispatch,
and lifecycle controls describe or affect only the Workerkit runtime in the
process that handles the request.

When a Service load-balances requests across replicas:

- `/readyz` answers whether this pod is ready.
- `/admin/components/{name}` shows this pod's runtime state.
- `opshttp` command dispatch hits one replica.
- `opshttp` lifecycle controls start, drain, or stop workers in one replica.

Workerkit does not provide deployment-wide worker orchestration, distributed
locking, leader election, replica coordination, or fleet-wide command
broadcast.

Workerkit is safe in multiple replicas when work distribution is handled by
queues, streams, leases, databases, partition ownership, or idempotent
reconciliation. Singleton workers require explicit external coordination such
as a Kubernetes Lease, database lock, queue partition ownership, or a
controller.

Cluster-wide controls require a separate control-plane design. Do not assume an
HTTP request through a Kubernetes Service controls every pod in a Deployment.

## Managed Service Path

Use `servekitservice.NewManaged` when a Workerkit runtime belongs inside a
Servekit-backed HTTP service:

```go
ops := opskit.NewRegistry()

service, err := servekitservice.NewManaged(runtime,
	servekitservice.WithOpsRegistry(ops, servekit.WithOpsAdmin()),
	servekitservice.WithServekitOptions(
		servekit.WithAddr(":8080"),
	),
)
if err != nil {
	return err
}

server := service.Server()
```

`NewManaged` runs the Servekit service shell and coordinates Workerkit startup
and graceful shutdown. It wires Workerkit readiness into Servekit through
Opskit. Workerkit still owns worker semantics; Servekit still owns HTTP serving
and `/readyz`.

If `WithOpsRegistry` is omitted, `NewManaged` creates a private Opskit registry
for convenience. Composed Kit Series services should pass the application's
shared registry so Workerkit, Configkit, Clientkit, Dependkit, and other
components appear in one Opskit read model.

Do not pre-register the Workerkit runtime in that registry when using
`NewManaged`; `NewManaged` registers it as a required Opskit component.

`Service.Server()` exposes the Servekit server so the application can register
normal HTTP routes.

## Readiness and Read-Only Inspection

The preferred path is Opskit:

- `Runtime.Status(ctx)` maps Workerkit lifecycle into Opskit's generic state vocabulary.
- `Runtime.Readiness(ctx)` returns Workerkit's cached aggregate readiness.
- `Runtime.Inspect(ctx)` returns safe Workerkit runtime and worker details for generic admin routes.

Servekit owns the HTTP endpoint that reports readiness. Workerkit owns the
readiness semantics. Opskit is the shared contract between them.

`servekitservice.ReadinessOptions(runtime)` also returns a Servekit option that
creates a small private Opskit registry for Workerkit-only readiness. The older
`opshttp.ReadinessCheck(runtime)` adapter remains available for standalone
Servekit users who do not want an Opskit registry.

## Optional Workerkit-Specific HTTP Operations

Use `opshttp.Mount` when you need Workerkit-specific HTTP routes. These are not
the primary composed read-only path, but they remain useful for command dispatch
and privileged lifecycle controls.

`opshttp.Mount` exposes read-only Workerkit-specific routes by default:

- `GET /admin/runtime`
- `GET /admin/workers`
- `GET /admin/worker?name=...`
- `GET /admin/commands?worker=...`

Read-only routes still expose operational information. Prefer generic Opskit
admin routes for composed services, and mount `opshttp` only when these
Workerkit-specific routes are useful.

## Command Dispatch

Opskit components can expose command descriptors and handler hooks without
depending on Workerkit. Opskit defines those contracts but does not invoke the
handler. `workerkit.CommandFromOpskit` binds one descriptor and handler into the
normal Workerkit command registry. This avoids pairwise Workerkit adapters in
domain kits while keeping routing, lifecycle admission,
timeouts, retries, concurrency, panic recovery, and observation in Workerkit.

The native Workerkit `CommandHandler` remains available for Workerkit-specific
or non-JSON command contracts. The Opskit adapter coexists with it.

HTTP command dispatch is opt-in:

```go
opshttp.Mount(server, runtime,
	opshttp.WithCommandDispatchEnabled(),
)
```

This enables:

- `POST /admin/commands/dispatch`

Core Workerkit commands are transport-neutral. `opshttp` owns HTTP decoding,
status codes, and response shape. Workerkit owns command routing and policy.

Protect command dispatch with authentication, authorization, and audit logging
in real deployments. In Kubernetes, command dispatch through a Service affects
whichever pod receives the request unless you route directly to a specific pod
or build a separate coordination plane.

## Admin Lifecycle Controls

Privileged lifecycle controls are also opt-in:

```go
opshttp.Mount(server, runtime,
	opshttp.WithAdminLifecycleControlsEnabled(),
)
```

This enables:

- `POST /admin/workers/start`
- `POST /admin/workers/drain`
- `POST /admin/workers/stop`
- `POST /admin/runtime/start`
- `POST /admin/runtime/drain`
- `POST /admin/runtime/stop`

These routes mutate runtime state. Do not expose them publicly. Protect them
with authentication, authorization, request limits, route-specific timeouts,
and audit logging. These are pod-local controls, not Deployment-wide controls.

The stop routes do not wait for or cancel commands already in flight. For a
graceful HTTP sequence, drain, poll the worker or runtime status until
`inFlight` is zero, then stop.

## Endpoint Policy

Servekit endpoint options belong at the HTTP boundary:

```go
opshttp.Mount(server, runtime,
	opshttp.WithEndpointOptions(
		servekit.WithAuthGate(requireOpsCaller),
		servekit.WithEndpointTimeout(10*time.Second),
	),
	opshttp.WithDispatchOptions(
		servekit.WithBodyLimit(1 << 20),
	),
	opshttp.WithLifecycleOptions(
		servekit.WithBodyLimit(1 << 20),
	),
)
```

Keep authentication, authorization, audit logging, request limits, and
route-specific timeouts in Servekit. Use `WithEndpointOptions` for policy that
belongs on the whole operations surface, including read-only inspection routes.
Keep worker semantics in Workerkit.

## When to Enable Ops HTTP

Enable read-only ops routes when operators need HTTP inspection.

Enable command dispatch only when remote command execution is a real
operational need and the route can be protected.

Enable admin lifecycle routes only for privileged operations surfaces.

Leave ops HTTP disabled for simple services that only need app routes and
readiness.

## Examples

- [`examples/managed-service`](../examples/managed-service)
- [`examples/opskit-checks`](../examples/opskit-checks)
- [`examples/opskit-command`](../examples/opskit-command)
- [`examples/production-composition`](../examples/production-composition)

Optional Workerkit-specific HTTP controls:

- [`examples/opshttp-basic`](../examples/opshttp-basic)
- [`examples/opshttp-commands`](../examples/opshttp-commands)
- [`examples/admin-lifecycle`](../examples/admin-lifecycle)
