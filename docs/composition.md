# Composition with Servekit

Workerkit and Servekit are separate kits with separate responsibilities.

Workerkit owns worker runtime semantics. Servekit owns HTTP and service
semantics.

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

`opshttp` is the bridge between the kits. It exposes Workerkit operations
through Servekit without making HTTP part of Workerkit core.

## Managed Service Path

Use `servekitservice.NewManaged` when a Workerkit runtime belongs inside a
Servekit-backed HTTP service:

```go
service, err := servekitservice.NewManaged(runtime,
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
and graceful shutdown. Workerkit still owns worker semantics; Servekit still
owns HTTP serving and `/readyz`.

`Service.Server()` exposes the Servekit server so the application can register
normal HTTP routes.

## Readiness

Workerkit readiness reaches Servekit through:

- `servekitservice.NewManaged`
- `servekitservice.ReadinessOptions`
- `opshttp.ReadinessCheck`

The meaning of readiness still comes from Workerkit. Servekit owns the HTTP
endpoint that reports it.

## Read-Only Operations

`opshttp.Mount` exposes read-only operations routes by default:

- `GET /admin/runtime`
- `GET /admin/workers`
- `GET /admin/worker?name=...`
- `GET /admin/commands?worker=...`

Read-only routes still expose operational information. Mount them only on an
appropriate operations surface.

## Command Dispatch

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
in real deployments.

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
and audit logging.

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
- [`examples/opshttp-basic`](../examples/opshttp-basic)
- [`examples/opshttp-commands`](../examples/opshttp-commands)
- [`examples/admin-lifecycle`](../examples/admin-lifecycle)
- [`examples/production-composition`](../examples/production-composition)
