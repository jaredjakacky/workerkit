// Package opshttp mounts Workerkit's Servekit-backed HTTP operations surface.
//
// By default, Mount adds these routes under DefaultPrefix:
//
//	GET  /admin/runtime
//	GET  /admin/workers
//	GET  /admin/worker?name=runtime/worker
//	GET  /admin/commands?worker=runtime/worker
//
// WithCommandDispatchEnabled also mounts:
//
//	POST /admin/commands/dispatch
//
// WithAdminLifecycleControlsEnabled also mounts these privileged lifecycle
// routes:
//
//	POST /admin/workers/start
//	POST /admin/workers/drain
//	POST /admin/workers/stop
//	POST /admin/runtime/start
//	POST /admin/runtime/drain
//	POST /admin/runtime/stop
//
// WithPrefix changes the route prefix.
// WithLifecycleTimeout changes the timeout used by lifecycle routes.
//
// Servekit owns HTTP service construction, middleware, authentication,
// readiness endpoints, request policy, and lifecycle. Workerkit owns runtime
// semantics. This package adapts Workerkit readiness, status, worker
// inspection, command discovery, command dispatch, and command errors into a
// Servekit-native operations surface without making HTTP part of the core
// workerkit runtime.
//
// Use Mount to add Workerkit routes to a Servekit server, ReadinessCheck to
// wire Workerkit readiness into Servekit, WithEndpointOptions for shared route
// policy, WithCommandDispatchEnabled to expose command dispatch, and
// WithAdminLifecycleControlsEnabled to expose lifecycle controls.
// WithDispatchOptions and WithLifecycleOptions apply stricter policy to those
// mutating route groups.
//
// Command dispatch accepts payload as raw JSON and forwards those bytes to the
// worker-owned command handler as workerkit.CommandRequest.Payload. Workerkit
// does not interpret the payload. Command dispatch responses expose
// workerkit.CommandResult.Payload as raw JSON. Non-empty response payloads must
// contain valid JSON.
//
// Lifecycle mutations are detached from HTTP client disconnect cancellation, but
// receive a cooperative context deadline from WithLifecycleTimeout unless the
// timeout is explicitly disabled. Worker code must observe ctx.Done() for that
// deadline to take effect.
package opshttp
