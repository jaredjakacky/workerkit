// Package opshttp mounts explicitly enabled Workerkit controls into Servekit.
//
// Register Runtime with Opskit and pass that registry to Servekit with
// servekit.WithOps(...) for /readyz, generic component inspection, and passive
// command inventory. This package does not duplicate those read-only surfaces.
//
// Mount exposes no routes by default. WithCommandDispatchEnabled mounts:
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
// semantics. This package adapts Workerkit command dispatch, lifecycle controls,
// and control errors into a Servekit-native operations surface without making
// HTTP part of the core workerkit runtime.
//
// Use WithEndpointOptions for policy shared by every enabled control route,
// WithCommandDispatchEnabled to expose command dispatch, and
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
//
// Worker and runtime stop routes close command admission but do not wait for or
// cancel commands that were already admitted. For graceful command completion,
// drain through the lifecycle route, inspect the runtime through Servekit's
// generic Opskit component route until InFlight is zero, then call the
// corresponding stop route.
package opshttp
