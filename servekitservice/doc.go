// Package servekitservice wires Workerkit Runtime and Servekit Server together
// for the common microservice execution path.
//
// The package starts workers before serving, runs Servekit, and gracefully
// drains/stops workers when the service exits. Graceful worker shutdown is
// bounded by a service-level timeout unless disabled with WithShutdownTimeout.
//
// Workerkit opshttp routes are disabled by default. Enable them intentionally
// with WithOpsHTTPEnabled(true), and use WithOpsHTTPOptions to apply the
// authentication, authorization, middleware, and endpoint policy required by
// the deployment.
//
// NewManaged is preferred for Kubernetes and microservice services. It
// constructs Servekit with Workerkit readiness already wired into /readyz. Use
// New when the application already owns Servekit construction, and construct
// that server with ReadinessOptions(runtime) or
// servekit.WithReadinessChecks(servekitservice.ReadinessCheck(runtime)).
package servekitservice
