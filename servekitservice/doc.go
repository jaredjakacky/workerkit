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
// NewManaged is a convenience constructor for services that want this package
// to construct the Servekit server. It registers Workerkit with Opskit so
// Servekit can include Workerkit readiness in /readyz. Applications composing
// multiple Opskit components should provide their shared registry with
// WithOpsRegistry. Use New when the application already owns Servekit
// construction, and construct that server with servekit.WithOps.
package servekitservice
