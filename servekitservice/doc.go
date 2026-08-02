// Package servekitservice coordinates Workerkit lifecycle around an
// application-owned Servekit server.
//
// The package starts workers before serving, runs Servekit, and gracefully
// drains/stops workers when the service exits. Graceful worker shutdown is
// bounded by a service-level timeout unless disabled with WithShutdownTimeout.
//
// Applications construct the shared Opskit registry and Servekit server
// explicitly, then pass the server to New. This package does not create a
// private registry, register components, or mount HTTP controls. Use
// servekit.WithOps for passive presentation and mount opshttp separately only
// when privileged Workerkit controls are required.
package servekitservice
