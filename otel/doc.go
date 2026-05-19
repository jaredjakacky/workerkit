// Package otel adapts Workerkit runtime observations into OpenTelemetry.
//
// Workerkit core emits backend-neutral Observer events. This package implements
// that observer interface with OpenTelemetry traces and metrics while leaving
// SDK setup, exporters, resources, sampling, and provider ownership to the host
// application.
//
// The adapter uses global OpenTelemetry providers by default, matching common
// service setup and Servekit's default integration style. Use WithTracerProvider
// and WithMeterProvider when an application wants explicit providers.
//
// Command observations create a command-scoped context before the worker-owned
// command handler runs, so spans created by application code inside the handler
// can be children of the Workerkit command span.
package otel
