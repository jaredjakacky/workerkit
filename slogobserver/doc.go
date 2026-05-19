// Package slogobserver provides a log/slog-backed Workerkit observer.
//
// It is intended as a small first-party diagnostics adapter for applications
// that want structured Workerkit lifecycle, readiness, failure, and command
// completion logs without configuring an OpenTelemetry exporter.
package slogobserver
