// Package workerkit provides a small runtime for managing domain workers inside
// one service boundary.
//
// Applications register workers that own their business logic. Workerkit owns
// the operational shell around those workers: lifecycle control, readiness,
// command dispatch, execution policy, status snapshots, and telemetry hooks.
// Workerkit does not interpret worker payloads, enforce domain rules, or
// provide durable workflow state.
//
// Workerkit names are operational identifiers, not display labels. Runtime
// names and worker local names are flat identifiers using lowercase ASCII
// letters, digits, '-' and '_' only. Workerkit constructs qualified worker
// names as runtime/worker. Worker-owned command names are path names made from
// the same identifier segments, such as cache/refresh or queue/drain. These
// rules keep logs, metrics, traces, operations APIs, config, and command routing
// predictable without escaping or normalization.
package workerkit
