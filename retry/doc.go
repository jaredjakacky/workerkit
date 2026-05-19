// Package retry provides bounded retry, backoff, and jitter policy primitives
// for Workerkit execution paths.
//
// The package describes retry policy only. Callers that execute retryable work
// own execution, context cancellation, attempt timeouts, and failure handling.
//
// Retries are opt-in. In service meshes and other distributed systems, keep
// attempts bounded, use backoff with jitter, and only retry operations that are
// safe to repeat or are guarded by a retry predicate.
package retry
