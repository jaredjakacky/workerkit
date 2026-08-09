package workerkit

import (
	"context"
	"errors"

	opskit "github.com/jaredjakacky/opskit"
)

const (
	// FailureCodeWorkerFailed is the default public code for a worker lifecycle
	// or background failure whose private cause has no explicit presentation.
	FailureCodeWorkerFailed = "worker_failed"
	// FailureCodeLoopCleanupFailed is the default public code for a LoopWorker
	// cleanup attempt whose private cause has no explicit presentation.
	FailureCodeLoopCleanupFailed = "loop_cleanup_failed"
	// FailureCodeCommandFailed is the default public code for a command failure
	// whose private cause has no explicit presentation.
	FailureCodeCommandFailed = "command_failed"
	// FailureCodeDeadlineExceeded is the public code for context deadline expiry.
	FailureCodeDeadlineExceeded = "deadline_exceeded"
	// FailureCodeCanceled is the public code for context cancellation.
	FailureCodeCanceled = "canceled"
	// FailureCodePanic is the public code for a recovered worker or command panic.
	FailureCodePanic = "panic"
)

var (
	workerFailure = opskit.Failure{
		Code:    FailureCodeWorkerFailed,
		Message: "worker operation failed",
	}
	loopCleanupFailure = opskit.Failure{
		Code:    FailureCodeLoopCleanupFailed,
		Message: "loop worker cleanup failed",
	}
	commandFailure = opskit.Failure{
		Code:    FailureCodeCommandFailed,
		Message: "command failed",
	}
)

type loopCleanupError struct {
	cause error
}

func newLoopCleanupError(cause error) error {
	if cause == nil {
		return nil
	}
	if _, ok := cause.(*loopCleanupError); ok {
		return cause
	}
	return &loopCleanupError{cause: cause}
}

func (e *loopCleanupError) Error() string {
	if e == nil || e.cause == nil {
		return loopCleanupFailure.Message
	}
	return e.cause.Error()
}

func (e *loopCleanupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *loopCleanupError) operationalFailure() opskit.Failure {
	if e == nil {
		return loopCleanupFailure
	}
	if explicit := safeExplicitOperationalFailure(e.cause); explicit != (opskit.Failure{}) {
		return explicit
	}
	return loopCleanupFailure
}

func safeExplicitOperationalFailure(err error) (failure opskit.Failure) {
	defer func() {
		if recover() != nil {
			failure = opskit.Failure{}
		}
	}()
	var provider operationalFailureProvider
	if errors.As(err, &provider) {
		if explicit := provider.operationalFailure(); explicit != (opskit.Failure{}) {
			return explicit
		}
	}
	return opskit.Failure{}
}

// WithOperationalFailure associates an explicit safe public presentation with
// a private cause. The returned error formats only failure.Message and unwraps
// to cause so errors.Is and errors.As continue to work.
//
// Code and Message flow to Workerkit status, observer events, logs, telemetry,
// diagnostics, support tools, and tests. They must be bounded and safe to
// expose. The cause remains available to direct error consumers and observer
// implementations through errors.Unwrap or an event's Cause field; it must not
// be published without application-owned policy.
func WithOperationalFailure(cause error, failure opskit.Failure) error {
	if failure == (opskit.Failure{}) {
		return cause
	}
	return &operationalFailureError{
		failure: failure,
		cause:   cause,
	}
}

type operationalFailureError struct {
	failure opskit.Failure
	cause   error
}

func (e *operationalFailureError) Error() string {
	if e == nil {
		return ""
	}
	if e.failure.Message != "" {
		return e.failure.Message
	}
	if e.failure.Code != "" {
		return e.failure.Code
	}
	return "operational failure"
}

func (e *operationalFailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *operationalFailureError) operationalFailure() opskit.Failure {
	if e == nil {
		return opskit.Failure{}
	}
	return e.failure
}

type operationalFailureProvider interface {
	operationalFailure() opskit.Failure
}

func operationalFailure(err error, fallback opskit.Failure) (failure opskit.Failure) {
	if err == nil {
		return opskit.Failure{}
	}
	failure = fallback
	defer func() {
		if recover() != nil {
			failure = fallback
		}
	}()

	var provider operationalFailureProvider
	if errors.As(err, &provider) {
		if explicit := provider.operationalFailure(); explicit != (opskit.Failure{}) {
			return explicit
		}
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return opskit.Failure{
			Code:    FailureCodeDeadlineExceeded,
			Message: "operation deadline exceeded",
		}
	case errors.Is(err, context.Canceled):
		return opskit.Failure{
			Code:    FailureCodeCanceled,
			Message: "operation canceled",
		}
	default:
		return fallback
	}
}
