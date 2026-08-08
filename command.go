package workerkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	opskit "github.com/jaredjakacky/opskit"
)

var (
	// ErrOpsCommandRejected reports that an Opskit command handler did not
	// accept an admitted Workerkit command invocation.
	ErrOpsCommandRejected = errors.New("opskit command rejected")
	// ErrOpsCommandFailed reports that an Opskit command handler accepted an
	// invocation but returned a failed result.
	ErrOpsCommandFailed = errors.New("opskit command failed")
)

const (
	// FailureCodeOpskitCommandRejected is the default public code for a rejected
	// Opskit command result without an explicit failure code.
	FailureCodeOpskitCommandRejected = "opskit_command_rejected"
	// FailureCodeOpskitCommandFailed is the default public code for a failed
	// Opskit command result without an explicit failure code.
	FailureCodeOpskitCommandFailed = "opskit_command_failed"
	// FailureCodeOpskitResultEncodingFailed is the stable public code used when
	// an Opskit command result cannot be encoded into a Workerkit payload.
	FailureCodeOpskitResultEncodingFailed = "result_encoding_failed"
)

// OpskitCommandError reports an Opskit command result that Workerkit adapted
// into its private command error channel. Failure is a value copy of explicit
// public Opskit failure detail; Message is the command's public result message.
// Use errors.Is with ErrOpsCommandRejected or ErrOpsCommandFailed to identify
// the broad outcome and errors.As to inspect Failure.Code for application retry
// or routing policy.
type OpskitCommandError struct {
	Failure opskit.Failure
	Message string

	kind  error
	cause error
}

// Error implements error using only Opskit public operational text.
func (e *OpskitCommandError) Error() string {
	if e == nil {
		return ""
	}

	detail := e.Failure.Message
	if detail == "" {
		detail = e.Message
	}
	if e.kind == nil {
		if detail != "" {
			return detail
		}
		return "opskit command failed"
	}
	if detail == "" {
		return e.kind.Error()
	}
	return fmt.Sprintf("%s: %s", e.kind, detail)
}

// Unwrap exposes the existing broad Opskit command sentinel.
func (e *OpskitCommandError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.kind
}

// Cause returns a private internal cause when Workerkit itself failed while
// adapting the result. The cause is deliberately excluded from Error and from
// public operational failure detail. Callers must not publish it without an
// application-owned logging or presentation policy.
func (e *OpskitCommandError) Cause() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// OperationalFailure returns the explicit safe Opskit failure presentation.
func (e *OpskitCommandError) OperationalFailure() opskit.Failure {
	if e == nil {
		return opskit.Failure{}
	}
	failure := e.Failure
	if failure.Code == "" {
		switch {
		case errors.Is(e.kind, ErrOpsCommandRejected):
			failure.Code = FailureCodeOpskitCommandRejected
		default:
			failure.Code = FailureCodeOpskitCommandFailed
		}
	}
	if failure.Message == "" {
		failure.Message = e.Message
	}
	if failure.Message == "" {
		if e.kind != nil {
			failure.Message = e.kind.Error()
		} else {
			failure.Message = "opskit command failed"
		}
	}
	return failure
}

func (e *OpskitCommandError) operationalFailure() opskit.Failure {
	return e.OperationalFailure()
}

// CommandRequest is one transport-agnostic worker-owned command invocation.
//
// Worker and Name identify the registered command target. Worker may be a
// runtime-local worker name such as `order-router` or a fully qualified worker
// name such as `trading-core/order-router`. Payload is opaque to Workerkit.
// Adapters and applications own encoding, decoding, and domain semantics.
type CommandRequest struct {
	// Worker is the local or fully qualified worker name.
	Worker string
	// Name is the worker-owned command name.
	Name string
	// Payload is opaque command input.
	Payload []byte
	// RequestedAt is when the command was requested. Dispatch fills it when zero.
	RequestedAt time.Time
}

// Validate reports whether the request is structurally valid for dispatch.
func (cmd CommandRequest) Validate() error {
	if err := ValidateWorkerName(cmd.Worker); err != nil {
		return fmt.Errorf("invalid command target: %w", err)
	}
	if err := ValidateCommandName(cmd.Name); err != nil {
		return fmt.Errorf("invalid command name: %w", err)
	}
	return nil
}

// CommandResult is the transport-agnostic result of one worker-owned command
// invocation. Message and Payload may flow to HTTP responses, admin tools,
// logs, diagnostics, support tools, and tests. Handlers must return only result
// data that is safe for their configured presentation surfaces.
type CommandResult struct {
	// Message is optional public human-readable result text.
	Message string
	// Payload is opaque command output.
	Payload []byte
}

// CommandHandler handles one worker-owned command invocation.
//
// Handlers must honor context cancellation. Command timeouts are delivered
// through ctx.Done(). Workerkit cannot interrupt handlers that block without
// observing the context. Arbitrary returned error text remains private and is
// not copied into status or built-in telemetry. Use WithOperationalFailure only
// for explicit bounded, redacted public detail.
type CommandHandler interface {
	HandleCommand(context.Context, CommandRequest) (CommandResult, error)
}

// CommandHandlerFunc adapts a function into a CommandHandler.
type CommandHandlerFunc func(context.Context, CommandRequest) (CommandResult, error)

// HandleCommand implements CommandHandler.
func (f CommandHandlerFunc) HandleCommand(ctx context.Context, cmd CommandRequest) (CommandResult, error) {
	return f(ctx, cmd)
}

// CommandSpec binds one worker-owned command name to the handler that runs it.
//
// Commands are attached to a worker through WorkerOption values such as
// WithCommand and WithCommandSpec. Runtime lifecycle operations such as Start,
// Stop and worker drain are not CommandSpec values.
type CommandSpec struct {
	// Name is the command name local to its worker.
	Name string
	// Description is optional discovery text for admin and inspection surfaces.
	Description string
	// PayloadKind is advisory Opskit payload metadata. Workerkit does not
	// validate payloads from this value.
	PayloadKind string
	// Dangerous is an advisory hint for presentation and execution layers.
	Dangerous bool
	// Idempotent is an advisory hint. It does not enable command retries.
	Idempotent bool
	// Attributes are optional Opskit discovery metadata.
	Attributes []opskit.Attribute
	// Handler executes the command.
	Handler CommandHandler
}

// Validate reports whether the spec is structurally valid.
func (reg CommandSpec) Validate() error {
	if err := ValidateCommandName(reg.Name); err != nil {
		return fmt.Errorf("invalid command name: %w", err)
	}
	if reg.Handler == nil {
		return fmt.Errorf("command handler must not be nil")
	}
	return nil
}

// CommandInfo is discovery metadata for one registered worker-owned command.
//
// CommandInfo is part of Workerkit's public JSON discovery contract. JSON field
// names and meanings are stable within a major version. Minor versions may add
// fields, so clients should ignore unknown fields.
type CommandInfo struct {
	// Worker is the fully qualified worker name that owns the command.
	Worker string `json:"worker"`
	// Name is the command name local to its worker.
	Name string `json:"name"`
	// Description is optional discovery text.
	Description string `json:"description,omitempty"`
	// PayloadKind is advisory Opskit payload metadata.
	PayloadKind string `json:"payload_kind,omitempty"`
	// Dangerous is an advisory safety hint.
	Dangerous bool `json:"dangerous,omitempty"`
	// Idempotent is an advisory retry-safety hint.
	Idempotent bool `json:"idempotent,omitempty"`
	// Attributes are optional Opskit discovery metadata.
	Attributes []opskit.Attribute `json:"attributes,omitempty"`
}

// CommandFromOpskit adapts one Opskit command descriptor and handler into a
// Workerkit CommandSpec. The returned spec runs through normal Workerkit
// command admission, timeout, retry, concurrency, panic, observation, and
// lifecycle handling.
func CommandFromOpskit(descriptor opskit.CommandDescriptor, handler opskit.CommandHandler) CommandSpec {
	spec := CommandSpec{
		Name:        descriptor.Name,
		Description: descriptor.Description,
		PayloadKind: descriptor.PayloadKind,
		Dangerous:   descriptor.Dangerous,
		Idempotent:  descriptor.Idempotent,
		Attributes:  cloneCommandAttributes(descriptor.Attributes),
	}
	if handler != nil {
		spec.Handler = opskitCommandHandler{handler: handler}
	}
	return spec
}

type opskitCommandHandler struct {
	handler opskit.CommandHandler
}

func (h opskitCommandHandler) HandleCommand(ctx context.Context, req CommandRequest) (CommandResult, error) {
	if h.handler == nil {
		return CommandResult{}, fmt.Errorf("opskit command handler must not be nil")
	}

	requestedAt := req.RequestedAt
	result := h.handler.HandleCommand(ctx, opskit.CommandRequest{
		Name:        req.Name,
		Payload:     json.RawMessage(req.Payload),
		RequestedAt: &requestedAt,
	})

	if err := ctx.Err(); err != nil {
		return CommandResult{}, err
	}
	if result.State == opskit.StateFailed {
		return CommandResult{}, newOpskitCommandError(ErrOpsCommandFailed, result)
	}
	if !result.Accepted {
		return CommandResult{}, newOpskitCommandError(ErrOpsCommandRejected, result)
	}
	if result.Failure != nil && *result.Failure != (opskit.Failure{}) {
		return CommandResult{}, newOpskitCommandError(ErrOpsCommandFailed, result)
	}

	var payload []byte
	if result.Result != nil {
		encoded, err := json.Marshal(result.Result)
		if err != nil {
			return CommandResult{}, newOpskitCommandAdaptationError(
				ErrOpsCommandFailed,
				opskit.Failure{
					Code:    FailureCodeOpskitResultEncodingFailed,
					Message: "opskit command result encoding failed",
				},
				err,
			)
		}
		payload = encoded
	}
	return CommandResult{Message: result.Message, Payload: payload}, nil
}

func newOpskitCommandAdaptationError(kind error, failure opskit.Failure, cause error) error {
	return &OpskitCommandError{
		Failure: failure,
		kind:    kind,
		cause:   cause,
	}
}

func newOpskitCommandError(kind error, result opskit.CommandResult) error {
	adapted := &OpskitCommandError{
		kind:    kind,
		Message: result.Message,
	}
	if result.Failure != nil {
		adapted.Failure = *result.Failure
	}
	return adapted
}

func cloneCommandAttributes(attributes []opskit.Attribute) []opskit.Attribute {
	if len(attributes) == 0 {
		return nil
	}
	return append([]opskit.Attribute(nil), attributes...)
}
