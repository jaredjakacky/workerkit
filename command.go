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
// invocation.
type CommandResult struct {
	// Message is optional human-readable result text.
	Message string
	// Payload is opaque command output.
	Payload []byte
}

// CommandHandler handles one worker-owned command invocation.
//
// Handlers must honor context cancellation. Command timeouts are delivered
// through ctx.Done(). Workerkit cannot interrupt handlers that block without
// observing the context.
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
	if result.State == opskit.StateFailed || result.Error != "" {
		return CommandResult{}, newOpskitCommandError(ErrOpsCommandFailed, result)
	}
	if !result.Accepted {
		return CommandResult{}, newOpskitCommandError(ErrOpsCommandRejected, result)
	}

	var payload []byte
	if result.Result != nil {
		encoded, err := json.Marshal(result.Result)
		if err != nil {
			return CommandResult{}, fmt.Errorf("%w: marshal result: %v", ErrOpsCommandFailed, err)
		}
		payload = encoded
	}
	return CommandResult{Message: result.Message, Payload: payload}, nil
}

func newOpskitCommandError(kind error, result opskit.CommandResult) error {
	detail := result.Error
	if detail == "" {
		detail = result.Message
	}
	if detail == "" {
		return kind
	}
	return fmt.Errorf("%w: %s", kind, detail)
}

func cloneCommandAttributes(attributes []opskit.Attribute) []opskit.Attribute {
	if len(attributes) == 0 {
		return nil
	}
	return append([]opskit.Attribute(nil), attributes...)
}
