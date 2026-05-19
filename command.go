package workerkit

import (
	"context"
	"fmt"
	"time"
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
}
