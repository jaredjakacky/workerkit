package opshttp

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit-incubator"
)

type dispatchRequest struct {
	Worker  string          `json:"worker"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type dispatchResponse struct {
	Worker string            `json:"worker"`
	Name   string            `json:"name"`
	Result commandResultBody `json:"result"`
}

type commandResultBody struct {
	Message string          `json:"message,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// registerCommandRoutes mounts the command dispatch endpoint when explicitly
// enabled.
//
// Command dispatch is an opt-in mutating route, so it uses dispatch-specific
// endpoint options in addition to the shared operations route options.
func registerCommandRoutes(server *servekit.Server, runtime *workerkit.Runtime, cfg config) {
	server.Handle(http.MethodPost, routePath(cfg.prefix, commandDispatchRoute), func(r *http.Request) (any, error) {
		req, err := decodeDispatchRequest(r)
		if err != nil {
			return nil, err
		}

		command := workerkit.CommandRequest{
			Worker:  req.Worker,
			Name:    req.Name,
			Payload: []byte(req.Payload),
		}
		if err := command.Validate(); err != nil {
			return nil, badRequestError(err.Error())
		}

		result, err := runtime.Dispatch(r.Context(), command)
		if err != nil {
			return nil, mapDispatchError(runtime, command.Worker, command.Name, err)
		}
		workerName := command.Worker
		if descriptor, ok := runtime.Worker(command.Worker); ok {
			workerName = descriptor.QualifiedName
		}
		responseResult, err := encodeCommandResult(result)
		if err != nil {
			return nil, err
		}
		return dispatchResponse{
			Worker: workerName,
			Name:   command.Name,
			Result: responseResult,
		}, nil
	}, dispatchEndpointOptions(cfg)...)
}

// decodeDispatchRequest reads the HTTP JSON shape accepted by the dispatch
// endpoint and rejects unknown fields or multiple JSON objects.
func decodeDispatchRequest(r *http.Request) (dispatchRequest, error) {
	var req dispatchRequest
	return req, decodeStrictJSON(r, &req, "dispatch request")
}

func encodeCommandResult(result workerkit.CommandResult) (commandResultBody, error) {
	body := commandResultBody{
		Message: result.Message,
	}
	if len(result.Payload) == 0 {
		return body, nil
	}
	if !json.Valid(result.Payload) {
		return commandResultBody{}, servekit.Error(
			http.StatusInternalServerError,
			"command result payload must be valid JSON",
			nil,
		)
	}
	body.Payload = json.RawMessage(result.Payload)
	return body, nil
}

// mapDispatchError gives Workerkit command failures stable HTTP meanings for
// the Servekit operations surface.
func mapDispatchError(runtime *workerkit.Runtime, workerName, commandName string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, workerkit.ErrWorkerNotFound):
		return notFoundError("worker", workerName)
	case errors.Is(err, workerkit.ErrCommandNotFound):
		return notFoundError("command", commandName)
	case errors.Is(err, workerkit.ErrRuntimeNotAcceptingWork):
		return servekit.Error(http.StatusServiceUnavailable, err.Error(), err)
	case errors.Is(err, workerkit.ErrWorkerNotAcceptingWork), errors.Is(err, workerkit.ErrInvalidWorkerState):
		return servekit.Error(http.StatusConflict, err.Error(), err)
	case errors.Is(err, workerkit.ErrRuntimeSaturated), errors.Is(err, workerkit.ErrWorkerSaturated):
		return servekit.Error(http.StatusTooManyRequests, err.Error(), err)
	case containsWorker(workerName, runtime):
		// If the runtime did not expose a sentinel error, preserve lookup
		// semantics before returning the original error.
		if containsCommand(workerName, commandName, runtime) {
			return err
		}
		return notFoundError("command", commandName)
	default:
		return notFoundError("worker", workerName)
	}
}

func containsWorker(name string, runtime *workerkit.Runtime) bool {
	_, ok := runtime.Worker(name)
	return ok
}

func containsCommand(workerName, commandName string, runtime *workerkit.Runtime) bool {
	commands, ok := runtime.Commands(workerName)
	if !ok {
		return false
	}
	for _, command := range commands {
		if command.Name == commandName {
			return true
		}
	}
	return false
}
