# Commands

Workerkit commands are worker-owned domain operations. They are not lifecycle
controls, and they are not inherently HTTP.

The same command can be dispatched directly from Go, tested without network
infrastructure, or exposed later through `opshttp`.

## Command Ownership

A command belongs to one worker. The worker owns:

- the command name
- the payload contract
- the result contract
- validation
- side effects
- domain error meaning

Workerkit owns:

- command registration
- discovery
- routing
- lifecycle admission checks
- retry policy
- timeout policy
- concurrency limits
- command failure recording
- observer events

## Register a Command

Use `WithCommand` for a simple command:

```go
runtime.Register(spec,
	workerkit.WithCommand("refresh", workerkit.CommandHandlerFunc(func(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
		return workerkit.CommandResult{Message: "refreshed"}, nil
	})),
)
```

Use `WithCommandSpec` when discovery should include a description:

```go
runtime.Register(spec,
	workerkit.WithCommandSpec(workerkit.CommandSpec{
		Name:        "rebuild",
		Description: "rebuilds the local index",
		Handler:     rebuildHandler,
	}),
)
```

## Discover Commands

`Runtime.Commands` returns registered command metadata for one worker:

```go
commands, ok := runtime.Commands("index")
if !ok {
	return errors.New("worker not found")
}
```

Command discovery is useful for CLIs, operations planes, and tests. Discovery
does not execute anything.

## Dispatch Directly

`Runtime.Dispatch` routes a command to a worker:

```go
result, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
	Worker:  "index",
	Name:    "rebuild",
	Payload: []byte(`{"since":"2026-01-01T00:00:00Z"}`),
})
```

The payload is opaque to Workerkit. If it is JSON, the worker chooses to treat
it as JSON. Workerkit stores and forwards bytes.

The result payload is also opaque:

```go
return workerkit.CommandResult{
	Message: "index rebuilt",
	Payload: []byte(`{"documents":1200}`),
}, nil
```

## Command Errors

A command handler error records command failure status for the worker and emits
observer events. It does not automatically fail the worker lifecycle.

This is intentional. A failed `refresh` or `rebuild` command does not always
mean the background worker itself is broken.

If a command failure reflects worker health and should affect lifecycle or
aggregate runtime status, the command handler can get the worker-scoped runtime
handle with `WorkerRuntimeFromContext(ctx)` and call `ReportFailure(err)`.
Returned command errors alone are recorded as command failures; they do not
automatically move the worker lifecycle to failed.

## Admission and Saturation

Dispatch must pass runtime and worker checks:

- target worker exists
- command exists
- runtime is accepting work
- worker lifecycle accepts command dispatch
- worker is accepting work
- runtime concurrency limit has capacity
- worker concurrency limit has capacity

Saturation errors are explicit:

- `ErrRuntimeSaturated`
- `ErrWorkerSaturated`

`opshttp` maps saturation to HTTP `429 Too Many Requests`.

## Retry

Command retry is opt-in:

```go
runtime.Register(spec,
	workerkit.WithWorkerCommandRetry(
		retry.AttemptsIf(3,
			retry.Exponential(50*time.Millisecond, 2, time.Second),
			retry.Full(),
			isTemporary,
		),
	),
)
```

Only retry commands that are safe to repeat. Use predicates to reject permanent
failures.

Command timeouts are cooperative: handlers receive a context deadline and must
observe `ctx.Done()` for timeout or cancellation to take effect.

## HTTP Exposure

Core commands are transport-neutral.

`opshttp` adapts them to HTTP when mounted into Servekit:

- `GET /admin/commands?worker=...` discovers commands
- `POST /admin/commands/dispatch` dispatches commands when enabled

Command dispatch over HTTP is opt-in because it can mutate state or trigger
domain work. Protect it with authentication, authorization, and audit logging.

## Examples

- [`examples/commands`](../examples/commands)
- [`examples/retry-policy`](../examples/retry-policy)
- [`examples/concurrency-limits`](../examples/concurrency-limits)
- [`examples/opshttp-commands`](../examples/opshttp-commands)
