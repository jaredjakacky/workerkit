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

## Execute Opskit Commands

Use `CommandFromOpskit` when a component already implements Opskit's command
descriptor and handler contracts. Opskit defines these contracts but does not
invoke the handler:

```go
runtime.Register(spec,
	workerkit.WithCommandSpec(workerkit.CommandFromOpskit(
		opskit.CommandDescriptor{
			Name:        "cache/refresh",
			Description: "refresh cache entries",
			PayloadKind: "cache_refresh",
			Idempotent:  true,
		},
		cache,
	)),
)
```

The adapter translates the request and marshals `opskit.CommandResult.Result`
as JSON into Workerkit's result payload. It returns errors wrapping
`ErrOpsCommandRejected` or `ErrOpsCommandFailed` for rejected and failed Opskit
results. If the command context is canceled, the context error takes precedence.

An accepted Opskit result, including `AcceptedCommand` for asynchronous work,
is a successful Workerkit dispatch. Workerkit releases its in-flight slot when
the handler returns; it does not track work that the Opskit handler continues in
another goroutine or external system.

Opskit descriptor metadata is available through Workerkit discovery.
`Dangerous` and `Idempotent` are advisory: they do not authorize execution or
enable retries. Authentication, authorization, and retry policy remain explicit
application and Workerkit configuration.

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

Opskit rejected and failed results become command handler errors through
`CommandFromOpskit`, so they follow the same failure recording, observation,
and configured retry path. A rejected result means the domain handler declined
work after Workerkit admitted the dispatch; it is distinct from Workerkit's
lifecycle and concurrency admission errors.

An admitted command may call `ReportFailure` while Stop is running. Workerkit
records that worker health failure without replacing the stopping lifecycle. A
command calling through the same handle after Stop completes, or after the
worker restarts into a new generation, receives `ErrInvalidWorkerState` and
cannot mutate the stopped or restarted worker.

Command panics are also recorded in `LastCommandFailure` and emit a panic
failure event. A panic from the current worker generation moves a running or
draining worker to `failed`.

Stop closes command admission but does not wait for admitted commands or cancel
their contexts. If an admitted command returns an error or panics after Stop has
begun or completed, Workerkit preserves the stopping/stopped lifecycle, updates
`LastCommandFailure`, and emits the failure event. Successful completion only
releases the in-flight slot. A command retained from an older generation cannot
mutate the restarted worker's status, though its failure event is still emitted.
Until that handler exits, it continues to count toward the restarted worker's
and runtime's `InFlight`, concurrency limits, and idle waits.

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

This is especially important for Opskit adapters: JSON result encoding happens
after the domain handler returns. An encoding failure is reported as
`ErrOpsCommandFailed` and may be retried by the configured policy even though
the handler may already have produced side effects. The descriptor's
`Idempotent` field is only metadata and does not alter retry behavior.

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
