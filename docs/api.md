# API Map

This is the fast way to orient yourself in Workerkit's public API.

It covers the exported surface of the root `workerkit` package and the first-party subpackages: `opshttp`, `servekitservice`, `retry`, `otel`, and `slogobserver`.

Go doc comments remain the canonical symbol-level reference. This file is the companion view that groups the exported surface by the decisions you make when using the package.

If you only remember the common path, remember this:

- `New(...)` creates one runtime for a service boundary
- `Register(...)` attaches workers and their operational policy
- `StartAll(...)` and `Shutdown(...)` cover the core runtime lifecycle
- `servekitservice.NewManaged(...)` wires Workerkit into the common Servekit service lifecycle
- `WithCommand(...)` and `WithCommandSpec(...)` expose worker-owned operations
- `opshttp.Mount(...)` adds an optional Servekit-backed operations plane

Everything else in this file exists to customize that path without turning workers into a framework-specific application model.

## Package `workerkit`

### Start here

- `Runtime`

  The main runtime object. It owns worker registration, lifecycle control, worker-owned command dispatch, readiness aggregation, status snapshots, failure policy, concurrency limits, retry execution, and observer callbacks inside one service boundary.

- `New(...)`

  Creates a runtime with a validated `Identity` and production-oriented defaults: bounded lifecycle and command attempt timeouts, no retries unless configured, panic recovery, isolated worker failure policy, readiness derived from readiness-contributing workers, and no command concurrency caps unless configured.

- `Identity`

  The runtime identity used for worker qualification, status, telemetry, and operations surfaces. Runtime names are operational identifiers, not display names.

- `Identity.Validate()`

  Validates the runtime identity.

- `RuntimeOption`

  Runtime-wide configuration hook applied during `New(...)`. Runtime defaults are copied into each worker at registration time.

### Worker model

- `Worker`

  The lifecycle contract managed by the runtime.

  Shape:

  ```go
  type Worker interface {
      Start(context.Context) error
      Stop(context.Context) error
  }
  ```

- `WorkerSpec`

  Registration metadata for one worker: local name, optional description, and worker implementation.

- `WorkerOption`

  Per-worker configuration hook applied during `Register(...)`. Worker options override runtime defaults for that registered worker.

- `Register(...)`

  Adds a worker to the runtime while the runtime is still in the registered state. The runtime qualifies the local worker name as `runtime/worker`.

- `WorkerRuntime`

  Worker-scoped runtime handle available in contexts passed to `Start`, `Stop`, and command handlers. It lets worker code inspect its own status, set readiness, control Workerkit command admission, and report asynchronous failures without receiving full runtime authority.

- `WorkerRuntimeFromContext(...)`

  Extracts the worker-scoped handle from a managed worker or command context.

### Lifecycle control

- `Start(...)`

  Starts one registered, stopped, or failed worker. It applies start timeout, start retry, panic policy, and startup readiness/accepting-work defaults.

- `StartAll(...)`

  Starts registered workers in registration order. It is fail-fast and does not roll back partial startup.

- `Drain(...)`

  Marks one running worker as draining, unready, and not accepting new Workerkit command dispatches.

- `DrainAll(...)`

  Drains running workers in registration order and returns on the first error.

- `DrainAllBestEffort(...)`

  Attempts to drain all running workers and returns the combined error when any drain fails.

- `WaitIdle(...)`

  Waits until one worker has no in-flight commands.

- `WaitAllIdle(...)`

  Waits until the runtime has no in-flight commands.

- `Stop(...)`

  Stops one running, draining, or failed worker. Stop does not wait for in-flight commands; compose `Drain`, `WaitIdle`, and `Stop` when graceful command drain is required.

- `StopAll(...)`

  Stops registered workers in reverse registration order and continues after individual stop failures.

- `Shutdown(...)`

  Convenience graceful shutdown path for non-HTTP callers. It drains all workers best-effort, waits for runtime idle, then stops all workers using the caller's context.

### Commands

- `CommandSpec`

  Full command registration shape with name, optional description, and handler. Use this when command discovery should be useful to operators.

- `CommandSpec.Validate()`

  Validates command name syntax and handler presence.

- `WithCommand(...)`

  Registers one simple worker-owned command by name and handler.

- `WithCommandSpec(...)`

  Registers one full `CommandSpec`, including optional discovery text.

- `CommandHandler`

  Handles one worker-owned command invocation.

  Shape:

  ```go
  type CommandHandler interface {
      HandleCommand(context.Context, CommandRequest) (CommandResult, error)
  }
  ```

- `CommandHandlerFunc`

  Adapts a function into `CommandHandler`.

- `CommandHandlerFunc.HandleCommand(...)`

  Implements `CommandHandler`.

- `CommandRequest`

  Transport-neutral command input: worker target, command name, opaque payload bytes, and request time.

- `CommandRequest.Validate()`

  Validates command target and command name syntax.

- `CommandResult`

  Transport-neutral command output: optional message and opaque payload bytes.

- `Dispatch(...)`

  Routes one command to a registered worker command handler. It validates the target, checks lifecycle and accepting-work state, enforces runtime and worker concurrency limits, applies command timeout and retry policy, records command failures, and emits command observations.

- `Commands(...)`

  Returns registered command discovery metadata for one worker in stable name order.

### Status and discovery

- `RuntimeStatus()`

  Returns aggregate runtime status.

- `Identity()`

  Returns the runtime identity used for qualification, telemetry, and operations surfaces.

- `RuntimeStatus`

  Runtime-level operational snapshot. It includes runtime name, aggregate lifecycle state, readiness, in-flight command count, registered worker count, and last aggregate lifecycle transition.

- `Workers()`

  Returns worker inspection snapshots in registration order.

- `Worker(...)`

  Returns one worker inspection snapshot by local or fully qualified name.

- `WorkerSnapshot`

  Worker registration metadata plus current `WorkerStatus`.

- `WorkerStatus`

  Worker-level operational snapshot. It separates lifecycle state, readiness, accepting-work state, in-flight command count, last lifecycle transition, last worker failure, and last command failure.

- `LifecycleTransition`

  The most recent lifecycle transition recorded in a status snapshot.

- `FailureInfo`

  The most recent worker lifecycle or background failure recorded in worker status.

- `CommandFailureInfo`

  The most recent command handler returned error recorded in worker status.

- `CommandInfo`

  Discovery metadata for one registered worker-owned command.

`RuntimeStatus`, `WorkerSnapshot`, `WorkerStatus`, `CommandInfo`, and nested status structs are public JSON contracts because `opshttp` returns them directly. JSON field names and meanings are stable within a major version. Minor versions may add fields, so clients should ignore unknown fields.

### Lifecycle states

- `LifecycleState`

  Shared lifecycle state type used by workers and runtime aggregate status.

- `StateRegistered`

  Known to the runtime but not started.

- `StateStarting`

  Transitioning into service.

- `StateRunning`

  Active.

- `StateDraining`

  Alive but refusing new Workerkit command dispatches.

- `StateStopping`

  Actively shutting down.

- `StateStopped`

  Intentionally shut down.

- `StateFailed`

  Normal execution cannot continue without intervention.

### Runtime options

#### Runtime-wide policy

- `WithRuntimeCommandConcurrency(limit int)`

  Caps total concurrent command executions across the runtime. Zero or negative leaves the runtime-wide cap unbounded.

- `WithReadinessPolicy(policy ReadinessPolicy)`

  Sets how runtime readiness is derived from worker readiness and lifecycle state.

- `WithObserver(observer Observer)`

  Sets the transport-neutral observer hook. Nil installs a no-op observer, and non-nil observers are wrapped so telemetry panics do not escape runtime paths.

#### Default worker policy

Default worker options are copied into each worker when it is registered. Later changes to runtime defaults do not mutate already-registered workers.

- `WithDefaultStartTimeout(timeout time.Duration)`

  Sets the default per-attempt `Start` timeout.

- `WithDefaultStopTimeout(timeout time.Duration)`

  Sets the default per-attempt `Stop` timeout.

- `WithDefaultCommandTimeout(timeout time.Duration)`

  Sets the default per-attempt command timeout.

- `WithDefaultStartRetry(policy retry.Policy)`

  Sets the default `Start` retry policy. Configure this only when `Worker.Start` is safe to call again after failure.

- `WithDefaultCommandRetry(policy retry.Policy)`

  Sets the default command retry policy for command handler returned errors.

- `WithDefaultWorkerCommandConcurrency(limit int)`

  Sets the default per-worker command concurrency cap.

- `WithDefaultPanicPolicy(policy PanicPolicy)`

  Sets the default panic handling policy.

- `WithDefaultFailurePolicy(policy FailurePolicy)`

  Sets the default worker failure policy.

- `WithDefaultReadyOnStart(ready bool)`

  Sets the default ready state assigned after successful `Start`.

- `WithDefaultAcceptingWorkOnStart(accepting bool)`

  Sets the default command admission state assigned after successful `Start`.

- `WithDefaultWorkerReadinessContribution(contributes bool)`

  Sets whether workers contribute to aggregate runtime readiness by default.

### Worker options

- `WithWorkerStartTimeout(timeout time.Duration)`

  Overrides the `Start` timeout for one worker.

- `WithWorkerStopTimeout(timeout time.Duration)`

  Overrides the `Stop` timeout for one worker.

- `WithWorkerCommandTimeout(timeout time.Duration)`

  Overrides the command timeout for one worker.

- `WithWorkerStartRetry(policy retry.Policy)`

  Overrides the `Start` retry policy for one worker.

- `WithWorkerCommandRetry(policy retry.Policy)`

  Overrides command retry policy for one worker.

- `WithWorkerCommandConcurrency(limit int)`

  Caps concurrent command executions for one worker.

- `WithWorkerPanicPolicy(policy PanicPolicy)`

  Overrides panic policy for one worker.

- `WithWorkerFailurePolicy(policy FailurePolicy)`

  Overrides failure policy for one worker.

- `WithWorkerReadyOnStart(ready bool)`

  Overrides the ready state assigned after one worker starts.

- `WithWorkerAcceptingWorkOnStart(accepting bool)`

  Overrides whether one worker accepts Workerkit command dispatches after start.

- `WithWorkerReadinessContribution(contributes bool)`

  Controls whether one worker contributes to aggregate runtime readiness.

### Policy types

- `ReadinessPolicy`

  Controls runtime readiness derivation.

- `ReadyWhenContributingWorkersReady`

  Runtime readiness requires all readiness-contributing workers to be running and ready. This is the default.

- `ReadyWhenAllWorkersReady`

  Runtime readiness requires every registered worker to be running and ready.

- `PanicPolicy`

  Controls how the runtime treats panics inside managed `Start`, `Stop`, and command paths.

- `PanicPolicyRecover`

  Recover, record the panic as failure, and apply failure policy. This is the default.

- `PanicPolicyCrash`

  Surface the panic after best-effort failure handling so the process can crash.

- `FailurePolicy`

  Controls how worker failure affects the runtime.

- `FailurePolicyIsolate`

  Only the failing worker moves to failed. This is the default.

- `FailurePolicyMarkRuntimeUnready`

  Keeps the process alive but forces runtime readiness down.

- `FailurePolicyFailRuntime`

  Forces aggregate runtime state to failed and stops runtime command admission.

### Observability

- `Observer`

  Backend-neutral runtime telemetry hook. The method set is intended to remain stable within a major version; future telemetry details should usually be added as fields on existing event structs.

- `TransitionEvent`

  One worker or runtime lifecycle transition.

- `CommandStartEvent`

  Start of one command dispatch. Observers may return a derived context and command observation.

- `CommandObservation`

  Receives the final command dispatch observation.

- `CommandEndEvent`

  End of one command dispatch. Includes final success/failure, duration, dispatch id, and attempt count.

- `FailureEvent`

  One worker lifecycle, background, command, or panic failure. Command retry failures include dispatch id and attempt number.

- `ReadinessEvent`

  One worker or runtime readiness change.

- `NopObserver`

  Discards all telemetry callbacks.

- `NopCommandObservation`

  Discards command end observations.

- `CommandObservationFunc`

  Adapts a function into `CommandObservation`.

- `MultiObserver(...)`

  Fans telemetry out to multiple observers and recovers panics from child observers.

- `SafeObserver(...)`

  Wraps an observer so telemetry panics do not escape runtime lifecycle or command dispatch paths.

### Loop worker

- `LoopWorker`

  Worker implementation for long-running background loops.

- `LoopWorker.Start(...)`

  Starts the loop worker and launches the managed loop goroutine.

- `LoopWorker.Stop(...)`

  Cancels the managed loop and waits for it to exit.

- `LoopFunc`

  Long-running function managed by `LoopWorker`.

  Shape:

  ```go
  func(context.Context, WorkerRuntime) error
  ```

- `NewLoopWorker(...)`

  Constructs a loop-backed worker. Auto-ready is enabled by default.

- `LoopWorkerOption`

  Configures a `LoopWorker`.

- `WithLoopStart(...)`

  Sets an optional hook that runs before the loop goroutine starts.

- `WithLoopStop(...)`

  Sets an optional cleanup hook that runs after the loop goroutine stops.

- `WithLoopAutoReady(enabled bool)`

  Controls whether `Start` marks the worker ready after launching the loop goroutine. Disable this when readiness depends on domain warmup inside the loop.

- `ErrLoopExitedUnexpectedly`

  Reports that a loop returned nil before `Stop` canceled it.

- `ErrLoopWorkerActive`

  Reports that `Start` found an existing loop lifecycle in progress.

### Opskit check loops

- `NewCheckLoop(...)`

  Constructs a worker that periodically executes one `opskit.Checker`.
  Workerkit owns background execution policy; the checked component remains
  responsible for any cached dependency health state.

- `NewCheckGroupLoop(...)`

  Constructs a worker that periodically executes one `opskit.CheckGroup`.

- `CheckLoopOption`

  Configures an Opskit check loop worker.

- `WithCheckInterval(...)`

  Sets the steady-state interval between check executions.

- `WithCheckInitialDelay(...)`

  Delays the first check loop action after `Start`.

- `WithCheckRunImmediately(...)`

  Controls whether the loop executes once before waiting for the first interval.

- `WithCheckTimeout(...)`

  Sets a per-execution timeout.

- `WithCheckJitter(...)`

  Sets an optional function that adjusts each interval wait.

- `WithCheckReadyOnSuccess(...)`

  Controls whether ready check results mark the worker ready and not-ready
  results mark it unready.

- `WithCheckReportFailureOnNotReady(...)`

  Controls whether not-ready check results are also reported as Workerkit worker
  failures. Disabled by default.

- `WithCheckResultObserver(...)`

  Observes completed single-check results.

- `WithCheckSummaryObserver(...)`

  Observes completed check-group summaries.

- `ErrNilChecker`

  Reports that a check loop was constructed without a checker.

- `ErrNilCheckGroup`

  Reports that a check group loop was constructed without a group.

### Name validation

- `ValidateRuntimeName(...)`

  Validates runtime operational identifiers.

- `ValidateWorkerLocalName(...)`

  Validates local worker names.

- `ValidateQualifiedWorkerName(...)`

  Validates fully qualified worker names in `runtime/worker` form.

- `ValidateWorkerName(...)`

  Validates either local or fully qualified worker identifiers.

- `ValidateCommandName(...)`

  Validates path-like worker-owned command names.

### Errors

- `ErrNilWorker`

  Registration rejected a nil worker.

- `ErrWorkerAlreadyRegistered`

  A worker with that qualified name is already registered.

- `ErrWorkerNotFound`

  A worker lookup or command target did not exist.

- `ErrCommandAlreadyRegistered`

  A command with that worker-local name is already registered.

- `ErrCommandNotFound`

  A command lookup did not exist.

- `ErrInvalidWorkerState`

  The requested lifecycle or command operation is not valid for the worker's current state.

- `ErrRuntimeNotAcceptingWork`

  The runtime is not accepting command dispatches.

- `ErrWorkerNotAcceptingWork`

  The worker is not accepting command dispatches.

- `ErrRuntimeSaturated`

  Runtime command concurrency capacity is exhausted.

- `ErrWorkerSaturated`

  Worker command concurrency capacity is exhausted.

## Package `opshttp`

`opshttp` is the optional Servekit-backed HTTP operations plane for Workerkit.

### Mounting

- `Mount(...)`

  Adds Workerkit operations routes to an existing Servekit server.

- `Option`

  Configures the mounted operations routes.

- `DefaultPrefix`

  Default route prefix: `/admin`.

- `ErrNilRuntime`

  The caller provided a nil Workerkit runtime.

- `ErrNilServer`

  The caller provided a nil Servekit server.

### Readiness

- `ReadinessCheck(...)`

  Deprecated compatibility adapter for Servekit readiness checks. Prefer
  registering the runtime with Opskit and passing the registry to Servekit with
  `servekit.WithOps`.

### Route groups

By default, `Mount(...)` adds only read-only routes:

- `GET /admin/runtime`
- `GET /admin/workers`
- `GET /admin/worker?name=runtime/worker`
- `GET /admin/commands?worker=runtime/worker`

Even the read-only routes expose operational state, worker names, command inventory, and failure information, so mount them only on an appropriate operations surface.

Command dispatch is mutating and opt-in:

- `WithCommandDispatchEnabled()`

  Mounts `POST /admin/commands/dispatch`.

Lifecycle controls are privileged and opt-in:

- `WithAdminLifecycleControlsEnabled()`

  Mounts worker and runtime start, drain, and stop routes.

### Route policy

- `WithPrefix(prefix string)`

  Changes the operations route prefix. Empty input mounts at root.

- `WithEndpointOptions(opts ...servekit.EndpointOption)`

  Applies Servekit endpoint options to every mounted Workerkit route.

- `WithDispatchOptions(opts ...servekit.EndpointOption)`

  Applies Servekit endpoint options only to command dispatch routes.

- `WithLifecycleOptions(opts ...servekit.EndpointOption)`

  Applies Servekit endpoint options only to lifecycle control routes.

- `WithLifecycleTimeout(timeout time.Duration)`

  Sets the timeout for lifecycle control operations. Zero keeps the default, and negative disables this opshttp timeout.

## Package `servekitservice`

`servekitservice` wires Workerkit and Servekit together for the common microservice execution path. The managed path registers the Workerkit runtime with Opskit so Servekit can consume readiness and inspection through `servekit.WithOps`.

### Constructors

- `NewManaged(...)`

  Constructs a service with a Servekit server and Workerkit readiness wired into `/readyz` through Opskit. This is the preferred helper when a Workerkit runtime belongs inside a Servekit service shell and the application does not need to build its own Opskit registry.

- `New(...)`

  Wraps an existing Servekit server. In composed Kit Series services, construct the server with an Opskit registry that contains the Workerkit runtime and pass it with `servekit.WithOps(...)`.

- `Service`

  Owns the common Workerkit plus Servekit microservice lifecycle.

- `Server()`

  Returns the Servekit server owned by the service so callers using `NewManaged` can register application routes before `Run`.

### Running

- `Run(...)`

  Starts workers, runs Servekit, and performs graceful worker shutdown when configured.

- `ReadinessOptions(...)`

  Returns Servekit options that register Workerkit runtime readiness with Servekit through Opskit.

- `ReadinessCheck(...)`

  Deprecated compatibility adapter for standalone Servekit readiness checks when an Opskit registry is not used.

### Options

- `WithServekitOptions(opts ...servekit.Option)`

  Appends options used when `NewManaged` constructs the Servekit server. `New` rejects this option because the caller already supplied a server.

- `WithOpsHTTPEnabled(enabled bool)`

  Controls whether Workerkit ops HTTP routes are mounted. Ops HTTP is disabled by default.

- `WithOpsHTTPOptions(opts ...opshttp.Option)`

  Appends options used when mounting Workerkit ops HTTP routes.

- `WithStartWorkers(enabled bool)`

  Controls whether `Run` starts all workers before serving.

- `WithGracefulWorkerShutdown(enabled bool)`

  Controls whether `Run` drains, waits for idle, and stops workers after Servekit exits or worker startup fails.

- `WithShutdownTimeout(timeout time.Duration)`

  Sets the outer service-level budget for graceful worker shutdown. Zero keeps the default, and negative disables this timeout.

## Package `retry`

`retry` provides bounded retry, backoff, and jitter primitives used by Workerkit execution paths and reusable by callers.

### Policy

- `Policy`

  Decides whether a failed operation should be retried and how long to wait before the next attempt.

- `PolicyFunc`

  Adapts a function into `Policy`.

- `Config`

  Structured retry configuration: max attempts, backoff, jitter, and retry predicate.

- `RetryableFunc`

  Predicate for deciding whether an error should be retried.

- `New(...)`

  Constructs a policy from `Config`.

- `Attempts(...)`

  Retries every failure up to a bounded number of total attempts.

- `AttemptsIf(...)`

  Retries accepted failures up to a bounded number of total attempts.

- `Never()`

  Returns a policy that never retries.

### Backoff

- `Backoff`

  Computes the base delay for a retry attempt.

- `BackoffFunc`

  Adapts a function into `Backoff`.

- `Constant(...)`

  Uses the same delay for every retry.

- `Linear(...)`

  Grows by one step per failed attempt.

- `Exponential(...)`

  Grows exponentially from an initial delay and optional cap.

### Jitter

- `Jitter`

  Perturbs a backoff delay to avoid synchronized retries.

- `JitterFunc`

  Adapts a function into `Jitter`.

- `None()`

  Leaves the base delay unchanged.

- `Full()`

  Randomizes the delay between zero and the base delay.

- `FullWithRand(...)`

  Full jitter with a caller-supplied random source for deterministic tests or simulations.

- `Symmetric(...)`

  Perturbs delay around the base value by a fraction.

- `SymmetricWithRand(...)`

  Symmetric jitter with a caller-supplied random source.

## Package `otel`

`otel` adapts Workerkit observer events into OpenTelemetry spans and metrics.

- `Observer`

  OpenTelemetry-backed implementation of `workerkit.Observer`.

- `New(...)`

  Constructs the observer and OpenTelemetry instruments.

- `Option`

  Configures the observer.

- `WithTracerProvider(...)`

  Sets the tracer provider. Nil uses the global OpenTelemetry provider.

- `WithMeterProvider(...)`

  Sets the meter provider. Nil uses the global OpenTelemetry provider.

- `WithAttributes(...)`

  Appends attributes to emitted spans and metrics. Service identity should usually be configured on the OpenTelemetry resource instead.

The adapter records command dispatches as spans, lifecycle/readiness/failure events on the current span, and counters/histograms for runtime activity. Dispatch ids appear on spans and span events, not metrics, to avoid high-cardinality metric labels.

## Package `slogobserver`

`slogobserver` adapts Workerkit observer events into structured `log/slog` records.

- `Observer`

  `slog`-backed implementation of `workerkit.Observer`.

- `New(...)`

  Constructs the observer. Nil logger uses `slog.Default()`.

- `Option`

  Configures the observer.

- `WithLevel(...)`

  Sets the level for routine Workerkit logs. Failure logs are always emitted at error level.

- `WithAttributes(...)`

  Appends attributes to every log record.

## Suggested reading order

If you are new to the codebase:

1. [README](../README.md)
2. API Map
3. [Examples Directory](../examples/README.md)
4. [`examples/managed-service`](../examples/managed-service)
5. [`examples/opshttp-basic`](../examples/opshttp-basic)
6. [`examples/production-composition`](../examples/production-composition)
