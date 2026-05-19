# Workerkit

[![Release](https://img.shields.io/github/v/release/jaredjakacky/workerkit?sort=semver)](https://github.com/jaredjakacky/workerkit/releases)
[![CI](https://github.com/jaredjakacky/workerkit/actions/workflows/ci.yaml/badge.svg)](https://github.com/jaredjakacky/workerkit/actions/workflows/ci.yaml)
[![Go Support](https://img.shields.io/badge/go%20support-1.25.x%20%7C%201.26.x-00ADD8)](https://github.com/jaredjakacky/workerkit/actions/workflows/ci.yaml)
[![License](https://img.shields.io/github/license/jaredjakacky/workerkit)](https://github.com/jaredjakacky/workerkit/blob/main/LICENSE)

## Overview
Workerkit is a transport-agnostic Go runtime for domain workers: long-running loops, pollers, subscribers, command-driven workers, and other worker-oriented service components.

It gives ordinary Go workers a real production shell: lifecycle control, readiness, graceful shutdown, retries, jitter, concurrency limits, failure policy, panic handling, status inspection, and structured observability — without turning your service into a framework.

If your service owns domain workers that actually matter, Workerkit gives them the same kind of operational story HTTP servers already expect.

## Why Workerkit exists

Worker-oriented services deserve the same kind of coherent runtime story that HTTP services already expect.

The hard part is not starting a goroutine. The hard part is everything that appears after the goroutine matters: lifecycle, readiness, deploy drain, shutdown deadlines, retry policy, failure visibility, status inspection, command routing, concurrency limits, panic recovery, and useful telemetry.

Those concerns are not domain logic, but they show up in every production service that owns workers. Without a runtime, they tend to spread across channels, health flags, admin handlers, retry loops, shutdown hooks, and disconnected logs.

Workerkit pulls that operational layer into one reusable runtime. Applications register workers, define the policies that matter, and keep the worker code focused on domain behavior.

A Workerkit worker is still normal Go code. It can run a long-lived loop, watch external systems, consume from a broker, poll an API, maintain in-memory state, or expose domain-specific commands. Workerkit does not decide what the worker does. It gives the worker a predictable operational envelope.

Workerkit also stands next to `servekit` instead of reinventing an HTTP service layer. When a service needs an operations plane, the optional `opshttp` package mounts Workerkit status, inspection, command discovery, command dispatch, and readiness integration into a Servekit server. Servekit keeps owning the HTTP baseline. Workerkit adds worker-aware operations.

## What you get

With one runtime, Workerkit gives you:

- explicit worker registration
- startup, drain, and graceful shutdown
- aggregate runtime status and per-worker snapshots
- readiness reporting and readiness aggregation
- worker-owned command registration and direct dispatch
- bounded retry with backoff and jitter
- runtime-wide and worker-local concurrency limits
- panic and failure policy
- structured observer hooks
- optional `slog` and OpenTelemetry adapters
- optional Servekit-backed HTTP operations routes

This is the operational layer teams rebuild around serious worker components. Workerkit makes it the baseline instead of the afterthought.

## What Workerkit is not

Workerkit is not a workflow engine, job queue, scheduler, or durable orchestration system.

It does not provide durable workflow state, queue persistence, distributed leasing, task assignment, or cross-service coordination. It does not replace brokers, databases, schedulers, or orchestrators. It does not own your domain model.

It is also not an application framework. You still write normal Go workers, your own business loops, your own side effects, and your own command contracts. Workerkit is the runtime and control surface around those workers, not the application itself.

And Workerkit is not fundamentally tied to HTTP. The core runtime is transport-agnostic. Commands, readiness, lifecycle, failure handling, and status all exist as ordinary Go concepts first. If you want an HTTP operations plane, the optional `opshttp` package mounts one into Servekit. If you do not, the same runtime works directly from Go code, tests, or another control surface.

## Good fit / not a fit

Workerkit is a good fit when:

- your service runs domain workers, long-lived loops, pollers, subscribers, schedulers, or command-driven workers that need explicit operational control
- you want one runtime to own lifecycle, readiness, shutdown, status, retries, failure handling, concurrency limits, and observability
- your workers own business logic, but you want a consistent production shell around them
- some workers expose operational or domain commands like `index/rebuild`, `cache/refresh`, `snapshot/prune`, or `queue/drain`
- you want a transport-agnostic worker runtime with the option to add an HTTP operations surface when HTTP is useful
- you want production-oriented defaults without adopting a full framework

Workerkit is probably not a fit when:

- you need a durable workflow engine, queue system, distributed lock manager, or fleet-wide orchestrator
- you want built-in persistence for workflow state, retries across restarts, or task coordination across services
- you want the runtime to understand and enforce your business domain instead of leaving that logic inside your workers
- your service already has a mature worker runtime and Workerkit would mostly duplicate it
- you only need a tiny helper around one short-lived goroutine, not a managed runtime

## Installation

```bash
go get github.com/jaredjakacky/workerkit
```

```go
import workerkit "github.com/jaredjakacky/workerkit"
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
)

type printerWorker struct{}

func (printerWorker) Start(ctx context.Context) error {
	fmt.Println("worker started")
	return nil
}

func (printerWorker) Stop(ctx context.Context) error {
	fmt.Println("worker stopped")
	return nil
}

func main() {
	ctx := context.Background()

	runtime, err := workerkit.New(workerkit.Identity{Name: "quickstart"})
	if err != nil {
		log.Fatal(err)
	}

	err = runtime.Register(workerkit.WorkerSpec{
		Name:        "printer",
		Description: "prints worker-owned output",
		Worker:      printerWorker{},
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	status := runtime.Status()
	fmt.Printf("runtime=%s state=%s ready=%t workers=%d\n",
		status.Name, status.State, status.Ready, status.Workers)

	for _, worker := range runtime.Workers() {
		fmt.Printf("worker=%s state=%s ready=%t\n",
			worker.QualifiedName, worker.Status.State, worker.Status.Ready)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}
```

That one runtime already gives you the operational pieces that usually get rebuilt around production workers:

- explicit worker registration
- managed startup and graceful shutdown
- aggregate runtime status and per-worker snapshots
- readiness aggregation
- drain-before-stop behavior
- production-oriented panic and failure handling
- extension points for commands, retry, concurrency limits, and observers

In practice, you get a real worker runtime without hand-building lifecycle bookkeeping, readiness flags, shutdown ordering, status inspection, command admission, and failure reporting yourself.

## The Core Model

Workerkit is deliberately built around one runtime boundary and ordinary Go workers.

### Runtime

`Runtime` represents one service boundary. It owns worker registration, lifecycle control, readiness aggregation, status snapshots, worker-owned command dispatch, retry execution, concurrency limits, failure policy, and observer callbacks.

Most services should start with one runtime.

### Worker

`Worker` is the lifecycle contract managed by the runtime:

```go
type Worker interface {
    Start(context.Context) error
    Stop(context.Context) error
}
```

The worker owns domain behavior: loops, input sources, side effects, persistence, broker clients, caches, indexes, and business rules. Workerkit owns how the worker starts, stops, reports readiness, accepts commands, records failure, and exposes status.

Use `NewLoopWorker` when the worker is primarily a long-running loop that should stop when its context is canceled.

### Worker Runtime

`WorkerRuntimeFromContext` gives managed worker code a worker-scoped handle for runtime signals:

- `SetReady`
- `SetAcceptingWork`
- `ReportFailure`
- `Status`

That lets a worker report warmup, pause command admission, or record asynchronous background failure without receiving full runtime authority.

### Commands

Commands are worker-owned domain operations. They are not lifecycle controls, and they are not inherently HTTP.

Register commands with `WithCommand` or `WithCommandSpec`, discover them with `Runtime.Commands`, and execute them directly with `Runtime.Dispatch`. Workerkit routes and observes commands, but it does not interpret payloads. The worker owns the command contract.

## Why This Works

Workerkit rests on three choices:

1. It keeps worker code ordinary Go.
2. It gives worker-oriented service components a coherent operational envelope.
3. It keeps HTTP optional and outside the core runtime.

That is why the package can stay small while still feeling production-ready from the first constructor call.

## Advanced capabilities

Workerkit has a short normal path, but it is not limited to startup and shutdown. Advanced hooks include:

- readiness contribution policy per worker
- runtime-wide and worker-local command concurrency limits
- bounded retry with backoff, jitter, and retry predicates
- lifecycle and command attempt timeouts
- panic recovery or crash policy
- isolated, unready, or failed aggregate runtime failure policy
- worker-owned command discovery and dispatch
- structured `slog` observer support
- OpenTelemetry observer support
- Servekit-backed read-only operations routes
- opt-in HTTP command dispatch and privileged lifecycle controls
- managed Servekit service composition with `servekitservice.NewManaged`

The advanced path is documented in [docs/advanced.md](docs/advanced.md), with policy details in [docs/policy.md](docs/policy.md) and Servekit composition in [docs/composition.md](docs/composition.md).

## Servekit operations plane

Workerkit and Servekit can be used independently, but the optional `opshttp` package provides the canonical bridge between them.

Servekit owns the HTTP service baseline: server construction, middleware, authentication, readiness endpoints, request policy, endpoint timeouts, response handling, and service lifecycle. Workerkit owns worker runtime semantics: lifecycle, readiness, status, command dispatch, admission, failure policy, and telemetry.

`opshttp` connects the two by mounting a Servekit-native operations surface for Workerkit runtime status, worker inspection, command discovery, command dispatch, and readiness integration.

```go
server := servekit.New(
    servekit.WithAddr(":8080"),
    servekit.WithReadinessChecks(opshttp.ReadinessCheck(runtime)),
)

err := opshttp.Mount(server, runtime,
    opshttp.WithEndpointOptions(
        servekit.WithAuthGate(requireOpsCaller),
        servekit.WithEndpointTimeout(10*time.Second),
    ),
    opshttp.WithCommandDispatchEnabled(),
    opshttp.WithDispatchOptions(
        servekit.WithBodyLimit(1 << 20),
    ),
)
```

By default, `opshttp.Mount` exposes read-only operations routes:

- `GET /admin/runtime` returns runtime identity and aggregate status
- `GET /admin/workers` returns worker snapshots
- `GET /admin/worker?name=runtime/worker` returns one worker snapshot
- `GET /admin/commands?worker=runtime/worker` returns worker-owned command discovery

Even the read-only routes expose operational state, worker names, command inventory, and failure information, so mount them only on an appropriate operations surface.

Command dispatch is intentionally opt-in because it can trigger domain work or mutate worker state:

- `POST /admin/commands/dispatch` dispatches a worker-owned command when `opshttp.WithCommandDispatchEnabled()` is enabled

Privileged lifecycle controls are also opt-in because they can start, drain, and stop workers through HTTP:

- `POST /admin/workers/start`
- `POST /admin/workers/drain`
- `POST /admin/workers/stop`
- `POST /admin/runtime/start`
- `POST /admin/runtime/drain`
- `POST /admin/runtime/stop`

Enable lifecycle controls with `opshttp.WithAdminLifecycleControlsEnabled()` and protect them with authentication, authorization, and audit middleware appropriate for the deployment.

The route prefix defaults to `/admin` and can be changed with `opshttp.WithPrefix`. Common Servekit endpoint options can be applied to every mounted route with `opshttp.WithEndpointOptions`. Stricter policy can be applied only to command dispatch routes with `opshttp.WithDispatchOptions`, and only to lifecycle control routes with `opshttp.WithLifecycleOptions`.

Command dispatch accepts raw JSON payloads and passes those bytes to `workerkit.CommandRequest.Payload`. Command responses expose `workerkit.CommandResult.Payload` as raw JSON. Workerkit does not interpret either payload; the worker owns the command contract.

`opshttp` provides stable HTTP meanings for Workerkit command dispatch failures:

- malformed command requests return `400 Bad Request`
- missing workers or commands return `404 Not Found`
- runtime not accepting work returns `503 Service Unavailable`
- worker not accepting work or invalid worker state returns `409 Conflict`
- runtime or worker saturation returns `429 Too Many Requests`

## Documentation

- [Getting Started](docs/getting-started.md): build the smallest useful Workerkit runtime
- [Usage Guide](docs/usage.md): normal runtime, worker, command, status, and shutdown path
- [Lifecycle and Readiness](docs/lifecycle.md): startup, readiness, drain, stop, shutdown, and failure reporting
- [Commands](docs/commands.md): worker-owned domain commands without tying them to HTTP
- [Policy Guide](docs/policy.md): retry, backoff, jitter, concurrency, readiness, and failure policy
- [Observability](docs/observability.md): core runtime observer events, structured logs, and OpenTelemetry
- [Composition with Servekit](docs/composition.md): `servekitservice`, `opshttp`, and the Kit-series boundary
- [Examples Guide](docs/examples.md): guided walkthrough of the runnable examples
- [Advanced Guide](docs/advanced.md): advanced composition and customization patterns
- [API Map](docs/api.md): human-friendly map of the exported surface
- [Examples Directory](examples/README.md): short index of the runnable example programs

## Examples

Runnable programs live in [`examples/`](examples), which includes a guided tour of the example set.

Recommended reading order:

1. [`examples/basic`](examples/basic)
2. [`examples/loop-worker`](examples/loop-worker)
3. [`examples/readiness`](examples/readiness)
4. [`examples/commands`](examples/commands)
5. [`examples/retry-policy`](examples/retry-policy)
6. [`examples/concurrency-limits`](examples/concurrency-limits)
7. [`examples/failure-policy`](examples/failure-policy)
8. [`examples/multi-worker`](examples/multi-worker)
9. [`examples/testing`](examples/testing)
10. [`examples/observability-slog`](examples/observability-slog)
11. [`examples/observability-otel`](examples/observability-otel)
12. [`examples/managed-service`](examples/managed-service)
13. [`examples/opshttp-basic`](examples/opshttp-basic)
14. [`examples/opshttp-commands`](examples/opshttp-commands)
15. [`examples/admin-lifecycle`](examples/admin-lifecycle)
16. [`examples/production-composition`](examples/production-composition)

## API Reference

The canonical symbol-level API documentation should live in Go doc comments so it stays accurate in editors and Go tooling. The repository-level companion is [docs/api.md](docs/api.md), which groups the exported surface into a human-oriented map.

## Development

This repository uses `make` for local verification:

```bash
make verify
make build-examples
make test-race
make govulncheck
```

`make verify` checks formatting, runs `go vet`, runs tests, builds the runnable examples, and verifies that `go.mod` and `go.sum` are tidy. `make build-examples` is available when you only want to compile the runnable examples.

CI runs verification and race tests on the supported Go versions. Release tags are gated by those jobs plus `govulncheck` before publishing.

## Issues and Scope

Workerkit is maintained as a small bootstrap library for worker lifecycle, readiness, command dispatch, retry policy, observability, and optional operations HTTP integration.

Bug reports, documentation fixes, and compatibility issues are welcome. Large feature additions are evaluated conservatively because Workerkit is intentionally not a workflow engine, job queue, scheduler, distributed orchestrator, or application framework.

## Maintenance

Workerkit is a small open source library maintained on a best-effort basis.

The active development line lives on `main`, and that is the only line actively maintained unless explicitly noted otherwise. The minimum supported Go version is declared in [`go.mod`](go.mod), and the Go versions currently verified in CI are listed in [`.github/workflows/ci.yaml`](.github/workflows/ci.yaml).

Compatibility-impacting changes should be called out explicitly in release notes or release descriptions. Long-lived maintenance branches and backports are not planned unless explicitly noted.

## License

[MIT](LICENSE)
