# Getting Started

This guide builds the smallest useful Workerkit program: one runtime, one
worker, explicit startup, status inspection, and graceful shutdown.

For the runnable version, see [`examples/basic`](../examples/basic).

## Install

```bash
go get github.com/jaredjakacky/workerkit
```

## Build a First Runtime

```go
package main

import (
	"context"
	"fmt"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
)

type worker struct{}

func (worker) Start(ctx context.Context) error {
	fmt.Println("worker started")
	return nil
}

func (worker) Stop(ctx context.Context) error {
	fmt.Println("worker stopped")
	return nil
}

func main() {
	ctx := context.Background()

	runtime, err := workerkit.New(workerkit.Identity{Name: "getting-started"})
	if err != nil {
		panic(err)
	}

	err = runtime.Register(workerkit.WorkerSpec{
		Name:        "main",
		Description: "first managed worker",
		Worker:      worker{},
	})
	if err != nil {
		panic(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		panic(err)
	}

	fmt.Printf("runtime ready: %v\n", runtime.Status().Ready)
	fmt.Printf("workers: %d\n", len(runtime.Workers()))

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := runtime.Shutdown(shutdownCtx); err != nil {
		panic(err)
	}
}
```

## What This Creates

`workerkit.New` creates one runtime for a service boundary. The runtime owns
registration, lifecycle control, readiness aggregation, command dispatch,
status snapshots, failure policy, retry execution, concurrency limits, and
observer callbacks.

`Register` attaches a worker to the runtime. The worker keeps ownership of its
domain logic. Workerkit owns the operational lifecycle around it.

`StartAll` starts registered workers in registration order. `Shutdown` drains,
waits for in-flight commands, and stops workers.

## What You Get Without HTTP

The core Workerkit runtime does not require Servekit, HTTP, Kubernetes, or an
operations plane.

You can directly:

- start and stop workers
- inspect runtime status
- inspect worker status
- dispatch worker-owned commands
- test behavior with Go tests
- attach observers

Servekit integration is optional. It becomes useful when you want HTTP
readiness, inspection, command dispatch, or lifecycle controls.

## Next Steps

- Read [`usage.md`](usage.md) for the normal Workerkit path.
- Read [`lifecycle.md`](lifecycle.md) for readiness, drain, stop, and shutdown.
- Read [`examples.md`](examples.md) for the guided example sequence.
