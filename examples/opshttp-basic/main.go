package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	opskit "github.com/jaredjakacky/opskit"
	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
	"github.com/jaredjakacky/workerkit/opshttp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runtime, err := workerkit.New(workerkit.Identity{Name: "ops_demo"})
	if err != nil {
		log.Fatal(err)
	}

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "indexer",
		Description: "Example worker exposed through read-only ops routes.",
		Worker:      indexerWorker{},
	}, workerkit.WithCommandSpec(workerkit.CommandSpec{
		Name:        "index/rebuild",
		Description: "Discoverable domain command; dispatch is not enabled in this example.",
		Handler: workerkit.CommandHandlerFunc(func(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{Message: "index rebuild requested"}, nil
		}),
	})); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := runtime.Shutdown(shutdownCtx); err != nil {
			log.Printf("worker runtime shutdown: %v", err)
		}
	}()

	ops := opskit.NewRegistry()
	ops.MustRegister(runtime, opskit.Required())

	// Opskit is still the readiness path in this optional opshttp example.
	// Workerkit owns runtime status while Servekit owns HTTP presentation.
	server := servekit.New(
		servekit.WithAddr(":8080"),
		servekit.WithOps(ops),
	)

	// opshttp adds Workerkit-specific inspection only because this example is
	// explicitly about that optional surface.
	if err := opshttp.Mount(server, runtime,
		opshttp.WithEndpointOptions(servekit.WithEndpointTimeout(2*time.Second)),
	); err != nil {
		log.Fatal(err)
	}

	printCurlCommands()

	if err := server.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

type indexerWorker struct{}

func (indexerWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return fmt.Errorf("worker runtime handle unavailable")
	}
	return runtime.SetReady(true)
}

func (indexerWorker) Stop(ctx context.Context) error {
	return nil
}

func printCurlCommands() {
	fmt.Println("list read-only Workerkit operations routes:")
	fmt.Println("  curl -s http://localhost:8080/admin/runtime")
	fmt.Println("  curl -s http://localhost:8080/admin/workers")
	fmt.Println("  curl -s 'http://localhost:8080/admin/worker?name=ops_demo/indexer'")
	fmt.Println("  curl -s 'http://localhost:8080/admin/commands?worker=ops_demo/indexer'")
	fmt.Println()
	fmt.Println("Servekit readiness includes Workerkit runtime readiness:")
	fmt.Println("  curl -i http://localhost:8080/readyz")
}
