package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
	"github.com/jaredjakacky/workerkit/servekitservice"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runtime, err := workerkit.New(workerkit.Identity{Name: "managed_service"})
	if err != nil {
		log.Fatal(err)
	}

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "processor",
		Description: "Background worker started and stopped by the managed service lifecycle.",
		Worker:      processorWorker{},
	}); err != nil {
		log.Fatal(err)
	}

	// This example demonstrates the managed service path: Workerkit starts and
	// drains workers while Servekit owns HTTP serving, readiness endpoints, and
	// server shutdown.
	service, err := servekitservice.NewManaged(runtime,
		servekitservice.WithServekitOptions(
			servekit.WithAddr(":8080"),
			servekit.WithBuildInfo("dev", "local", time.Now().UTC().Format(time.RFC3339)),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	// NewManaged constructs the Servekit server for us; Server exposes it so the
	// application can still register normal HTTP routes.
	service.Server().Handle(http.MethodGet, "/app/status", func(r *http.Request) (any, error) {
		status := runtime.RuntimeStatus()
		return map[string]any{
			"service": "managed-service",
			"runtime": status.Name,
			"state":   status.State,
			"ready":   status.Ready,
		}, nil
	})

	printCurlCommands()

	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

type processorWorker struct{}

func (processorWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return fmt.Errorf("worker runtime handle unavailable")
	}
	fmt.Println("processor started")
	return runtime.SetReady(true)
}

func (processorWorker) Stop(ctx context.Context) error {
	fmt.Println("processor stopped")
	return nil
}

func printCurlCommands() {
	fmt.Println("managed service listening on :8080")
	fmt.Println("try:")
	fmt.Println("  curl -i http://localhost:8080/app/status")
	fmt.Println("  curl -i http://localhost:8080/readyz")
	fmt.Println("ops HTTP is disabled by default; /admin/runtime should return 404")
	fmt.Println("  curl -i http://localhost:8080/admin/runtime")
}
