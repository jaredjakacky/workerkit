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

	opskit "github.com/jaredjakacky/opskit"
	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
	"github.com/jaredjakacky/workerkit/opshttp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runtime, err := workerkit.New(workerkit.Identity{Name: "admin_lifecycle"})
	if err != nil {
		log.Fatal(err)
	}

	for _, name := range []string{"ingest", "index"} {
		workerName := name
		if err := runtime.Register(workerkit.WorkerSpec{
			Name:        workerName,
			Description: "Worker controlled through privileged lifecycle routes.",
			Worker:      lifecycleWorker{name: workerName},
		}); err != nil {
			log.Fatal(err)
		}
	}

	ops := opskit.NewRegistry()
	ops.MustRegister(runtime, opskit.Required())
	server := servekit.New(
		servekit.WithAddr(":8080"),
		servekit.WithOps(ops),
	)

	// Servekit owns endpoint policy here: authentication, authorization,
	// audit logging, request limits, and route-specific timeouts belong at the HTTP boundary.
	// opshttp.Mount exposes read-only /admin routes by default, so the shared
	// admin policy applies to every mounted operations route, not only lifecycle controls.
	adminPolicy := []servekit.EndpointOption{
		servekit.WithAuthGate(requireAdminToken),
		servekit.WithEndpointMiddleware(auditAdminRequest),
		servekit.WithEndpointTimeout(5 * time.Second),
	}
	lifecyclePolicy := []servekit.EndpointOption{
		servekit.WithBodyLimit(1 << 20),
	}

	// This example demonstrates privileged lifecycle controls over HTTP;
	// production deployments must protect the whole admin operations plane.
	if err := opshttp.Mount(server, runtime,
		opshttp.WithEndpointOptions(adminPolicy...),
		opshttp.WithAdminLifecycleControlsEnabled(),
		opshttp.WithLifecycleOptions(lifecyclePolicy...),
	); err != nil {
		log.Fatal(err)
	}

	printCurlCommands()

	if err := server.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

type lifecycleWorker struct {
	name string
}

func (w lifecycleWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return fmt.Errorf("worker runtime handle unavailable")
	}
	if err := runtime.SetReady(true); err != nil {
		return err
	}
	fmt.Printf("%s started\n", w.name)
	return nil
}

func (w lifecycleWorker) Stop(ctx context.Context) error {
	fmt.Printf("%s stopped\n", w.name)
	return nil
}

func requireAdminToken(r *http.Request) error {
	if r.Header.Get("X-Admin-Token") != "dev-secret" {
		return servekit.Error(http.StatusUnauthorized, "admin token required", nil)
	}
	return nil
}

func auditAdminRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("audit admin route method=%s path=%s remote=%s", r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}

func printCurlCommands() {
	fmt.Println("admin operations routes require X-Admin-Token: dev-secret")
	fmt.Println()
	fmt.Println("workers are registered but not running at startup")
	fmt.Println("Servekit /readyz may fail until workers are started through the lifecycle routes")
	fmt.Println()
	fmt.Println("readiness before and after starting workers:")
	fmt.Println("  curl -i http://localhost:8080/readyz")
	fmt.Println()
	fmt.Println("worker lifecycle:")
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/workers/start -H 'Content-Type: application/json' -H 'X-Admin-Token: dev-secret' -d '{"name":"ingest"}'`)
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/workers/drain -H 'Content-Type: application/json' -H 'X-Admin-Token: dev-secret' -d '{"name":"ingest"}'`)
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/workers/stop -H 'Content-Type: application/json' -H 'X-Admin-Token: dev-secret' -d '{"name":"ingest"}'`)
	fmt.Println()
	fmt.Println("runtime lifecycle:")
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/runtime/start -H 'X-Admin-Token: dev-secret'`)
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/runtime/drain -H 'X-Admin-Token: dev-secret'`)
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/runtime/stop -H 'X-Admin-Token: dev-secret'`)
}
