package main

import (
	"context"
	"encoding/json"
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

	runtime, err := workerkit.New(workerkit.Identity{Name: "ops_commands"})
	if err != nil {
		log.Fatal(err)
	}

	cache := &cacheWorker{}
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "cache",
		Description: "Owns cache-domain commands exposed through opshttp.",
		Worker:      cache,
	},
		workerkit.WithWorkerCommandConcurrency(1),
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "cache/put",
			Description: "Store one cache value from a JSON payload.",
			Handler:     workerkit.CommandHandlerFunc(cache.put),
		}),
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "cache/slow",
			Description: "Hold one command slot long enough to demonstrate HTTP 429 saturation.",
			Handler:     workerkit.CommandHandlerFunc(cache.slow),
		}),
	); err != nil {
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
	server := servekit.New(
		servekit.WithAddr(":8080"),
		servekit.WithOps(ops),
	)

	dispatchPolicy := []servekit.EndpointOption{
		servekit.WithAuthGate(requireOpsToken),
		servekit.WithEndpointTimeout(5 * time.Second),
	}

	// This example demonstrates opt-in HTTP command dispatch: Servekit owns HTTP
	// encoding, endpoint policy, and status codes, while Workerkit owns command
	// routing and policy. Mount also exposes read-only operations routes; mount
	// those only on an appropriate operations surface.
	if err := opshttp.Mount(server, runtime,
		opshttp.WithCommandDispatchEnabled(),
		opshttp.WithDispatchOptions(dispatchPolicy...),
	); err != nil {
		log.Fatal(err)
	}

	printCurlCommands()

	if err := server.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

type cacheWorker struct {
	values map[string]string
}

func (w *cacheWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return fmt.Errorf("worker runtime handle unavailable")
	}
	w.values = make(map[string]string)
	return runtime.SetReady(true)
}

func (w *cacheWorker) Stop(ctx context.Context) error {
	return nil
}

func (w *cacheWorker) put(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	var input struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(req.Payload, &input); err != nil {
		return workerkit.CommandResult{}, fmt.Errorf("decode put payload: %w", err)
	}
	if input.Key == "" {
		return workerkit.CommandResult{}, errors.New("key is required")
	}

	w.values[input.Key] = input.Value
	payload, err := json.Marshal(map[string]string{
		"key":   input.Key,
		"value": w.values[input.Key],
	})
	if err != nil {
		return workerkit.CommandResult{}, err
	}

	return workerkit.CommandResult{
		Message: "cache value stored",
		Payload: payload,
	}, nil
}

func (w *cacheWorker) slow(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	var input struct {
		SleepMillis int `json:"sleepMillis"`
	}
	if err := json.Unmarshal(req.Payload, &input); err != nil {
		return workerkit.CommandResult{}, fmt.Errorf("decode slow payload: %w", err)
	}
	if input.SleepMillis <= 0 {
		input.SleepMillis = 5000
	}

	timer := time.NewTimer(time.Duration(input.SleepMillis) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return workerkit.CommandResult{}, ctx.Err()
	case <-timer.C:
	}

	return workerkit.CommandResult{
		Message: "slow command completed",
		Payload: []byte(`{"status":"done"}`),
	}, nil
}

func requireOpsToken(r *http.Request) error {
	if r.Header.Get("X-Ops-Token") != "dev-secret" {
		return servekit.Error(http.StatusUnauthorized, "ops token required", nil)
	}
	return nil
}

func printCurlCommands() {
	fmt.Println("successful command dispatch:")
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/commands/dispatch \`)
	fmt.Println(`    -H 'Content-Type: application/json' \`)
	fmt.Println(`    -H 'X-Ops-Token: dev-secret' \`)
	fmt.Println(`    -d '{"worker":"cache","name":"cache/put","payload":{"key":"homepage","value":"warm"}}'`)
	fmt.Println()
	fmt.Println("invalid request, missing target, and saturation examples are documented in docs/examples.md")
	fmt.Println("admin lifecycle controls are not enabled in this example")
}
