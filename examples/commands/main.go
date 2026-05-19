package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
)

func main() {
	ctx := context.Background()

	runtime, err := workerkit.New(workerkit.Identity{Name: "commands"})
	if err != nil {
		log.Fatal(err)
	}

	cache := &cacheWorker{}

	// This example demonstrates worker-owned domain commands without HTTP;
	// Workerkit routes and observes commands but does not interpret payloads.
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "cache",
		Description: "Owns cache-domain commands.",
		Worker:      cache,
	},
		workerkit.WithCommand("cache/echo", workerkit.CommandHandlerFunc(cache.echo)),
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "cache/refresh",
			Description: "Refresh one cache key and return the refreshed value.",
			Handler:     workerkit.CommandHandlerFunc(cache.refresh),
		}),
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "cache/fail",
			Description: "Return a domain command error without failing the worker.",
			Handler:     workerkit.CommandHandlerFunc(cache.fail),
		}),
	); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	printCommands(runtime, "cache")

	echoPayload := mustJSON(map[string]string{"message": "hello from raw JSON bytes"})
	echoResult, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker:  "cache",
		Name:    "cache/echo",
		Payload: echoPayload,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("echo result message=%q payload=%s\n", echoResult.Message, echoResult.Payload)

	refreshPayload := mustJSON(refreshRequest{Key: "homepage"})
	refreshResult, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker:  "cache",
		Name:    "cache/refresh",
		Payload: refreshPayload,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("refresh result message=%q payload=%s\n", refreshResult.Message, refreshResult.Payload)

	_, err = runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker:  "cache",
		Name:    "cache/fail",
		Payload: mustJSON(map[string]string{"reason": "demonstrate command failure status"}),
	})
	if err != nil {
		fmt.Printf("expected command error: %v\n", err)
	}

	printStatus(runtime, "after command error")

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

type cacheWorker struct{}

func (w *cacheWorker) Start(ctx context.Context) error {
	return nil
}

func (w *cacheWorker) Stop(ctx context.Context) error {
	return nil
}

func (w *cacheWorker) echo(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	return workerkit.CommandResult{
		Message: "echo",
		Payload: req.Payload,
	}, nil
}

func (w *cacheWorker) refresh(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	var input refreshRequest
	if err := json.Unmarshal(req.Payload, &input); err != nil {
		return workerkit.CommandResult{}, fmt.Errorf("decode refresh request: %w", err)
	}
	if input.Key == "" {
		return workerkit.CommandResult{}, errors.New("refresh key is required")
	}

	payload := mustJSON(refreshResult{
		Key:       input.Key,
		Value:     "fresh-value",
		Refreshed: true,
	})
	return workerkit.CommandResult{
		Message: "cache refreshed",
		Payload: payload,
	}, nil
}

func (w *cacheWorker) fail(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	return workerkit.CommandResult{}, errors.New("domain command rejected")
}

type refreshRequest struct {
	Key string `json:"key"`
}

type refreshResult struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Refreshed bool   `json:"refreshed"`
}

func printCommands(runtime *workerkit.Runtime, worker string) {
	commands, ok := runtime.Commands(worker)
	if !ok {
		log.Fatalf("worker %q not found", worker)
	}

	fmt.Println("registered commands:")
	for _, command := range commands {
		fmt.Printf("- worker=%s name=%s description=%q\n",
			command.Worker, command.Name, command.Description)
	}
}

func printStatus(runtime *workerkit.Runtime, label string) {
	fmt.Printf("\n== %s ==\n", label)
	status := runtime.Status()
	fmt.Printf("runtime=%s state=%s ready=%t\n", status.Name, status.State, status.Ready)

	for _, worker := range runtime.Workers() {
		fmt.Printf("worker=%s state=%s ready=%t\n",
			worker.QualifiedName, worker.Status.State, worker.Status.Ready)
		if worker.Status.LastCommandFailure != nil {
			failure := worker.Status.LastCommandFailure
			fmt.Printf("last command failure command=%s message=%q\n",
				failure.Command, failure.Message)
		}
	}
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
