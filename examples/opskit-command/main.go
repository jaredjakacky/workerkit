package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	opskit "github.com/jaredjakacky/opskit"
	workerkit "github.com/jaredjakacky/workerkit"
)

func main() {
	ctx := context.Background()
	cache := &cacheComponent{}
	descriptor := opskit.CommandDescriptor{
		Name:        "cache/refresh",
		Description: "refresh one cache key",
		PayloadKind: "cache_refresh",
		Idempotent:  true,
		Attributes:  []opskit.Attribute{opskit.Attr("scope", "cache")},
	}

	runtime, err := workerkit.New(workerkit.Identity{Name: "opskit-command"})
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.Register(
		workerkit.WorkerSpec{Name: "cache", Worker: cache},
		workerkit.WithCommandSpec(workerkit.CommandFromOpskit(descriptor, cache)),
	); err != nil {
		log.Fatal(err)
	}
	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	commands, _ := runtime.Commands("cache")
	fmt.Printf("command=%s payload_kind=%s idempotent=%t\n",
		commands[0].Name, commands[0].PayloadKind, commands[0].Idempotent)

	result, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker:  "cache",
		Name:    descriptor.Name,
		Payload: []byte(`{"key":"homepage"}`),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("message=%q payload=%s\n", result.Message, result.Payload)

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

type cacheComponent struct{}

func (*cacheComponent) Start(context.Context) error { return nil }

func (*cacheComponent) Stop(context.Context) error { return nil }

func (*cacheComponent) HandleCommand(_ context.Context, req opskit.CommandRequest) opskit.CommandResult {
	var input struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(req.Payload, &input); err != nil {
		return opskit.RejectedCommand("invalid refresh payload")
	}
	if input.Key == "" {
		return opskit.RejectedCommand("cache key is required")
	}
	return opskit.CompletedCommand(
		"cache refreshed",
		map[string]any{"key": input.Key, "refreshed": true},
		5*time.Millisecond,
	)
}
