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

	runtime, err := workerkit.New(workerkit.Identity{Name: "search_service"})
	if err != nil {
		log.Fatal(err)
	}

	// This example demonstrates a runtime as one service boundary containing
	// multiple independently managed workers with shared lifecycle and status.
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "ingest",
		Description: "Accepts incoming documents.",
		Worker:      serviceWorker{name: "ingest"},
	}, workerkit.WithCommand("ingest/enqueue", workerkit.CommandHandlerFunc(enqueueDocument))); err != nil {
		log.Fatal(err)
	}

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "index",
		Description: "Builds searchable index entries.",
		Worker:      serviceWorker{name: "index"},
	}, workerkit.WithCommand("index/rebuild", workerkit.CommandHandlerFunc(rebuildIndex))); err != nil {
		log.Fatal(err)
	}

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "maintenance",
		Description: "Runs service housekeeping work.",
		Worker:      serviceWorker{name: "maintenance"},
	}, workerkit.WithCommand("maintenance/sweep", workerkit.CommandHandlerFunc(sweepMaintenance))); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	printStatus(runtime, "after StartAll")

	dispatch(ctx, runtime, workerkit.CommandRequest{
		Worker:  "ingest",
		Name:    "ingest/enqueue",
		Payload: mustJSON(map[string]string{"documentID": "doc-123"}),
	})
	dispatch(ctx, runtime, workerkit.CommandRequest{
		Worker: "index",
		Name:   "index/rebuild",
	})
	dispatch(ctx, runtime, workerkit.CommandRequest{
		Worker: "maintenance",
		Name:   "maintenance/sweep",
	})

	printStatus(runtime, "after commands")

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}

	printStatus(runtime, "after shutdown")
}

type serviceWorker struct {
	name string
}

func (w serviceWorker) Start(ctx context.Context) error {
	fmt.Printf("%s started\n", w.name)
	return nil
}

func (w serviceWorker) Stop(ctx context.Context) error {
	fmt.Printf("%s stopped\n", w.name)
	return nil
}

func enqueueDocument(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	var payload struct {
		DocumentID string `json:"documentID"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return workerkit.CommandResult{}, err
	}
	if payload.DocumentID == "" {
		return workerkit.CommandResult{}, errors.New("documentID is required")
	}
	return workerkit.CommandResult{
		Message: "document enqueued",
		Payload: mustJSON(map[string]string{"documentID": payload.DocumentID, "status": "queued"}),
	}, nil
}

func rebuildIndex(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	return workerkit.CommandResult{
		Message: "index rebuild requested",
		Payload: mustJSON(map[string]string{"scope": "all"}),
	}, nil
}

func sweepMaintenance(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	return workerkit.CommandResult{
		Message: "maintenance sweep completed",
		Payload: mustJSON(map[string]int{"expiredItems": 7}),
	}, nil
}

func dispatch(ctx context.Context, runtime *workerkit.Runtime, req workerkit.CommandRequest) {
	result, err := runtime.Dispatch(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("dispatch worker=%s command=%s message=%q payload=%s\n",
		req.Worker, req.Name, result.Message, result.Payload)
}

func printStatus(runtime *workerkit.Runtime, label string) {
	status := runtime.RuntimeStatus()
	fmt.Printf("\n== %s ==\n", label)
	fmt.Printf("runtime=%s state=%s ready=%t workers=%d\n",
		status.Name, status.State, status.Ready, status.Workers)

	for index, worker := range runtime.Workers() {
		fmt.Printf("%d. worker=%s state=%s ready=%t accepting=%t\n",
			index+1,
			worker.QualifiedName,
			worker.Status.State,
			worker.Status.Ready,
			worker.Status.AcceptingWork)
	}
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
