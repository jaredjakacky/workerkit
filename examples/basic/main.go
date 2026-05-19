package main

import (
	"context"
	"fmt"
	"log"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
)

func main() {
	ctx := context.Background()

	// This example demonstrates the core Workerkit model: the worker owns domain
	// work, while the runtime owns lifecycle, status, and graceful shutdown.
	runtime, err := workerkit.New(workerkit.Identity{Name: "basic"})
	if err != nil {
		log.Fatal(err)
	}

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "printer",
		Description: "Shows the smallest custom Workerkit worker.",
		Worker:      printerWorker{},
	}); err != nil {
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

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}

	status = runtime.Status()
	fmt.Printf("runtime=%s state=%s ready=%t workers=%d\n",
		status.Name, status.State, status.Ready, status.Workers)
}

type printerWorker struct{}

func (printerWorker) Start(ctx context.Context) error {
	fmt.Println("printer worker started")
	return nil
}

func (printerWorker) Stop(ctx context.Context) error {
	fmt.Println("printer worker stopped")
	return nil
}
