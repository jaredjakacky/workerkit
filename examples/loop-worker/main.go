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

	runtime, err := workerkit.New(workerkit.Identity{Name: "loop_demo"})
	if err != nil {
		log.Fatal(err)
	}

	// This example demonstrates a managed long-running loop: Workerkit owns
	// lifecycle and cancellation, while the loop owns the domain work.
	worker := workerkit.NewLoopWorker(func(ctx context.Context, runtime workerkit.WorkerRuntime) error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for count := 1; ; count++ {
			select {
			case <-ctx.Done():
				fmt.Println("loop observed cancellation")
				return ctx.Err()
			case <-ticker.C:
				fmt.Printf("processed item batch=%d worker=%s\n", count, runtime.Name())
			}
		}
	})

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "processor",
		Description: "Processes work on a managed ticker loop.",
		Worker:      worker,
	}); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	printStatus(runtime, "after start")
	time.Sleep(350 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}

	printStatus(runtime, "after shutdown")
}

func printStatus(runtime *workerkit.Runtime, label string) {
	status := runtime.Status()
	fmt.Printf("\n== %s ==\n", label)
	fmt.Printf("runtime=%s state=%s ready=%t workers=%d\n",
		status.Name, status.State, status.Ready, status.Workers)

	for _, worker := range runtime.Workers() {
		fmt.Printf("worker=%s state=%s ready=%t accepting=%t\n",
			worker.QualifiedName,
			worker.Status.State,
			worker.Status.Ready,
			worker.Status.AcceptingWork)
	}
}
