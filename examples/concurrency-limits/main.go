package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
)

func main() {
	ctx := context.Background()

	// This example demonstrates layered command backpressure: a dispatch must pass
	// both the runtime-wide concurrency gate and the worker-local concurrency gate.
	runtime, err := workerkit.New(
		workerkit.Identity{Name: "concurrency"},
		workerkit.WithRuntimeCommandConcurrency(3),
	)
	if err != nil {
		log.Fatal(err)
	}

	release := make(chan struct{})
	entered := make(chan string, 3)
	results := make(chan dispatchResult, 3)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		workerName := name
		worker := slowWorker{}
		if err := runtime.Register(workerkit.WorkerSpec{
			Name:        workerName,
			Description: "Runs one slow command at a time.",
			Worker:      worker,
		},
			workerkit.WithWorkerCommandConcurrency(1),
			workerkit.WithCommand("work/slow", workerkit.CommandHandlerFunc(func(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
				entered <- req.Worker
				select {
				case <-release:
					return workerkit.CommandResult{Message: "slow work finished"}, nil
				case <-ctx.Done():
					return workerkit.CommandResult{}, ctx.Err()
				}
			})),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	dispatchAsync(ctx, runtime, results, "beta")
	waitEntered(entered)
	printStatus(runtime, "one beta command active")

	_, err = runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker: "beta",
		Name:   "work/slow",
	})
	printSaturation("second beta dispatch", err)

	dispatchAsync(ctx, runtime, results, "alpha")
	dispatchAsync(ctx, runtime, results, "gamma")
	waitEntered(entered)
	waitEntered(entered)
	printStatus(runtime, "three commands active")

	_, err = runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker: "alpha",
		Name:   "work/slow",
	})
	printSaturation("fourth runtime dispatch", err)

	close(release)
	for range 3 {
		result := <-results
		if result.err != nil {
			log.Fatal(result.err)
		}
		fmt.Printf("%s completed: %s\n", result.worker, result.message)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

type slowWorker struct{}

func (slowWorker) Start(context.Context) error {
	return nil
}

func (slowWorker) Stop(context.Context) error {
	return nil
}

type dispatchResult struct {
	worker  string
	message string
	err     error
}

func dispatchAsync(ctx context.Context, runtime *workerkit.Runtime, results chan<- dispatchResult, worker string) {
	go func() {
		result, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
			Worker: worker,
			Name:   "work/slow",
		})
		results <- dispatchResult{
			worker:  worker,
			message: result.Message,
			err:     err,
		}
	}()
}

func waitEntered(entered <-chan string) {
	worker := <-entered
	fmt.Printf("%s entered slow command\n", worker)
}

func printSaturation(label string, err error) {
	switch {
	case errors.Is(err, workerkit.ErrRuntimeSaturated):
		fmt.Printf("%s rejected: %v\n", label, workerkit.ErrRuntimeSaturated)
	case errors.Is(err, workerkit.ErrWorkerSaturated):
		fmt.Printf("%s rejected: %v\n", label, workerkit.ErrWorkerSaturated)
	case err != nil:
		fmt.Printf("%s rejected with unexpected error: %v\n", label, err)
	default:
		fmt.Printf("%s unexpectedly accepted\n", label)
	}
}

func printStatus(runtime *workerkit.Runtime, label string) {
	status := runtime.RuntimeStatus()
	fmt.Printf("\n== %s ==\n", label)
	fmt.Printf("runtime=%s state=%s ready=%t inflight=%d\n",
		status.Name, status.State, status.Ready, status.InFlight)

	for _, worker := range runtime.Workers() {
		fmt.Printf("worker=%s state=%s ready=%t inflight=%d\n",
			worker.QualifiedName,
			worker.Status.State,
			worker.Status.Ready,
			worker.Status.InFlight)
	}
}
