package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
)

func main() {
	ctx := context.Background()

	runtime, err := workerkit.New(workerkit.Identity{Name: "readiness"})
	if err != nil {
		log.Fatal(err)
	}

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "primary",
		Description: "Warms up before reporting ready.",
		Worker:      newWarmupWorker("primary", 300*time.Millisecond, true),
	}); err != nil {
		log.Fatal(err)
	}

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "optional",
		Description: "Runs but does not contribute to aggregate readiness.",
		Worker:      newWarmupWorker("optional", 0, false),
	}, workerkit.WithWorkerReadinessContribution(false)); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	printStatus(runtime, "after start")
	time.Sleep(500 * time.Millisecond)
	printStatus(runtime, "after warmup")

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}

	printStatus(runtime, "after shutdown")
}

type warmupWorker struct {
	name       string
	warmup     time.Duration
	readyAfter bool

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
}

func newWarmupWorker(name string, warmup time.Duration, readyAfter bool) *warmupWorker {
	return &warmupWorker{
		name:       name,
		warmup:     warmup,
		readyAfter: readyAfter,
		done:       make(chan struct{}),
	}
}

func (w *warmupWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return errors.New("worker runtime handle unavailable")
	}

	// This example demonstrates that lifecycle and readiness are separate:
	// a worker can be running before it is ready to serve production work.
	if err := runtime.SetReady(false); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	w.mu.Lock()
	w.cancel = cancel
	w.done = make(chan struct{})
	done := w.done
	w.mu.Unlock()

	go func() {
		defer close(done)

		if w.readyAfter {
			timer := time.NewTimer(w.warmup)
			defer timer.Stop()

			select {
			case <-runCtx.Done():
				fmt.Printf("%s stopped before warmup completed\n", w.name)
				return
			case <-timer.C:
				if err := runtime.SetReady(true); err != nil {
					_ = runtime.ReportFailure(err)
					return
				}
				fmt.Printf("%s warmup complete\n", w.name)
			}
		}

		<-runCtx.Done()
		fmt.Printf("%s stopped\n", w.name)
	}()

	return nil
}

func (w *warmupWorker) Stop(ctx context.Context) error {
	w.mu.Lock()
	cancel := w.cancel
	done := w.done
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func printStatus(runtime *workerkit.Runtime, label string) {
	status := runtime.RuntimeStatus()
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
