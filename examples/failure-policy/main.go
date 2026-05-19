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
	runScenario("FailurePolicyIsolate", workerkit.FailurePolicyIsolate)
	runScenario("FailurePolicyMarkRuntimeUnready", workerkit.FailurePolicyMarkRuntimeUnready)
	runScenario("FailurePolicyFailRuntime", workerkit.FailurePolicyFailRuntime)
}

func runScenario(label string, policy workerkit.FailurePolicy) {
	ctx := context.Background()

	fmt.Printf("\n== %s ==\n", label)

	runtime, err := workerkit.New(workerkit.Identity{Name: scenarioName(policy)})
	if err != nil {
		log.Fatal(err)
	}

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "healthy",
		Description: "Keeps the runtime active while another worker fails.",
		Worker:      healthyWorker{},
	}); err != nil {
		log.Fatal(err)
	}

	failing := &failingWorker{}

	// This example demonstrates how worker failures affect aggregate runtime
	// status differently depending on the configured failure policy.
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "failing",
		Description: "Reports a background failure after startup.",
		Worker:      failing,
	},
		workerkit.WithWorkerFailurePolicy(policy),
		workerkit.WithWorkerReadinessContribution(false),
	); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	if err := failing.Fail(errors.New("background poll failed")); err != nil {
		log.Fatal(err)
	}

	printStatus(runtime)

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

type healthyWorker struct{}

func (healthyWorker) Start(context.Context) error {
	return nil
}

func (healthyWorker) Stop(context.Context) error {
	return nil
}

type failingWorker struct {
	runtime workerkit.WorkerRuntime
}

func (w *failingWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return errors.New("worker runtime handle unavailable")
	}
	w.runtime = runtime
	return nil
}

func (w *failingWorker) Stop(context.Context) error {
	return nil
}

func (w *failingWorker) Fail(err error) error {
	if w.runtime == nil {
		return errors.New("worker runtime handle unavailable")
	}
	return w.runtime.ReportFailure(err)
}

func printStatus(runtime *workerkit.Runtime) {
	status := runtime.Status()
	fmt.Printf("runtime=%s state=%s ready=%t workers=%d\n",
		status.Name, status.State, status.Ready, status.Workers)

	for _, worker := range runtime.Workers() {
		fmt.Printf("worker=%s state=%s ready=%t accepting=%t\n",
			worker.QualifiedName,
			worker.Status.State,
			worker.Status.Ready,
			worker.Status.AcceptingWork)
		if worker.Status.LastFailure != nil {
			fmt.Printf("last failure worker=%s message=%q\n",
				worker.QualifiedName,
				worker.Status.LastFailure.Message)
		}
	}
}

func scenarioName(policy workerkit.FailurePolicy) string {
	switch policy {
	case workerkit.FailurePolicyIsolate:
		return "failure_isolate"
	case workerkit.FailurePolicyMarkRuntimeUnready:
		return "failure_unready"
	case workerkit.FailurePolicyFailRuntime:
		return "failure_runtime"
	default:
		return "failure_unknown"
	}
}
