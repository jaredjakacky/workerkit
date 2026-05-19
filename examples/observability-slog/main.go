package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
	"github.com/jaredjakacky/workerkit/retry"
	"github.com/jaredjakacky/workerkit/slogobserver"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// This example demonstrates Workerkit's backend-neutral observer events
	// mapped into structured slog records for production diagnostics.
	observer := slogobserver.New(logger,
		slogobserver.WithAttributes(slog.String("service", "observability-slog")),
	)

	runtime, err := workerkit.New(
		workerkit.Identity{Name: "slog_demo"},
		workerkit.WithObserver(observer),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker := &observedWorker{}
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "worker",
		Description: "Emits lifecycle, readiness, command, and failure observer events.",
		Worker:      worker,
	},
		workerkit.WithWorkerCommandRetry(retry.Attempts(3, retry.Constant(25*time.Millisecond), retry.None())),
		workerkit.WithCommand("demo/succeed", workerkit.CommandHandlerFunc(worker.succeed)),
		workerkit.WithCommand("demo/fail", workerkit.CommandHandlerFunc(worker.fail)),
		workerkit.WithCommand("demo/fail-once", workerkit.CommandHandlerFunc(worker.failOnce)),
	); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	if _, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker: "worker",
		Name:   "demo/succeed",
	}); err != nil {
		log.Fatal(err)
	}

	if _, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker: "worker",
		Name:   "demo/fail",
	}); err != nil {
		fmt.Printf("expected command failure: %v\n", err)
	}

	if _, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker: "worker",
		Name:   "demo/fail-once",
	}); err != nil {
		log.Fatal(err)
	}

	if err := worker.ReportBackgroundFailure(errors.New("background poll failed")); err != nil {
		log.Fatal(err)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

type observedWorker struct {
	runtime          workerkit.WorkerRuntime
	failOnceAttempts atomic.Int32
}

func (w *observedWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return errors.New("worker runtime handle unavailable")
	}
	w.runtime = runtime

	if err := runtime.SetReady(false); err != nil {
		return err
	}
	if err := runtime.SetReady(true); err != nil {
		return err
	}
	return nil
}

func (w *observedWorker) Stop(ctx context.Context) error {
	return nil
}

func (w *observedWorker) succeed(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	return workerkit.CommandResult{Message: "command succeeded"}, nil
}

func (w *observedWorker) fail(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	return workerkit.CommandResult{}, errors.New("command failed")
}

func (w *observedWorker) failOnce(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	if w.failOnceAttempts.Add(1) == 1 {
		return workerkit.CommandResult{}, errors.New("temporary command failure")
	}
	return workerkit.CommandResult{Message: "retry succeeded"}, nil
}

func (w *observedWorker) ReportBackgroundFailure(err error) error {
	if w.runtime == nil {
		return errors.New("worker runtime handle unavailable")
	}
	return w.runtime.ReportFailure(err)
}
