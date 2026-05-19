package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
	"github.com/jaredjakacky/workerkit/retry"
)

func main() {
	ctx := context.Background()

	runtime, err := workerkit.New(workerkit.Identity{Name: "retry_demo"})
	if err != nil {
		log.Fatal(err)
	}

	worker := &retryWorker{}

	// This example demonstrates bounded, jittered, predicate-gated retry;
	// Workerkit retries only failures that the policy says are safe to repeat.
	commandRetry := retry.AttemptsIf(
		4,
		retry.Exponential(100*time.Millisecond, 2, time.Second),
		retry.Full(),
		isTemporary,
	)

	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "payments",
		Description: "Owns retryable payment-domain commands.",
		Worker:      worker,
	},
		workerkit.WithWorkerCommandRetry(commandRetry),
		workerkit.WithCommand("payments/capture", workerkit.CommandHandlerFunc(worker.capture)),
		workerkit.WithCommand("payments/validate", workerkit.CommandHandlerFunc(worker.validate)),
	); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	result, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker: "payments",
		Name:   "payments/capture",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("capture result message=%q attempts=%d\n", result.Message, worker.captureAttempts.Load())

	_, err = runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker: "payments",
		Name:   "payments/validate",
	})
	if err != nil {
		fmt.Printf("validate stopped without retry: %v attempts=%d\n", err, worker.validateAttempts.Load())
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

type retryWorker struct {
	captureAttempts  atomic.Int32
	validateAttempts atomic.Int32
}

func (w *retryWorker) Start(ctx context.Context) error {
	return nil
}

func (w *retryWorker) Stop(ctx context.Context) error {
	return nil
}

func (w *retryWorker) capture(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	attempt := w.captureAttempts.Add(1)
	fmt.Printf("capture attempt=%d\n", attempt)
	if attempt < 3 {
		return workerkit.CommandResult{}, temporaryError{err: fmt.Errorf("gateway timeout on attempt %d", attempt)}
	}
	return workerkit.CommandResult{Message: "capture accepted"}, nil
}

func (w *retryWorker) validate(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	attempt := w.validateAttempts.Add(1)
	fmt.Printf("validate attempt=%d\n", attempt)
	return workerkit.CommandResult{}, errors.New("card number failed validation")
}

type temporaryError struct {
	err error
}

func (e temporaryError) Error() string {
	return e.err.Error()
}

func (e temporaryError) Unwrap() error {
	return e.err
}

func (e temporaryError) Temporary() bool {
	return true
}

func isTemporary(err error) bool {
	var temporary interface {
		Temporary() bool
	}
	return errors.As(err, &temporary) && temporary.Temporary()
}
