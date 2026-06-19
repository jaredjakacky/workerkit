package workerkit_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit"
	"strings"
	"sync"
	"testing"
	"time"

	retrykit "github.com/jaredjakacky/workerkit/retry"
)

type testWorker struct {
	start func(context.Context) error
	stop  func(context.Context) error
}

func (w testWorker) Start(ctx context.Context) error {
	if w.start == nil {
		return nil
	}
	return w.start(ctx)
}

func (w testWorker) Stop(ctx context.Context) error {
	if w.stop == nil {
		return nil
	}
	return w.stop(ctx)
}

type recordingObserver struct {
	mu sync.Mutex

	transitions   []TransitionEvent
	commandStarts []CommandStartEvent
	failures      []FailureEvent
	commandEnds   []CommandEndEvent
}

func (o *recordingObserver) ObserveTransition(_ context.Context, event TransitionEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.transitions = append(o.transitions, event)
}

func (o *recordingObserver) StartCommand(ctx context.Context, event CommandStartEvent) (context.Context, CommandObservation) {
	o.mu.Lock()
	o.commandStarts = append(o.commandStarts, event)
	o.mu.Unlock()
	return ctx, CommandObservationFunc(func(_ context.Context, event CommandEndEvent) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.commandEnds = append(o.commandEnds, event)
	})
}

func (o *recordingObserver) ObserveFailure(_ context.Context, event FailureEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures = append(o.failures, event)
}

func (o *recordingObserver) ObserveReadiness(context.Context, ReadinessEvent) {}

type recordingObserverSnapshot struct {
	transitions   []TransitionEvent
	commandStarts []CommandStartEvent
	failures      []FailureEvent
	commandEnds   []CommandEndEvent
}

func (o *recordingObserver) snapshot() recordingObserverSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	return recordingObserverSnapshot{
		transitions:   append([]TransitionEvent(nil), o.transitions...),
		commandStarts: append([]CommandStartEvent(nil), o.commandStarts...),
		failures:      append([]FailureEvent(nil), o.failures...),
		commandEnds:   append([]CommandEndEvent(nil), o.commandEnds...),
	}
}

const testNoSignalTimeout = 20 * time.Millisecond

func newTestRuntime(t *testing.T, opts ...RuntimeOption) *Runtime {
	t.Helper()

	rt, err := New(Identity{Name: "test-runtime"}, opts...)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return rt
}

func requireWorker(t *testing.T, rt *Runtime, name string) WorkerSnapshot {
	t.Helper()

	snapshot, ok := rt.Worker(name)
	if !ok {
		t.Fatalf("worker %q missing", name)
	}
	return snapshot
}

func requireWorkerState(t *testing.T, rt *Runtime, name string, want LifecycleState) WorkerSnapshot {
	t.Helper()

	snapshot := requireWorker(t, rt, name)
	if snapshot.Status.State != want {
		t.Fatalf("%s state = %s, want %s", name, snapshot.Status.State, want)
	}
	return snapshot
}

func TestStopAllStopsFailedWorkersAndPreservesLastFailure(t *testing.T) {
	var workerRuntime WorkerRuntime
	var stops int
	rt := newTestRuntime(t)
	err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(ctx context.Context) error {
				var ok bool
				workerRuntime, ok = WorkerRuntimeFromContext(ctx)
				if !ok {
					t.Fatal("missing WorkerRuntime")
				}
				return nil
			},
			stop: func(context.Context) error {
				stops++
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	failure := errors.New("background failed")
	if err := workerRuntime.ReportFailure(failure); err != nil {
		t.Fatalf("ReportFailure returned error: %v", err)
	}
	if err := rt.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll returned error: %v", err)
	}

	if stops != 1 {
		t.Fatalf("StopAll called Stop %d times, want 1", stops)
	}
	snapshot := requireWorker(t, rt, "worker")
	status := snapshot.Status
	if status.State != StateStopped {
		t.Fatalf("worker state = %s, want %s", status.State, StateStopped)
	}
	if status.LastFailure == nil || status.LastFailure.Message != failure.Error() {
		t.Fatalf("LastFailure = %#v, want %q", status.LastFailure, failure.Error())
	}
}

func TestSuccessfulStartClearsPriorLastFailure(t *testing.T) {
	var workerRuntime WorkerRuntime
	rt := newTestRuntime(t)
	err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(ctx context.Context) error {
				var ok bool
				workerRuntime, ok = WorkerRuntimeFromContext(ctx)
				if !ok {
					t.Fatal("missing WorkerRuntime")
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := workerRuntime.ReportFailure(errors.New("background failed")); err != nil {
		t.Fatalf("ReportFailure returned error: %v", err)
	}

	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("second Start returned error: %v", err)
	}

	snapshot := requireWorker(t, rt, "worker")
	status := snapshot.Status
	if status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", status.LastFailure)
	}
}

func TestBackgroundReportFailureDuringStartMarksWorkerFailedWithoutStartError(t *testing.T) {
	startFailure := errors.New("reported during start")
	failureReported := make(chan struct{})
	releaseStart := make(chan struct{})
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(ctx context.Context) error {
				workerRuntime, ok := WorkerRuntimeFromContext(ctx)
				if !ok {
					t.Fatal("missing WorkerRuntime")
				}
				reported := make(chan error, 1)
				go func() {
					reported <- workerRuntime.ReportFailure(startFailure)
				}()
				err := <-reported
				close(failureReported)
				<-releaseStart
				return err
			},
		},
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	startDone := make(chan error, 1)
	go func() {
		startDone <- rt.Start(context.Background(), "worker")
	}()
	<-failureReported

	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateStarting {
		t.Fatalf("worker state during Start = %s, want %s", snapshot.Status.State, StateStarting)
	}
	if snapshot.Status.LastFailure == nil || snapshot.Status.LastFailure.Message != startFailure.Error() {
		t.Fatalf("LastFailure during Start = %#v, want %q", snapshot.Status.LastFailure, startFailure.Error())
	}
	concurrentCtx, cancelConcurrent := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelConcurrent()
	if err := rt.Start(concurrentCtx, "worker"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent Start error = %v, want DeadlineExceeded", err)
	}
	concurrentCtx, cancelConcurrent = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelConcurrent()
	if err := rt.Stop(concurrentCtx, "worker"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent Stop error = %v, want DeadlineExceeded", err)
	}

	close(releaseStart)
	if err := <-startDone; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	snapshot = requireWorker(t, rt, "worker")
	status := snapshot.Status
	if status.State != StateFailed {
		t.Fatalf("worker state = %s, want %s", status.State, StateFailed)
	}
	if status.LastFailure == nil || status.LastFailure.Message != startFailure.Error() {
		t.Fatalf("LastFailure = %#v, want %q", status.LastFailure, startFailure.Error())
	}
	events := observer.snapshot()
	var transitions []string
	for _, event := range events.transitions {
		if event.Worker == "test-runtime/worker" {
			transitions = append(transitions, string(event.From)+"->"+string(event.To))
		}
	}
	if got := strings.Join(transitions, ","); got != "registered->starting,starting->failed" {
		t.Fatalf("worker transitions = %q, want registered->starting,starting->failed", got)
	}
}

func TestLifecycleAttemptFailureDoesNotEmitDuplicateFailureEvent(t *testing.T) {
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	startFailure := errors.New("start failed")
	err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(context.Context) error {
				return startFailure
			},
		},
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	err = rt.Start(context.Background(), "worker")
	if !errors.Is(err, startFailure) {
		t.Fatalf("Start error = %v, want %v", err, startFailure)
	}
	events := observer.snapshot()
	if got := len(events.failures); got != 1 {
		t.Fatalf("failure events = %d, want 1", got)
	}
}

func TestFailedWorkerFailureDoesNotEmitDuplicateLifecycleFailureEvent(t *testing.T) {
	observer := &recordingObserver{}
	reportedFailure := errors.New("reported during start")
	returnedFailure := errors.New("returned after report")
	rt := newTestRuntime(t, WithObserver(observer))
	err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(ctx context.Context) error {
				workerRuntime, ok := WorkerRuntimeFromContext(ctx)
				if !ok {
					t.Fatal("missing WorkerRuntime")
				}
				if err := workerRuntime.ReportFailure(reportedFailure); err != nil {
					t.Fatalf("ReportFailure returned error: %v", err)
				}
				return returnedFailure
			},
		},
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	err = rt.Start(context.Background(), "worker")
	if !errors.Is(err, returnedFailure) {
		t.Fatalf("Start error = %v, want %v", err, returnedFailure)
	}
	snapshot := requireWorker(t, rt, "worker")
	status := snapshot.Status
	if status.State != StateFailed {
		t.Fatalf("worker state = %s, want %s", status.State, StateFailed)
	}
	if status.LastFailure == nil || status.LastFailure.Message != returnedFailure.Error() {
		t.Fatalf("LastFailure = %#v, want %q", status.LastFailure, returnedFailure.Error())
	}

	events := observer.snapshot()
	var failedToFailed int
	for _, event := range events.transitions {
		if event.Worker == "test-runtime/worker" && event.From == StateFailed && event.To == StateFailed {
			failedToFailed++
		}
	}
	if failedToFailed != 0 {
		t.Fatalf("failed -> failed worker transitions = %d, want 0", failedToFailed)
	}
	if got := len(events.failures); got != 1 {
		t.Fatalf("failure events = %d, want 1", got)
	}
	if !errors.Is(events.failures[0].Err, reportedFailure) {
		t.Fatalf("failure event error = %v, want %v", events.failures[0].Err, reportedFailure)
	}
	if events.failures[0].Command != "" {
		t.Fatalf("failure event command = %q, want empty", events.failures[0].Command)
	}
}

func TestCommandRetryEmitsFailurePerFailedAttemptAndSuccessfulCommandEnd(t *testing.T) {
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	var attempts int
	err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithWorkerCommandRetry(retrykit.Attempts(2, nil, nil)),
		WithCommand("retry", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			attempts++
			if attempts == 1 {
				return CommandResult{}, errors.New("transient command failure")
			}
			return CommandResult{Message: "ok"}, nil
		})),
	)
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	result, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "retry"})
	if err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if result.Message != "ok" {
		t.Fatalf("result message = %q, want ok", result.Message)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	events := observer.snapshot()
	if got := len(events.commandStarts); got != 1 {
		t.Fatalf("command start events = %d, want 1", got)
	}
	if events.commandStarts[0].DispatchID == "" {
		t.Fatal("command start dispatch id is empty")
	}
	if got := len(events.failures); got != 1 {
		t.Fatalf("failure events = %d, want 1", got)
	}
	if events.failures[0].DispatchID != events.commandStarts[0].DispatchID {
		t.Fatalf("failure dispatch id = %q, want %q", events.failures[0].DispatchID, events.commandStarts[0].DispatchID)
	}
	if events.failures[0].Attempt != 1 {
		t.Fatalf("failure attempt = %d, want 1", events.failures[0].Attempt)
	}
	if got := len(events.commandEnds); got != 1 {
		t.Fatalf("command end events = %d, want 1", got)
	}
	if events.commandEnds[0].DispatchID != events.commandStarts[0].DispatchID {
		t.Fatalf("command end dispatch id = %q, want %q", events.commandEnds[0].DispatchID, events.commandStarts[0].DispatchID)
	}
	if events.commandEnds[0].Attempts != 2 {
		t.Fatalf("command end attempts = %d, want 2", events.commandEnds[0].Attempts)
	}
	if !events.commandEnds[0].Success {
		t.Fatalf("command end success = false, want true: %#v", events.commandEnds[0])
	}
	if events.commandEnds[0].Err != nil {
		t.Fatalf("command end error = %v, want nil", events.commandEnds[0].Err)
	}
}

func TestRecordingObserverSupportsConcurrentDispatches(t *testing.T) {
	const dispatches = 32
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("work", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, dispatches)
	for range dispatches {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Dispatch returned error: %v", err)
		}
	}

	events := observer.snapshot()
	if got := len(events.commandStarts); got != dispatches {
		t.Fatalf("command start events = %d, want %d", got, dispatches)
	}
	if got := len(events.commandEnds); got != dispatches {
		t.Fatalf("command end events = %d, want %d", got, dispatches)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})
}

func TestNewRejectsInvalidIdentity(t *testing.T) {
	_, err := New(Identity{Name: "TestRuntime"})
	if err == nil {
		t.Fatal("New returned nil, want invalid identity error")
	}
}

func TestIdentityReturnsRuntimeIdentity(t *testing.T) {
	rt := newTestRuntime(t)

	identity := rt.Identity()
	if identity.Name != "test-runtime" {
		t.Fatalf("identity name = %q, want test-runtime", identity.Name)
	}
}

func TestRegisterRejectsInvalidInputsAndClosedRegistration(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "nil"}); !errors.Is(err, ErrNilWorker) {
		t.Fatalf("Register nil worker error = %v, want ErrNilWorker", err)
	}
	if err := rt.Register(WorkerSpec{Name: "Invalid", Worker: testWorker{}}); err == nil {
		t.Fatal("Register invalid worker name returned nil, want error")
	}
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); !errors.Is(err, ErrWorkerAlreadyRegistered) {
		t.Fatalf("Register duplicate error = %v, want ErrWorkerAlreadyRegistered", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{Name: "late", Worker: testWorker{}}); !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("Register after start error = %v, want ErrInvalidWorkerState", err)
	}
}

func TestCommandsReturnsSortedDiscoveryForLocalAndQualifiedWorkerNames(t *testing.T) {
	rt := newTestRuntime(t)
	err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommandSpec(CommandSpec{
			Name:        "zeta",
			Description: "last",
			Handler:     CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) { return CommandResult{}, nil }),
		}),
		WithCommandSpec(CommandSpec{
			Name:        "alpha",
			Description: "first",
			Handler:     CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) { return CommandResult{}, nil }),
		}),
	)
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	commands, ok := rt.Commands("worker")
	if !ok {
		t.Fatal("Commands local name ok = false, want true")
	}
	if len(commands) != 2 {
		t.Fatalf("commands length = %d, want 2", len(commands))
	}
	if commands[0].Name != "alpha" || commands[1].Name != "zeta" {
		t.Fatalf("command order = %q, %q, want alpha, zeta", commands[0].Name, commands[1].Name)
	}
	if commands[0].Worker != "test-runtime/worker" || commands[0].Description != "first" {
		t.Fatalf("first command = %#v, want qualified worker and description", commands[0])
	}

	qualified, ok := rt.Commands("test-runtime/worker")
	if !ok {
		t.Fatal("Commands qualified name ok = false, want true")
	}
	if len(qualified) != len(commands) || qualified[0].Name != commands[0].Name || qualified[1].Name != commands[1].Name {
		t.Fatalf("qualified commands = %#v, want %#v", qualified, commands)
	}
	if _, ok := rt.Commands("missing"); ok {
		t.Fatal("Commands missing worker ok = true, want false")
	}
}

func TestCommandsReturnsEmptySliceForWorkerWithoutCommands(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	commands, ok := rt.Commands("worker")
	if !ok {
		t.Fatal("Commands ok = false, want true")
	}
	if commands == nil {
		t.Fatal("Commands returned nil, want empty slice")
	}
	if len(commands) != 0 {
		t.Fatalf("commands length = %d, want 0", len(commands))
	}
}

func TestStartAllStartsInRegistrationOrderAndDoesNotRollback(t *testing.T) {
	startFailure := errors.New("second failed")
	var calls []string
	var firstStops int
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{
		Name: "first",
		Worker: testWorker{
			start: func(context.Context) error {
				calls = append(calls, "first")
				return nil
			},
			stop: func(context.Context) error {
				firstStops++
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "second",
		Worker: testWorker{
			start: func(context.Context) error {
				calls = append(calls, "second")
				return startFailure
			},
		},
	}); err != nil {
		t.Fatalf("Register second returned error: %v", err)
	}

	err := rt.StartAll(context.Background())
	if !errors.Is(err, startFailure) {
		t.Fatalf("StartAll error = %v, want %v", err, startFailure)
	}
	if got := strings.Join(calls, ","); got != "first,second" {
		t.Fatalf("start order = %q, want first,second", got)
	}
	if firstStops != 0 {
		t.Fatalf("first stops = %d, want 0", firstStops)
	}
	requireWorkerState(t, rt, "first", StateRunning)
	requireWorkerState(t, rt, "second", StateFailed)
}

func TestDrainAllDrainsRunningWorkersInRegistrationOrderAndSkipsInactive(t *testing.T) {
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	for _, name := range []string{"first", "second", "third"} {
		if err := rt.Register(WorkerSpec{Name: name, Worker: testWorker{}}); err != nil {
			t.Fatalf("Register %s returned error: %v", name, err)
		}
	}
	if err := rt.Start(context.Background(), "first"); err != nil {
		t.Fatalf("Start first returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "second"); err != nil {
		t.Fatalf("Start second returned error: %v", err)
	}

	if err := rt.DrainAll(context.Background()); err != nil {
		t.Fatalf("DrainAll returned error: %v", err)
	}

	events := observer.snapshot()
	var drained []string
	for _, event := range events.transitions {
		if event.Worker != "" && event.From == StateRunning && event.To == StateDraining {
			drained = append(drained, event.Worker)
		}
	}
	if got := strings.Join(drained, ","); got != "test-runtime/first,test-runtime/second" {
		t.Fatalf("drain order = %q, want test-runtime/first,test-runtime/second", got)
	}

	requireWorkerState(t, rt, "first", StateDraining)
	requireWorkerState(t, rt, "second", StateDraining)
	requireWorkerState(t, rt, "third", StateRegistered)
}

func TestLifecycleOperationsSerializeStartAndStop(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	startDone := make(chan error, 1)
	stopEntered := make(chan struct{})
	stopDone := make(chan error, 1)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(context.Context) error {
				close(startEntered)
				<-releaseStart
				return nil
			},
			stop: func(context.Context) error {
				close(stopEntered)
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	go func() {
		startDone <- rt.Start(context.Background(), "worker")
	}()
	<-startEntered
	go func() {
		stopDone <- rt.Stop(context.Background(), "worker")
	}()

	select {
	case <-stopEntered:
		t.Fatal("Stop entered worker callback before Start completed")
	case err := <-stopDone:
		t.Fatalf("Stop returned before Start completed: %v", err)
	case <-time.After(testNoSignalTimeout):
	}

	close(releaseStart)
	if err := <-startDone; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	select {
	case <-stopEntered:
	default:
		t.Fatal("Stop did not enter worker callback")
	}
	requireWorkerState(t, rt, "worker", StateStopped)
}

func TestLifecycleOperationWaitingForGateHonorsContext(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	startDone := make(chan error, 1)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{start: func(context.Context) error {
			close(startEntered)
			<-releaseStart
			return nil
		}},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	go func() {
		startDone <- rt.Start(context.Background(), "worker")
	}()
	<-startEntered

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := rt.Stop(ctx, "worker"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop error = %v, want DeadlineExceeded", err)
	}

	close(releaseStart)
	if err := <-startDone; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	requireWorkerState(t, rt, "worker", StateRunning)
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})
}

func TestRegisterWaitsForConcurrentStartAll(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	startAllDone := make(chan error, 1)
	registerDone := make(chan error, 1)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{
		Name: "first",
		Worker: testWorker{start: func(context.Context) error {
			close(startEntered)
			<-releaseStart
			return nil
		}},
	}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	go func() {
		startAllDone <- rt.StartAll(context.Background())
	}()
	<-startEntered
	go func() {
		registerDone <- rt.Register(WorkerSpec{Name: "second", Worker: testWorker{}})
	}()

	select {
	case err := <-registerDone:
		t.Fatalf("Register returned before StartAll completed: %v", err)
	case <-time.After(testNoSignalTimeout):
	}

	close(releaseStart)
	if err := <-startAllDone; err != nil {
		t.Fatalf("StartAll returned error: %v", err)
	}
	if err := <-registerDone; !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("Register error = %v, want ErrInvalidWorkerState", err)
	}
	if _, ok := rt.Worker("second"); ok {
		t.Fatal("second worker registered after runtime started")
	}
	t.Cleanup(func() {
		_ = rt.StopAll(context.Background())
	})
}

func TestDispatchRemainsConcurrentWithLifecycleOperation(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	startDone := make(chan error, 1)
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "running", Worker: testWorker{}},
		WithCommand("ping", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{Message: "pong"}, nil
		})),
	); err != nil {
		t.Fatalf("Register running returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "starting",
		Worker: testWorker{start: func(context.Context) error {
			close(startEntered)
			<-releaseStart
			return nil
		}},
	}); err != nil {
		t.Fatalf("Register starting returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "running"); err != nil {
		t.Fatalf("Start running returned error: %v", err)
	}
	go func() {
		startDone <- rt.Start(context.Background(), "starting")
	}()
	<-startEntered

	result, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "running", Name: "ping"})
	if err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if result.Message != "pong" {
		t.Fatalf("Dispatch message = %q, want pong", result.Message)
	}

	close(releaseStart)
	if err := <-startDone; err != nil {
		t.Fatalf("Start starting returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.StopAll(context.Background())
	})
}

func TestDrainAllBestEffortWaitsForConcurrentStart(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	startDone := make(chan error, 1)
	drainDone := make(chan error, 1)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "first", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "second",
		Worker: testWorker{start: func(context.Context) error {
			close(startEntered)
			<-releaseStart
			return nil
		}},
	}); err != nil {
		t.Fatalf("Register second returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "first"); err != nil {
		t.Fatalf("Start first returned error: %v", err)
	}
	go func() {
		startDone <- rt.Start(context.Background(), "second")
	}()
	<-startEntered
	go func() {
		drainDone <- rt.DrainAllBestEffort(context.Background())
	}()

	select {
	case err := <-drainDone:
		t.Fatalf("DrainAllBestEffort returned before Start completed: %v", err)
	case <-time.After(testNoSignalTimeout):
	}
	requireWorkerState(t, rt, "first", StateRunning)

	close(releaseStart)
	if err := <-startDone; err != nil {
		t.Fatalf("Start second returned error: %v", err)
	}
	if err := <-drainDone; err != nil {
		t.Fatalf("DrainAllBestEffort returned error: %v", err)
	}

	requireWorkerState(t, rt, "first", StateDraining)
	requireWorkerState(t, rt, "second", StateDraining)
}

func TestStopAllStopsInReverseOrderAndReturnsJoinedErrors(t *testing.T) {
	firstErr := errors.New("first stop failed")
	secondErr := errors.New("second stop failed")
	var stops []string
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{
		Name: "first",
		Worker: testWorker{
			stop: func(context.Context) error {
				stops = append(stops, "first")
				return firstErr
			},
		},
	}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "second",
		Worker: testWorker{
			stop: func(context.Context) error {
				stops = append(stops, "second")
				return secondErr
			},
		},
	}); err != nil {
		t.Fatalf("Register second returned error: %v", err)
	}
	if err := rt.StartAll(context.Background()); err != nil {
		t.Fatalf("StartAll returned error: %v", err)
	}

	err := rt.StopAll(context.Background())
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("StopAll error = %v, want both stop errors", err)
	}
	if got := strings.Join(stops, ","); got != "second,first" {
		t.Fatalf("stop order = %q, want second,first", got)
	}
}

func TestShutdownDrainsWaitsForInFlightCommandsAndStopsInReverseOrder(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var stops []string
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{
			Name: "first",
			Worker: testWorker{
				stop: func(context.Context) error {
					stops = append(stops, "first")
					return nil
				},
			},
		},
		WithCommand("block", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			return CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "second",
		Worker: testWorker{
			stop: func(context.Context) error {
				stops = append(stops, "second")
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register second returned error: %v", err)
	}
	if err := rt.StartAll(context.Background()); err != nil {
		t.Fatalf("StartAll returned error: %v", err)
	}

	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "first", Name: "block"})
		dispatchDone <- err
	}()
	<-entered

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- rt.Shutdown(context.Background())
	}()

	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before command completed: %v", err)
	case <-time.After(testNoSignalTimeout):
	}
	if len(stops) != 0 {
		t.Fatalf("stops before command release = %#v, want none", stops)
	}

	close(release)
	if err := <-dispatchDone; err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	if got := strings.Join(stops, ","); got != "second,first" {
		t.Fatalf("stop order = %q, want second,first", got)
	}
}

func TestShutdownTimeoutBeforeStopAllDoesNotFailWorker(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var stops int
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				stop: func(ctx context.Context) error {
					stops++
					return ctx.Err()
				},
			},
		},
		WithCommand("block", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			return CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "block"})
		dispatchDone <- err
	}()
	<-entered

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := rt.Shutdown(shutdownCtx)
	close(release)
	if dispatchErr := <-dispatchDone; dispatchErr != nil {
		t.Fatalf("Dispatch returned error: %v", dispatchErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want DeadlineExceeded", err)
	}
	if stops != 0 {
		t.Fatalf("stop calls = %d, want 0", stops)
	}
	snapshot := requireWorker(t, rt, "worker")
	status := snapshot.Status
	if status.State != StateDraining {
		t.Fatalf("worker state = %s, want %s", status.State, StateDraining)
	}
	if status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", status.LastFailure)
	}
}

func TestDrainMarksWorkerUnreadyAndRejectsDispatch(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("work", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) { return CommandResult{}, nil })),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := rt.Drain(context.Background(), "worker"); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	if err := rt.Drain(context.Background(), "worker"); err != nil {
		t.Fatalf("second Drain returned error: %v", err)
	}

	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateDraining {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateDraining)
	}
	if snapshot.Status.Ready {
		t.Fatal("worker ready = true, want false")
	}
	if snapshot.Status.AcceptingWork {
		t.Fatal("worker accepting work = true, want false")
	}
	_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work"})
	if !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("Dispatch error = %v, want ErrInvalidWorkerState", err)
	}
}

func TestDrainWaitIdleAndStopComposeForOneWorkerGracefulShutdown(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	stopped := make(chan struct{})
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				stop: func(context.Context) error {
					close(stopped)
					return nil
				},
			},
		},
		WithCommand("block", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			return CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "block"})
		dispatchDone <- err
	}()
	<-entered

	drainDone := make(chan error, 1)
	go func() {
		if err := rt.Drain(context.Background(), "worker"); err != nil {
			drainDone <- err
			return
		}
		if err := rt.WaitIdle(context.Background(), "worker"); err != nil {
			drainDone <- err
			return
		}
		drainDone <- rt.Stop(context.Background(), "worker")
	}()

	select {
	case <-stopped:
		t.Fatal("worker stopped before command was released")
	case <-time.After(testNoSignalTimeout):
	}

	close(release)
	if err := <-dispatchDone; err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if err := <-drainDone; err != nil {
		t.Fatalf("composed drain/wait/stop returned error: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("worker was not stopped")
	}
}

func TestWaitIdleAndWaitAllIdleReturnContextErrorWhileBusy(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("block", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			return CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "block"})
		dispatchDone <- err
	}()
	<-entered

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := rt.WaitIdle(waitCtx, "worker"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitIdle error = %v, want DeadlineExceeded", err)
	}
	waitAllCtx, cancelAll := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelAll()
	if err := rt.WaitAllIdle(waitAllCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitAllIdle error = %v, want DeadlineExceeded", err)
	}

	close(release)
	if err := <-dispatchDone; err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if err := rt.WaitIdle(context.Background(), "worker"); err != nil {
		t.Fatalf("WaitIdle after release returned error: %v", err)
	}
	if err := rt.WaitAllIdle(context.Background()); err != nil {
		t.Fatalf("WaitAllIdle after release returned error: %v", err)
	}
}

func TestDispatchAdmissionErrors(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithWorkerAcceptingWorkOnStart(false),
		WithCommand("work", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) { return CommandResult{}, nil })),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "missing", Name: "work"})
	if !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("missing worker Dispatch error = %v, want ErrWorkerNotFound", err)
	}
	_, err = rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "missing"})
	if !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("missing command Dispatch error = %v, want ErrCommandNotFound", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	_, err = rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work"})
	if !errors.Is(err, ErrWorkerNotAcceptingWork) {
		t.Fatalf("not accepting Dispatch error = %v, want ErrWorkerNotAcceptingWork", err)
	}
	if err := rt.Stop(context.Background(), "worker"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	_, err = rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work"})
	if !errors.Is(err, ErrRuntimeNotAcceptingWork) {
		t.Fatalf("stopped runtime Dispatch error = %v, want ErrRuntimeNotAcceptingWork", err)
	}
}

func TestDispatchSetsRequestedAtWhenZeroAndPreservesProvidedValue(t *testing.T) {
	var captured []time.Time
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("work", CommandHandlerFunc(func(_ context.Context, req CommandRequest) (CommandResult, error) {
			captured = append(captured, req.RequestedAt)
			return CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	before := time.Now()
	if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work"}); err != nil {
		t.Fatalf("Dispatch zero RequestedAt returned error: %v", err)
	}
	after := time.Now()
	if len(captured) != 1 {
		t.Fatalf("captured requests = %d, want 1", len(captured))
	}
	if captured[0].IsZero() {
		t.Fatal("RequestedAt was zero, want runtime-filled timestamp")
	}
	if captured[0].Before(before) || captured[0].After(after) {
		t.Fatalf("RequestedAt = %s, want between %s and %s", captured[0], before, after)
	}

	provided := time.Unix(123, 456).UTC()
	if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work", RequestedAt: provided}); err != nil {
		t.Fatalf("Dispatch provided RequestedAt returned error: %v", err)
	}
	if len(captured) != 2 {
		t.Fatalf("captured requests = %d, want 2", len(captured))
	}
	if !captured[1].Equal(provided) {
		t.Fatalf("RequestedAt = %s, want preserved %s", captured[1], provided)
	}
}

func TestDispatchCommandConcurrencyLimits(t *testing.T) {
	t.Run("runtime", func(t *testing.T) {
		rt := newTestRuntime(t, WithRuntimeCommandConcurrency(1))
		firstEntered := make(chan struct{})
		release := make(chan struct{})
		blocking := CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			select {
			case <-firstEntered:
			default:
				close(firstEntered)
			}
			<-release
			return CommandResult{}, nil
		})
		if err := rt.Register(WorkerSpec{Name: "first", Worker: testWorker{}}, WithCommand("block", blocking)); err != nil {
			t.Fatalf("Register first returned error: %v", err)
		}
		if err := rt.Register(WorkerSpec{Name: "second", Worker: testWorker{}}, WithCommand("block", blocking)); err != nil {
			t.Fatalf("Register second returned error: %v", err)
		}
		if err := rt.StartAll(context.Background()); err != nil {
			t.Fatalf("StartAll returned error: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "first", Name: "block"})
			done <- err
		}()
		<-firstEntered

		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "second", Name: "block"})
		if !errors.Is(err, ErrRuntimeSaturated) {
			t.Fatalf("Dispatch error = %v, want ErrRuntimeSaturated", err)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("blocking Dispatch returned error: %v", err)
		}
	})

	t.Run("worker", func(t *testing.T) {
		rt := newTestRuntime(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		if err := rt.Register(
			WorkerSpec{Name: "worker", Worker: testWorker{}},
			WithWorkerCommandConcurrency(1),
			WithCommand("block", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
				close(entered)
				<-release
				return CommandResult{}, nil
			})),
		); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "block"})
			done <- err
		}()
		<-entered

		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "block"})
		if !errors.Is(err, ErrWorkerSaturated) {
			t.Fatalf("Dispatch error = %v, want ErrWorkerSaturated", err)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("blocking Dispatch returned error: %v", err)
		}
	})
}

func TestCommandPanicRecoverFailsWorkerAndReturnsError(t *testing.T) {
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("panic", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			panic("boom")
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "panic"})
	if err == nil {
		t.Fatal("Dispatch returned nil, want panic error")
	}
	if !strings.Contains(err.Error(), "recovered panic") {
		t.Fatalf("Dispatch error = %v, want recovered panic", err)
	}
	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateFailed {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateFailed)
	}
	if snapshot.Status.LastFailure == nil || !strings.Contains(snapshot.Status.LastFailure.Message, "recovered panic") {
		t.Fatalf("LastFailure = %#v, want recovered panic", snapshot.Status.LastFailure)
	}
	if snapshot.Status.LastCommandFailure == nil || !strings.Contains(snapshot.Status.LastCommandFailure.Message, "recovered panic") {
		t.Fatalf("LastCommandFailure = %#v, want recovered panic", snapshot.Status.LastCommandFailure)
	}
	events := observer.snapshot()
	if got := len(events.failures); got != 1 {
		t.Fatalf("failure events = %d, want 1", got)
	}
	if events.failures[0].DispatchID == "" {
		t.Fatal("failure dispatch id is empty")
	}
	if events.failures[0].Attempt != 1 {
		t.Fatalf("failure attempt = %d, want 1", events.failures[0].Attempt)
	}
	if !events.failures[0].Panic {
		t.Fatal("failure panic = false, want true")
	}
	if got := len(events.commandEnds); got != 1 {
		t.Fatalf("command end events = %d, want 1", got)
	}
	if events.commandEnds[0].DispatchID != events.failures[0].DispatchID {
		t.Fatalf("command end dispatch id = %q, want %q", events.commandEnds[0].DispatchID, events.failures[0].DispatchID)
	}
	if events.commandEnds[0].Attempts != 1 {
		t.Fatalf("command end attempts = %d, want 1", events.commandEnds[0].Attempts)
	}
	if events.commandEnds[0].Success {
		t.Fatal("command end success = true, want false")
	}
}

func TestStopCompletesWhileAdmittedCommandRemainsInFlight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	dispatchDone := make(chan error, 1)
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("work", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			return CommandResult{Message: "done"}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work"})
		dispatchDone <- err
	}()
	<-entered

	if err := rt.Stop(context.Background(), "worker"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateStopped {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateStopped)
	}
	if snapshot.Status.InFlight != 1 {
		t.Fatalf("worker in-flight = %d, want 1", snapshot.Status.InFlight)
	}
	if snapshot.Status.Ready || snapshot.Status.AcceptingWork {
		t.Fatalf("worker ready/accepting = %t/%t, want false/false", snapshot.Status.Ready, snapshot.Status.AcceptingWork)
	}
	if status := rt.RuntimeStatus(); status.InFlight != 1 {
		t.Fatalf("runtime in-flight = %d, want 1", status.InFlight)
	}
	if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "work"}); !errors.Is(err, ErrRuntimeNotAcceptingWork) {
		t.Fatalf("new Dispatch error = %v, want ErrRuntimeNotAcceptingWork", err)
	}

	close(release)
	if err := <-dispatchDone; err != nil {
		t.Fatalf("admitted Dispatch returned error: %v", err)
	}
	snapshot = requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateStopped || snapshot.Status.InFlight != 0 {
		t.Fatalf("worker state/in-flight = %s/%d, want stopped/0", snapshot.Status.State, snapshot.Status.InFlight)
	}
	if snapshot.Status.LastFailure != nil || snapshot.Status.LastCommandFailure != nil {
		t.Fatalf("failure status = %#v/%#v, want nil/nil", snapshot.Status.LastFailure, snapshot.Status.LastCommandFailure)
	}
}

func TestCommandErrorAfterStopRecordsCommandFailureWithoutChangingLifecycle(t *testing.T) {
	commandErr := errors.New("late command error")
	entered := make(chan struct{})
	release := make(chan struct{})
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("fail", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			return CommandResult{}, commandErr
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "fail"})
		dispatchDone <- err
	}()
	<-entered
	if err := rt.Stop(context.Background(), "worker"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}

	close(release)
	if err := <-dispatchDone; !errors.Is(err, commandErr) {
		t.Fatalf("Dispatch error = %v, want %v", err, commandErr)
	}
	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateStopped {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateStopped)
	}
	if snapshot.Status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", snapshot.Status.LastFailure)
	}
	if snapshot.Status.LastCommandFailure == nil || snapshot.Status.LastCommandFailure.Message != commandErr.Error() {
		t.Fatalf("LastCommandFailure = %#v, want %q", snapshot.Status.LastCommandFailure, commandErr.Error())
	}
	events := observer.snapshot()
	if got := len(events.failures); got != 1 || events.failures[0].Panic || !errors.Is(events.failures[0].Err, commandErr) {
		t.Fatalf("failure events = %#v, want one returned command error", events.failures)
	}
}

func TestStaleCommandErrorDoesNotMutateRestartedWorker(t *testing.T) {
	commandErr := errors.New("stale command error")
	entered := make(chan struct{})
	release := make(chan struct{})
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("fail", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			return CommandResult{}, commandErr
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("first Start returned error: %v", err)
	}
	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "fail"})
		dispatchDone <- err
	}()
	<-entered
	if err := rt.Stop(context.Background(), "worker"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("second Start returned error: %v", err)
	}

	close(release)
	if err := <-dispatchDone; !errors.Is(err, commandErr) {
		t.Fatalf("Dispatch error = %v, want %v", err, commandErr)
	}
	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateRunning {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateRunning)
	}
	if snapshot.Status.LastFailure != nil || snapshot.Status.LastCommandFailure != nil {
		t.Fatalf("failure status = %#v/%#v, want nil/nil", snapshot.Status.LastFailure, snapshot.Status.LastCommandFailure)
	}
	events := observer.snapshot()
	if got := len(events.failures); got != 1 || !errors.Is(events.failures[0].Err, commandErr) {
		t.Fatalf("failure events = %#v, want stale command error observation", events.failures)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})
}

func TestCommandPanicAfterStopPreservesStoppedLifecycle(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("panic", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			panic("late command panic")
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "panic"})
		dispatchDone <- err
	}()
	<-entered
	if err := rt.Stop(context.Background(), "worker"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}

	close(release)
	if err := <-dispatchDone; err == nil || !strings.Contains(err.Error(), "recovered panic") {
		t.Fatalf("Dispatch error = %v, want recovered panic", err)
	}
	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateStopped {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateStopped)
	}
	if snapshot.Status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", snapshot.Status.LastFailure)
	}
	if snapshot.Status.LastCommandFailure == nil || !strings.Contains(snapshot.Status.LastCommandFailure.Message, "recovered panic") {
		t.Fatalf("LastCommandFailure = %#v, want recovered panic", snapshot.Status.LastCommandFailure)
	}
	events := observer.snapshot()
	if got := len(events.failures); got != 1 || !events.failures[0].Panic {
		t.Fatalf("failure events = %#v, want one panic event", events.failures)
	}
}

func TestCommandPanicWhileStoppingPreservesStoppingLifecycle(t *testing.T) {
	commandEntered := make(chan struct{})
	commandRelease := make(chan struct{})
	stopEntered := make(chan struct{})
	stopRelease := make(chan struct{})
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{
			Name: "worker",
			Worker: testWorker{stop: func(context.Context) error {
				close(stopEntered)
				<-stopRelease
				return nil
			}},
		},
		WithCommand("panic", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(commandEntered)
			<-commandRelease
			panic("command panic while stopping")
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "panic"})
		dispatchDone <- err
	}()
	<-commandEntered
	stopDone := make(chan error, 1)
	go func() {
		stopDone <- rt.Stop(context.Background(), "worker")
	}()
	<-stopEntered

	close(commandRelease)
	if err := <-dispatchDone; err == nil || !strings.Contains(err.Error(), "recovered panic") {
		t.Fatalf("Dispatch error = %v, want recovered panic", err)
	}
	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateStopping {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateStopping)
	}
	if snapshot.Status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", snapshot.Status.LastFailure)
	}
	if snapshot.Status.LastCommandFailure == nil || !strings.Contains(snapshot.Status.LastCommandFailure.Message, "recovered panic") {
		t.Fatalf("LastCommandFailure = %#v, want recovered panic", snapshot.Status.LastCommandFailure)
	}

	close(stopRelease)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if snapshot := requireWorker(t, rt, "worker"); snapshot.Status.State != StateStopped {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateStopped)
	}
}

func TestStaleCommandPanicDoesNotFailRestartedWorker(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	observer := &recordingObserver{}
	rt := newTestRuntime(t, WithObserver(observer))
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("panic", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			close(entered)
			<-release
			panic("stale command panic")
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("first Start returned error: %v", err)
	}

	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "panic"})
		dispatchDone <- err
	}()
	<-entered

	if err := rt.Stop(context.Background(), "worker"); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("second Start returned error: %v", err)
	}

	close(release)
	if err := <-dispatchDone; err == nil || !strings.Contains(err.Error(), "recovered panic") {
		t.Fatalf("Dispatch error = %v, want recovered panic", err)
	}

	snapshot := requireWorker(t, rt, "worker")
	if snapshot.Status.State != StateRunning {
		t.Fatalf("worker state = %s, want %s", snapshot.Status.State, StateRunning)
	}
	if snapshot.Status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil", snapshot.Status.LastFailure)
	}
	if snapshot.Status.LastCommandFailure != nil {
		t.Fatalf("LastCommandFailure = %#v, want nil", snapshot.Status.LastCommandFailure)
	}
	events := observer.snapshot()
	if got := len(events.failures); got != 1 || !events.failures[0].Panic {
		t.Fatalf("failure events = %#v, want one panic event", events.failures)
	}

	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})
}

func TestStartRetryRetriesFailedAttemptsAndClearsLastFailureOnSuccess(t *testing.T) {
	startFailure := errors.New("transient start failure")
	var attempts int
	rt := newTestRuntime(t)
	if err := rt.Register(
		WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				start: func(context.Context) error {
					attempts++
					if attempts == 1 {
						return startFailure
					}
					return nil
				},
			},
		},
		WithWorkerStartRetry(retrykit.Attempts(2, nil, nil)),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	snapshot := requireWorkerState(t, rt, "worker", StateRunning)
	if snapshot.Status.LastFailure != nil {
		t.Fatalf("LastFailure = %#v, want nil after successful start", snapshot.Status.LastFailure)
	}
}
