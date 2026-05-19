package workerkit

import (
	"context"
	"errors"
	"strings"
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
	transitions   []TransitionEvent
	commandStarts []CommandStartEvent
	failures      []FailureEvent
	commandEnds   []CommandEndEvent
}

func (o *recordingObserver) ObserveTransition(_ context.Context, event TransitionEvent) {
	o.transitions = append(o.transitions, event)
}

func (o *recordingObserver) StartCommand(ctx context.Context, event CommandStartEvent) (context.Context, CommandObservation) {
	o.commandStarts = append(o.commandStarts, event)
	return ctx, CommandObservationFunc(func(_ context.Context, event CommandEndEvent) {
		o.commandEnds = append(o.commandEnds, event)
	})
}

func (o *recordingObserver) ObserveFailure(_ context.Context, event FailureEvent) {
	o.failures = append(o.failures, event)
}

func (o *recordingObserver) ObserveReadiness(context.Context, ReadinessEvent) {}

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

func TestReportFailureDuringStartCausesStartToReturnError(t *testing.T) {
	startFailure := errors.New("reported during start")
	rt := newTestRuntime(t)
	err := rt.Register(WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(ctx context.Context) error {
				workerRuntime, ok := WorkerRuntimeFromContext(ctx)
				if !ok {
					t.Fatal("missing WorkerRuntime")
				}
				if err := workerRuntime.ReportFailure(startFailure); err != nil {
					t.Fatalf("ReportFailure returned error: %v", err)
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	err = rt.Start(context.Background(), "worker")
	if err == nil {
		t.Fatal("Start returned nil, want error")
	}
	if !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("Start error = %v, want ErrInvalidWorkerState", err)
	}
	if !strings.Contains(err.Error(), startFailure.Error()) {
		t.Fatalf("Start error = %v, want last failure message", err)
	}
	snapshot := requireWorker(t, rt, "worker")
	status := snapshot.Status
	if status.State != StateFailed {
		t.Fatalf("worker state = %s, want %s", status.State, StateFailed)
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
	if got := len(observer.failures); got != 1 {
		t.Fatalf("failure events = %d, want 1", got)
	}
}

func TestFailedWorkerFailureDoesNotEmitNoopTransition(t *testing.T) {
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

	var failedToFailed int
	for _, event := range observer.transitions {
		if event.Worker == "test-runtime/worker" && event.From == StateFailed && event.To == StateFailed {
			failedToFailed++
		}
	}
	if failedToFailed != 0 {
		t.Fatalf("failed -> failed worker transitions = %d, want 0", failedToFailed)
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
	if got := len(observer.commandStarts); got != 1 {
		t.Fatalf("command start events = %d, want 1", got)
	}
	if observer.commandStarts[0].DispatchID == "" {
		t.Fatal("command start dispatch id is empty")
	}
	if got := len(observer.failures); got != 1 {
		t.Fatalf("failure events = %d, want 1", got)
	}
	if observer.failures[0].DispatchID != observer.commandStarts[0].DispatchID {
		t.Fatalf("failure dispatch id = %q, want %q", observer.failures[0].DispatchID, observer.commandStarts[0].DispatchID)
	}
	if observer.failures[0].Attempt != 1 {
		t.Fatalf("failure attempt = %d, want 1", observer.failures[0].Attempt)
	}
	if got := len(observer.commandEnds); got != 1 {
		t.Fatalf("command end events = %d, want 1", got)
	}
	if observer.commandEnds[0].DispatchID != observer.commandStarts[0].DispatchID {
		t.Fatalf("command end dispatch id = %q, want %q", observer.commandEnds[0].DispatchID, observer.commandStarts[0].DispatchID)
	}
	if observer.commandEnds[0].Attempts != 2 {
		t.Fatalf("command end attempts = %d, want 2", observer.commandEnds[0].Attempts)
	}
	if !observer.commandEnds[0].Success {
		t.Fatalf("command end success = false, want true: %#v", observer.commandEnds[0])
	}
	if observer.commandEnds[0].Err != nil {
		t.Fatalf("command end error = %v, want nil", observer.commandEnds[0].Err)
	}
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

	var drained []string
	for _, event := range observer.transitions {
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

func TestDrainAllBestEffortContinuesAfterInvalidWorkerState(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	startDone := make(chan error, 1)
	rt := newTestRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "first", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{
		Name: "starting",
		Worker: testWorker{
			start: func(context.Context) error {
				close(startEntered)
				<-releaseStart
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register starting returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{Name: "third", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register third returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "first"); err != nil {
		t.Fatalf("Start first returned error: %v", err)
	}
	go func() {
		startDone <- rt.Start(context.Background(), "starting")
	}()
	<-startEntered
	if err := rt.Start(context.Background(), "third"); err != nil {
		t.Fatalf("Start third returned error: %v", err)
	}

	err := rt.DrainAllBestEffort(context.Background())
	if !errors.Is(err, ErrInvalidWorkerState) {
		t.Fatalf("DrainAllBestEffort error = %v, want ErrInvalidWorkerState", err)
	}
	close(releaseStart)
	if err := <-startDone; err != nil {
		t.Fatalf("Start starting returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.StopAll(context.Background())
	})

	requireWorkerState(t, rt, "first", StateDraining)
	requireWorkerState(t, rt, "third", StateDraining)
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
	if got := len(observer.failures); got != 1 {
		t.Fatalf("failure events = %d, want 1", got)
	}
	if observer.failures[0].DispatchID == "" {
		t.Fatal("failure dispatch id is empty")
	}
	if observer.failures[0].Attempt != 1 {
		t.Fatalf("failure attempt = %d, want 1", observer.failures[0].Attempt)
	}
	if !observer.failures[0].Panic {
		t.Fatal("failure panic = false, want true")
	}
	if got := len(observer.commandEnds); got != 1 {
		t.Fatalf("command end events = %d, want 1", got)
	}
	if observer.commandEnds[0].DispatchID != observer.failures[0].DispatchID {
		t.Fatalf("command end dispatch id = %q, want %q", observer.commandEnds[0].DispatchID, observer.failures[0].DispatchID)
	}
	if observer.commandEnds[0].Attempts != 1 {
		t.Fatalf("command end attempts = %d, want 1", observer.commandEnds[0].Attempts)
	}
	if observer.commandEnds[0].Success {
		t.Fatal("command end success = true, want false")
	}
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
