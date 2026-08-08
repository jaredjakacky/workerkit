package workerkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	opskit "github.com/jaredjakacky/opskit"
	. "github.com/jaredjakacky/workerkit"
)

func TestRuntimeImplementsOpskitContracts(t *testing.T) {
	t.Parallel()

	rt := newOpskitRuntime(t)

	var _ opskit.Component = rt
	var _ opskit.ReadinessContributor = rt
	var _ opskit.Inspector = rt
}

func TestRuntimeComponentInfoUsesSafeRuntimeName(t *testing.T) {
	t.Parallel()

	rt := newOpskitRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	info := rt.ComponentInfo()
	if info.Name != "runtime" {
		t.Fatalf("ComponentInfo.Name = %q, want runtime", info.Name)
	}
	if err := opskit.ValidateComponentName(info.Name); err != nil {
		t.Fatalf("ComponentInfo.Name is not a valid Opskit component name: %v", err)
	}
	if info.Name == "runtime/worker" {
		t.Fatal("ComponentInfo.Name used qualified worker name")
	}
	if info.Kind != "worker_runtime" {
		t.Fatalf("ComponentInfo.Kind = %q, want worker_runtime", info.Kind)
	}
}

func TestRuntimeOpskitStatusUsesCachedRuntimeState(t *testing.T) {
	t.Parallel()

	rt := newOpskitRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	status := rt.Status(context.Background())
	if status.Ready {
		t.Fatal("Status.Ready = true before worker start, want false")
	}
	if status.State != opskit.StateStopped {
		t.Fatalf("Status.State = %s, want %s", status.State, opskit.StateStopped)
	}

	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})

	status = rt.Status(context.Background())
	if !status.Ready {
		t.Fatal("Status.Ready = false after worker start, want true")
	}
	if status.State != opskit.StateReady {
		t.Fatalf("Status.State = %s, want %s", status.State, opskit.StateReady)
	}
}

func TestRuntimeOpskitStatusKeepsIsolatedWorkerFailureOutOfSummaryState(t *testing.T) {
	t.Parallel()

	rt := newOpskitRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "main", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register main returned error: %v", err)
	}
	if err := rt.Register(
		WorkerSpec{Name: "optional", Worker: testWorker{start: func(context.Context) error {
			return errors.New("optional failed")
		}}},
		WithWorkerReadinessContribution(false),
		WithWorkerFailurePolicy(FailurePolicyIsolate),
	); err != nil {
		t.Fatalf("Register optional returned error: %v", err)
	}

	if err := rt.StartAll(context.Background()); err == nil {
		t.Fatal("StartAll returned nil, want optional worker failure")
	}
	t.Cleanup(func() {
		_ = rt.StopAll(context.Background())
	})

	status := rt.Status(context.Background())
	if !status.Ready {
		t.Fatalf("Status.Ready = false, want true: %+v", status)
	}
	if status.State != opskit.StateReady {
		t.Fatalf("Status.State = %s, want %s", status.State, opskit.StateReady)
	}
}

func TestRuntimeOpskitStatusAttributesAreLowCardinality(t *testing.T) {
	t.Parallel()

	rt, err := New(Identity{Name: "runtime"}, WithReadinessPolicy(ReadyWhenAllWorkersReady))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	attrs := attributeMap(rt.Status(context.Background()).Attributes)
	want := map[string]string{
		"runtime_state":    string(StateRegistered),
		"worker_count":     "1",
		"in_flight":        "0",
		"readiness_policy": string(ReadyWhenAllWorkersReady),
	}
	for key, value := range want {
		if attrs[key] != value {
			t.Fatalf("attribute %s = %q, want %q", key, attrs[key], value)
		}
	}
	if _, ok := attrs["workerkit.state"]; ok {
		t.Fatal("Status attributes include old workerkit.state key")
	}
	for _, attr := range rt.Status(context.Background()).Attributes {
		if attr.Value == "runtime/worker" || attr.Value == "worker" {
			t.Fatalf("Status attribute leaks worker name: %+v", attr)
		}
	}
}

func TestRuntimeOpskitReadinessUsesRuntimeAggregate(t *testing.T) {
	t.Parallel()

	rt := newOpskitRuntime(t)
	readiness := rt.Readiness(context.Background())
	if readiness.Ready {
		t.Fatal("Readiness.Ready = true before worker registration, want false")
	}
	if len(readiness.Items) != 0 {
		t.Fatalf("Readiness.Items = %#v before worker registration, want empty child detail", readiness.Items)
	}

	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})

	readiness = rt.Readiness(context.Background())
	if !readiness.Ready {
		t.Fatalf("Readiness.Ready = false after worker start, want true: %+v", readiness)
	}
	if len(readiness.Items) != 1 || readiness.Items[0].Name != "worker" || readiness.Items[0].Kind != "worker" {
		t.Fatalf("Readiness.Items = %#v, want worker-scoped detail", readiness.Items)
	}
	if !readiness.Items[0].Ready || readiness.Items[0].State != opskit.StateReady || readiness.Items[0].Impact != opskit.ReadinessImpactBlocking {
		t.Fatalf("worker readiness item = %#v, want blocking ready worker", readiness.Items[0])
	}
}

func TestRuntimeOpskitReadinessTreatsStartingWorkerAsNotReady(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	rt := newOpskitRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "worker", Worker: testWorker{start: func(ctx context.Context) error {
		workerRuntime, ok := WorkerRuntimeFromContext(ctx)
		if !ok {
			return errors.New("worker runtime unavailable")
		}
		if err := workerRuntime.SetReady(true); err != nil {
			return err
		}
		close(entered)
		<-release
		return nil
	}}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	started := make(chan error, 1)
	go func() { started <- rt.Start(context.Background(), "worker") }()
	<-entered

	readiness := rt.Readiness(context.Background())
	if readiness.Ready || len(readiness.Items) != 1 || readiness.Items[0].Ready || readiness.Items[0].State != opskit.StateInitializing {
		t.Fatalf("Readiness during Start = %#v, want authoritative not-ready initializing item", readiness)
	}
	close(release)
	if err := <-started; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if readiness := rt.Readiness(context.Background()); !readiness.Ready || !readiness.Items[0].Ready || readiness.Items[0].State != opskit.StateReady {
		t.Fatalf("Readiness after Start = %#v, want ready item", readiness)
	}
	t.Cleanup(func() { _ = rt.Stop(context.Background(), "worker") })
}

func TestRuntimeOpskitReadinessPreservesWorkerkitReadinessPolicy(t *testing.T) {
	t.Parallel()

	contributingRuntime := newRuntimeWithPolicy(t, ReadyWhenContributingWorkersReady)
	allWorkersRuntime := newRuntimeWithPolicy(t, ReadyWhenAllWorkersReady)

	if readiness := contributingRuntime.Readiness(context.Background()); !readiness.Ready {
		t.Fatalf("contributing-workers readiness = false, want true: %+v", readiness)
	}
	if readiness := allWorkersRuntime.Readiness(context.Background()); readiness.Ready {
		t.Fatalf("all-workers readiness = true, want false: %+v", readiness)
	}

	contributing := contributingRuntime.Readiness(context.Background())
	if len(contributing.Items) != 2 || contributing.Items[0].Impact != opskit.ReadinessImpactBlocking || contributing.Items[1].Impact != opskit.ReadinessImpactNonBlocking {
		t.Fatalf("contributing-workers items = %#v, want blocking main and non-blocking optional", contributing.Items)
	}
	allWorkers := allWorkersRuntime.Readiness(context.Background())
	if len(allWorkers.Items) != 2 || allWorkers.Items[0].Impact != opskit.ReadinessImpactBlocking || allWorkers.Items[1].Impact != opskit.ReadinessImpactBlocking {
		t.Fatalf("all-workers items = %#v, want both blocking", allWorkers.Items)
	}
}

func TestRuntimeOpskitReadinessImpactIncludesFailurePolicy(t *testing.T) {
	t.Parallel()

	rt := newOpskitRuntime(t)
	if err := rt.Register(WorkerSpec{Name: "main", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register main returned error: %v", err)
	}
	for name, policy := range map[string]FailurePolicy{
		"isolated": FailurePolicyIsolate,
		"unready":  FailurePolicyMarkRuntimeUnready,
		"fatal":    FailurePolicyFailRuntime,
	} {
		if err := rt.Register(
			WorkerSpec{Name: name, Worker: testWorker{}},
			WithWorkerReadinessContribution(false),
			WithWorkerFailurePolicy(policy),
		); err != nil {
			t.Fatalf("Register %s returned error: %v", name, err)
		}
	}

	readiness := rt.Readiness(context.Background())
	items := make(map[string]opskit.ReadinessItem, len(readiness.Items))
	for _, item := range readiness.Items {
		items[item.Name] = item
	}
	if items["isolated"].Impact != opskit.ReadinessImpactNonBlocking {
		t.Fatalf("isolated impact = %q, want non-blocking", items["isolated"].Impact)
	}
	for _, name := range []string{"unready", "fatal"} {
		if items[name].Impact != opskit.ReadinessImpactBlocking {
			t.Fatalf("%s impact = %q, want blocking", name, items[name].Impact)
		}
	}
}

func TestRuntimeInspectContainsWorkerDetail(t *testing.T) {
	t.Parallel()

	var commandRuns atomic.Int32
	rt := newOpskitRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Description: "test worker", Worker: testWorker{}},
		WithCommand("sync", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			commandRuns.Add(1)
			return CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	inspection, err := rt.Inspect(context.Background())
	if err != nil {
		t.Fatalf("Inspect returned error: %v", err)
	}
	if commandRuns.Load() != 0 {
		t.Fatalf("Inspect executed command handler %d times, want 0", commandRuns.Load())
	}

	body, err := json.Marshal(inspection.Details)
	if err != nil {
		t.Fatalf("Marshal details returned error: %v", err)
	}
	var details struct {
		Workers []struct {
			QualifiedName string `json:"qualifiedName"`
			Name          string `json:"name"`
			Description   string `json:"description,omitempty"`
			Status        struct {
				State         LifecycleState `json:"state"`
				Ready         bool           `json:"ready"`
				AcceptingWork bool           `json:"acceptingWork"`
				InFlight      int            `json:"inFlight"`
			} `json:"status"`
		} `json:"workers,omitempty"`
		Commands map[string][]CommandInfo `json:"commands,omitempty"`
	}
	if err := json.Unmarshal(body, &details); err != nil {
		t.Fatalf("Unmarshal details returned error: %v", err)
	}
	if len(details.Workers) != 1 {
		t.Fatalf("Inspection workers length = %d, want 1", len(details.Workers))
	}
	if details.Workers[0].QualifiedName != "runtime/worker" {
		t.Fatalf("worker qualified name = %q, want runtime/worker", details.Workers[0].QualifiedName)
	}
	if len(details.Commands["runtime/worker"]) != 1 {
		t.Fatalf("commands for runtime/worker = %d, want 1", len(details.Commands["runtime/worker"]))
	}
	if details.Commands["runtime/worker"][0].Name != "sync" {
		t.Fatalf("command name = %q, want sync", details.Commands["runtime/worker"][0].Name)
	}
}

func TestRuntimeInspectIncludesPolicySummary(t *testing.T) {
	t.Parallel()

	rt, err := New(Identity{Name: "runtime"}, WithReadinessPolicy(ReadyWhenAllWorkersReady))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	inspection, err := rt.Inspect(context.Background())
	if err != nil {
		t.Fatalf("Inspect returned error: %v", err)
	}

	body, err := json.Marshal(inspection.Summary)
	if err != nil {
		t.Fatalf("Marshal summary returned error: %v", err)
	}
	var summary struct {
		Policy struct {
			ReadinessPolicy ReadinessPolicy `json:"readinessPolicy"`
		} `json:"policy"`
	}
	if err := json.Unmarshal(body, &summary); err != nil {
		t.Fatalf("Unmarshal summary returned error: %v", err)
	}
	if summary.Policy.ReadinessPolicy != ReadyWhenAllWorkersReady {
		t.Fatalf("readiness policy = %q, want %q", summary.Policy.ReadinessPolicy, ReadyWhenAllWorkersReady)
	}
}

func TestRuntimeInspectRedactsFailureMessages(t *testing.T) {
	t.Parallel()

	const secret = "postgres://user:pass@example.test/db"
	rt := newOpskitRuntime(t)
	if err := rt.Register(
		WorkerSpec{Name: "worker", Worker: testWorker{}},
		WithCommand("sync", CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
			return CommandResult{}, errors.New(secret)
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})
	if _, err := rt.Dispatch(context.Background(), CommandRequest{Worker: "worker", Name: "sync"}); err == nil {
		t.Fatal("Dispatch returned nil, want command failure")
	}

	inspection, err := rt.Inspect(context.Background())
	if err != nil {
		t.Fatalf("Inspect returned error: %v", err)
	}
	body, err := json.Marshal(inspection)
	if err != nil {
		t.Fatalf("Marshal inspection returned error: %v", err)
	}
	if strings.Contains(string(body), secret) {
		t.Fatalf("inspection leaked failure message: %s", body)
	}
	if !strings.Contains(string(body), `"lastCommandFailureCommand":"sync"`) {
		t.Fatalf("inspection did not include safe command failure name: %s", body)
	}
}

func newOpskitRuntime(t *testing.T) *Runtime {
	t.Helper()

	rt, err := New(Identity{Name: "runtime"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return rt
}

func newRuntimeWithPolicy(t *testing.T, policy ReadinessPolicy) *Runtime {
	t.Helper()

	rt, err := New(Identity{Name: "runtime"}, WithReadinessPolicy(policy))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(WorkerSpec{Name: "main", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register main returned error: %v", err)
	}
	if err := rt.Register(
		WorkerSpec{Name: "optional", Worker: testWorker{}},
		WithWorkerReadinessContribution(false),
	); err != nil {
		t.Fatalf("Register optional returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "main"); err != nil {
		t.Fatalf("Start main returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.StopAll(context.Background())
	})
	return rt
}

func attributeMap(attrs []opskit.Attribute) map[string]string {
	values := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		values[attr.Key] = attr.Value
	}
	return values
}
