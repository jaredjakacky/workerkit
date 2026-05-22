package servekitservice_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit/servekitservice"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
	"github.com/jaredjakacky/workerkit/opshttp"
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

func TestNewRejectsInvalidInputsAndServekitOptions(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(t)
	server := servekit.New()

	if _, err := New(nil, server); !errors.Is(err, opshttp.ErrNilRuntime) {
		t.Fatalf("New nil runtime error = %v, want ErrNilRuntime", err)
	}
	if _, err := New(rt, nil); !errors.Is(err, opshttp.ErrNilServer) {
		t.Fatalf("New nil server error = %v, want ErrNilServer", err)
	}
	if _, err := New(rt, server, WithServekitOptions(servekit.WithAddr("127.0.0.1:0"))); err == nil {
		t.Fatal("New with servekit options returned nil, want error")
	}
}

func TestNewDoesNotMountOpsHTTPByDefaultAndCanEnableIt(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(t)
	server := servekit.New(servekit.WithAccessLogEnabled(false))
	if _, err := New(rt, server); err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	rec := performRequest(server, http.MethodGet, "/admin/runtime")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("default ops route status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	enabledServer := servekit.New(servekit.WithAccessLogEnabled(false))
	if _, err := New(rt, enabledServer, WithOpsHTTPEnabled(true)); err != nil {
		t.Fatalf("New enabled ops returned error: %v", err)
	}
	rec = performRequest(enabledServer, http.MethodGet, "/admin/runtime")
	if rec.Code != http.StatusOK {
		t.Fatalf("enabled ops route status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestNewManagedWiresReadiness(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(t)
	if err := rt.Register(workerkit.WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	service, err := NewManaged(rt, WithServekitOptions(servekit.WithAddr("127.0.0.1:0")))
	if err != nil {
		t.Fatalf("NewManaged returned error: %v", err)
	}
	service.Server().SetReady(true)

	rec := performRequest(service.Server(), http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before worker start status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "worker runtime not ready") {
		t.Fatalf("readyz body = %s, want worker runtime readiness reason", rec.Body.String())
	}

	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})

	rec = performRequest(service.Server(), http.MethodGet, "/readyz")
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz after worker start status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestServerReturnsManagedServerAndAllowsNilReceiver(t *testing.T) {
	t.Parallel()

	if got := (*Service)(nil).Server(); got != nil {
		t.Fatalf("nil service Server = %#v, want nil", got)
	}

	rt := newTestRuntime(t)
	service, err := NewManaged(rt)
	if err != nil {
		t.Fatalf("NewManaged returned error: %v", err)
	}
	if service.Server() == nil {
		t.Fatal("Server returned nil")
	}

	service.Server().Handle(http.MethodGet, "/app", func(r *http.Request) (any, error) {
		return map[string]string{"ok": "true"}, nil
	})
	rec := performRequest(service.Server(), http.MethodGet, "/app")
	if rec.Code != http.StatusOK {
		t.Fatalf("app route status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestReadinessCheck(t *testing.T) {
	t.Parallel()

	if err := ReadinessCheck(nil)(context.Background()); !errors.Is(err, opshttp.ErrNilRuntime) {
		t.Fatalf("nil readiness error = %v, want ErrNilRuntime", err)
	}

	rt := newTestRuntime(t)
	if err := ReadinessCheck(rt)(context.Background()); err == nil || !strings.Contains(err.Error(), "worker runtime not ready") {
		t.Fatalf("unready readiness error = %v, want not ready error", err)
	}

	if err := rt.Register(workerkit.WorkerSpec{Name: "worker", Worker: testWorker{}}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})
	if err := ReadinessCheck(rt)(context.Background()); err != nil {
		t.Fatalf("ready readiness error = %v, want nil", err)
	}
	if opts := ReadinessOptions(rt); len(opts) != 1 {
		t.Fatalf("ReadinessOptions length = %d, want 1", len(opts))
	}
}

func TestRunCleansUpStartedWorkersAfterStartupFailure(t *testing.T) {
	t.Parallel()

	startFailure := errors.New("second start failed")
	var firstStops atomic.Int32
	rt := newTestRuntime(t)
	if err := rt.Register(workerkit.WorkerSpec{
		Name: "first",
		Worker: testWorker{
			stop: func(context.Context) error {
				firstStops.Add(1)
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	if err := rt.Register(workerkit.WorkerSpec{
		Name: "second",
		Worker: testWorker{
			start: func(context.Context) error {
				return startFailure
			},
		},
	}); err != nil {
		t.Fatalf("Register second returned error: %v", err)
	}
	service := newTestService(t, rt)

	err := service.Run(context.Background())
	if !errors.Is(err, startFailure) {
		t.Fatalf("Run error = %v, want start failure", err)
	}
	if got := firstStops.Load(); got != 1 {
		t.Fatalf("first stop calls = %d, want 1", got)
	}
	if state := requireWorkerState(t, rt, "first"); state != workerkit.StateStopped {
		t.Fatalf("first state = %s, want stopped", state)
	}
}

func TestRunCanSkipStartupFailureCleanup(t *testing.T) {
	t.Parallel()

	startFailure := errors.New("second start failed")
	var firstStops atomic.Int32
	rt := newTestRuntime(t)
	if err := rt.Register(workerkit.WorkerSpec{
		Name: "first",
		Worker: testWorker{
			stop: func(context.Context) error {
				firstStops.Add(1)
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register first returned error: %v", err)
	}
	if err := rt.Register(workerkit.WorkerSpec{
		Name: "second",
		Worker: testWorker{
			start: func(context.Context) error {
				return startFailure
			},
		},
	}); err != nil {
		t.Fatalf("Register second returned error: %v", err)
	}
	service := newTestService(t, rt, WithGracefulWorkerShutdown(false))
	t.Cleanup(func() {
		_ = rt.StopAll(context.Background())
	})

	err := service.Run(context.Background())
	if !errors.Is(err, startFailure) {
		t.Fatalf("Run error = %v, want start failure", err)
	}
	if got := firstStops.Load(); got != 0 {
		t.Fatalf("first stop calls = %d, want 0", got)
	}
	if state := requireWorkerState(t, rt, "first"); state != workerkit.StateRunning {
		t.Fatalf("first state = %s, want running", state)
	}
}

func TestShutdownWorkersStopsStartedWorkers(t *testing.T) {
	t.Parallel()

	var stops atomic.Int32
	rt := newTestRuntime(t)
	if err := rt.Register(workerkit.WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			stop: func(context.Context) error {
				stops.Add(1)
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	if got := stops.Load(); got != 1 {
		t.Fatalf("stop calls = %d, want 1", got)
	}
	if state := requireWorkerState(t, rt, "worker"); state != workerkit.StateStopped {
		t.Fatalf("worker state = %s, want stopped", state)
	}
}

func TestNewManagedCanMountOpsHTTP(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(t)
	service, err := NewManaged(
		rt,
		WithOpsHTTPEnabled(true),
		WithStartWorkers(false),
		WithGracefulWorkerShutdown(false),
		WithShutdownTimeout(-1),
		WithServekitOptions(servekit.WithAddr("127.0.0.1:0")),
	)
	if err != nil {
		t.Fatalf("NewManaged returned error: %v", err)
	}
	if service.Server() == nil {
		t.Fatal("Server returned nil")
	}
	rec := performRequest(service.Server(), http.MethodGet, "/admin/runtime")
	if rec.Code != http.StatusOK {
		t.Fatalf("ops runtime status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestRunCanSkipWorkerStartup(t *testing.T) {
	t.Parallel()

	var starts atomic.Int32
	rt := newTestRuntime(t)
	if err := rt.Register(workerkit.WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			start: func(context.Context) error {
				starts.Add(1)
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	service := newTestService(t, rt, WithStartWorkers(false))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = service.Run(ctx)

	if got := starts.Load(); got != 0 {
		t.Fatalf("start calls = %d, want 0", got)
	}
	if state := requireWorkerState(t, rt, "worker"); state != workerkit.StateRegistered {
		t.Fatalf("worker state = %s, want registered", state)
	}
}

func TestRunCanSkipGracefulShutdownAfterServekitReturns(t *testing.T) {
	t.Parallel()

	var stops atomic.Int32
	rt := newTestRuntime(t)
	if err := rt.Register(workerkit.WorkerSpec{
		Name: "worker",
		Worker: testWorker{
			stop: func(context.Context) error {
				stops.Add(1)
				return nil
			},
		},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})
	service := newTestService(t, rt, WithStartWorkers(false), WithGracefulWorkerShutdown(false))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = service.Run(ctx)

	if got := stops.Load(); got != 0 {
		t.Fatalf("stop calls = %d, want 0", got)
	}
	if state := requireWorkerState(t, rt, "worker"); state != workerkit.StateRunning {
		t.Fatalf("worker state = %s, want running", state)
	}
}

func TestRunAppliesShutdownTimeout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var stops atomic.Int32
	rt := newTestRuntime(t)
	if err := rt.Register(
		workerkit.WorkerSpec{
			Name: "worker",
			Worker: testWorker{
				stop: func(context.Context) error {
					stops.Add(1)
					return nil
				},
			},
		},
		workerkit.WithCommand("block", workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			close(entered)
			<-release
			return workerkit.CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	dispatchDone := make(chan error, 1)
	go func() {
		_, err := rt.Dispatch(context.Background(), workerkit.CommandRequest{Worker: "worker", Name: "block"})
		dispatchDone <- err
	}()
	<-entered

	service := newTestService(t, rt, WithStartWorkers(false), WithShutdownTimeout(time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := service.Run(ctx)
	close(release)
	if dispatchErr := <-dispatchDone; dispatchErr != nil {
		t.Fatalf("Dispatch returned error: %v", dispatchErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want DeadlineExceeded", err)
	}
	if got := stops.Load(); got != 1 {
		t.Fatalf("stop calls = %d, want 1", got)
	}
}

func TestRunRejectsInvalidService(t *testing.T) {
	t.Parallel()

	var nilService *Service
	if err := nilService.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "workerkit service must not be nil") {
		t.Fatalf("nil service Run error = %v, want nil service error", err)
	}
	if err := (&Service{}).Run(context.Background()); !errors.Is(err, opshttp.ErrNilRuntime) {
		t.Fatalf("missing runtime Run error = %v, want ErrNilRuntime", err)
	}
	if _, err := New(newTestRuntime(t), nil); !errors.Is(err, opshttp.ErrNilServer) {
		t.Fatalf("New missing server error = %v, want ErrNilServer", err)
	}
}

func newTestRuntime(t *testing.T) *workerkit.Runtime {
	t.Helper()

	rt, err := workerkit.New(workerkit.Identity{Name: "service"})
	if err != nil {
		t.Fatalf("New runtime returned error: %v", err)
	}
	return rt
}

func newTestService(t *testing.T, rt *workerkit.Runtime, opts ...Option) *Service {
	t.Helper()

	baseOpts := []Option{
		WithOpsHTTPOptions(nil),
		WithServekitOptions(
			servekit.WithAddr("127.0.0.1:0"),
			servekit.WithAccessLogEnabled(false),
		),
	}
	baseOpts = append(baseOpts, opts...)
	service, err := NewManaged(rt, baseOpts...)
	if err != nil {
		t.Fatalf("NewManaged returned error: %v", err)
	}
	return service
}

func requireWorkerState(t *testing.T, rt *workerkit.Runtime, name string) workerkit.LifecycleState {
	t.Helper()

	worker, ok := rt.Worker(name)
	if !ok {
		t.Fatalf("worker %q missing", name)
	}
	return worker.Status.State
}

func performRequest(server *servekit.Server, method string, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	server.Handler().ServeHTTP(rec, req)
	return rec
}
