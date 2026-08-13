package servekitservice_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
	. "github.com/jaredjakacky/workerkit/servekitservice"
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

func TestNewRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(t)
	server := servekit.New()

	if _, err := New(nil, server); !errors.Is(err, ErrNilRuntime) {
		t.Fatalf("New nil runtime error = %v, want ErrNilRuntime", err)
	}
	if _, err := New(rt, nil); !errors.Is(err, ErrNilServer) {
		t.Fatalf("New nil server error = %v, want ErrNilServer", err)
	}
}

func TestNewPreservesApplicationOwnedServer(t *testing.T) {
	t.Parallel()

	rt := newTestRuntime(t)
	server := servekit.New(servekit.WithAccessLogEnabled(false))
	server.Handle(http.MethodGet, "/app", func(r *http.Request) (any, error) {
		return map[string]string{"ok": "true"}, nil
	})
	service, err := New(rt, server)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if service.Server() != server {
		t.Fatal("Server did not return the application-owned server")
	}

	rec := performRequest(service.Server(), http.MethodGet, "/app")
	if rec.Code != http.StatusOK {
		t.Fatalf("app route status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestServerAllowsNilReceiver(t *testing.T) {
	t.Parallel()

	if got := (*Service)(nil).Server(); got != nil {
		t.Fatalf("nil service Server = %#v, want nil", got)
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

func TestRunSharesShutdownBudgetWithServekit(t *testing.T) {
	addr := reserveLoopbackAddr(t)
	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})
	handlerReleased := false
	defer func() {
		if !handlerReleased {
			close(handlerRelease)
		}
	}()
	stopCalled := make(chan struct{})

	rt := newTestRuntime(t)
	if err := rt.Register(workerkit.WorkerSpec{
		Name: "worker",
		Worker: testWorker{stop: func(context.Context) error {
			close(stopCalled)
			return nil
		}},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	server := servekit.New(
		servekit.WithAddr(addr),
		servekit.WithAccessLogEnabled(false),
		servekit.WithOpenTelemetryEnabled(false),
		servekit.WithShutdownTimeout(2*time.Second),
	)
	server.HandleHTTP(http.MethodGet, "/stuck", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		<-handlerRelease
		_, _ = io.WriteString(w, "too late")
	}))
	service, err := New(rt, server, WithShutdownTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- service.Run(ctx) }()
	waitForHTTPStatus(t, "http://"+addr+"/readyz", http.StatusOK, 2*time.Second)

	requestErrCh := make(chan error, 1)
	go func() {
		resp, requestErr := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + addr + "/stuck")
		if requestErr == nil {
			_ = resp.Body.Close()
		}
		requestErrCh <- requestErr
	}()
	select {
	case <-handlerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck request did not reach handler")
	}

	startedAt := time.Now()
	cancel()
	runErr := waitForRunResult(t, runErrCh, 2*time.Second)
	if !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want context deadline exceeded", runErr)
	}
	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("Run returned after %v, want one shared shutdown budget", elapsed)
	}
	select {
	case <-stopCalled:
	default:
		t.Fatal("worker Stop was not attempted through the fallback")
	}
	select {
	case requestErr := <-requestErrCh:
		if requestErr == nil {
			t.Fatal("stuck request completed without a transport error after forced close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stuck request connection remained open after shutdown")
	}

	close(handlerRelease)
	handlerReleased = true
}

func TestRunKeepsLoopWorkerAliveUntilHTTPDrainCompletes(t *testing.T) {
	addr := reserveLoopbackAddr(t)
	loopStarted := make(chan struct{})
	loopCanceled := make(chan struct{})
	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})
	handlerReleased := false
	defer func() {
		if !handlerReleased {
			close(handlerRelease)
		}
	}()

	rt := newTestRuntime(t)
	loopWorker := workerkit.NewLoopWorker(func(ctx context.Context, _ workerkit.WorkerRuntime) error {
		close(loopStarted)
		<-ctx.Done()
		close(loopCanceled)
		return ctx.Err()
	})
	if err := rt.Register(workerkit.WorkerSpec{Name: "loop", Worker: loopWorker}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	server := servekit.New(
		servekit.WithAddr(addr),
		servekit.WithAccessLogEnabled(false),
		servekit.WithOpenTelemetryEnabled(false),
		servekit.WithShutdownTimeout(2*time.Second),
	)
	server.HandleHTTP(http.MethodGet, "/work", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		<-handlerRelease
		w.WriteHeader(http.StatusNoContent)
	}))
	service, err := New(rt, server, WithShutdownTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- service.Run(ctx) }()
	waitForHTTPStatus(t, "http://"+addr+"/readyz", http.StatusOK, 2*time.Second)
	select {
	case <-loopStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("loop worker did not start")
	}

	requestErrCh := make(chan error, 1)
	go func() {
		resp, requestErr := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + addr + "/work")
		if requestErr == nil {
			_ = resp.Body.Close()
		}
		requestErrCh <- requestErr
	}()
	select {
	case <-handlerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("active request did not reach handler")
	}

	cancel()
	select {
	case <-loopCanceled:
		t.Fatal("loop worker was canceled while active HTTP handler was draining")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case runErr := <-runErrCh:
		t.Fatalf("Run returned before active HTTP handler completed: %v", runErr)
	default:
	}

	close(handlerRelease)
	handlerReleased = true
	if requestErr := <-requestErrCh; requestErr != nil {
		t.Fatalf("active request error = %v, want graceful completion", requestErr)
	}
	if runErr := waitForRunResult(t, runErrCh, 2*time.Second); runErr != nil {
		t.Fatalf("Run error = %v, want nil", runErr)
	}
	select {
	case <-loopCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("loop worker was not canceled after HTTP drain completed")
	}
	if state := requireWorkerState(t, rt, "loop"); state != workerkit.StateStopped {
		t.Fatalf("loop state = %s, want stopped", state)
	}
}

func TestRunRejectsInvalidService(t *testing.T) {
	t.Parallel()

	var nilService *Service
	if err := nilService.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "workerkit service must not be nil") {
		t.Fatalf("nil service Run error = %v, want nil service error", err)
	}
	if err := (&Service{}).Run(context.Background()); !errors.Is(err, ErrNilRuntime) {
		t.Fatalf("missing runtime Run error = %v, want ErrNilRuntime", err)
	}
	if _, err := New(newTestRuntime(t), nil); !errors.Is(err, ErrNilServer) {
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

	server := servekit.New(
		servekit.WithAddr("127.0.0.1:0"),
		servekit.WithAccessLogEnabled(false),
	)
	service, err := New(rt, server, opts...)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
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

func reserveLoopbackAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release loopback address: %v", err)
	}
	return addr
}

func waitForHTTPStatus(t *testing.T, url string, want int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 100 * time.Millisecond}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not return status %d within %v", url, want, timeout)
}

func waitForRunResult(t *testing.T, errCh <-chan error, timeout time.Duration) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(timeout):
		t.Fatalf("Run did not return within %v", timeout)
		return nil
	}
}
