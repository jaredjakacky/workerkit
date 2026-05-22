package opshttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	. "github.com/jaredjakacky/workerkit/opshttp"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
)

func TestWorkerLifecycleRoutesStartDrainAndStopWorker(t *testing.T) {
	t.Parallel()

	worker := &lifecycleWorker{}
	rt := newLifecycleRuntime(t, "ops", map[string]workerkit.Worker{
		"worker": worker,
	})
	server := newLifecycleServer(t, rt)

	rec := postLifecycle(t, server, "/admin/workers/start", `{"name":"worker"}`)
	assertStatus(t, rec, http.StatusOK)
	if worker.starts != 1 {
		t.Fatalf("starts = %d, want 1", worker.starts)
	}
	assertWorkerState(t, rt, "worker", workerkit.StateRunning)

	rec = postLifecycle(t, server, "/admin/workers/drain", `{"name":"worker"}`)
	assertStatus(t, rec, http.StatusOK)
	assertWorkerState(t, rt, "worker", workerkit.StateDraining)

	rec = postLifecycle(t, server, "/admin/workers/stop", `{"name":"worker"}`)
	assertStatus(t, rec, http.StatusOK)
	if worker.stops != 1 {
		t.Fatalf("stops = %d, want 1", worker.stops)
	}
	assertWorkerState(t, rt, "worker", workerkit.StateStopped)
}

func TestRuntimeLifecycleRoutesStartDrainAndStopAllWorkers(t *testing.T) {
	t.Parallel()

	first := &lifecycleWorker{}
	second := &lifecycleWorker{}
	rt := newLifecycleRuntime(t, "ops", map[string]workerkit.Worker{
		"first":  first,
		"second": second,
	})
	server := newLifecycleServer(t, rt)

	rec := postLifecycle(t, server, "/admin/runtime/start", "")
	assertStatus(t, rec, http.StatusOK)
	if first.starts != 1 || second.starts != 1 {
		t.Fatalf("starts = %d/%d, want 1/1", first.starts, second.starts)
	}
	assertWorkerState(t, rt, "first", workerkit.StateRunning)
	assertWorkerState(t, rt, "second", workerkit.StateRunning)

	rec = postLifecycle(t, server, "/admin/runtime/drain", "")
	assertStatus(t, rec, http.StatusOK)
	assertWorkerState(t, rt, "first", workerkit.StateDraining)
	assertWorkerState(t, rt, "second", workerkit.StateDraining)

	rec = postLifecycle(t, server, "/admin/runtime/stop", "")
	assertStatus(t, rec, http.StatusOK)
	if first.stops != 1 || second.stops != 1 {
		t.Fatalf("stops = %d/%d, want 1/1", first.stops, second.stops)
	}
	assertWorkerState(t, rt, "first", workerkit.StateStopped)
	assertWorkerState(t, rt, "second", workerkit.StateStopped)
}

func TestLifecycleRoutesAreOptIn(t *testing.T) {
	t.Parallel()

	rt := newLifecycleRuntime(t, "ops", map[string]workerkit.Worker{
		"worker": &lifecycleWorker{},
	})
	server := servekit.New()
	if err := Mount(server, rt); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}

	rec := postLifecycle(t, server, "/admin/workers/start", `{"name":"worker"}`)
	assertStatus(t, rec, http.StatusNotFound)
}

func TestWorkerLifecycleRouteValidationAndErrorMapping(t *testing.T) {
	t.Parallel()

	rt := newLifecycleRuntime(t, "ops", map[string]workerkit.Worker{
		"worker": &lifecycleWorker{},
	})
	server := newLifecycleServer(t, rt)

	rec := postLifecycle(t, server, "/admin/workers/start", `{}`)
	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorBody(t, rec, `missing required JSON field "name"`)

	rec = postLifecycle(t, server, "/admin/workers/start", `{"name":"missing"}`)
	assertStatus(t, rec, http.StatusNotFound)
	assertErrorBody(t, rec, `worker "missing" not found`)

	rec = postLifecycle(t, server, "/admin/workers/start", `{"name":"worker"}`)
	assertStatus(t, rec, http.StatusOK)

	rec = postLifecycle(t, server, "/admin/workers/start", `{"name":"worker"}`)
	assertStatus(t, rec, http.StatusConflict)
	assertErrorBody(t, rec, workerkit.ErrInvalidWorkerState.Error())
}

func TestLifecycleRouteAppliesOperationTimeout(t *testing.T) {
	rt := newLifecycleRuntime(t, "ops", map[string]workerkit.Worker{
		"worker": lifecycleWorkerFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}),
	})
	server := servekit.New()
	if err := Mount(server, rt, WithAdminLifecycleControlsEnabled(), WithLifecycleTimeout(time.Millisecond)); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}

	rec := postLifecycle(t, server, "/admin/workers/start", `{"name":"worker"}`)
	assertStatus(t, rec, http.StatusGatewayTimeout)
	assertErrorBody(t, rec, "request timed out")
}

type lifecycleWorker struct {
	starts int
	stops  int
}

func (w *lifecycleWorker) Start(context.Context) error {
	w.starts++
	return nil
}

func (w *lifecycleWorker) Stop(context.Context) error {
	w.stops++
	return nil
}

type lifecycleWorkerFunc func(context.Context) error

func (f lifecycleWorkerFunc) Start(ctx context.Context) error {
	return f(ctx)
}

func (f lifecycleWorkerFunc) Stop(context.Context) error {
	return nil
}

func newLifecycleRuntime(t *testing.T, name string, workers map[string]workerkit.Worker) *workerkit.Runtime {
	t.Helper()

	rt, err := workerkit.New(workerkit.Identity{Name: name})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	for workerName, worker := range workers {
		if err := rt.Register(workerkit.WorkerSpec{Name: workerName, Worker: worker}); err != nil {
			t.Fatalf("Register(%q) returned error: %v", workerName, err)
		}
	}
	return rt
}

func newLifecycleServer(t *testing.T, rt *workerkit.Runtime) *servekit.Server {
	t.Helper()

	server := servekit.New()
	if err := Mount(server, rt, WithAdminLifecycleControlsEnabled()); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}
	return server
}

func postLifecycle(t *testing.T, server *servekit.Server, path string, body string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(rec, req)
	return rec
}

func assertStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()

	if rec.Code != want {
		t.Fatalf("status = %d, want %d: %s", rec.Code, want, rec.Body.String())
	}
}

func assertErrorBody(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()

	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if !bytes.Contains([]byte(body.Error), []byte(want)) {
		t.Fatalf("error = %q, want to contain %q", body.Error, want)
	}
}

func assertWorkerState(t *testing.T, rt *workerkit.Runtime, name string, want workerkit.LifecycleState) {
	t.Helper()

	worker, ok := rt.Worker(name)
	if !ok {
		t.Fatalf("worker %q not found", name)
	}
	if worker.Status.State != want {
		t.Fatalf("worker %q state = %s, want %s", name, worker.Status.State, want)
	}
}
