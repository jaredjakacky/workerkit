package opshttp_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit/opshttp"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
)

func TestMountValidatesInputs(t *testing.T) {
	t.Parallel()

	rt := newLifecycleRuntime(t, "ops", nil)
	server := servekit.New()

	if err := Mount(nil, rt); !errors.Is(err, ErrNilServer) {
		t.Fatalf("Mount nil server error = %v, want ErrNilServer", err)
	}
	if err := Mount(server, nil); !errors.Is(err, ErrNilRuntime) {
		t.Fatalf("Mount nil runtime error = %v, want ErrNilRuntime", err)
	}
}

func TestMountRegistersStatusRoutesByDefault(t *testing.T) {
	t.Parallel()

	rt := newLifecycleRuntime(t, "ops", map[string]workerkit.Worker{
		"worker": &lifecycleWorker{},
	})
	server := servekit.New()
	if err := Mount(server, rt, nil); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}

	assertRouteStatus(t, server, http.MethodGet, "/admin/runtime", "", http.StatusOK)
	assertRouteStatus(t, server, http.MethodGet, "/admin/workers", "", http.StatusOK)
	assertRouteStatus(t, server, http.MethodGet, "/admin/worker?name=worker", "", http.StatusOK)
	assertRouteStatus(t, server, http.MethodGet, "/admin/commands?worker=worker", "", http.StatusOK)
	assertRouteStatus(t, server, http.MethodPost, "/admin/commands/dispatch", `{}`, http.StatusNotFound)
	assertRouteStatus(t, server, http.MethodPost, "/admin/workers/start", `{"name":"worker"}`, http.StatusNotFound)
}

func TestMountUsesCustomPrefix(t *testing.T) {
	t.Parallel()

	rt := newLifecycleRuntime(t, "ops", nil)
	server := servekit.New()
	if err := Mount(server, rt, WithPrefix(" ops/ ")); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}

	assertRouteStatus(t, server, http.MethodGet, "/ops/runtime", "", http.StatusOK)
	assertRouteStatus(t, server, http.MethodGet, "/admin/runtime", "", http.StatusNotFound)
}

func TestMountUsesRootPrefix(t *testing.T) {
	t.Parallel()

	rt := newLifecycleRuntime(t, "ops", nil)
	server := servekit.New()
	if err := Mount(server, rt, WithPrefix("")); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}

	assertRouteStatus(t, server, http.MethodGet, "/runtime", "", http.StatusOK)
	assertRouteStatus(t, server, http.MethodGet, "/admin/runtime", "", http.StatusNotFound)
}

func TestMountAppliesEndpointDispatchAndLifecycleOptions(t *testing.T) {
	t.Parallel()

	rt := newLifecycleRuntime(t, "ops", map[string]workerkit.Worker{
		"worker": &lifecycleWorker{},
	})
	if err := rt.Register(
		workerkit.WorkerSpec{Name: "command-worker", Worker: &lifecycleWorker{}},
		workerkit.WithCommand("echo", workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{Payload: []byte(`{"ok":true}`)}, nil
		})),
	); err != nil {
		t.Fatalf("Register command worker returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "command-worker"); err != nil {
		t.Fatalf("Start command worker returned error: %v", err)
	}

	server := servekit.New()
	if err := Mount(
		server,
		rt,
		WithEndpointOptions(headerMiddleware("X-Ops-Endpoint", "true")),
		WithDispatchOptions(headerMiddleware("X-Ops-Dispatch", "true")),
		WithLifecycleOptions(headerMiddleware("X-Ops-Lifecycle", "true")),
		WithCommandDispatchEnabled(),
		WithAdminLifecycleControlsEnabled(),
	); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}

	status := serveHTTP(server, http.MethodGet, "/admin/runtime", "")
	assertStatus(t, status, http.StatusOK)
	assertHeader(t, status, "X-Ops-Endpoint", "true")
	assertHeader(t, status, "X-Ops-Dispatch", "")
	assertHeader(t, status, "X-Ops-Lifecycle", "")

	dispatch := serveHTTP(server, http.MethodPost, "/admin/commands/dispatch", `{"worker":"command-worker","name":"echo","payload":{}}`)
	assertStatus(t, dispatch, http.StatusOK)
	assertHeader(t, dispatch, "X-Ops-Endpoint", "true")
	assertHeader(t, dispatch, "X-Ops-Dispatch", "true")
	assertHeader(t, dispatch, "X-Ops-Lifecycle", "")

	lifecycle := serveHTTP(server, http.MethodPost, "/admin/workers/start", `{"name":"worker"}`)
	assertStatus(t, lifecycle, http.StatusOK)
	assertHeader(t, lifecycle, "X-Ops-Endpoint", "true")
	assertHeader(t, lifecycle, "X-Ops-Dispatch", "")
	assertHeader(t, lifecycle, "X-Ops-Lifecycle", "true")
}

func TestReadinessCheck(t *testing.T) {
	t.Parallel()

	check := ReadinessCheck(nil)
	if err := check(context.Background()); !errors.Is(err, ErrNilRuntime) {
		t.Fatalf("nil runtime readiness error = %v, want ErrNilRuntime", err)
	}

	rt := newLifecycleRuntime(t, "ops", map[string]workerkit.Worker{
		"worker": &lifecycleWorker{},
	})
	check = ReadinessCheck(rt)
	err := check(context.Background())
	if err == nil {
		t.Fatal("readiness check returned nil for non-ready runtime")
	}
	if !strings.Contains(err.Error(), "worker runtime not ready") {
		t.Fatalf("readiness error = %q, want not-ready message", err.Error())
	}

	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := check(context.Background()); err != nil {
		t.Fatalf("readiness check returned error after start: %v", err)
	}
}

func headerMiddleware(key string, value string) servekit.EndpointOption {
	return servekit.WithEndpointMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(key, value)
			next.ServeHTTP(w, r)
		})
	})
}

func assertRouteStatus(t *testing.T, server *servekit.Server, method string, path string, body string, want int) {
	t.Helper()

	rec := serveHTTP(server, method, path, body)
	assertStatus(t, rec, want)
}

func serveHTTP(server *servekit.Server, method string, path string, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(rec, req)
	return rec
}

func assertHeader(t *testing.T, rec *httptest.ResponseRecorder, key string, want string) {
	t.Helper()

	if got := rec.Header().Get(key); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}
