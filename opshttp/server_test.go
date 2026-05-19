package opshttp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit-incubator"
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

func TestNormalizePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{name: "empty", prefix: "", want: "/"},
		{name: "spaces", prefix: "   ", want: "/"},
		{name: "root", prefix: "/", want: "/"},
		{name: "adds leading slash", prefix: "admin", want: "/admin"},
		{name: "trims trailing slash", prefix: "/admin/", want: "/admin"},
		{name: "trims spaces and slash", prefix: " admin/ ", want: "/admin"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := normalizePrefix(tt.prefix); got != tt.want {
				t.Fatalf("normalizePrefix(%q) = %q, want %q", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestRoutePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		suffix string
		want   string
	}{
		{name: "root empty suffix", prefix: "/", suffix: "", want: "/"},
		{name: "root suffix", prefix: "/", suffix: "runtime", want: "/runtime"},
		{name: "root slash suffix", prefix: "/", suffix: "/runtime", want: "/runtime"},
		{name: "prefix empty suffix", prefix: "/admin", suffix: "", want: "/admin"},
		{name: "prefix suffix", prefix: "/admin", suffix: "runtime", want: "/admin/runtime"},
		{name: "prefix slash suffix", prefix: "/admin", suffix: "/runtime", want: "/admin/runtime"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := routePath(tt.prefix, tt.suffix); got != tt.want {
				t.Fatalf("routePath(%q, %q) = %q, want %q", tt.prefix, tt.suffix, got, tt.want)
			}
		})
	}
}

func TestOptionConfiguration(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	WithPrefix("ops")(&cfg)
	WithEndpointOptions(servekit.WithEndpointTimeout(time.Second))(&cfg)
	WithDispatchOptions(servekit.WithBodyLimit(1024))(&cfg)
	WithLifecycleOptions(servekit.WithEndpointTimeout(2 * time.Second))(&cfg)
	WithLifecycleTimeout(5 * time.Second)(&cfg)
	WithLifecycleTimeout(0)(&cfg)
	WithCommandDispatchEnabled()(&cfg)
	WithAdminLifecycleControlsEnabled()(&cfg)

	if cfg.prefix != "/ops" {
		t.Fatalf("prefix = %q, want /ops", cfg.prefix)
	}
	if cfg.lifecycleTimeout != 5*time.Second {
		t.Fatalf("lifecycleTimeout = %s, want 5s", cfg.lifecycleTimeout)
	}
	if !cfg.commandDispatchEnabled {
		t.Fatal("commandDispatchEnabled = false, want true")
	}
	if !cfg.adminLifecycleControls {
		t.Fatal("adminLifecycleControls = false, want true")
	}
	if len(cfg.endpointOptions) != 1 {
		t.Fatalf("endpointOptions len = %d, want 1", len(cfg.endpointOptions))
	}
	if len(cfg.dispatchOptions) != 1 {
		t.Fatalf("dispatchOptions len = %d, want 1", len(cfg.dispatchOptions))
	}
	if len(cfg.lifecycleOptions) != 1 {
		t.Fatalf("lifecycleOptions len = %d, want 1", len(cfg.lifecycleOptions))
	}
	if len(dispatchEndpointOptions(cfg)) != 2 {
		t.Fatalf("dispatchEndpointOptions len = %d, want 2", len(dispatchEndpointOptions(cfg)))
	}
	if len(lifecycleEndpointOptions(cfg)) != 2 {
		t.Fatalf("lifecycleEndpointOptions len = %d, want 2", len(lifecycleEndpointOptions(cfg)))
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
