package opshttp

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
)

func TestStatusRuntimeRoute(t *testing.T) {
	t.Parallel()

	rt := newStatusRuntime(t)
	server := newStatusServer(t, rt)

	rec := serveHTTP(server, http.MethodGet, "/admin/runtime", "")
	assertStatus(t, rec, http.StatusOK)

	var body struct {
		Data runtimeDescriptor `json:"data"`
	}
	decodeResponse(t, rec.Body.Bytes(), &body)
	if body.Data.Identity.Name != "ops" {
		t.Fatalf("identity name = %q, want ops", body.Data.Identity.Name)
	}
	if body.Data.Status.Name != "ops" {
		t.Fatalf("status name = %q, want ops", body.Data.Status.Name)
	}
	if body.Data.Status.State != workerkit.StateRegistered {
		t.Fatalf("runtime state = %s, want %s", body.Data.Status.State, workerkit.StateRegistered)
	}
}

func TestStatusWorkersRoute(t *testing.T) {
	t.Parallel()

	rt := newStatusRuntime(t)
	server := newStatusServer(t, rt)

	rec := serveHTTP(server, http.MethodGet, "/admin/workers", "")
	assertStatus(t, rec, http.StatusOK)

	var body struct {
		Data []workerkit.WorkerSnapshot `json:"data"`
	}
	decodeResponse(t, rec.Body.Bytes(), &body)
	if len(body.Data) != 2 {
		t.Fatalf("workers len = %d, want 2", len(body.Data))
	}
	if body.Data[0].QualifiedName != "ops/alpha" {
		t.Fatalf("first worker = %q, want ops/alpha", body.Data[0].QualifiedName)
	}
	if body.Data[1].QualifiedName != "ops/beta" {
		t.Fatalf("second worker = %q, want ops/beta", body.Data[1].QualifiedName)
	}
}

func TestStatusWorkerRoute(t *testing.T) {
	t.Parallel()

	rt := newStatusRuntime(t)
	server := newStatusServer(t, rt)

	rec := serveHTTP(server, http.MethodGet, "/admin/worker?name=alpha", "")
	assertStatus(t, rec, http.StatusOK)

	var body struct {
		Data workerkit.WorkerSnapshot `json:"data"`
	}
	decodeResponse(t, rec.Body.Bytes(), &body)
	if body.Data.QualifiedName != "ops/alpha" {
		t.Fatalf("qualified name = %q, want ops/alpha", body.Data.QualifiedName)
	}
	if body.Data.Description != "alpha worker" {
		t.Fatalf("description = %q, want alpha worker", body.Data.Description)
	}
	if body.Data.Status.LocalName != "alpha" {
		t.Fatalf("local name = %q, want alpha", body.Data.Status.LocalName)
	}

	rec = serveHTTP(server, http.MethodGet, "/admin/worker?name=missing", "")
	assertStatus(t, rec, http.StatusNotFound)
	assertErrorBody(t, rec, `worker "missing" not found`)

	rec = serveHTTP(server, http.MethodGet, "/admin/worker", "")
	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorBody(t, rec, `missing required query parameter "name"`)
}

func TestStatusCommandsRoute(t *testing.T) {
	t.Parallel()

	rt := newStatusRuntime(t)
	server := newStatusServer(t, rt)

	rec := serveHTTP(server, http.MethodGet, "/admin/commands?worker=alpha", "")
	assertStatus(t, rec, http.StatusOK)

	var body struct {
		Data []workerkit.CommandInfo `json:"data"`
	}
	decodeResponse(t, rec.Body.Bytes(), &body)
	if len(body.Data) != 2 {
		t.Fatalf("commands len = %d, want 2", len(body.Data))
	}
	if body.Data[0].Worker != "ops/alpha" || body.Data[0].Name != "a-command" {
		t.Fatalf("first command = %#v, want sorted a-command for ops/alpha", body.Data[0])
	}
	if body.Data[1].Worker != "ops/alpha" || body.Data[1].Name != "z-command" {
		t.Fatalf("second command = %#v, want sorted z-command for ops/alpha", body.Data[1])
	}
	if body.Data[1].Description != "last command" {
		t.Fatalf("second description = %q, want last command", body.Data[1].Description)
	}

	rec = serveHTTP(server, http.MethodGet, "/admin/commands?worker=missing", "")
	assertStatus(t, rec, http.StatusNotFound)
	assertErrorBody(t, rec, `worker "missing" not found`)

	rec = serveHTTP(server, http.MethodGet, "/admin/commands", "")
	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorBody(t, rec, `missing required query parameter "worker"`)
}

func newStatusRuntime(t *testing.T) *workerkit.Runtime {
	t.Helper()

	rt, err := workerkit.New(workerkit.Identity{Name: "ops"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	err = rt.Register(
		workerkit.WorkerSpec{
			Name:        "alpha",
			Description: "alpha worker",
			Worker:      &lifecycleWorker{},
		},
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "z-command",
			Description: "last command",
			Handler:     nopCommandHandler,
		}),
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "a-command",
			Description: "first command",
			Handler:     nopCommandHandler,
		}),
	)
	if err != nil {
		t.Fatalf("Register alpha returned error: %v", err)
	}
	if err := rt.Register(workerkit.WorkerSpec{Name: "beta", Worker: &lifecycleWorker{}}); err != nil {
		t.Fatalf("Register beta returned error: %v", err)
	}
	return rt
}

func newStatusServer(t *testing.T, rt *workerkit.Runtime) *servekit.Server {
	t.Helper()

	server := servekit.New()
	if err := Mount(server, rt); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}
	return server
}

var nopCommandHandler = workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
	return workerkit.CommandResult{}, nil
})

func decodeResponse(t *testing.T, body []byte, dst any) {
	t.Helper()

	if err := json.Unmarshal(body, dst); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}
