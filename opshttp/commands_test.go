package opshttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
)

type testWorker struct{}

func (testWorker) Start(context.Context) error { return nil }

func (testWorker) Stop(context.Context) error { return nil }

func TestDispatchAcceptsRawJSONPayload(t *testing.T) {
	var capturedPayload []byte
	handler := workerkit.CommandHandlerFunc(func(_ context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
		capturedPayload = append([]byte(nil), req.Payload...)
		return workerkit.CommandResult{Payload: req.Payload}, nil
	})
	server := newDispatchServer(t, handler)

	rec := postDispatch(t, server, `{"worker":"worker","name":"echo","payload":{"message":"hello"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	assertJSONEqual(t, capturedPayload, []byte(`{"message":"hello"}`))
}

func TestDispatchReturnsRawJSONPayload(t *testing.T) {
	handler := workerkit.CommandHandlerFunc(func(_ context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
		return workerkit.CommandResult{
			Message: "echoed",
			Payload: req.Payload,
		}, nil
	})
	server := newDispatchServer(t, handler)

	rec := postDispatch(t, server, `{"worker":"worker","name":"echo","payload":{"message":"hello"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body struct {
		Data struct {
			Worker string `json:"worker"`
			Name   string `json:"name"`
			Result struct {
				Message string          `json:"message"`
				Payload json.RawMessage `json:"payload"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Data.Worker != "ops/worker" {
		t.Fatalf("worker = %q, want ops/worker", body.Data.Worker)
	}
	if body.Data.Name != "echo" {
		t.Fatalf("name = %q, want echo", body.Data.Name)
	}
	if body.Data.Result.Message != "echoed" {
		t.Fatalf("message = %q, want echoed", body.Data.Result.Message)
	}
	assertJSONEqual(t, body.Data.Result.Payload, []byte(`{"message":"hello"}`))
}

func TestDispatchRejectsInvalidResultPayload(t *testing.T) {
	handler := workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
		return workerkit.CommandResult{Payload: []byte("not-json")}, nil
	})
	server := newDispatchServer(t, handler)

	rec := postDispatch(t, server, `{"worker":"worker","name":"echo","payload":{"message":"hello"}}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestDispatchRejectsInvalidCommandRequest(t *testing.T) {
	server := newDispatchServer(t, workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
		t.Fatal("handler should not be called")
		return workerkit.CommandResult{}, nil
	}))

	rec := postDispatch(t, server, `{"worker":"worker","name":"Bad Command"}`)
	assertStatus(t, rec, http.StatusBadRequest)
	assertErrorBody(t, rec, "invalid command name")
}

func TestMapDispatchError(t *testing.T) {
	t.Parallel()

	rt, err := workerkit.New(workerkit.Identity{Name: "ops"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(
		workerkit.WorkerSpec{Name: "worker", Worker: testWorker{}},
		workerkit.WithCommand("echo", workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	if err := mapDispatchError(rt, "worker", "echo", nil); err != nil {
		t.Fatalf("nil error mapped to %v, want nil", err)
	}

	tests := []struct {
		name       string
		worker     string
		command    string
		err        error
		wantStatus int
		want       string
	}{
		{
			name:       "worker not found sentinel",
			worker:     "missing",
			command:    "echo",
			err:        workerkit.ErrWorkerNotFound,
			wantStatus: http.StatusNotFound,
			want:       `worker "missing" not found`,
		},
		{
			name:       "command not found sentinel",
			worker:     "worker",
			command:    "missing",
			err:        workerkit.ErrCommandNotFound,
			wantStatus: http.StatusNotFound,
			want:       `command "missing" not found`,
		},
		{
			name:       "runtime not accepting work",
			worker:     "worker",
			command:    "echo",
			err:        workerkit.ErrRuntimeNotAcceptingWork,
			wantStatus: http.StatusServiceUnavailable,
			want:       workerkit.ErrRuntimeNotAcceptingWork.Error(),
		},
		{
			name:       "worker not accepting work",
			worker:     "worker",
			command:    "echo",
			err:        workerkit.ErrWorkerNotAcceptingWork,
			wantStatus: http.StatusConflict,
			want:       workerkit.ErrWorkerNotAcceptingWork.Error(),
		},
		{
			name:       "invalid worker state",
			worker:     "worker",
			command:    "echo",
			err:        workerkit.ErrInvalidWorkerState,
			wantStatus: http.StatusConflict,
			want:       workerkit.ErrInvalidWorkerState.Error(),
		},
		{
			name:       "runtime saturated",
			worker:     "worker",
			command:    "echo",
			err:        workerkit.ErrRuntimeSaturated,
			wantStatus: http.StatusTooManyRequests,
			want:       workerkit.ErrRuntimeSaturated.Error(),
		},
		{
			name:       "worker saturated",
			worker:     "worker",
			command:    "echo",
			err:        workerkit.ErrWorkerSaturated,
			wantStatus: http.StatusTooManyRequests,
			want:       workerkit.ErrWorkerSaturated.Error(),
		},
		{
			name:       "unknown error missing command",
			worker:     "worker",
			command:    "missing",
			err:        errors.New("handler failed before lookup"),
			wantStatus: http.StatusNotFound,
			want:       `command "missing" not found`,
		},
		{
			name:       "unknown error missing worker",
			worker:     "missing",
			command:    "echo",
			err:        errors.New("handler failed before lookup"),
			wantStatus: http.StatusNotFound,
			want:       `worker "missing" not found`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := mapDispatchError(rt, tt.worker, tt.command, tt.err)
			assertHTTPError(t, err, tt.wantStatus, tt.want)
			if tt.wantStatus != http.StatusNotFound && !errors.Is(err, tt.err) {
				t.Fatalf("mapped error does not wrap original error: %v", err)
			}
		})
	}

	boom := errors.New("handler failed")
	if got := mapDispatchError(rt, "worker", "echo", boom); got != boom {
		t.Fatalf("known command unknown error = %v, want original error", got)
	}
}

func TestContainsWorkerAndCommand(t *testing.T) {
	t.Parallel()

	rt, err := workerkit.New(workerkit.Identity{Name: "ops"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if err := rt.Register(
		workerkit.WorkerSpec{Name: "worker", Worker: testWorker{}},
		workerkit.WithCommand("echo", workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{}, nil
		})),
	); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	if !containsWorker("worker", rt) {
		t.Fatal("containsWorker(worker) = false, want true")
	}
	if containsWorker("missing", rt) {
		t.Fatal("containsWorker(missing) = true, want false")
	}
	if !containsCommand("worker", "echo", rt) {
		t.Fatal("containsCommand(worker, echo) = false, want true")
	}
	if containsCommand("worker", "missing", rt) {
		t.Fatal("containsCommand(worker, missing) = true, want false")
	}
	if containsCommand("missing", "echo", rt) {
		t.Fatal("containsCommand(missing, echo) = true, want false")
	}
}

func newDispatchServer(t *testing.T, handler workerkit.CommandHandler) *servekit.Server {
	t.Helper()

	rt, err := workerkit.New(workerkit.Identity{Name: "ops"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	err = rt.Register(
		workerkit.WorkerSpec{Name: "worker", Worker: testWorker{}},
		workerkit.WithCommand("echo", handler),
	)
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := rt.Start(context.Background(), "worker"); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Stop(context.Background(), "worker")
	})

	server := servekit.New()
	if err := Mount(server, rt, WithCommandDispatchEnabled()); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}
	return server
}

func postDispatch(t *testing.T, server *servekit.Server, body string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/commands/dispatch", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	server.Handler().ServeHTTP(rec, req)
	return rec
}

func assertJSONEqual(t *testing.T, got []byte, want []byte) {
	t.Helper()

	var gotCompact bytes.Buffer
	if err := json.Compact(&gotCompact, got); err != nil {
		t.Fatalf("got invalid JSON %q: %v", got, err)
	}
	var wantCompact bytes.Buffer
	if err := json.Compact(&wantCompact, want); err != nil {
		t.Fatalf("want invalid JSON %q: %v", want, err)
	}
	if !bytes.Equal(gotCompact.Bytes(), wantCompact.Bytes()) {
		t.Fatalf("JSON = %s, want %s", gotCompact.String(), wantCompact.String())
	}
}
