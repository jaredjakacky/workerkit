package opshttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	. "github.com/jaredjakacky/workerkit/opshttp"
	"net/http"
	"net/http/httptest"
	"testing"

	opskit "github.com/jaredjakacky/opskit"
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

func TestDispatchMapsOpskitCommandOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		result     opskit.CommandResult
		wantStatus int
		wantError  string
	}{
		{name: "rejected", result: opskit.RejectedCommand("maintenance disabled"), wantStatus: http.StatusConflict, wantError: workerkit.ErrOpsCommandRejected.Error()},
		{name: "rejected with error detail", result: opskit.CommandResult{State: opskit.StateNotReady, Accepted: false, Error: "maintenance disabled"}, wantStatus: http.StatusConflict, wantError: workerkit.ErrOpsCommandRejected.Error()},
		{name: "failed", result: opskit.FailedCommand("refresh failed", errors.New("backend unavailable"), 0), wantStatus: http.StatusInternalServerError, wantError: "internal server error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := workerkit.CommandFromOpskit(opskit.CommandDescriptor{Name: "echo"}, opskit.CommandHandlerFunc(
				func(context.Context, opskit.CommandRequest) opskit.CommandResult { return tt.result },
			))
			server := newDispatchServer(t, spec.Handler)
			rec := postDispatch(t, server, `{"worker":"worker","name":"echo"}`)
			assertStatus(t, rec, tt.wantStatus)
			assertErrorBody(t, rec, tt.wantError)
		})
	}
}

func TestDispatchMapsAdmissionErrors(t *testing.T) {
	t.Run("missing worker", func(t *testing.T) {
		server := newDispatchServer(t, workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{}, nil
		}))

		rec := postDispatch(t, server, `{"worker":"missing","name":"echo"}`)
		assertStatus(t, rec, http.StatusNotFound)
		assertErrorBody(t, rec, `worker "missing" not found`)
	})

	t.Run("missing command", func(t *testing.T) {
		server := newDispatchServer(t, workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{}, nil
		}))

		rec := postDispatch(t, server, `{"worker":"worker","name":"missing"}`)
		assertStatus(t, rec, http.StatusNotFound)
		assertErrorBody(t, rec, `command "missing" not found`)
	})

	t.Run("runtime not accepting work", func(t *testing.T) {
		server := newDispatchServerWithOptions(t, nil, nil, false, workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{}, nil
		}))

		rec := postDispatch(t, server, `{"worker":"worker","name":"echo"}`)
		assertStatus(t, rec, http.StatusServiceUnavailable)
		assertErrorBody(t, rec, workerkit.ErrRuntimeNotAcceptingWork.Error())
	})

	t.Run("worker not accepting work", func(t *testing.T) {
		server := newDispatchServerWithOptions(t, nil, []workerkit.WorkerOption{
			workerkit.WithWorkerAcceptingWorkOnStart(false),
		}, true, workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{}, nil
		}))

		rec := postDispatch(t, server, `{"worker":"worker","name":"echo"}`)
		assertStatus(t, rec, http.StatusConflict)
		assertErrorBody(t, rec, workerkit.ErrWorkerNotAcceptingWork.Error())
	})

	t.Run("invalid worker state", func(t *testing.T) {
		rt, server := newDispatchRuntimeServer(t, nil, nil, true, workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
			return workerkit.CommandResult{}, nil
		}))
		if err := rt.Drain(context.Background(), "worker"); err != nil {
			t.Fatalf("Drain returned error: %v", err)
		}

		rec := postDispatch(t, server, `{"worker":"worker","name":"echo"}`)
		assertStatus(t, rec, http.StatusConflict)
		assertErrorBody(t, rec, workerkit.ErrInvalidWorkerState.Error())
	})

	t.Run("runtime saturated", func(t *testing.T) {
		server, release, done := newSaturatedDispatchServer(t, []workerkit.RuntimeOption{
			workerkit.WithRuntimeCommandConcurrency(1),
		}, nil)

		rec := postDispatch(t, server, `{"worker":"worker","name":"echo"}`)
		assertStatus(t, rec, http.StatusTooManyRequests)
		assertErrorBody(t, rec, workerkit.ErrRuntimeSaturated.Error())
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("blocking dispatch returned error: %v", err)
		}
	})

	t.Run("worker saturated", func(t *testing.T) {
		server, release, done := newSaturatedDispatchServer(t, nil, []workerkit.WorkerOption{
			workerkit.WithWorkerCommandConcurrency(1),
		})

		rec := postDispatch(t, server, `{"worker":"worker","name":"echo"}`)
		assertStatus(t, rec, http.StatusTooManyRequests)
		assertErrorBody(t, rec, workerkit.ErrWorkerSaturated.Error())
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("blocking dispatch returned error: %v", err)
		}
	})
}

func newDispatchServer(t *testing.T, handler workerkit.CommandHandler) *servekit.Server {
	t.Helper()

	return newDispatchServerWithOptions(t, nil, nil, true, handler)
}

func newDispatchServerWithOptions(
	t *testing.T,
	runtimeOpts []workerkit.RuntimeOption,
	workerOpts []workerkit.WorkerOption,
	start bool,
	handler workerkit.CommandHandler,
) *servekit.Server {
	t.Helper()

	_, server := newDispatchRuntimeServer(t, runtimeOpts, workerOpts, start, handler)
	return server
}

func newDispatchRuntimeServer(
	t *testing.T,
	runtimeOpts []workerkit.RuntimeOption,
	workerOpts []workerkit.WorkerOption,
	start bool,
	handler workerkit.CommandHandler,
) (*workerkit.Runtime, *servekit.Server) {
	t.Helper()

	rt, err := workerkit.New(workerkit.Identity{Name: "ops"}, runtimeOpts...)
	if err != nil {
		t.Fatalf("New with options returned error: %v", err)
	}
	workerOpts = append([]workerkit.WorkerOption{
		workerkit.WithCommand("echo", handler),
	}, workerOpts...)
	err = rt.Register(
		workerkit.WorkerSpec{Name: "worker", Worker: testWorker{}},
		workerOpts...,
	)
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if start {
		if err := rt.Start(context.Background(), "worker"); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		t.Cleanup(func() {
			_ = rt.Stop(context.Background(), "worker")
		})
	}

	server := servekit.New()
	if err := Mount(server, rt, WithCommandDispatchEnabled()); err != nil {
		t.Fatalf("Mount returned error: %v", err)
	}
	return rt, server
}

func newSaturatedDispatchServer(
	t *testing.T,
	runtimeOpts []workerkit.RuntimeOption,
	workerOpts []workerkit.WorkerOption,
) (*servekit.Server, chan struct{}, chan error) {
	t.Helper()

	entered := make(chan struct{})
	release := make(chan struct{})
	handler := workerkit.CommandHandlerFunc(func(context.Context, workerkit.CommandRequest) (workerkit.CommandResult, error) {
		close(entered)
		<-release
		return workerkit.CommandResult{}, nil
	})
	server := newDispatchServerWithOptions(t, runtimeOpts, workerOpts, true, handler)
	done := make(chan error, 1)
	go func() {
		rec := postDispatch(t, server, `{"worker":"worker","name":"echo"}`)
		if rec.Code != http.StatusOK {
			done <- errors.New(rec.Body.String())
			return
		}
		done <- nil
	}()
	<-entered
	return server, release, done
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
