package workerkit

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCommandRequestValidate(t *testing.T) {
	t.Parallel()

	valid := []CommandRequest{
		{Worker: "worker", Name: "sync"},
		{Worker: "runtime/worker", Name: "queue/drain"},
		{
			Worker:      "worker",
			Name:        "sync",
			Payload:     []byte(`{"id":1}`),
			RequestedAt: time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC),
		},
	}
	for _, req := range valid {
		req := req
		t.Run("valid "+req.Worker+" "+req.Name, func(t *testing.T) {
			t.Parallel()

			if err := req.Validate(); err != nil {
				t.Fatalf("Validate returned error: %v", err)
			}
		})
	}

	invalid := []struct {
		req  CommandRequest
		want string
	}{
		{
			req:  CommandRequest{Worker: "", Name: "sync"},
			want: "invalid command target",
		},
		{
			req:  CommandRequest{Worker: "Runtime/worker", Name: "sync"},
			want: "invalid command target",
		},
		{
			req:  CommandRequest{Worker: "worker", Name: ""},
			want: "invalid command name",
		},
		{
			req:  CommandRequest{Worker: "worker", Name: "Sync"},
			want: "invalid command name",
		},
	}
	for _, tt := range invalid {
		tt := tt
		t.Run("invalid "+testName(tt.req.Worker)+" "+testName(tt.req.Name), func(t *testing.T) {
			t.Parallel()

			err := tt.req.Validate()
			if err == nil {
				t.Fatalf("Validate(%#v) returned nil, want error", tt.req)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %q, want to contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestCommandSpecValidate(t *testing.T) {
	t.Parallel()

	handler := CommandHandlerFunc(func(context.Context, CommandRequest) (CommandResult, error) {
		return CommandResult{Message: "ok"}, nil
	})

	valid := []CommandSpec{
		{Name: "sync", Handler: handler},
		{Name: "queue/drain", Description: "drain a queue", Handler: handler},
	}
	for _, spec := range valid {
		spec := spec
		t.Run("valid "+spec.Name, func(t *testing.T) {
			t.Parallel()

			if err := spec.Validate(); err != nil {
				t.Fatalf("Validate returned error: %v", err)
			}
		})
	}

	invalid := []struct {
		spec CommandSpec
		want string
	}{
		{
			spec: CommandSpec{Name: "", Handler: handler},
			want: "invalid command name",
		},
		{
			spec: CommandSpec{Name: "Sync", Handler: handler},
			want: "invalid command name",
		},
		{
			spec: CommandSpec{Name: "sync"},
			want: "command handler must not be nil",
		},
	}
	for _, tt := range invalid {
		tt := tt
		t.Run("invalid "+testName(tt.spec.Name), func(t *testing.T) {
			t.Parallel()

			err := tt.spec.Validate()
			if err == nil {
				t.Fatalf("Validate(%#v) returned nil, want error", tt.spec)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %q, want to contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestCommandHandlerFunc(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("command failed")
	wantReq := CommandRequest{Worker: "worker", Name: "sync", Payload: []byte(`{"id":1}`)}
	handler := CommandHandlerFunc(func(ctx context.Context, req CommandRequest) (CommandResult, error) {
		if ctx == nil {
			t.Fatal("context = nil")
		}
		if req.Worker != wantReq.Worker || req.Name != wantReq.Name || string(req.Payload) != string(wantReq.Payload) {
			t.Fatalf("request = %#v, want %#v", req, wantReq)
		}
		return CommandResult{Message: "handled", Payload: []byte(`{"ok":true}`)}, wantErr
	})

	result, err := handler.HandleCommand(context.Background(), wantReq)
	if !errors.Is(err, wantErr) {
		t.Fatalf("HandleCommand error = %v, want %v", err, wantErr)
	}
	if result.Message != "handled" || string(result.Payload) != `{"ok":true}` {
		t.Fatalf("result = %#v, want handled result", result)
	}
}

func TestCommandInfoJSON(t *testing.T) {
	t.Parallel()

	body, err := json.Marshal(CommandInfo{
		Worker:      "runtime/worker",
		Name:        "sync",
		Description: "synchronize worker state",
	})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if got, want := string(body), `{"worker":"runtime/worker","name":"sync","description":"synchronize worker state"}`; got != want {
		t.Fatalf("json = %s, want %s", got, want)
	}

	body, err = json.Marshal(CommandInfo{
		Worker: "runtime/worker",
		Name:   "sync",
	})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if got, want := string(body), `{"worker":"runtime/worker","name":"sync"}`; got != want {
		t.Fatalf("json = %s, want %s", got, want)
	}
}
