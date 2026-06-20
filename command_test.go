package workerkit_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/jaredjakacky/workerkit"
	"strings"
	"testing"
	"time"

	opskit "github.com/jaredjakacky/opskit"
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

func TestCommandFromOpskitTranslatesRequestAndCompletedResult(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	descriptor := opskit.CommandDescriptor{
		Name:        "cache/refresh",
		Description: "refresh cache entries",
		PayloadKind: "cache_refresh",
		Dangerous:   true,
		Idempotent:  true,
		Attributes:  []opskit.Attribute{opskit.Attr("scope", "cache")},
	}
	handler := opskit.CommandHandlerFunc(func(ctx context.Context, req opskit.CommandRequest) opskit.CommandResult {
		if ctx == nil {
			t.Fatal("context = nil")
		}
		if req.Name != descriptor.Name || string(req.Payload) != `{"force":true}` {
			t.Fatalf("request = %#v, want translated name and payload", req)
		}
		if req.RequestedAt == nil || !req.RequestedAt.Equal(requestedAt) {
			t.Fatalf("RequestedAt = %v, want %v", req.RequestedAt, requestedAt)
		}
		return opskit.CompletedCommand("refreshed", map[string]any{"count": 2}, time.Millisecond)
	})

	spec := CommandFromOpskit(descriptor, handler)
	descriptor.Attributes[0] = opskit.Attr("scope", "mutated")
	if spec.Name != "cache/refresh" || spec.Description != "refresh cache entries" || spec.PayloadKind != "cache_refresh" {
		t.Fatalf("spec = %#v, want descriptor metadata", spec)
	}
	if !spec.Dangerous || !spec.Idempotent || spec.Attributes[0] != opskit.Attr("scope", "cache") {
		t.Fatalf("spec metadata = %#v, want cloned advisory metadata", spec)
	}

	result, err := spec.Handler.HandleCommand(context.Background(), CommandRequest{
		Name:        descriptor.Name,
		Payload:     []byte(`{"force":true}`),
		RequestedAt: requestedAt,
	})
	if err != nil {
		t.Fatalf("HandleCommand error = %v", err)
	}
	if result.Message != "refreshed" || string(result.Payload) != `{"count":2}` {
		t.Fatalf("result = %#v, want completed Opskit result", result)
	}
}

func TestCommandFromOpskitMapsOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		result  opskit.CommandResult
		wantErr error
	}{
		{name: "rejected", result: opskit.RejectedCommand("disabled"), wantErr: ErrOpsCommandRejected},
		{name: "rejected with error detail", result: opskit.CommandResult{State: opskit.StateNotReady, Accepted: false, Error: "disabled"}, wantErr: ErrOpsCommandRejected},
		{name: "failed", result: opskit.FailedCommand("refresh failed", errors.New("backend unavailable"), 0), wantErr: ErrOpsCommandFailed},
		{name: "error text implies failure", result: opskit.CommandResult{State: opskit.StateReady, Accepted: true, Error: "inconsistent failure"}, wantErr: ErrOpsCommandFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := CommandFromOpskit(opskit.CommandDescriptor{Name: "refresh"}, opskit.CommandHandlerFunc(
				func(context.Context, opskit.CommandRequest) opskit.CommandResult { return tt.result },
			))
			_, err := spec.Handler.HandleCommand(context.Background(), CommandRequest{Name: "refresh"})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestCommandFromOpskitMapsCancellationAndResultEncoding(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	spec := CommandFromOpskit(opskit.CommandDescriptor{Name: "refresh"}, opskit.CommandHandlerFunc(
		func(context.Context, opskit.CommandRequest) opskit.CommandResult {
			return opskit.CompletedCommand("ignored", nil, 0)
		},
	))
	if _, err := spec.Handler.HandleCommand(canceled, CommandRequest{Name: "refresh"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want context.Canceled", err)
	}
	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	if _, err := spec.Handler.HandleCommand(deadline, CommandRequest{Name: "refresh"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error = %v, want context.DeadlineExceeded", err)
	}

	spec = CommandFromOpskit(opskit.CommandDescriptor{Name: "refresh"}, opskit.CommandHandlerFunc(
		func(context.Context, opskit.CommandRequest) opskit.CommandResult {
			return opskit.CompletedCommand("invalid", make(chan int), 0)
		},
	))
	if _, err := spec.Handler.HandleCommand(context.Background(), CommandRequest{Name: "refresh"}); !errors.Is(err, ErrOpsCommandFailed) || !strings.Contains(err.Error(), "marshal result") {
		t.Fatalf("encoding error = %v, want ErrOpsCommandFailed marshal error", err)
	}

	var nilHandler opskit.CommandHandler
	if err := CommandFromOpskit(opskit.CommandDescriptor{Name: "refresh"}, nilHandler).Validate(); err == nil {
		t.Fatal("Validate nil Opskit handler error = nil")
	}
}

func TestCommandFromOpskitAcceptedAsyncAndNilResult(t *testing.T) {
	t.Parallel()

	for name, opsResult := range map[string]opskit.CommandResult{
		"accepted":  opskit.AcceptedCommand("queued"),
		"completed": opskit.CompletedCommand("done", nil, 0),
	} {
		t.Run(name, func(t *testing.T) {
			spec := CommandFromOpskit(opskit.CommandDescriptor{Name: "refresh"}, opskit.CommandHandlerFunc(
				func(context.Context, opskit.CommandRequest) opskit.CommandResult { return opsResult },
			))
			result, err := spec.Handler.HandleCommand(context.Background(), CommandRequest{Name: "refresh"})
			if err != nil {
				t.Fatalf("HandleCommand error = %v", err)
			}
			if result.Message != opsResult.Message || result.Payload != nil {
				t.Fatalf("result = %#v, want message with nil payload", result)
			}
		})
	}
}

func ExampleCommandFromOpskit() {
	descriptor := opskit.CommandDescriptor{
		Name:        "cache/refresh",
		Description: "refresh cache entries",
		Idempotent:  true,
	}
	handler := opskit.CommandHandlerFunc(func(context.Context, opskit.CommandRequest) opskit.CommandResult {
		return opskit.CompletedCommand("refreshed", map[string]bool{"ok": true}, 0)
	})

	spec := CommandFromOpskit(descriptor, handler)
	result, _ := spec.Handler.HandleCommand(context.Background(), CommandRequest{Name: spec.Name})
	fmt.Printf("%s %s\n", result.Message, result.Payload)
	// Output: refreshed {"ok":true}
}
