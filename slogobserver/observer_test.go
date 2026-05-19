package slogobserver

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	workerkit "github.com/jaredjakacky/workerkit-incubator"
)

type captureHandler struct {
	mu      sync.Mutex
	records []capturedRecord
	attrs   []slog.Attr
}

type capturedRecord struct {
	level   slog.Level
	message string
	attrs   map[string]slog.Value
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := map[string]slog.Value{}
	for _, attr := range h.attrs {
		attrs[attr.Key] = attr.Value
	}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value
		return true
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, capturedRecord{
		level:   record.Level,
		message: record.Message,
		attrs:   attrs,
	})
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := &captureHandler{}
	next.attrs = append(next.attrs, h.attrs...)
	next.attrs = append(next.attrs, attrs...)
	return next
}

func (h *captureHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *captureHandler) lastRecord(t *testing.T) capturedRecord {
	t.Helper()

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) == 0 {
		t.Fatal("no log records captured")
	}
	return h.records[len(h.records)-1]
}

func TestNewAppliesDefaultsAndOptions(t *testing.T) {
	t.Parallel()

	observer := New(nil, nil, WithLevel(slog.LevelDebug), WithAttributes(slog.String("service", "orders")))
	if observer == nil {
		t.Fatal("New returned nil")
	}
	if observer.logger == nil {
		t.Fatal("logger = nil, want default logger")
	}
	if observer.config.level != slog.LevelDebug {
		t.Fatalf("level = %s, want debug", observer.config.level)
	}
	if len(observer.config.attrs) != 1 || observer.config.attrs[0].Key != "service" {
		t.Fatalf("attrs = %#v, want service attr", observer.config.attrs)
	}
}

func TestObserveTransitionLogsLifecycleAttrs(t *testing.T) {
	t.Parallel()

	handler := &captureHandler{}
	observer := New(slog.New(handler), WithLevel(slog.LevelDebug), WithAttributes(slog.String("service", "orders")))

	observer.ObserveTransition(context.Background(), workerkit.TransitionEvent{
		Runtime: "runtime",
		Worker:  "runtime/worker",
		From:    workerkit.StateRegistered,
		To:      workerkit.StateRunning,
	})

	record := handler.lastRecord(t)
	requireRecord(t, record, slog.LevelDebug, "workerkit lifecycle transition")
	requireStringAttr(t, record, "runtime", "runtime")
	requireStringAttr(t, record, "worker", "runtime/worker")
	requireStringAttr(t, record, "service", "orders")
	requireStringAttr(t, record, "lifecycle_from", string(workerkit.StateRegistered))
	requireStringAttr(t, record, "lifecycle_to", string(workerkit.StateRunning))
}

func TestStartCommandLogsCommandEnd(t *testing.T) {
	t.Parallel()

	handler := &captureHandler{}
	observer := New(slog.New(handler), WithLevel(slog.LevelWarn))
	ctx := context.WithValue(context.Background(), testContextKey("trace"), "value")

	gotCtx, observation := observer.StartCommand(ctx, workerkit.CommandStartEvent{
		Runtime:    "runtime",
		Worker:     "runtime/worker",
		Command:    "sync",
		DispatchID: "runtime-1",
	})
	if gotCtx != ctx {
		t.Fatal("StartCommand changed context")
	}

	observation.End(gotCtx, workerkit.CommandEndEvent{
		Success:    true,
		Duration:   25 * time.Millisecond,
		DispatchID: "runtime-1",
		Attempts:   3,
	})

	record := handler.lastRecord(t)
	requireRecord(t, record, slog.LevelWarn, "workerkit command dispatch completed")
	requireStringAttr(t, record, "runtime", "runtime")
	requireStringAttr(t, record, "worker", "runtime/worker")
	requireStringAttr(t, record, "command", "sync")
	requireBoolAttr(t, record, "success", true)
	requireDurationAttr(t, record, "duration", 25*time.Millisecond)
	requireStringAttr(t, record, "dispatch_id", "runtime-1")
	requireIntAttr(t, record, "attempts", 3)
}

func TestCommandEndLogsError(t *testing.T) {
	t.Parallel()

	handler := &captureHandler{}
	observer := New(slog.New(handler))
	wantErr := errors.New("command failed")

	_, observation := observer.StartCommand(context.Background(), workerkit.CommandStartEvent{
		Runtime: "runtime",
		Worker:  "runtime/worker",
		Command: "sync",
	})
	observation.End(context.Background(), workerkit.CommandEndEvent{
		Success: false,
		Err:     wantErr,
		Message: "fallback message",
	})

	record := handler.lastRecord(t)
	requireRecord(t, record, slog.LevelInfo, "workerkit command dispatch completed")
	requireErrorAttr(t, record, "error", wantErr)
}

func TestObserveFailureLogsAtErrorLevel(t *testing.T) {
	t.Parallel()

	handler := &captureHandler{}
	observer := New(slog.New(handler), WithLevel(slog.LevelDebug))
	wantErr := errors.New("worker failed")

	observer.ObserveFailure(context.Background(), workerkit.FailureEvent{
		Runtime:    "runtime",
		Worker:     "runtime/worker",
		Command:    "sync",
		DispatchID: "runtime-1",
		Attempt:    2,
		Err:        wantErr,
		Panic:      true,
	})

	record := handler.lastRecord(t)
	requireRecord(t, record, slog.LevelError, "workerkit failure")
	requireStringAttr(t, record, "runtime", "runtime")
	requireStringAttr(t, record, "worker", "runtime/worker")
	requireStringAttr(t, record, "command", "sync")
	requireStringAttr(t, record, "dispatch_id", "runtime-1")
	requireIntAttr(t, record, "attempt", 2)
	requireBoolAttr(t, record, "panic", true)
	requireErrorAttr(t, record, "error", wantErr)
}

func TestObserveReadinessLogsReadyWithoutWorkerAttr(t *testing.T) {
	t.Parallel()

	handler := &captureHandler{}
	observer := New(slog.New(handler), WithLevel(slog.LevelInfo))

	observer.ObserveReadiness(context.Background(), workerkit.ReadinessEvent{
		Runtime: "runtime",
		Ready:   true,
	})

	record := handler.lastRecord(t)
	requireRecord(t, record, slog.LevelInfo, "workerkit readiness change")
	requireStringAttr(t, record, "runtime", "runtime")
	requireBoolAttr(t, record, "ready", true)
	if _, ok := record.attrs["worker"]; ok {
		t.Fatalf("worker attr present, want omitted")
	}
}

type testContextKey string

func requireRecord(t *testing.T, record capturedRecord, level slog.Level, message string) {
	t.Helper()

	if record.level != level {
		t.Fatalf("level = %s, want %s", record.level, level)
	}
	if record.message != message {
		t.Fatalf("message = %q, want %q", record.message, message)
	}
}

func requireStringAttr(t *testing.T, record capturedRecord, key string, want string) {
	t.Helper()

	value, ok := record.attrs[key]
	if !ok {
		t.Fatalf("%s attr omitted, want %q", key, want)
	}
	if got := value.String(); got != want {
		t.Fatalf("%s attr = %q, want %q", key, got, want)
	}
}

func requireBoolAttr(t *testing.T, record capturedRecord, key string, want bool) {
	t.Helper()

	value, ok := record.attrs[key]
	if !ok {
		t.Fatalf("%s attr omitted, want %v", key, want)
	}
	if got := value.Bool(); got != want {
		t.Fatalf("%s attr = %v, want %v", key, got, want)
	}
}

func requireIntAttr(t *testing.T, record capturedRecord, key string, want int64) {
	t.Helper()

	value, ok := record.attrs[key]
	if !ok {
		t.Fatalf("%s attr omitted, want %d", key, want)
	}
	if got := value.Int64(); got != want {
		t.Fatalf("%s attr = %d, want %d", key, got, want)
	}
}

func requireDurationAttr(t *testing.T, record capturedRecord, key string, want time.Duration) {
	t.Helper()

	value, ok := record.attrs[key]
	if !ok {
		t.Fatalf("%s attr omitted, want %s", key, want)
	}
	if got := value.Duration(); got != want {
		t.Fatalf("%s attr = %s, want %s", key, got, want)
	}
}

func requireErrorAttr(t *testing.T, record capturedRecord, key string, want error) {
	t.Helper()

	value, ok := record.attrs[key]
	if !ok {
		t.Fatalf("%s attr omitted, want %v", key, want)
	}
	got, ok := value.Any().(error)
	if !ok {
		t.Fatalf("%s attr = %T, want error", key, value.Any())
	}
	if !errors.Is(got, want) {
		t.Fatalf("%s attr = %v, want %v", key, got, want)
	}
}
