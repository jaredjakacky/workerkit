package otel_test

import (
	"context"
	"errors"
	. "github.com/jaredjakacky/workerkit/otel"
	"strings"
	"sync"
	"testing"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestNewConfiguresProvidersAttributesAndInstruments(t *testing.T) {
	t.Parallel()

	tracerProvider := newRecordingTracerProvider()
	meterProvider := newRecordingMeterProvider()

	observer, err := New(
		nil,
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithAttributes(attribute.String("service.name", "workerkit-test")),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	if observer == nil {
		t.Fatal("New returned nil observer")
	}
	if tracerProvider.name != "github.com/jaredjakacky/workerkit/otel" {
		t.Fatalf("tracer name = %q, want %q", tracerProvider.name, "github.com/jaredjakacky/workerkit/otel")
	}
	if meterProvider.name != "github.com/jaredjakacky/workerkit/otel" {
		t.Fatalf("meter name = %q, want %q", meterProvider.name, "github.com/jaredjakacky/workerkit/otel")
	}

	for _, name := range []string{
		"workerkit.command.dispatches",
		"workerkit.command.duration",
		"workerkit.failures",
		"workerkit.readiness.changes",
		"workerkit.lifecycle.transitions",
	} {
		if !meterProvider.meter.created(name) {
			t.Fatalf("metric %q was not created", name)
		}
	}

	observer.ObserveReadiness(context.Background(), workerkit.ReadinessEvent{
		Runtime: "runtime-a",
		Ready:   true,
	})
	add := meterProvider.meter.counter("workerkit.readiness.changes").adds[0]
	assertStringAttr(t, add.attrs, "workerkit.runtime", "runtime-a")
	assertStringAttr(t, add.attrs, "service.name", "workerkit-test")
	assertBoolAttr(t, add.attrs, "workerkit.ready", true)
}

func TestNewUsesGlobalProvidersWhenOptionsAreNil(t *testing.T) {
	t.Parallel()

	observer, err := New(WithTracerProvider(nil), WithMeterProvider(nil))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if observer == nil {
		t.Fatal("New returned nil observer")
	}
}

func TestNewReturnsMetricCreationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		metricName string
		want       string
	}{
		{
			name:       "command count",
			metricName: "workerkit.command.dispatches",
			want:       "create command count metric",
		},
		{
			name:       "command duration",
			metricName: "workerkit.command.duration",
			want:       "create command duration metric",
		},
		{
			name:       "failure count",
			metricName: "workerkit.failures",
			want:       "create failure count metric",
		},
		{
			name:       "readiness change count",
			metricName: "workerkit.readiness.changes",
			want:       "create readiness change count metric",
		},
		{
			name:       "lifecycle change count",
			metricName: "workerkit.lifecycle.transitions",
			want:       "create lifecycle change count metric",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			meterProvider := newRecordingMeterProvider()
			meterProvider.meter.fail[tt.metricName] = errors.New("instrument failed")

			_, err := New(
				WithTracerProvider(newRecordingTracerProvider()),
				WithMeterProvider(meterProvider),
			)
			if err == nil {
				t.Fatal("New returned nil error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want to contain %q", err.Error(), tt.want)
			}
			if !errors.Is(err, meterProvider.meter.fail[tt.metricName]) {
				t.Fatalf("error does not wrap instrument error: %v", err)
			}
		})
	}
}

func TestStartCommandRecordsSpanAndCommandMetrics(t *testing.T) {
	t.Parallel()

	observer, tracerProvider, meterProvider := newTestObserver(t)
	startedAt := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	endedAt := startedAt.Add(125 * time.Millisecond)

	ctx, observation := observer.StartCommand(context.Background(), workerkit.CommandStartEvent{
		Runtime:    "runtime-a",
		Worker:     "worker-a",
		Command:    "sync",
		DispatchID: "runtime-a-1",
		StartedAt:  startedAt,
	})
	observation.End(ctx, workerkit.CommandEndEvent{
		DispatchID: "runtime-a-1",
		Attempts:   2,
		EndedAt:    endedAt,
		Duration:   125 * time.Millisecond,
		Success:    true,
	})

	span := tracerProvider.tracer.onlySpan(t)
	if span.name != "workerkit.command sync" {
		t.Fatalf("span name = %q", span.name)
	}
	if !span.start.Equal(startedAt) {
		t.Fatalf("span start = %s, want %s", span.start, startedAt)
	}
	if !span.end.Equal(endedAt) {
		t.Fatalf("span end = %s, want %s", span.end, endedAt)
	}
	if span.statusCode != codes.Ok {
		t.Fatalf("span status = %s, want %s", span.statusCode, codes.Ok)
	}
	assertStringAttr(t, span.attrs, "workerkit.runtime", "runtime-a")
	assertStringAttr(t, span.attrs, "workerkit.worker", "worker-a")
	assertStringAttr(t, span.attrs, "workerkit.command", "sync")
	assertStringAttr(t, span.attrs, "workerkit.command.dispatch_id", "runtime-a-1")
	assertStringAttr(t, span.attrs, "service.name", "workerkit-test")
	assertBoolAttr(t, span.attrs, "workerkit.command.success", true)
	assertIntAttr(t, span.attrs, "workerkit.command.attempts", 2)

	commandCount := meterProvider.meter.counter("workerkit.command.dispatches")
	if got := commandCount.total(); got != 1 {
		t.Fatalf("command count = %d, want 1", got)
	}
	assertBoolAttr(t, commandCount.adds[0].attrs, "workerkit.command.success", true)
	assertNoAttr(t, commandCount.adds[0].attrs, "workerkit.command.dispatch_id")
	assertNoAttr(t, commandCount.adds[0].attrs, "workerkit.command.attempts")

	commandDuration := meterProvider.meter.histogram("workerkit.command.duration")
	if len(commandDuration.records) != 1 {
		t.Fatalf("duration record count = %d, want 1", len(commandDuration.records))
	}
	if commandDuration.records[0].value != 0.125 {
		t.Fatalf("duration = %v, want 0.125", commandDuration.records[0].value)
	}
	assertBoolAttr(t, commandDuration.records[0].attrs, "workerkit.command.success", true)
	assertNoAttr(t, commandDuration.records[0].attrs, "workerkit.command.dispatch_id")
	assertNoAttr(t, commandDuration.records[0].attrs, "workerkit.command.attempts")
}

func TestCommandEndRecordsFailureSpanAndMetrics(t *testing.T) {
	t.Parallel()

	observer, tracerProvider, meterProvider := newTestObserver(t)
	startedAt := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	endedAt := startedAt.Add(time.Second)
	commandErr := errors.New("handler failed")

	ctx, observation := observer.StartCommand(context.Background(), workerkit.CommandStartEvent{
		Runtime:   "runtime-a",
		Worker:    "worker-a",
		Command:   "sync",
		StartedAt: startedAt,
	})
	observation.End(ctx, workerkit.CommandEndEvent{
		EndedAt:  endedAt,
		Duration: time.Second,
		Success:  false,
		Err:      commandErr,
		Message:  "handler failed",
	})

	span := tracerProvider.tracer.onlySpan(t)
	if span.statusCode != codes.Error {
		t.Fatalf("span status = %s, want %s", span.statusCode, codes.Error)
	}
	if span.statusDescription != "handler failed" {
		t.Fatalf("span status description = %q", span.statusDescription)
	}
	if len(span.errors) != 1 || !errors.Is(span.errors[0].err, commandErr) {
		t.Fatalf("recorded errors = %#v, want command error", span.errors)
	}
	assertBoolAttr(t, span.attrs, "workerkit.command.success", false)

	commandCount := meterProvider.meter.counter("workerkit.command.dispatches")
	if got := commandCount.total(); got != 1 {
		t.Fatalf("command count = %d, want 1", got)
	}
	assertBoolAttr(t, commandCount.adds[0].attrs, "workerkit.command.success", false)
}

func TestCommandEndRecordsMessageWhenErrorIsNil(t *testing.T) {
	t.Parallel()

	observer, tracerProvider, _ := newTestObserver(t)

	ctx, observation := observer.StartCommand(context.Background(), workerkit.CommandStartEvent{
		Runtime: "runtime-a",
		Worker:  "worker-a",
		Command: "sync",
	})
	observation.End(ctx, workerkit.CommandEndEvent{
		EndedAt: time.Now(),
		Success: false,
		Message: "admission rejected",
	})

	span := tracerProvider.tracer.onlySpan(t)
	if len(span.errors) != 1 || span.errors[0].err.Error() != "admission rejected" {
		t.Fatalf("recorded errors = %#v, want synthesized message error", span.errors)
	}
}

func TestObserveTransitionRecordsEventAndMetric(t *testing.T) {
	t.Parallel()

	observer, _, meterProvider := newTestObserver(t)
	parent := &recordingSpan{}
	at := time.Date(2026, 5, 15, 11, 0, 0, 0, time.UTC)

	observer.ObserveTransition(trace.ContextWithSpan(context.Background(), parent), workerkit.TransitionEvent{
		Runtime: "runtime-a",
		Worker:  "worker-a",
		From:    workerkit.StateStarting,
		To:      workerkit.StateRunning,
		At:      at,
	})

	event := parent.onlyEvent(t)
	if event.name != "workerkit.lifecycle.change" {
		t.Fatalf("event name = %q, want %q", event.name, "workerkit.lifecycle.change")
	}
	if !event.at.Equal(at) {
		t.Fatalf("event time = %s, want %s", event.at, at)
	}
	assertStringAttr(t, event.attrs, "workerkit.runtime", "runtime-a")
	assertStringAttr(t, event.attrs, "workerkit.worker", "worker-a")
	assertStringAttr(t, event.attrs, "workerkit.lifecycle.from", string(workerkit.StateStarting))
	assertStringAttr(t, event.attrs, "workerkit.lifecycle.to", string(workerkit.StateRunning))

	counter := meterProvider.meter.counter("workerkit.lifecycle.transitions")
	if got := counter.total(); got != 1 {
		t.Fatalf("lifecycle metric count = %d, want 1", got)
	}
	assertStringAttr(t, counter.adds[0].attrs, "workerkit.lifecycle.to", string(workerkit.StateRunning))
}

func TestObserveFailureRecordsEventStatusErrorAndMetric(t *testing.T) {
	t.Parallel()

	observer, _, meterProvider := newTestObserver(t)
	parent := &recordingSpan{}
	at := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	failureErr := errors.New("worker failed")

	observer.ObserveFailure(trace.ContextWithSpan(context.Background(), parent), workerkit.FailureEvent{
		Runtime:    "runtime-a",
		Worker:     "worker-a",
		Command:    "sync",
		DispatchID: "runtime-a-1",
		Attempt:    2,
		At:         at,
		Err:        failureErr,
		Message:    "worker failed",
		Panic:      true,
	})

	event := parent.onlyEvent(t)
	if event.name != "workerkit.failure" {
		t.Fatalf("event name = %q, want %q", event.name, "workerkit.failure")
	}
	assertStringAttr(t, event.attrs, "workerkit.runtime", "runtime-a")
	assertStringAttr(t, event.attrs, "workerkit.worker", "worker-a")
	assertStringAttr(t, event.attrs, "workerkit.command", "sync")
	assertStringAttr(t, event.attrs, "workerkit.command.dispatch_id", "runtime-a-1")
	assertIntAttr(t, event.attrs, "workerkit.command.attempt", 2)
	assertBoolAttr(t, event.attrs, "workerkit.failure.panic", true)

	if parent.statusCode != codes.Error {
		t.Fatalf("span status = %s, want %s", parent.statusCode, codes.Error)
	}
	if parent.statusDescription != "worker failed" {
		t.Fatalf("span status description = %q", parent.statusDescription)
	}
	if len(parent.errors) != 1 || !errors.Is(parent.errors[0].err, failureErr) {
		t.Fatalf("recorded errors = %#v, want failure error", parent.errors)
	}

	counter := meterProvider.meter.counter("workerkit.failures")
	if got := counter.total(); got != 1 {
		t.Fatalf("failure metric count = %d, want 1", got)
	}
	assertBoolAttr(t, counter.adds[0].attrs, "workerkit.failure.panic", true)
	assertNoAttr(t, counter.adds[0].attrs, "workerkit.command.dispatch_id")
	assertNoAttr(t, counter.adds[0].attrs, "workerkit.command.attempt")
}

func TestObserveFailureRecordsMessageWhenErrorIsNil(t *testing.T) {
	t.Parallel()

	observer, _, _ := newTestObserver(t)
	parent := &recordingSpan{}

	observer.ObserveFailure(trace.ContextWithSpan(context.Background(), parent), workerkit.FailureEvent{
		Runtime: "runtime-a",
		Worker:  "worker-a",
		Message: "background loop exited",
	})

	if len(parent.errors) != 1 || parent.errors[0].err.Error() != "background loop exited" {
		t.Fatalf("recorded errors = %#v, want synthesized message error", parent.errors)
	}
}

func TestObserveReadinessRecordsEventAndMetric(t *testing.T) {
	t.Parallel()

	observer, _, meterProvider := newTestObserver(t)
	parent := &recordingSpan{}
	at := time.Date(2026, 5, 15, 13, 0, 0, 0, time.UTC)

	observer.ObserveReadiness(trace.ContextWithSpan(context.Background(), parent), workerkit.ReadinessEvent{
		Runtime: "runtime-a",
		Worker:  "worker-a",
		Ready:   true,
		At:      at,
	})

	event := parent.onlyEvent(t)
	if event.name != "workerkit.readiness.change" {
		t.Fatalf("event name = %q, want %q", event.name, "workerkit.readiness.change")
	}
	if !event.at.Equal(at) {
		t.Fatalf("event time = %s, want %s", event.at, at)
	}
	assertStringAttr(t, event.attrs, "workerkit.runtime", "runtime-a")
	assertStringAttr(t, event.attrs, "workerkit.worker", "worker-a")
	assertBoolAttr(t, event.attrs, "workerkit.ready", true)

	counter := meterProvider.meter.counter("workerkit.readiness.changes")
	if got := counter.total(); got != 1 {
		t.Fatalf("readiness metric count = %d, want 1", got)
	}
	assertBoolAttr(t, counter.adds[0].attrs, "workerkit.ready", true)
}

func TestNilInstrumentsDoNotPanic(t *testing.T) {
	t.Parallel()

	observer, err := New(WithTracerProvider(tracenoop.NewTracerProvider()), WithMeterProvider(metricnoop.NewMeterProvider()))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	observer.ObserveTransition(context.Background(), workerkit.TransitionEvent{Runtime: "runtime-a"})
	ctx, observation := observer.StartCommand(context.Background(), workerkit.CommandStartEvent{
		Runtime: "runtime-a",
		Worker:  "worker-a",
		Command: "sync",
	})
	observation.End(ctx, workerkit.CommandEndEvent{Success: true})
	observer.ObserveFailure(context.Background(), workerkit.FailureEvent{Runtime: "runtime-a"})
	observer.ObserveReadiness(context.Background(), workerkit.ReadinessEvent{Runtime: "runtime-a"})
}

func TestObserverSupportsConcurrentUse(t *testing.T) {
	t.Parallel()

	observer, err := New(
		WithTracerProvider(tracenoop.NewTracerProvider()),
		WithMeterProvider(metricnoop.NewMeterProvider()),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, observation := observer.StartCommand(context.Background(), workerkit.CommandStartEvent{})
			observer.ObserveTransition(ctx, workerkit.TransitionEvent{})
			observer.ObserveFailure(ctx, workerkit.FailureEvent{})
			observer.ObserveReadiness(ctx, workerkit.ReadinessEvent{})
			observation.End(ctx, workerkit.CommandEndEvent{})
		}()
	}
	wg.Wait()
}

func newTestObserver(t *testing.T) (*Observer, *recordingTracerProvider, *recordingMeterProvider) {
	t.Helper()

	tracerProvider := newRecordingTracerProvider()
	meterProvider := newRecordingMeterProvider()
	observer, err := New(
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithAttributes(attribute.String("service.name", "workerkit-test")),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return observer, tracerProvider, meterProvider
}

type recordingTracerProvider struct {
	tracenoop.TracerProvider
	name   string
	tracer *recordingTracer
}

func newRecordingTracerProvider() *recordingTracerProvider {
	return &recordingTracerProvider{
		tracer: &recordingTracer{},
	}
}

func (p *recordingTracerProvider) Tracer(name string, _ ...trace.TracerOption) trace.Tracer {
	p.name = name
	return p.tracer
}

type recordingTracer struct {
	tracenoop.Tracer
	spans []*recordingSpan
}

func (t *recordingTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	span := &recordingSpan{
		name:  name,
		start: cfg.Timestamp(),
		attrs: append([]attribute.KeyValue{}, cfg.Attributes()...),
	}
	t.spans = append(t.spans, span)
	return trace.ContextWithSpan(ctx, span), span
}

func (t *recordingTracer) onlySpan(tb testing.TB) *recordingSpan {
	tb.Helper()

	if len(t.spans) != 1 {
		tb.Fatalf("span count = %d, want 1", len(t.spans))
	}
	return t.spans[0]
}

type recordingSpan struct {
	tracenoop.Span
	name              string
	start             time.Time
	end               time.Time
	attrs             []attribute.KeyValue
	events            []recordingSpanEvent
	errors            []recordingSpanError
	statusCode        codes.Code
	statusDescription string
}

func (s *recordingSpan) End(opts ...trace.SpanEndOption) {
	cfg := trace.NewSpanEndConfig(opts...)
	s.end = cfg.Timestamp()
}

func (s *recordingSpan) AddEvent(name string, opts ...trace.EventOption) {
	cfg := trace.NewEventConfig(opts...)
	s.events = append(s.events, recordingSpanEvent{
		name:  name,
		at:    cfg.Timestamp(),
		attrs: append([]attribute.KeyValue{}, cfg.Attributes()...),
	})
}

func (s *recordingSpan) RecordError(err error, opts ...trace.EventOption) {
	cfg := trace.NewEventConfig(opts...)
	s.errors = append(s.errors, recordingSpanError{
		err: err,
		at:  cfg.Timestamp(),
	})
}

func (s *recordingSpan) SetStatus(code codes.Code, description string) {
	s.statusCode = code
	s.statusDescription = description
}

func (s *recordingSpan) SetAttributes(attrs ...attribute.KeyValue) {
	s.attrs = append(s.attrs, attrs...)
}

func (s *recordingSpan) onlyEvent(tb testing.TB) recordingSpanEvent {
	tb.Helper()

	if len(s.events) != 1 {
		tb.Fatalf("event count = %d, want 1", len(s.events))
	}
	return s.events[0]
}

type recordingSpanEvent struct {
	name  string
	at    time.Time
	attrs []attribute.KeyValue
}

type recordingSpanError struct {
	err error
	at  time.Time
}

type recordingMeterProvider struct {
	metric.MeterProvider
	name  string
	meter *recordingMeter
}

func newRecordingMeterProvider() *recordingMeterProvider {
	return &recordingMeterProvider{
		MeterProvider: metricnoop.NewMeterProvider(),
		meter:         newRecordingMeter(),
	}
}

func (p *recordingMeterProvider) Meter(name string, _ ...metric.MeterOption) metric.Meter {
	p.name = name
	return p.meter
}

type recordingMeter struct {
	metric.Meter
	fail       map[string]error
	counters   map[string]*recordingCounter
	histograms map[string]*recordingHistogram
	names      []string
}

func newRecordingMeter() *recordingMeter {
	return &recordingMeter{
		Meter:      metricnoop.NewMeterProvider().Meter("noop"),
		fail:       map[string]error{},
		counters:   map[string]*recordingCounter{},
		histograms: map[string]*recordingHistogram{},
	}
}

func (m *recordingMeter) Int64Counter(name string, _ ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	m.names = append(m.names, name)
	if err := m.fail[name]; err != nil {
		return nil, err
	}
	counter := &recordingCounter{}
	m.counters[name] = counter
	return counter, nil
}

func (m *recordingMeter) Float64Histogram(name string, _ ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	m.names = append(m.names, name)
	if err := m.fail[name]; err != nil {
		return nil, err
	}
	histogram := &recordingHistogram{}
	m.histograms[name] = histogram
	return histogram, nil
}

func (m *recordingMeter) created(name string) bool {
	for _, created := range m.names {
		if created == name {
			return true
		}
	}
	return false
}

func (m *recordingMeter) counter(name string) *recordingCounter {
	return m.counters[name]
}

func (m *recordingMeter) histogram(name string) *recordingHistogram {
	return m.histograms[name]
}

type recordingCounter struct {
	metricnoop.Int64Counter
	adds []recordingAdd
}

func (c *recordingCounter) Add(ctx context.Context, incr int64, opts ...metric.AddOption) {
	cfg := metric.NewAddConfig(opts)
	attrs := cfg.Attributes()
	c.adds = append(c.adds, recordingAdd{
		value: incr,
		attrs: attrs.ToSlice(),
	})
}

func (c *recordingCounter) Enabled(context.Context) bool {
	return true
}

func (c *recordingCounter) total() int64 {
	var total int64
	for _, add := range c.adds {
		total += add.value
	}
	return total
}

type recordingAdd struct {
	value int64
	attrs []attribute.KeyValue
}

type recordingHistogram struct {
	metricnoop.Float64Histogram
	records []recordingRecord
}

func (h *recordingHistogram) Record(ctx context.Context, value float64, opts ...metric.RecordOption) {
	cfg := metric.NewRecordConfig(opts)
	attrs := cfg.Attributes()
	h.records = append(h.records, recordingRecord{
		value: value,
		attrs: attrs.ToSlice(),
	})
}

func (h *recordingHistogram) Enabled(context.Context) bool {
	return true
}

type recordingRecord struct {
	value float64
	attrs []attribute.KeyValue
}

func assertStringAttr(tb testing.TB, attrs []attribute.KeyValue, key string, want string) {
	tb.Helper()

	for _, attr := range attrs {
		if string(attr.Key) == key {
			if got := attr.Value.AsString(); got != want {
				tb.Fatalf("attribute %q = %q, want %q", key, got, want)
			}
			return
		}
	}
	tb.Fatalf("attribute %q not found in %#v", key, attrs)
}

func assertBoolAttr(tb testing.TB, attrs []attribute.KeyValue, key string, want bool) {
	tb.Helper()

	for _, attr := range attrs {
		if string(attr.Key) == key {
			if got := attr.Value.AsBool(); got != want {
				tb.Fatalf("attribute %q = %v, want %v", key, got, want)
			}
			return
		}
	}
	tb.Fatalf("attribute %q not found in %#v", key, attrs)
}

func assertIntAttr(tb testing.TB, attrs []attribute.KeyValue, key string, want int64) {
	tb.Helper()

	for _, attr := range attrs {
		if string(attr.Key) == key {
			if got := attr.Value.AsInt64(); got != want {
				tb.Fatalf("attribute %q = %d, want %d", key, got, want)
			}
			return
		}
	}
	tb.Fatalf("attribute %q not found in %#v", key, attrs)
}

func assertNoAttr(tb testing.TB, attrs []attribute.KeyValue, key string) {
	tb.Helper()

	for _, attr := range attrs {
		if string(attr.Key) == key {
			tb.Fatalf("attribute %q unexpectedly found in %#v", key, attrs)
		}
	}
}
