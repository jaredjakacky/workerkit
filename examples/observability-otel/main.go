package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	workerkit "github.com/jaredjakacky/workerkit"
	workerkitotel "github.com/jaredjakacky/workerkit/otel"
	"github.com/jaredjakacky/workerkit/retry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func main() {
	ctx := context.Background()

	// This example demonstrates Workerkit observer events flowing into OpenTelemetry
	// while the core runtime remains transport-agnostic.
	observer, err := workerkitotel.New(
		workerkitotel.WithTracerProvider(stdoutTracerProvider{}),
		workerkitotel.WithMeterProvider(stdoutMeterProvider{}),
		workerkitotel.WithAttributes(attribute.String("service.name", "observability-otel")),
	)
	if err != nil {
		log.Fatal(err)
	}

	runtime, err := workerkit.New(
		workerkit.Identity{Name: "otel_demo"},
		workerkit.WithObserver(observer),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker := &observedWorker{}
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "worker",
		Description: "Emits command spans, retry events, and metrics through the OTel observer.",
		Worker:      worker,
	},
		workerkit.WithWorkerCommandRetry(retry.Attempts(2, retry.Constant(25*time.Millisecond), retry.None())),
		workerkit.WithCommand("demo/fail-once", workerkit.CommandHandlerFunc(worker.failOnce)),
	); err != nil {
		log.Fatal(err)
	}

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}

	result, err := runtime.Dispatch(ctx, workerkit.CommandRequest{
		Worker: "worker",
		Name:   "demo/fail-once",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("command result: %s\n", result.Message)

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

type observedWorker struct {
	attempts atomic.Int32
}

func (w *observedWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return errors.New("worker runtime handle unavailable")
	}
	return runtime.SetReady(true)
}

func (w *observedWorker) Stop(context.Context) error {
	return nil
}

func (w *observedWorker) failOnce(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	if w.attempts.Add(1) == 1 {
		return workerkit.CommandResult{}, errors.New("temporary command failure")
	}
	return workerkit.CommandResult{Message: "retry succeeded"}, nil
}

type stdoutTracerProvider struct {
	tracenoop.TracerProvider
}

func (stdoutTracerProvider) Tracer(name string, _ ...trace.TracerOption) trace.Tracer {
	fmt.Printf("otel tracer name=%s\n", name)
	return stdoutTracer{}
}

type stdoutTracer struct {
	tracenoop.Tracer
}

func (stdoutTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	span := &stdoutSpan{
		name:  name,
		start: cfg.Timestamp(),
		attrs: append([]attribute.KeyValue{}, cfg.Attributes()...),
	}
	fmt.Printf("span start name=%q attrs=%s\n", name, formatAttrs(span.attrs))
	return trace.ContextWithSpan(ctx, span), span
}

type stdoutSpan struct {
	tracenoop.Span
	name              string
	start             time.Time
	attrs             []attribute.KeyValue
	statusCode        codes.Code
	statusDescription string
}

func (s *stdoutSpan) End(opts ...trace.SpanEndOption) {
	cfg := trace.NewSpanEndConfig(opts...)
	fmt.Printf("span end name=%q status=%s description=%q attrs=%s ended_at=%s\n",
		s.name,
		s.statusCode,
		s.statusDescription,
		formatAttrs(s.attrs),
		cfg.Timestamp().Format(time.RFC3339Nano))
}

func (s *stdoutSpan) AddEvent(name string, opts ...trace.EventOption) {
	cfg := trace.NewEventConfig(opts...)
	fmt.Printf("span event span=%q name=%q attrs=%s\n", s.name, name, formatAttrs(cfg.Attributes()))
}

func (s *stdoutSpan) RecordError(err error, opts ...trace.EventOption) {
	fmt.Printf("span error span=%q error=%q\n", s.name, err)
}

func (s *stdoutSpan) SetStatus(code codes.Code, description string) {
	s.statusCode = code
	s.statusDescription = description
}

func (s *stdoutSpan) SetAttributes(attrs ...attribute.KeyValue) {
	s.attrs = append(s.attrs, attrs...)
}

type stdoutMeterProvider struct {
	metric.MeterProvider
}

func (stdoutMeterProvider) Meter(name string, _ ...metric.MeterOption) metric.Meter {
	fmt.Printf("otel meter name=%s\n", name)
	return stdoutMeter{
		Meter: metricnoop.NewMeterProvider().Meter("noop"),
	}
}

type stdoutMeter struct {
	metric.Meter
}

func (m stdoutMeter) Int64Counter(name string, _ ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return stdoutCounter{name: name}, nil
}

func (m stdoutMeter) Float64Histogram(name string, _ ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return stdoutHistogram{name: name}, nil
}

type stdoutCounter struct {
	metricnoop.Int64Counter
	name string
}

func (c stdoutCounter) Add(ctx context.Context, incr int64, opts ...metric.AddOption) {
	cfg := metric.NewAddConfig(opts)
	attrs := cfg.Attributes()
	fmt.Printf("metric counter name=%q value=%d attrs=%s\n",
		c.name, incr, formatAttrs(attrs.ToSlice()))
}

func (c stdoutCounter) Enabled(context.Context) bool {
	return true
}

type stdoutHistogram struct {
	metricnoop.Float64Histogram
	name string
}

func (h stdoutHistogram) Record(ctx context.Context, value float64, opts ...metric.RecordOption) {
	cfg := metric.NewRecordConfig(opts)
	attrs := cfg.Attributes()
	fmt.Printf("metric histogram name=%q value=%.6f attrs=%s\n",
		h.name, value, formatAttrs(attrs.ToSlice()))
}

func (h stdoutHistogram) Enabled(context.Context) bool {
	return true
}

func formatAttrs(attrs []attribute.KeyValue) string {
	if len(attrs) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		parts = append(parts, fmt.Sprintf("%s=%s", attr.Key, attr.Value.Emit()))
	}
	return "[" + strings.Join(parts, " ") + "]"
}
