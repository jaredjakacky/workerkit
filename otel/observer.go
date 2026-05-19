package otel

import (
	"context"
	"errors"
	"fmt"

	workerkit "github.com/jaredjakacky/workerkit"
	globalotel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/jaredjakacky/workerkit/otel"

	metricCommandCount         = "workerkit.command.dispatches"
	metricCommandDuration      = "workerkit.command.duration"
	metricFailureCount         = "workerkit.failures"
	metricReadinessChangeCount = "workerkit.readiness.changes"
	metricLifecycleChangeCount = "workerkit.lifecycle.transitions"

	eventLifecycleChange = "workerkit.lifecycle.change"
	eventFailure         = "workerkit.failure"
	eventReadinessChange = "workerkit.readiness.change"
	spanCommandPrefix    = "workerkit.command"

	attrRuntime         = "workerkit.runtime"
	attrWorker          = "workerkit.worker"
	attrCommand         = "workerkit.command"
	attrDispatchID      = "workerkit.command.dispatch_id"
	attrCommandAttempt  = "workerkit.command.attempt"
	attrCommandAttempts = "workerkit.command.attempts"
	attrCommandSuccess  = "workerkit.command.success"
	attrLifecycleFrom   = "workerkit.lifecycle.from"
	attrLifecycleTo     = "workerkit.lifecycle.to"
	attrFailurePanic    = "workerkit.failure.panic"
	attrReady           = "workerkit.ready"
)

type config struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	attributes     []attribute.KeyValue
}

// Option configures the OpenTelemetry observer adapter.
type Option func(*config)

// WithTracerProvider sets the tracer provider used by the observer.
//
// When nil, the observer uses otel.GetTracerProvider().
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(cfg *config) { cfg.tracerProvider = provider }
}

// WithMeterProvider sets the meter provider used by the observer.
//
// When nil, the observer uses otel.GetMeterProvider().
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(cfg *config) { cfg.meterProvider = provider }
}

// WithAttributes appends attributes to every span and metric emitted by the
// observer.
//
// Service identity such as service.name and service.version should usually be
// configured on the application's OpenTelemetry resource instead.
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return func(cfg *config) {
		cfg.attributes = append(cfg.attributes, attrs...)
	}
}

// Observer converts Workerkit runtime telemetry events into OpenTelemetry spans
// and metrics.
//
// It uses global OpenTelemetry providers by default, matching Servekit's
// default integration mode. Explicit providers can be supplied when the host
// application does not use globals.
type Observer struct {
	config config
	tracer trace.Tracer

	commandCount         metric.Int64Counter
	commandDuration      metric.Float64Histogram
	failureCount         metric.Int64Counter
	readinessChangeCount metric.Int64Counter
	lifecycleChangeCount metric.Int64Counter
}

// New constructs an OpenTelemetry-backed Workerkit observer.
func New(opts ...Option) (*Observer, error) {
	cfg := config{
		tracerProvider: globalotel.GetTracerProvider(),
		meterProvider:  globalotel.GetMeterProvider(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.tracerProvider == nil {
		cfg.tracerProvider = globalotel.GetTracerProvider()
	}
	if cfg.meterProvider == nil {
		cfg.meterProvider = globalotel.GetMeterProvider()
	}

	meter := cfg.meterProvider.Meter(instrumentationName)

	observer := &Observer{
		config: cfg,
		tracer: cfg.tracerProvider.Tracer(instrumentationName),
	}
	var err error
	observer.commandCount, err = meter.Int64Counter(
		metricCommandCount,
		metric.WithDescription("Workerkit command dispatch results."),
	)
	if err != nil {
		return nil, fmt.Errorf("create command count metric: %w", err)
	}
	observer.commandDuration, err = meter.Float64Histogram(
		metricCommandDuration,
		metric.WithDescription("Workerkit command dispatch duration in seconds."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("create command duration metric: %w", err)
	}
	observer.failureCount, err = meter.Int64Counter(
		metricFailureCount,
		metric.WithDescription("Workerkit runtime-observed worker and command failures."),
	)
	if err != nil {
		return nil, fmt.Errorf("create failure count metric: %w", err)
	}
	observer.readinessChangeCount, err = meter.Int64Counter(
		metricReadinessChangeCount,
		metric.WithDescription("Workerkit worker and runtime readiness changes."),
	)
	if err != nil {
		return nil, fmt.Errorf("create readiness change count metric: %w", err)
	}
	observer.lifecycleChangeCount, err = meter.Int64Counter(
		metricLifecycleChangeCount,
		metric.WithDescription("Workerkit worker and runtime lifecycle transitions."),
	)
	if err != nil {
		return nil, fmt.Errorf("create lifecycle change count metric: %w", err)
	}

	return observer, nil
}

// ObserveTransition implements workerkit.Observer.
func (o *Observer) ObserveTransition(ctx context.Context, event workerkit.TransitionEvent) {
	attrs := o.attrs(
		event.Runtime,
		event.Worker,
		"",
		attribute.String(attrLifecycleFrom, string(event.From)),
		attribute.String(attrLifecycleTo, string(event.To)),
	)

	trace.SpanFromContext(ctx).AddEvent(
		eventLifecycleChange,
		trace.WithTimestamp(event.At),
		trace.WithAttributes(attrs...),
	)
	if o.lifecycleChangeCount != nil {
		o.lifecycleChangeCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// StartCommand implements workerkit.Observer.
func (o *Observer) StartCommand(ctx context.Context, event workerkit.CommandStartEvent) (context.Context, workerkit.CommandObservation) {
	attrs := o.attrs(
		event.Runtime,
		event.Worker,
		event.Command,
	)

	ctx, span := o.tracer.Start(
		ctx,
		fmt.Sprintf("%s %s", spanCommandPrefix, event.Command),
		trace.WithTimestamp(event.StartedAt),
		trace.WithAttributes(attrs...),
	)
	return ctx, commandObservation{
		observer:   o,
		span:       span,
		attrs:      attrs,
		dispatchID: event.DispatchID,
	}
}

type commandObservation struct {
	observer   *Observer
	span       trace.Span
	attrs      []attribute.KeyValue
	dispatchID string
}

func (o commandObservation) End(ctx context.Context, event workerkit.CommandEndEvent) {
	metricAttrs := append(
		append([]attribute.KeyValue{}, o.attrs...),
		attribute.Bool(attrCommandSuccess, event.Success),
	)
	spanAttrs := append([]attribute.KeyValue{}, metricAttrs...)
	dispatchID := o.dispatchID
	if dispatchID == "" {
		dispatchID = event.DispatchID
	}
	if dispatchID != "" {
		spanAttrs = append(spanAttrs, attribute.String(attrDispatchID, dispatchID))
	}
	if event.Attempts > 0 {
		spanAttrs = append(spanAttrs, attribute.Int(attrCommandAttempts, event.Attempts))
	}

	if event.Success {
		o.span.SetStatus(codes.Ok, "")
	} else {
		if event.Err != nil {
			o.span.RecordError(event.Err, trace.WithTimestamp(event.EndedAt))
		} else if event.Message != "" {
			o.span.RecordError(errors.New(event.Message), trace.WithTimestamp(event.EndedAt))
		}
		o.span.SetStatus(codes.Error, event.Message)
	}
	o.span.SetAttributes(spanAttrs...)
	o.span.End(trace.WithTimestamp(event.EndedAt))

	if o.observer.commandCount != nil {
		o.observer.commandCount.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
	}
	if o.observer.commandDuration != nil {
		o.observer.commandDuration.Record(ctx, event.Duration.Seconds(), metric.WithAttributes(metricAttrs...))
	}
}

// ObserveFailure implements workerkit.Observer.
func (o *Observer) ObserveFailure(ctx context.Context, event workerkit.FailureEvent) {
	metricAttrs := o.attrs(
		event.Runtime,
		event.Worker,
		event.Command,
		attribute.Bool(attrFailurePanic, event.Panic),
	)
	eventAttrs := append([]attribute.KeyValue{}, metricAttrs...)
	if event.DispatchID != "" {
		eventAttrs = append(eventAttrs, attribute.String(attrDispatchID, event.DispatchID))
	}
	if event.Attempt > 0 {
		eventAttrs = append(eventAttrs, attribute.Int(attrCommandAttempt, event.Attempt))
	}

	span := trace.SpanFromContext(ctx)
	if event.Err != nil {
		span.RecordError(event.Err, trace.WithTimestamp(event.At))
	} else if event.Message != "" {
		span.RecordError(errors.New(event.Message), trace.WithTimestamp(event.At))
	}
	span.AddEvent(
		eventFailure,
		trace.WithTimestamp(event.At),
		trace.WithAttributes(eventAttrs...),
	)
	span.SetStatus(codes.Error, event.Message)

	if o.failureCount != nil {
		o.failureCount.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
	}
}

// ObserveReadiness implements workerkit.Observer.
func (o *Observer) ObserveReadiness(ctx context.Context, event workerkit.ReadinessEvent) {
	attrs := o.attrs(
		event.Runtime,
		event.Worker,
		"",
		attribute.Bool(attrReady, event.Ready),
	)

	trace.SpanFromContext(ctx).AddEvent(
		eventReadinessChange,
		trace.WithTimestamp(event.At),
		trace.WithAttributes(attrs...),
	)
	if o.readinessChangeCount != nil {
		o.readinessChangeCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

func (o *Observer) attrs(runtime string, worker string, command string, extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 1+len(o.config.attributes)+len(extra))
	attrs = append(attrs, attribute.String(attrRuntime, runtime))
	if worker != "" {
		attrs = append(attrs, attribute.String(attrWorker, worker))
	}
	if command != "" {
		attrs = append(attrs, attribute.String(attrCommand, command))
	}
	attrs = append(attrs, o.config.attributes...)
	return append(attrs, extra...)
}

var _ workerkit.Observer = (*Observer)(nil)
