package slogobserver

import (
	"context"
	"log/slog"

	workerkit "github.com/jaredjakacky/workerkit-incubator"
)

type config struct {
	level slog.Level
	attrs []slog.Attr
}

// Option configures the slog observer adapter.
type Option func(*config)

// WithLevel sets the slog level for routine Workerkit observer logs. Failure
// logs are always emitted at slog.LevelError.
func WithLevel(level slog.Level) Option {
	return func(cfg *config) {
		cfg.level = level
	}
}

// WithAttributes appends attributes to every log record emitted by the
// observer.
func WithAttributes(attrs ...slog.Attr) Option {
	return func(cfg *config) {
		cfg.attrs = append(cfg.attrs, attrs...)
	}
}

// Observer converts Workerkit runtime telemetry events into structured slog
// records.
type Observer struct {
	logger *slog.Logger
	config config
}

// New constructs a slog-backed Workerkit observer.
//
// When logger is nil, New uses slog.Default().
func New(logger *slog.Logger, opts ...Option) *Observer {
	cfg := config{
		level: slog.LevelInfo,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Observer{
		logger: logger,
		config: cfg,
	}
}

// ObserveTransition implements workerkit.Observer.
func (o *Observer) ObserveTransition(ctx context.Context, event workerkit.TransitionEvent) {
	o.logger.LogAttrs(ctx, o.config.level, "workerkit lifecycle transition", o.attrs(
		event.Runtime,
		event.Worker,
		"",
		slog.String("lifecycle_from", string(event.From)),
		slog.String("lifecycle_to", string(event.To)),
	)...)
}

// StartCommand implements workerkit.Observer.
func (o *Observer) StartCommand(ctx context.Context, event workerkit.CommandStartEvent) (context.Context, workerkit.CommandObservation) {
	return ctx, commandObservation{
		observer:   o,
		runtime:    event.Runtime,
		worker:     event.Worker,
		command:    event.Command,
		dispatchID: event.DispatchID,
	}
}

type commandObservation struct {
	observer   *Observer
	runtime    string
	worker     string
	command    string
	dispatchID string
}

func (o commandObservation) End(ctx context.Context, event workerkit.CommandEndEvent) {
	attrs := o.observer.attrs(
		o.runtime,
		o.worker,
		o.command,
		slog.Bool("success", event.Success),
		slog.Duration("duration", event.Duration),
	)
	dispatchID := o.dispatchID
	if dispatchID == "" {
		dispatchID = event.DispatchID
	}
	if dispatchID != "" {
		attrs = append(attrs, slog.String("dispatch_id", dispatchID))
	}
	if event.Attempts > 0 {
		attrs = append(attrs, slog.Int("attempts", event.Attempts))
	}
	if event.Err != nil {
		attrs = append(attrs, slog.Any("error", event.Err))
	} else if event.Message != "" {
		attrs = append(attrs, slog.String("error", event.Message))
	}

	o.observer.logger.LogAttrs(ctx, o.observer.config.level, "workerkit command dispatch completed", attrs...)
}

// ObserveFailure implements workerkit.Observer.
func (o *Observer) ObserveFailure(ctx context.Context, event workerkit.FailureEvent) {
	attrs := o.attrs(
		event.Runtime,
		event.Worker,
		event.Command,
		slog.Bool("panic", event.Panic),
	)
	if event.DispatchID != "" {
		attrs = append(attrs, slog.String("dispatch_id", event.DispatchID))
	}
	if event.Attempt > 0 {
		attrs = append(attrs, slog.Int("attempt", event.Attempt))
	}
	if event.Err != nil {
		attrs = append(attrs, slog.Any("error", event.Err))
	} else if event.Message != "" {
		attrs = append(attrs, slog.String("error", event.Message))
	}

	o.logger.LogAttrs(ctx, slog.LevelError, "workerkit failure", attrs...)
}

// ObserveReadiness implements workerkit.Observer.
func (o *Observer) ObserveReadiness(ctx context.Context, event workerkit.ReadinessEvent) {
	o.logger.LogAttrs(ctx, o.config.level, "workerkit readiness change", o.attrs(
		event.Runtime,
		event.Worker,
		"",
		slog.Bool("ready", event.Ready),
	)...)
}

func (o *Observer) attrs(runtime string, worker string, command string, extra ...slog.Attr) []slog.Attr {
	attrs := make([]slog.Attr, 0, 3+len(o.config.attrs)+len(extra))
	attrs = append(attrs, slog.String("runtime", runtime))
	if worker != "" {
		attrs = append(attrs, slog.String("worker", worker))
	}
	if command != "" {
		attrs = append(attrs, slog.String("command", command))
	}
	attrs = append(attrs, o.config.attrs...)
	return append(attrs, extra...)
}

var _ workerkit.Observer = (*Observer)(nil)
