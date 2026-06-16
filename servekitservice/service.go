package servekitservice

import (
	"context"
	"errors"
	"fmt"
	"time"

	opskit "github.com/jaredjakacky/opskit"
	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
	"github.com/jaredjakacky/workerkit/opshttp"
)

// stopFallbackTimeout is used only after the main service shutdown budget has
// already expired. It gives StopAll a short best-effort chance to release
// worker-owned resources. It is intentionally shorter than Workerkit's default
// worker stop timeout because the normal shutdown budget has already been
// consumed.
const stopFallbackTimeout = 5 * time.Second

type config struct {
	opsHTTPEnabled         bool
	opsHTTPOptions         []opshttp.Option
	opsRegistry            *opskit.Registry
	opsRegistrySet         bool
	opsOptions             []servekit.OpsOption
	servekitOptions        []servekit.Option
	startWorkers           bool
	gracefulWorkerShutdown bool
	shutdownTimeout        time.Duration
}

// Service owns the common Workerkit plus Servekit microservice lifecycle.
type Service struct {
	runtime *workerkit.Runtime
	server  *servekit.Server
	config  config
}

// Option configures Service construction and run behavior.
type Option func(*config)

// WithOpsHTTPEnabled controls whether Workerkit opshttp routes are mounted.
//
// Ops HTTP is disabled by default because even read-only operations routes
// expose runtime status, worker status, command inventory, and failure
// information. Enable it intentionally on a trusted operations plane, and use
// WithOpsHTTPOptions to apply the authentication, authorization, middleware,
// and endpoint policy required by the deployment.
//
// Mutating command dispatch and lifecycle routes remain opt-in through opshttp
// options such as opshttp.WithCommandDispatchEnabled and
// opshttp.WithAdminLifecycleControlsEnabled.
func WithOpsHTTPEnabled(enabled bool) Option {
	return func(cfg *config) {
		cfg.opsHTTPEnabled = enabled
	}
}

// WithOpsHTTPOptions appends options used when mounting Workerkit opshttp
// routes.
func WithOpsHTTPOptions(opts ...opshttp.Option) Option {
	return func(cfg *config) {
		cfg.opsHTTPOptions = append(cfg.opsHTTPOptions, opts...)
	}
}

// WithServekitOptions appends options used when NewManaged constructs the
// Servekit server. New rejects this option because the caller already supplied
// a server. When using NewManaged, pass Opskit registry composition through
// WithOpsRegistry instead of passing servekit.WithOps through this option.
func WithServekitOptions(opts ...servekit.Option) Option {
	return func(cfg *config) {
		cfg.servekitOptions = append(cfg.servekitOptions, opts...)
	}
}

// WithOpsRegistry configures the Opskit registry NewManaged passes to Servekit.
//
// NewManaged registers the Workerkit runtime into this registry as one required
// component. If this option is omitted, NewManaged creates a private registry as
// a convenience for services that only need Workerkit readiness. Applications
// composing multiple Kit Series components should provide their shared registry
// here.
//
// Do not pre-register the Workerkit runtime in this registry; NewManaged
// registers it as a required Opskit component.
func WithOpsRegistry(registry *opskit.Registry, opts ...servekit.OpsOption) Option {
	return func(cfg *config) {
		cfg.opsRegistry = registry
		cfg.opsRegistrySet = true
		cfg.opsOptions = append(cfg.opsOptions, opts...)
	}
}

// WithStartWorkers controls whether Run starts all registered workers before
// serving. Disabling startup does not disable worker shutdown. When graceful
// worker shutdown is enabled, Run may still drain, wait for idle, and stop
// workers on exit.
func WithStartWorkers(enabled bool) Option {
	return func(cfg *config) {
		cfg.startWorkers = enabled
	}
}

// WithGracefulWorkerShutdown controls whether Run drains, waits for idle, and
// stops workers after Servekit exits or after worker startup fails.
func WithGracefulWorkerShutdown(enabled bool) Option {
	return func(cfg *config) {
		cfg.gracefulWorkerShutdown = enabled
	}
}

// WithShutdownTimeout sets the outer service-level budget for graceful worker
// shutdown. The default is 20 seconds.
//
// This one budget covers DrainAllBestEffort, WaitAllIdle, and StopAll. If the
// service shutdown budget is smaller than a worker's configured stop timeout,
// it may cut that worker stop attempt short.
//
// This timeout is cooperative because Workerkit shutdown calls worker Stop
// methods with a context deadline. Workers that ignore ctx.Done() can still
// delay shutdown beyond this budget.
//
// A zero timeout keeps the default. A negative timeout explicitly disables this
// service-level timeout. Disabling the timeout can block indefinitely if workers
// or command handlers do not exit, which is dangerous during Kubernetes pod
// termination unless another outer supervisor enforces termination.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		if timeout == 0 {
			return
		}
		cfg.shutdownTimeout = timeout
	}
}

func defaultConfig() config {
	return config{
		startWorkers:           true,
		gracefulWorkerShutdown: true,
		// Keep the default outer shutdown budget below a common 30 second
		// Kubernetes termination grace period, leaving room for StopAll fallback
		// and process overhead.
		shutdownTimeout: 20 * time.Second,
	}
}

// NewManaged constructs a Workerkit service runner with a Servekit server.
//
// NewManaged wires Workerkit into Servekit's Opskit registry during server
// construction. It is a convenience constructor for services that want this
// package to construct the Servekit server. Applications still own composition;
// pass a shared Opskit registry with WithOpsRegistry when the service has other
// Opskit components. Use Service.Server to register application routes before
// calling Run.
func NewManaged(runtime *workerkit.Runtime, opts ...Option) (*Service, error) {
	if runtime == nil {
		return nil, opshttp.ErrNilRuntime
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	opsRegistry, err := managedOpsRegistry(runtime, cfg)
	if err != nil {
		return nil, err
	}
	serverOpts := append([]servekit.Option{}, cfg.servekitOptions...)
	serverOpts = append(serverOpts, servekit.WithOps(opsRegistry, cfg.opsOptions...))
	server := servekit.New(serverOpts...)

	return newWithConfig(runtime, server, cfg)
}

// New constructs a Workerkit service runner around an existing Servekit server.
//
// Ops HTTP is disabled by default. Enable it with WithOpsHTTPEnabled(true).
// When ops HTTP is enabled, read-only opshttp routes are mounted unless changed
// by WithOpsHTTPOptions. Mutating opshttp routes remain opt-in through options
// such as
// opshttp.WithCommandDispatchEnabled and
// opshttp.WithAdminLifecycleControlsEnabled.
//
// Servekit does not expose a public API for adding Opskit readiness after
// construction. Callers that want /readyz to include Workerkit readiness should
// register the runtime in a shared Opskit registry and construct the server
// with:
//
//	ops := opskit.NewRegistry()
//	ops.MustRegister(runtime, opskit.Required())
//	server := servekit.New(
//		servekit.WithOps(ops, servekit.WithOpsAdmin()),
//	)
//
// The standalone readiness adapter remains available for services that do not
// use an Opskit registry:
//
//	servekit.WithReadinessChecks(servekitservice.ReadinessCheck(runtime))
//
// Workerkit cannot verify that an externally constructed Servekit server has
// Workerkit readiness wired. Without this wiring, /readyz may report ready while
// the Workerkit runtime is unready.
//
// NewManaged is available when callers want this package to construct the
// Servekit server.
func New(runtime *workerkit.Runtime, server *servekit.Server, opts ...Option) (*Service, error) {
	if runtime == nil {
		return nil, opshttp.ErrNilRuntime
	}
	if server == nil {
		return nil, opshttp.ErrNilServer
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if len(cfg.servekitOptions) > 0 {
		return nil, errors.New("servekitservice: WithServekitOptions requires NewManaged")
	}

	return newWithConfig(runtime, server, cfg)
}

func managedOpsRegistry(runtime *workerkit.Runtime, cfg config) (*opskit.Registry, error) {
	registry := cfg.opsRegistry
	if cfg.opsRegistrySet {
		if registry == nil {
			return nil, errors.New("servekitservice: WithOpsRegistry requires non-nil registry")
		}
	} else {
		registry = opskit.NewRegistry()
	}
	if err := registry.Register(runtime, opskit.Required()); err != nil {
		return nil, fmt.Errorf("register workerkit runtime with opskit: %w", err)
	}
	return registry, nil
}

func newWithConfig(runtime *workerkit.Runtime, server *servekit.Server, cfg config) (*Service, error) {
	if cfg.opsHTTPEnabled {
		if err := opshttp.Mount(server, runtime, cfg.opsHTTPOptions...); err != nil {
			return nil, fmt.Errorf("mount workerkit ops http: %w", err)
		}
	}

	return &Service{
		runtime: runtime,
		server:  server,
		config:  cfg,
	}, nil
}

// Server returns the Servekit server owned by this service.
//
// Callers using NewManaged can use Server to register application routes before
// calling Run. Server returns nil when called on a nil Service.
func (s *Service) Server() *servekit.Server {
	if s == nil {
		return nil
	}
	return s.server
}

// ReadinessOptions returns the Servekit options needed to include Workerkit
// runtime readiness in /readyz through Opskit.
func ReadinessOptions(runtime *workerkit.Runtime, opts ...servekit.OpsOption) []servekit.Option {
	registry := opskit.NewRegistry()
	if runtime != nil {
		registry.MustRegister(runtime, opskit.Required())
	}

	return []servekit.Option{
		servekit.WithOps(registry, opts...),
	}
}

// ReadinessRegistry returns an Opskit registry with the Workerkit runtime
// registered as one required component.
func ReadinessRegistry(runtime *workerkit.Runtime) (*opskit.Registry, error) {
	if runtime == nil {
		return nil, opshttp.ErrNilRuntime
	}
	registry := opskit.NewRegistry()
	if err := registry.Register(runtime, opskit.Required()); err != nil {
		return nil, err
	}
	return registry, nil
}

// ReadinessCheck adapts Workerkit runtime readiness into a Servekit readiness
// check.
//
// Deprecated: register Runtime with Opskit and pass the registry to Servekit
// with servekit.WithOps instead.
func ReadinessCheck(runtime *workerkit.Runtime) servekit.ReadinessCheck {
	return opshttp.ReadinessCheck(runtime)
}

// Run starts workers, runs Servekit, and performs graceful worker shutdown when
// configured.
//
// When worker startup fails after some workers have started, Run attempts the
// configured graceful worker shutdown path before returning the startup error.
// This compensates for Runtime.StartAll's fail-fast, no-rollback semantics.
func (s *Service) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("workerkit service must not be nil")
	}
	if s.runtime == nil {
		return opshttp.ErrNilRuntime
	}
	if s.server == nil {
		return opshttp.ErrNilServer
	}

	manageWorkerShutdown := !s.config.startWorkers
	if s.config.startWorkers {
		if err := s.runtime.StartAll(ctx); err != nil {
			startErr := fmt.Errorf("start workerkit workers: %w", err)
			if !s.config.gracefulWorkerShutdown {
				return startErr
			}
			shutdownErr := s.shutdownWorkers(ctx)
			return errors.Join(
				startErr,
				shutdownErr,
			)
		}
		manageWorkerShutdown = true
	}

	runErr := s.server.Run(ctx)
	if s.config.gracefulWorkerShutdown && manageWorkerShutdown {
		if err := s.shutdownWorkers(ctx); err != nil {
			return errors.Join(runErr, err)
		}
	}
	return runErr
}

func (s *Service) shutdownWorkers(ctx context.Context) error {
	// Servekit often returns after ctx is canceled. Worker shutdown still needs a
	// usable context for drain, idle polling, and Stop calls, so keep context
	// values, detach cancellation, and apply the service shutdown timeout below.
	baseCtx := context.WithoutCancel(ctx)
	shutdownCtx := baseCtx
	if s.config.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(shutdownCtx, s.config.shutdownTimeout)
		defer cancel()
	}

	err := s.runtime.Shutdown(shutdownCtx)
	if err == nil || shutdownCtx.Err() == nil {
		return err
	}

	var stopCancel context.CancelFunc
	// Even if drain or idle wait consumed the shutdown budget, still give
	// StopAll a short chance to release worker-owned resources.
	stopCtx, stopCancel := context.WithTimeout(baseCtx, stopFallbackTimeout)
	defer stopCancel()
	if stopErr := s.runtime.StopAll(stopCtx); stopErr != nil {
		return errors.Join(err, fmt.Errorf("stop workerkit workers after shutdown timeout: %w", stopErr))
	}
	return err
}
