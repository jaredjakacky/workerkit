package servekitservice

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
)

var (
	// ErrNilRuntime reports that Service was constructed or run without a
	// Workerkit runtime.
	ErrNilRuntime = errors.New("worker runtime must not be nil")
	// ErrNilServer reports that Service was constructed or run without a
	// Servekit server.
	ErrNilServer = errors.New("servekit server must not be nil")
)

// stopFallbackTimeout is used only after the main service shutdown budget has
// already expired. It gives StopAll a short best-effort chance to release
// worker-owned resources. It is intentionally shorter than Workerkit's default
// worker stop timeout because the normal shutdown budget has already been
// consumed.
const stopFallbackTimeout = 5 * time.Second

type config struct {
	startWorkers           bool
	gracefulWorkerShutdown bool
	shutdownTimeout        time.Duration
}

// Service coordinates Workerkit lifecycle around an application-owned Servekit
// server.
type Service struct {
	runtime *workerkit.Runtime
	server  *servekit.Server
	config  config
}

// Option configures Service construction and run behavior.
type Option func(*config)

// WithStartWorkers controls whether Run starts all registered workers before
// serving. Disabling startup does not disable worker shutdown. When graceful
// worker shutdown is enabled, Run may still drain, wait for idle, and stop
// workers on exit.
func WithStartWorkers(enabled bool) Option {
	return func(cfg *config) {
		cfg.startWorkers = enabled
	}
}

// WithGracefulWorkerShutdown controls whether Run coordinates Servekit and
// Workerkit shutdown or cleans up workers after worker startup fails.
func WithGracefulWorkerShutdown(enabled bool) Option {
	return func(cfg *config) {
		cfg.gracefulWorkerShutdown = enabled
	}
}

// WithShutdownTimeout sets the outer service-level budget for coordinated
// Servekit and Workerkit shutdown. The default is 20 seconds.
//
// The budget begins when Servekit starts graceful shutdown and covers its drain
// delay and HTTP shutdown followed by Workerkit DrainAllBestEffort, WaitAllIdle,
// and StopAll. Servekit's configured shutdown timeout remains an inner HTTP
// shutdown cap. If the remaining service shutdown budget is smaller than a
// worker's configured stop timeout, it may cut that worker stop attempt short.
// If the shared budget expires, Run may give StopAll one additional five-second
// best-effort fallback window to release worker-owned resources.
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
		// Keep the coordinated shutdown budget below a common 30 second
		// Kubernetes termination grace period, leaving room for the StopAll
		// fallback and process overhead.
		shutdownTimeout: 20 * time.Second,
	}
}

// New constructs a lifecycle coordinator around an existing Servekit server.
//
// Applications own composition. Register the runtime in the shared Opskit
// registry and construct Servekit before calling New:
//
//	ops := opskit.NewRegistry()
//	ops.MustRegister(runtime, opskit.Required())
//	server := servekit.New(
//		servekit.WithOps(ops, servekit.WithOpsAdmin()),
//	)
//
// This package coordinates startup and shutdown only. It does not construct the
// server, create an Opskit registry, register components, or mount HTTP routes.
func New(runtime *workerkit.Runtime, server *servekit.Server, opts ...Option) (*Service, error) {
	if runtime == nil {
		return nil, ErrNilRuntime
	}
	if server == nil {
		return nil, ErrNilServer
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return newWithConfig(runtime, server, cfg)
}

func newWithConfig(runtime *workerkit.Runtime, server *servekit.Server, cfg config) (*Service, error) {
	return &Service{
		runtime: runtime,
		server:  server,
		config:  cfg,
	}, nil
}

// Server returns the application-owned Servekit server passed to New.
//
// Server returns nil when called on a nil Service.
func (s *Service) Server() *servekit.Server {
	if s == nil {
		return nil
	}
	return s.server
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
		return ErrNilRuntime
	}
	if s.server == nil {
		return ErrNilServer
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

	if !s.config.gracefulWorkerShutdown || !manageWorkerShutdown {
		return s.server.Run(ctx)
	}

	baseCtx := context.WithoutCancel(ctx)
	var shutdownCtx context.Context
	var shutdownCancel context.CancelFunc
	runErr := s.server.RunWithShutdownContext(ctx, func() context.Context {
		shutdownCtx, shutdownCancel = s.newShutdownContext(baseCtx)
		return shutdownCtx
	})
	if shutdownCtx == nil {
		// Servekit returned because listening or serving failed before graceful
		// shutdown began. Give Workerkit a fresh cleanup budget on that path.
		shutdownCtx, shutdownCancel = s.newShutdownContext(baseCtx)
	}
	defer shutdownCancel()
	if err := s.shutdownWorkersWithContext(baseCtx, shutdownCtx); err != nil {
		return errors.Join(runErr, err)
	}
	return runErr
}

func (s *Service) shutdownWorkers(ctx context.Context) error {
	// Startup may fail because ctx was canceled. Worker cleanup still needs a
	// usable context for drain, idle polling, and Stop calls, so keep context
	// values, detach cancellation, and apply a fresh shutdown timeout.
	baseCtx := context.WithoutCancel(ctx)
	shutdownCtx, cancel := s.newShutdownContext(baseCtx)
	defer cancel()
	return s.shutdownWorkersWithContext(baseCtx, shutdownCtx)
}

func (s *Service) newShutdownContext(baseCtx context.Context) (context.Context, context.CancelFunc) {
	if s.config.shutdownTimeout > 0 {
		return context.WithTimeout(baseCtx, s.config.shutdownTimeout)
	}
	return baseCtx, func() {}
}

func (s *Service) shutdownWorkersWithContext(baseCtx, shutdownCtx context.Context) error {
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
