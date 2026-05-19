package opshttp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
)

// DefaultPrefix is the route prefix used by Mount when WithPrefix is not
// supplied.
const DefaultPrefix = "/admin"

const (
	runtimeRoute         = "runtime"
	runtimeStartRoute    = "runtime/start"
	runtimeDrainRoute    = "runtime/drain"
	runtimeStopRoute     = "runtime/stop"
	workersRoute         = "workers"
	workerStartRoute     = "workers/start"
	workerDrainRoute     = "workers/drain"
	workerStopRoute      = "workers/stop"
	workerRoute          = "worker"
	commandsRoute        = "commands"
	commandDispatchRoute = "commands/dispatch"
)

var (
	// ErrNilRuntime reports that the caller provided a nil worker runtime.
	ErrNilRuntime = errors.New("worker runtime must not be nil")
	// ErrNilServer reports that the caller provided a nil servekit server.
	ErrNilServer = errors.New("servekit server must not be nil")
)

type config struct {
	prefix                 string
	endpointOptions        []servekit.EndpointOption
	dispatchOptions        []servekit.EndpointOption
	lifecycleOptions       []servekit.EndpointOption
	lifecycleTimeout       time.Duration
	commandDispatchEnabled bool
	adminLifecycleControls bool
}

// Option configures the Workerkit operations routes mounted into Servekit.
type Option func(*config)

func defaultConfig() config {
	return config{
		prefix:           DefaultPrefix,
		lifecycleTimeout: 30 * time.Second,
	}
}

// WithPrefix overrides the operations route prefix.
//
// Empty input uses the root path. A missing leading "/" is added and a trailing
// "/" is removed unless the prefix is root.
func WithPrefix(prefix string) Option {
	return func(cfg *config) {
		cfg.prefix = normalizePrefix(prefix)
	}
}

// WithEndpointOptions appends Servekit endpoint options to every mounted
// Workerkit operations route.
//
// Use this for policy that should apply to status, inspection, command
// discovery, command dispatch, and lifecycle control routes alike, such as
// authentication, endpoint middleware, response encoding, body limits, or
// timeouts.
func WithEndpointOptions(opts ...servekit.EndpointOption) Option {
	return func(cfg *config) {
		cfg.endpointOptions = append(cfg.endpointOptions, opts...)
	}
}

// WithDispatchOptions appends Servekit endpoint options only to command
// dispatch routes.
//
// Dispatch routes can mutate worker state or trigger domain work, so callers
// often protect them more strictly than read-only inspection routes.
func WithDispatchOptions(opts ...servekit.EndpointOption) Option {
	return func(cfg *config) {
		cfg.dispatchOptions = append(cfg.dispatchOptions, opts...)
	}
}

// WithLifecycleOptions appends Servekit endpoint options only to lifecycle
// control routes.
//
// Lifecycle controls mutate worker state, so callers often protect them more
// strictly than read-only inspection routes.
func WithLifecycleOptions(opts ...servekit.EndpointOption) Option {
	return func(cfg *config) {
		cfg.lifecycleOptions = append(cfg.lifecycleOptions, opts...)
	}
}

// WithLifecycleTimeout sets the timeout for lifecycle control route operations.
// The default is 30 seconds.
//
// Lifecycle mutations are detached from HTTP client disconnect cancellation so
// a dropped connection does not necessarily abort Start, Drain, or Stop. This
// timeout adds a deadline to the context passed to the lifecycle operation. The
// deadline is cooperative: Workerkit cannot interrupt worker code that ignores
// ctx.Done(). A zero timeout keeps the default, and a negative timeout
// explicitly disables the opshttp lifecycle timeout.
func WithLifecycleTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		if timeout == 0 {
			return
		}
		cfg.lifecycleTimeout = timeout
	}
}

// WithCommandDispatchEnabled mounts the mutating command dispatch route.
//
// Command dispatch can trigger domain work or mutate worker state, so Mount
// does not expose it by default.
func WithCommandDispatchEnabled() Option {
	return func(cfg *config) {
		cfg.commandDispatchEnabled = true
	}
}

// WithAdminLifecycleControlsEnabled mounts privileged worker and runtime
// lifecycle mutation routes.
//
// These routes can start, drain, and stop workers through HTTP. They should be
// exposed only on trusted operations planes and protected with authentication,
// authorization, and audit middleware appropriate for the deployment.
func WithAdminLifecycleControlsEnabled() Option {
	return func(cfg *config) {
		cfg.adminLifecycleControls = true
	}
}

// Mount adds Workerkit operations routes to an existing Servekit server.
//
// Servekit owns HTTP service construction, middleware, readiness endpoints,
// authentication, and lifecycle. Mount adds Workerkit's runtime status, worker
// inspection, and command discovery routes. Pass WithCommandDispatchEnabled to
// mount the mutating command dispatch route. Pass
// WithAdminLifecycleControlsEnabled to mount privileged lifecycle control
// routes. Pass
// ReadinessCheck(runtime) to servekit.New when the service should report
// Workerkit readiness through Servekit's /readyz endpoint.
func Mount(server *servekit.Server, runtime *workerkit.Runtime, opts ...Option) error {
	if server == nil {
		return ErrNilServer
	}
	if runtime == nil {
		return ErrNilRuntime
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	registerStatusRoutes(server, runtime, cfg)
	if cfg.commandDispatchEnabled {
		registerCommandRoutes(server, runtime, cfg)
	}
	if cfg.adminLifecycleControls {
		registerLifecycleRoutes(server, runtime, cfg)
	}
	return nil
}

// ReadinessCheck adapts Runtime.Status readiness into a Servekit readiness
// check.
func ReadinessCheck(runtime *workerkit.Runtime) servekit.ReadinessCheck {
	return func(_ context.Context) error {
		if runtime == nil {
			return ErrNilRuntime
		}
		status := runtime.Status()
		if !status.Ready {
			return fmt.Errorf("worker runtime not ready: state=%s", status.State)
		}
		return nil
	}
}

func normalizePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || prefix == "/" {
		return "/"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return strings.TrimRight(prefix, "/")
}

func routePath(prefix, suffix string) string {
	if prefix == "/" {
		if suffix == "" {
			return "/"
		}
		return "/" + strings.TrimLeft(suffix, "/")
	}
	if suffix == "" {
		return prefix
	}
	return prefix + "/" + strings.TrimLeft(suffix, "/")
}

func dispatchEndpointOptions(cfg config) []servekit.EndpointOption {
	opts := append([]servekit.EndpointOption{}, cfg.endpointOptions...)
	return append(opts, cfg.dispatchOptions...)
}

func lifecycleEndpointOptions(cfg config) []servekit.EndpointOption {
	opts := append([]servekit.EndpointOption{}, cfg.endpointOptions...)
	return append(opts, cfg.lifecycleOptions...)
}
