package opshttp

import (
	"errors"
	"strings"
	"time"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
)

// DefaultPrefix is the route prefix used by Mount when WithPrefix is not
// supplied.
const DefaultPrefix = "/admin"

const (
	runtimeStartRoute    = "runtime/start"
	runtimeDrainRoute    = "runtime/drain"
	runtimeStopRoute     = "runtime/stop"
	workerStartRoute     = "workers/start"
	workerDrainRoute     = "workers/drain"
	workerStopRoute      = "workers/stop"
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
// Workerkit control route.
//
// Use this for policy that should apply to command dispatch and lifecycle
// controls alike, such as authentication, endpoint middleware, response
// encoding, body limits, or timeouts.
func WithEndpointOptions(opts ...servekit.EndpointOption) Option {
	return func(cfg *config) {
		cfg.endpointOptions = append(cfg.endpointOptions, opts...)
	}
}

// WithDispatchOptions appends Servekit endpoint options only to command
// dispatch routes.
//
// Dispatch routes can mutate worker state or trigger domain work, so callers
// should protect them with appropriate authentication, authorization, and
// audit policy.
func WithDispatchOptions(opts ...servekit.EndpointOption) Option {
	return func(cfg *config) {
		cfg.dispatchOptions = append(cfg.dispatchOptions, opts...)
	}
}

// WithLifecycleOptions appends Servekit endpoint options only to lifecycle
// control routes.
//
// Lifecycle controls mutate worker state, so callers should protect them with
// appropriate authentication, authorization, and audit policy.
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
// deadline includes time waiting for another lifecycle operation and is
// cooperative once worker code is running: Workerkit cannot interrupt worker
// code that ignores ctx.Done(). A zero timeout keeps the default, and a
// negative timeout explicitly disables the opshttp lifecycle timeout.
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

// Mount adds explicitly enabled Workerkit control routes to an existing
// Servekit server.
//
// Servekit owns HTTP service construction, middleware, readiness endpoints,
// authentication, and lifecycle. Pass WithCommandDispatchEnabled to mount the
// mutating command dispatch route. Pass
// WithAdminLifecycleControlsEnabled to mount privileged lifecycle control
// routes. Mount exposes no routes unless at least one control group is enabled.
//
// Register Runtime with Opskit and pass that registry to Servekit with
// servekit.WithOps(...) for readiness, status, inspection, and command
// inventory. This package is only for active Workerkit-specific HTTP controls.
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

	if cfg.commandDispatchEnabled {
		registerCommandRoutes(server, runtime, cfg)
	}
	if cfg.adminLifecycleControls {
		registerLifecycleRoutes(server, runtime, cfg)
	}
	return nil
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
