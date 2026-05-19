package opshttp

import (
	"context"
	"errors"
	"net/http"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit-incubator"
)

type workerLifecycleRequest struct {
	Name string `json:"name"`
}

// registerLifecycleRoutes mounts opt-in mutating lifecycle controls.
//
// Runtime drain uses Runtime.DrainAll semantics: it drains workers
// sequentially and is not an atomic runtime-wide command admission cutoff.
// Runtime start uses Runtime.StartAll semantics: it is fail-fast and does not
// roll back workers already started by the same request.
func registerLifecycleRoutes(server *servekit.Server, runtime *workerkit.Runtime, cfg config) {
	opts := lifecycleEndpointOptions(cfg)

	registerWorkerLifecycleRoute(server, runtime, cfg, workerStartRoute, runtime.Start, opts)
	registerWorkerLifecycleRoute(server, runtime, cfg, workerDrainRoute, runtime.Drain, opts)
	registerWorkerLifecycleRoute(server, runtime, cfg, workerStopRoute, runtime.Stop, opts)
	registerRuntimeLifecycleRoute(server, runtime, cfg, runtimeStartRoute, runtime.StartAll, opts)
	registerRuntimeLifecycleRoute(server, runtime, cfg, runtimeDrainRoute, runtime.DrainAll, opts)
	registerRuntimeLifecycleRoute(server, runtime, cfg, runtimeStopRoute, runtime.StopAll, opts)
}

func registerWorkerLifecycleRoute(server *servekit.Server, runtime *workerkit.Runtime, cfg config, route string, action func(context.Context, string) error, opts []servekit.EndpointOption) {
	server.Handle(http.MethodPost, routePath(cfg.prefix, route), func(r *http.Request) (any, error) {
		name, err := decodeWorkerLifecycleRequest(r)
		if err != nil {
			return nil, err
		}
		ctx, cancel := lifecycleOperationContext(r, cfg)
		defer cancel()
		if err := action(ctx, name); err != nil {
			return nil, mapLifecycleError("worker", name, err)
		}
		return workerLifecycleResponse(runtime, name)
	}, opts...)
}

func registerRuntimeLifecycleRoute(server *servekit.Server, runtime *workerkit.Runtime, cfg config, route string, action func(context.Context) error, opts []servekit.EndpointOption) {
	server.Handle(http.MethodPost, routePath(cfg.prefix, route), func(r *http.Request) (any, error) {
		ctx, cancel := lifecycleOperationContext(r, cfg)
		defer cancel()
		if err := action(ctx); err != nil {
			return nil, mapLifecycleError("runtime", runtime.Identity().Name, err)
		}
		return runtimeLifecycleResponse(runtime), nil
	}, opts...)
}

func lifecycleOperationContext(r *http.Request, cfg config) (context.Context, context.CancelFunc) {
	baseCtx := context.WithoutCancel(r.Context())
	if cfg.lifecycleTimeout <= 0 {
		return baseCtx, func() {}
	}
	return context.WithTimeout(baseCtx, cfg.lifecycleTimeout)
}

func decodeWorkerLifecycleRequest(r *http.Request) (string, error) {
	var req workerLifecycleRequest
	if err := decodeStrictJSON(r, &req, "lifecycle request"); err != nil {
		return "", err
	}

	if req.Name == "" {
		return "", badRequestError(`missing required JSON field "name"`)
	}
	return req.Name, nil
}

func workerLifecycleResponse(runtime *workerkit.Runtime, name string) (workerkit.WorkerSnapshot, error) {
	worker, ok := runtime.Worker(name)
	if !ok {
		return workerkit.WorkerSnapshot{}, notFoundError("worker", name)
	}
	return worker, nil
}

func runtimeLifecycleResponse(runtime *workerkit.Runtime) runtimeDescriptor {
	return runtimeDescriptor{
		Identity: runtime.Identity(),
		Status:   runtime.Status(),
	}
}

func mapLifecycleError(kind, name string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, workerkit.ErrWorkerNotFound):
		return notFoundError(kind, name)
	case errors.Is(err, workerkit.ErrInvalidWorkerState):
		return servekit.Error(http.StatusConflict, err.Error(), err)
	default:
		return err
	}
}
