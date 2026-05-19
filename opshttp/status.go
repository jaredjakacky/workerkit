package opshttp

import (
	"net/http"

	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit-incubator"
)

type runtimeDescriptor struct {
	Identity workerkit.Identity      `json:"identity"`
	Status   workerkit.RuntimeStatus `json:"status"`
}

func registerStatusRoutes(server *servekit.Server, runtime *workerkit.Runtime, cfg config) {
	server.Handle(http.MethodGet, routePath(cfg.prefix, runtimeRoute), func(r *http.Request) (any, error) {
		return runtimeDescriptor{
			Identity: runtime.Identity(),
			Status:   runtime.Status(),
		}, nil
	}, cfg.endpointOptions...)

	server.Handle(http.MethodGet, routePath(cfg.prefix, workersRoute), func(r *http.Request) (any, error) {
		return runtime.Workers(), nil
	}, cfg.endpointOptions...)

	server.Handle(http.MethodGet, routePath(cfg.prefix, workerRoute), func(r *http.Request) (any, error) {
		name, err := requiredQueryValue(r, "name")
		if err != nil {
			return nil, err
		}

		worker, ok := runtime.Worker(name)
		if !ok {
			return nil, notFoundError("worker", name)
		}
		return worker, nil
	}, cfg.endpointOptions...)

	server.Handle(http.MethodGet, routePath(cfg.prefix, commandsRoute), func(r *http.Request) (any, error) {
		workerName, err := requiredQueryValue(r, "worker")
		if err != nil {
			return nil, err
		}

		commands, ok := runtime.Commands(workerName)
		if !ok {
			return nil, notFoundError("worker", workerName)
		}
		return commands, nil
	}, cfg.endpointOptions...)
}
