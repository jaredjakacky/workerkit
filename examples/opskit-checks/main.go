package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	opskit "github.com/jaredjakacky/opskit"
	workerkit "github.com/jaredjakacky/workerkit"
)

func main() {
	ctx := context.Background()
	database := newDependency("database")
	cache := newDependency("cache")

	runtime, err := workerkit.New(workerkit.Identity{Name: "opskit_checks"})
	if err != nil {
		log.Fatal(err)
	}

	// The database check controls Workerkit runtime readiness. The group worker
	// is observational, avoiding a second readiness gate for the same database.
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:   "database_check",
		Worker: workerkit.NewCheckLoop(database, checkOptions("database")...),
	}); err != nil {
		log.Fatal(err)
	}
	if err := runtime.Register(workerkit.WorkerSpec{
		Name: "dependency_group",
		Worker: workerkit.NewCheckGroupLoop(
			dependencyGroup{database: database, cache: cache},
			groupOptions()...,
		),
	}, workerkit.WithWorkerReadinessContribution(false)); err != nil {
		log.Fatal(err)
	}

	// Opskit remains a passive inventory/read-model registry. Components are
	// informational here because Workerkit runtime readiness is the chosen gate.
	registry := opskit.NewRegistry()
	registry.MustRegister(database, opskit.Informational())
	registry.MustRegister(cache, opskit.Informational())
	registry.MustRegister(runtime, opskit.Required())

	if err := runtime.StartAll(ctx); err != nil {
		log.Fatal(err)
	}
	if err := waitForInitialChecks(ctx, runtime, cache, time.Second); err != nil {
		log.Fatal(err)
	}

	readiness := registry.Readiness(ctx)
	fmt.Printf("registry_ready=%t reason=%q\n", readiness.Ready, readiness.Reason)
	for _, component := range registry.Components() {
		info := component.ComponentInfo()
		fmt.Printf("component=%s kind=%s state=%s\n", info.Name, info.Kind, component.Status(ctx).State)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

func waitForInitialChecks(ctx context.Context, runtime *workerkit.Runtime, cache *dependency, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if runtime.RuntimeStatus().Ready && cache.Status(ctx).State == opskit.StateReady {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for initial checks: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func checkOptions(name string) []workerkit.CheckLoopOption {
	return []workerkit.CheckLoopOption{
		workerkit.WithCheckInterval(time.Minute),
		workerkit.WithCheckTimeout(time.Second),
		workerkit.WithCheckResultObserver(func(_ context.Context, result opskit.CheckResult) {
			fmt.Printf("check=%s state=%s ready=%t\n", name, result.State, result.Ready)
		}),
	}
}

func groupOptions() []workerkit.CheckLoopOption {
	return []workerkit.CheckLoopOption{
		workerkit.WithCheckInterval(time.Minute),
		workerkit.WithCheckTimeout(time.Second),
		workerkit.WithCheckSummaryObserver(func(_ context.Context, summary opskit.CheckSummary) {
			fmt.Printf("group=dependencies state=%s ready=%t results=%d\n",
				summary.State, summary.Ready, len(summary.Results))
		}),
	}
}

type dependency struct {
	mu     sync.RWMutex
	name   string
	status opskit.Status
}

func newDependency(name string) *dependency {
	return &dependency{name: name, status: opskit.UnknownStatus("not checked")}
}

func (d *dependency) ComponentInfo() opskit.ComponentInfo {
	return opskit.ComponentInfo{Name: d.name, Kind: "dependency"}
}

func (d *dependency) Status(context.Context) opskit.Status {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.status
}

func (d *dependency) Check(context.Context) opskit.CheckResult {
	startedAt := time.Now()
	result := opskit.ReadyCheck(d.name+" reachable", time.Since(startedAt))
	d.mu.Lock()
	d.status = opskit.ReadyStatus(result.Message)
	d.mu.Unlock()
	return result
}

type dependencyGroup struct {
	database *dependency
	cache    *dependency
}

func (g dependencyGroup) CheckAll(ctx context.Context) opskit.CheckSummary {
	startedAt := time.Now()
	return opskit.SummarizeChecks("dependency checks complete", startedAt, []opskit.NamedCheck{
		{Name: "database", Kind: "dependency", Result: g.database.Check(ctx)},
		{Name: "cache", Kind: "dependency", Result: g.cache.Check(ctx)},
	})
}
