package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	opskit "github.com/jaredjakacky/opskit"
	"github.com/jaredjakacky/servekit"
	workerkit "github.com/jaredjakacky/workerkit"
	"github.com/jaredjakacky/workerkit/opshttp"
	"github.com/jaredjakacky/workerkit/retry"
	"github.com/jaredjakacky/workerkit/servekitservice"
	"github.com/jaredjakacky/workerkit/slogobserver"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	state := newServiceState()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// This example demonstrates the Kit Series composition: Workerkit owns worker
	// lifecycle, readiness, commands, retry, concurrency, and failure policy;
	// Servekit owns HTTP serving, readiness endpoints, route policy, and shutdown.
	runtime, err := workerkit.New(workerkit.Identity{Name: "catalog_service"},
		workerkit.WithRuntimeCommandConcurrency(8),
		workerkit.WithDefaultWorkerCommandConcurrency(2),
		workerkit.WithDefaultFailurePolicy(workerkit.FailurePolicyMarkRuntimeUnready),
		workerkit.WithObserver(slogobserver.New(logger,
			slogobserver.WithAttributes(slog.String("service", "catalog-service")),
		)),
	)
	if err != nil {
		log.Fatal(err)
	}

	commandRetry := retry.AttemptsIf(
		3,
		retry.Exponential(100*time.Millisecond, 2, time.Second),
		retry.Full(),
		isTemporary,
	)

	registerIngest(runtime, state)
	registerIndex(runtime, state, commandRetry)
	registerMaintenance(runtime, state)

	// Workerkit speaks Opskit directly. Register the runtime as one required
	// Opskit component so Servekit can use the generic registry for /readyz and
	// read-only admin component inspection.
	ops := opskit.NewRegistry()
	ops.MustRegister(runtime, opskit.Required())

	opsPolicy := []servekit.EndpointOption{
		servekit.WithAuthGate(requireOpsToken),
		servekit.WithEndpointMiddleware(auditOpsRequest(logger)),
		servekit.WithEndpointTimeout(10 * time.Second),
	}
	mutatingOpsPolicy := []servekit.EndpointOption{
		servekit.WithBodyLimit(1 << 20),
	}

	server := servekit.New(
		servekit.WithAddr(":8080"),
		servekit.WithBuildInfo("dev", "local", time.Now().UTC().Format(time.RFC3339)),
		servekit.WithOps(ops,
			servekit.WithOpsAdmin(),
			servekit.WithOpsAdminAuthGate(requireOpsToken),
		),
	)

	// Opskit is the primary read-only integration path. This example opts into
	// command dispatch, but leaves privileged lifecycle controls disabled.
	opsOptions := []opshttp.Option{
		// Apply the shared operations policy to every Workerkit-specific route.
		opshttp.WithEndpointOptions(opsPolicy...),
		opshttp.WithCommandDispatchEnabled(),
		// Command dispatch is intentionally opt-in and should be protected with real
		// authentication, authorization, and audit logging in production.
		opshttp.WithDispatchOptions(mutatingOpsPolicy...),
	}

	service, err := servekitservice.New(runtime, server,
		servekitservice.WithOpsHTTPEnabled(true),
		servekitservice.WithOpsHTTPOptions(opsOptions...),
		servekitservice.WithShutdownTimeout(20*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	server.Handle(http.MethodGet, "/app/status", func(r *http.Request) (any, error) {
		status := runtime.RuntimeStatus()
		snapshot := state.snapshot()
		return map[string]any{
			"service":      "catalog-service",
			"runtimeState": status.State,
			"runtimeReady": status.Ready,
			"documents":    snapshot.documents,
			"indexVersion": snapshot.indexVersion,
			"maintenance":  snapshot.maintenanceRuns,
			"runtimeName":  status.Name,
		}, nil
	})
	server.Handle(http.MethodGet, "/app/search", func(r *http.Request) (any, error) {
		query := r.URL.Query().Get("q")
		snapshot := state.snapshot()
		return map[string]any{
			"query":        query,
			"documents":    snapshot.documents,
			"indexVersion": snapshot.indexVersion,
		}, nil
	})

	printCurlCommands()

	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func registerIngest(runtime *workerkit.Runtime, state *serviceState) {
	worker := &appWorker{name: "ingest", warmup: 100 * time.Millisecond}
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "ingest",
		Description: "Accepts incoming catalog documents.",
		Worker:      worker,
	},
		workerkit.WithWorkerCommandConcurrency(4),
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "ingest/enqueue",
			Description: "Queue one catalog document for indexing.",
			Handler: workerkit.CommandHandlerFunc(func(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
				var input struct {
					DocumentID string `json:"documentID"`
					Title      string `json:"title"`
				}
				if err := json.Unmarshal(req.Payload, &input); err != nil {
					return workerkit.CommandResult{}, fmt.Errorf("decode ingest payload: %w", err)
				}
				if input.DocumentID == "" {
					return workerkit.CommandResult{}, errors.New("documentID is required")
				}
				state.addDocument(input.DocumentID)
				return workerkit.CommandResult{
					Message: "document enqueued",
					Payload: mustJSON(map[string]string{
						"documentID": input.DocumentID,
						"title":      input.Title,
					}),
				}, nil
			}),
		}),
	); err != nil {
		log.Fatal(err)
	}
}

func registerIndex(runtime *workerkit.Runtime, state *serviceState, policy retry.Policy) {
	worker := &indexWorker{
		appWorker: appWorker{name: "index", warmup: 250 * time.Millisecond},
		state:     state,
	}
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "index",
		Description: "Builds searchable catalog index entries.",
		Worker:      worker,
	},
		workerkit.WithWorkerCommandConcurrency(1),
		workerkit.WithWorkerCommandRetry(policy),
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "index/rebuild",
			Description: "Rebuild the catalog index with bounded retry for transient failures.",
			Handler:     workerkit.CommandHandlerFunc(worker.rebuild),
		}),
	); err != nil {
		log.Fatal(err)
	}
}

func registerMaintenance(runtime *workerkit.Runtime, state *serviceState) {
	worker := &maintenanceWorker{
		appWorker: appWorker{name: "maintenance", warmup: 50 * time.Millisecond},
		state:     state,
	}
	if err := runtime.Register(workerkit.WorkerSpec{
		Name:        "maintenance",
		Description: "Runs catalog service housekeeping.",
		Worker:      worker,
	},
		workerkit.WithWorkerReadinessContribution(false),
		workerkit.WithWorkerFailurePolicy(workerkit.FailurePolicyIsolate),
		workerkit.WithWorkerCommandConcurrency(1),
		workerkit.WithCommandSpec(workerkit.CommandSpec{
			Name:        "maintenance/prune",
			Description: "Run one maintenance prune pass.",
			Handler:     workerkit.CommandHandlerFunc(worker.prune),
		}),
	); err != nil {
		log.Fatal(err)
	}
}

type appWorker struct {
	name   string
	warmup time.Duration
}

func (w *appWorker) Start(ctx context.Context) error {
	runtime, ok := workerkit.WorkerRuntimeFromContext(ctx)
	if !ok {
		return errors.New("worker runtime handle unavailable")
	}
	if err := runtime.SetReady(false); err != nil {
		return err
	}

	timer := time.NewTimer(w.warmup)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	fmt.Printf("%s warmed up\n", w.name)
	return runtime.SetReady(true)
}

func (w *appWorker) Stop(ctx context.Context) error {
	fmt.Printf("%s stopped\n", w.name)
	return nil
}

type indexWorker struct {
	appWorker
	state    *serviceState
	attempts atomic.Int32
}

func (w *indexWorker) rebuild(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	if w.attempts.Add(1)%3 == 1 {
		return workerkit.CommandResult{}, temporaryError{err: errors.New("index store temporarily unavailable")}
	}
	version := w.state.rebuildIndex()
	return workerkit.CommandResult{
		Message: "index rebuilt",
		Payload: mustJSON(map[string]int64{"indexVersion": version}),
	}, nil
}

type maintenanceWorker struct {
	appWorker
	state *serviceState
}

func (w *maintenanceWorker) prune(ctx context.Context, req workerkit.CommandRequest) (workerkit.CommandResult, error) {
	runs := w.state.runMaintenance()
	return workerkit.CommandResult{
		Message: "maintenance complete",
		Payload: mustJSON(map[string]int64{"maintenanceRuns": runs}),
	}, nil
}

type serviceState struct {
	mu              sync.Mutex
	documents       int64
	indexVersion    int64
	maintenanceRuns int64
}

type stateSnapshot struct {
	documents       int64
	indexVersion    int64
	maintenanceRuns int64
}

func newServiceState() *serviceState {
	return &serviceState{}
}

func (s *serviceState) addDocument(string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.documents++
}

func (s *serviceState) rebuildIndex() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexVersion++
	return s.indexVersion
}

func (s *serviceState) runMaintenance() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maintenanceRuns++
	return s.maintenanceRuns
}

func (s *serviceState) snapshot() stateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return stateSnapshot{
		documents:       s.documents,
		indexVersion:    s.indexVersion,
		maintenanceRuns: s.maintenanceRuns,
	}
}

type temporaryError struct {
	err error
}

func (e temporaryError) Error() string {
	return e.err.Error()
}

func (e temporaryError) Unwrap() error {
	return e.err
}

func (e temporaryError) Temporary() bool {
	return true
}

func isTemporary(err error) bool {
	var temporary interface {
		Temporary() bool
	}
	return errors.As(err, &temporary) && temporary.Temporary()
}

func requireOpsToken(r *http.Request) error {
	if r.Header.Get("X-Ops-Token") != "dev-secret" {
		return servekit.Error(http.StatusUnauthorized, "ops token required", nil)
	}
	return nil
}

func auditOpsRequest(logger *slog.Logger) servekit.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.Info("ops route requested",
				"method", r.Method,
				"path", r.URL.Path,
				"remote", r.RemoteAddr,
			)
			next.ServeHTTP(w, r)
		})
	}
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func printCurlCommands() {
	fmt.Println("production composition listening on :8080")
	fmt.Println("try:")
	fmt.Println("  curl -i http://localhost:8080/app/status")
	fmt.Println("  curl -i http://localhost:8080/readyz")
	fmt.Println("  curl -s -H 'X-Ops-Token: dev-secret' http://localhost:8080/admin/components")
	fmt.Println("  curl -s -H 'X-Ops-Token: dev-secret' http://localhost:8080/admin/components/catalog_service")
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/commands/dispatch -H 'Content-Type: application/json' -H 'X-Ops-Token: dev-secret' -d '{"worker":"ingest","name":"ingest/enqueue","payload":{"documentID":"doc-123","title":"Workerkit"}}'`)
	fmt.Println(`  curl -i -X POST http://localhost:8080/admin/commands/dispatch -H 'Content-Type: application/json' -H 'X-Ops-Token: dev-secret' -d '{"worker":"index","name":"index/rebuild"}'`)
	fmt.Println("privileged lifecycle controls are intentionally not enabled")
}
