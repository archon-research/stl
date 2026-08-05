package temporal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
)

// WorkerConfig defines an on-demand Temporal worker: one that registers its
// workflows and then waits to be told to do something.
//
// It exists because RunCronjob cannot express a job that is triggered by hand:
// CronjobConfig requires an interval, RunCronjob always calls ensureSchedule,
// and cronjobWorkflow takes no parameters, so there is nowhere to pass a range.
// RunWorker creates NO schedule, which is what lets a backfill accept its own
// from/to window as workflow input instead of hard-coding one.
type WorkerConfig struct {
	// Name is the task queue and the OTel service name.
	Name string

	OpenDatabase func(ctx context.Context) (*pgxpool.Pool, error)

	// Register attaches the job's workflows and activities. It runs after the
	// database pool is open so activity structs can capture their dependencies.
	// Register the workflow via RegisterWorkflowWithOptions with an explicit
	// Name: that name is the "Workflow Type" a human types into the Temporal UI,
	// so it must not drift with Go function renames.
	Register func(ctx context.Context, deps Dependencies, r worker.Registry) error
}

// workerOptions is the worker configuration RunWorker applies. It is a named
// function rather than a literal at the call site so tests exercise the real
// wiring instead of a copy that can silently drift from it — the interceptor
// here is the only thing making on-demand jobs visible to the alerts, and a copy
// in the test file would keep passing after it was dropped from production.
func workerOptions(metrics *cronjobMetrics) worker.Options {
	return worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{newRunMetricsInterceptor(metrics)},
	}
}

func (c WorkerConfig) validate() error {
	if c.Name == "" {
		return fmt.Errorf("WorkerConfig.Name is required")
	}
	if c.OpenDatabase == nil {
		return fmt.Errorf("WorkerConfig.OpenDatabase is required")
	}
	if c.Register == nil {
		return fmt.Errorf("WorkerConfig.Register is required")
	}
	return nil
}

// RunWorker runs an on-demand Temporal worker until ctx is cancelled. It shares
// RunCronjob's bootstrap but registers the caller's workflows rather than
// cronjobWorkflow, and creates no schedule: nothing executes until someone
// starts a workflow explicitly (Temporal UI "Start Workflow", or
// `temporal workflow start`).
func RunWorker(ctx context.Context, meta BuildMeta, cfg WorkerConfig) error {
	if err := cfg.validate(); err != nil {
		return fmt.Errorf("validating worker config: %w", err)
	}

	boot, err := newBootstrap(ctx, meta, cfg.Name, cfg.OpenDatabase)
	if err != nil {
		return err
	}
	defer boot.close()

	metrics, err := newCronjobMetrics()
	if err != nil {
		return fmt.Errorf("creating run metrics: %w", err)
	}

	w := worker.New(boot.client, cfg.Name, workerOptions(metrics))
	if err := cfg.Register(ctx, boot.dependencies(), w); err != nil {
		return fmt.Errorf("registering %s workflows: %w", cfg.Name, err)
	}

	boot.logger.Info("starting on-demand worker; no schedule, awaiting manual runs",
		"taskQueue", cfg.Name)

	if err := w.Run(interruptFromContext(ctx)); err != nil {
		return fmt.Errorf("running worker: %w", err)
	}

	boot.logger.Info("worker stopped")
	return nil
}
