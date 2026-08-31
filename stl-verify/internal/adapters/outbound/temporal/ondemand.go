package temporal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
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

	// OpenDatabase opens the app database pool. Optional: a job that touches no
	// Postgres leaves it nil and gets a nil Dependencies.Pool, so its deployment
	// needs no database credential.
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
	if c.Register == nil {
		return fmt.Errorf("WorkerConfig.Register is required")
	}
	return nil
}

// RunnerJob is an on-demand job that takes no parameters: a one-shot repair or
// migration an operator starts by hand, whose scope the run derives for itself.
//
// Such a job needs less than WorkerConfig.Register's full freedom, and writing a
// workflow per job would restate the retry policy, the timeouts and the
// progress-preserving heartbeat at every site. RegisterRunner registers it
// against the same activity a scheduled cronjob runs on.
type RunnerJob struct {
	// WorkflowType is the name an operator passes to `--type` (the UI's
	// "Workflow Type" box). Explicit, so a Go rename cannot invalidate a runbook.
	WorkflowType string

	// Runner is the business logic — the same interface a cronjob implements.
	Runner Runner

	// Timeouts bounds one execution. Unlike a scheduled job's, these are bound
	// at registration rather than travelling as workflow input: there is no
	// schedule action to carry them, and an operator must not have to type them
	// to start a run. A redeploy is then enough to change them, where a
	// schedule's action bakes them in until the schedule is deleted.
	Timeouts ActivityTimeouts

	// Progress, when set, is the heartbeat-details store the runner records
	// through — the SAME instance the runner holds, because the liveness
	// heartbeat re-sends what it holds rather than erasing it with a bare ping.
	Progress ProgressHeartbeater
}

// RegisterRunner registers job as a workflow that accepts no input, plus the
// shared activity that executes its Runner. Call it from WorkerConfig.Register.
//
// The activity gets no metrics recorder on purpose: RunWorker's interceptor
// already records one cronjob.runs.total per activity execution, so a second
// recorder here would double every count.
func RegisterRunner(r worker.Registry, job RunnerJob) error {
	if job.WorkflowType == "" {
		return fmt.Errorf("RunnerJob.WorkflowType is required")
	}
	// A job that bothers to record resumable progress runs long, and with no
	// heartbeat timeout a dead worker goes undetected until StartToClose.
	if job.Progress != nil && job.Timeouts.Heartbeat <= 0 {
		return fmt.Errorf("RunnerJob.Progress needs a non-zero Timeouts.Heartbeat, or a worker that dies mid-run is only noticed when StartToClose expires")
	}
	activities, err := newCronjobActivities(job.Runner, nil, job.Timeouts.Heartbeat, job.Progress)
	if err != nil {
		return fmt.Errorf("creating the runner activity: %w", err)
	}
	r.RegisterWorkflowWithOptions(runnerWorkflow(job.Timeouts), workflow.RegisterOptions{Name: job.WorkflowType})
	r.RegisterActivity(activities)
	return nil
}

// runnerWorkflow is cronjobWorkflow with its bounds closed over instead of
// arriving as an argument, so a run starts with no input payload at all.
func runnerWorkflow(timeouts ActivityTimeouts) func(workflow.Context) error {
	return func(ctx workflow.Context) error { return cronjobWorkflow(ctx, timeouts) }
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
