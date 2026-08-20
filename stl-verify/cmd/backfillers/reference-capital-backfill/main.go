// Package main implements an on-demand Temporal worker that seeds the reference
// balance-sheet history predating STL's own observation of Sky's Star monitor.
//
// It carries no schedule. The monitor publishes no history, so the capital-stack
// syncer can only accumulate forward from its first run, and this fills the year
// before it from Sky's balance-sheet feed. The range it covers stops growing once
// the syncer takes over, which is precisely why it must not fire on a tick: the
// worker idles on its task queue until someone starts a run and supplies the
// window, either from the Temporal UI ("Start Workflow", Workflow Type
// "ReferenceCapitalBackfill") or via `temporal workflow start`. A backfill's
// window is an argument, and cronjobWorkflow accepts none.
//
// Re-running is safe — rows are insert-only and conflict away within a build.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

const (
	jobName = "reference-capital-backfill"

	// workflowTypeName is what an operator types into the Temporal UI's "Workflow
	// Type" field, so it is registered explicitly rather than derived from the Go
	// function name — a rename must not invalidate the runbook or muscle memory.
	workflowTypeName = "ReferenceCapitalBackfill"

	defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"
	defaultSkyDataURL  = "https://sky.data.blockanalitica.com/internal"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() { buildinfo.PopulateFromVCS(&GitCommit, &BuildTime) }

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:         jobName,
		OpenDatabase: postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", defaultDatabaseURL))),
		Register:     register,
	})
}

func register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	activities, err := newBackfillActivities(ctx, deps)
	if err != nil {
		return err
	}

	r.RegisterWorkflowWithOptions(backfillWorkflow, workflow.RegisterOptions{Name: workflowTypeName})
	r.RegisterActivity(activities)
	return nil
}
