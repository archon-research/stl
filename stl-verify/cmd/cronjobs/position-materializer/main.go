// Package main implements a Temporal cronjob worker that materializes the
// position projections (VEC-402). On each scheduled run it calls the shared
// materialize_position_projection() database function once per configured
// projection view; contract validation, the recency guard, and the
// classification upsert live in that function.
//
// The write path is the full-projection upsert, so the first scheduled run is
// also the history bootstrap — deploy gated at replicas 0 and bump once the
// projection list is confirmed (see k8s/base/position-materializer). The
// incremental write path + compression (VEC-566) replace the write path under
// this same runner; the dedicated stl_materialize role (VEC-562) replaces the
// interim credentials.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/position_materializer"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Require DATABASE_URL rather than default to localhost: a deployed worker that
	// silently connected to a local (empty) database would report healthy while
	// materializing nothing.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		slog.Error("position-materializer startup failed: missing configuration", "error", err)
		os.Exit(1)
	}
	// The projection list is explicit configuration, never discovery: a view is
	// materialized because an operator listed it, so a stray contract-shaped view
	// can never be picked up by accident (the per-view disjointness contract makes
	// an accidental extra writer a correctness hazard, not just noise).
	projectionsRaw, err := env.Require("POSITION_PROJECTIONS")
	if err != nil {
		slog.Error("position-materializer startup failed: missing configuration", "error", err)
		os.Exit(1)
	}
	views := splitProjections(projectionsRaw)

	serviceName := env.Get("SERVICE_NAME", "position-materializer")

	if err := temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name:              serviceName,
		IntervalEnv:       "MATERIALIZE_INTERVAL",
		IntervalDefault:   "1h",
		IntervalOffsetEnv: "MATERIALIZE_SCHEDULE_OFFSET",
		OpenDatabase:      postgres.PoolOpener(postgres.DefaultDBConfig(dbURL)),
		Setup: func(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
			return setupRunner(ctx, deps, views, changeReason(serviceName, GitCommit))
		},
	}); err != nil {
		slog.Error("position-materializer cronjob exited with error", "error", err)
		os.Exit(1)
	}
}

// Build metadata, populated from VCS in init() (GitBranch is set at link time).
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

// splitProjections parses the comma-separated POSITION_PROJECTIONS value,
// trimming whitespace and dropping empty segments (a trailing comma is not a
// blank view). Duplicate or genuinely blank entries are rejected downstream by
// the service constructor, which fails startup loudly.
func splitProjections(raw string) []string {
	var views []string
	for part := range strings.SplitSeq(raw, ",") {
		if v := strings.TrimSpace(part); v != "" {
			views = append(views, v)
		}
	}
	return views
}

// changeReason is the change_reason provenance stamped on every classification
// write this runner makes: which service, at which build, wrote it.
func changeReason(serviceName, commit string) string {
	if commit == "" {
		commit = "dev"
	}
	if len(commit) > 12 {
		commit = commit[:12]
	}
	return fmt.Sprintf("%s@%s", serviceName, commit)
}

func setupRunner(_ context.Context, deps temporal.Dependencies, views []string, reason string) (temporal.Runner, error) {
	telemetry, err := position_materializer.NewTelemetry()
	if err != nil {
		return nil, fmt.Errorf("creating position materializer telemetry: %w", err)
	}

	repo := postgres.NewPositionMaterializerRepository(deps.Pool, deps.Logger)

	service, err := position_materializer.NewService(views, repo, reason, deps.Logger, telemetry)
	if err != nil {
		return nil, fmt.Errorf("creating position materializer service: %w", err)
	}

	return temporal.RunnerFunc(service.RunOnce), nil
}
