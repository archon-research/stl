// Package main implements a Temporal cronjob worker that materializes the
// position projections (VEC-402). On each scheduled run it calls the shared
// materialize_position_projection() database function once per configured
// projection through its materialize_<projection>() wrapper; contract validation, the recency guard, and the
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
	"regexp"
	"strings"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/position_materializer"
)

func main() { os.Exit(run()) }

// run holds what main would otherwise do, so main is one statement. Splitting them is not
// cosmetic: os.Exit skips deferred calls, so `defer cancel()` beside an os.Exit in the same
// function silently never runs (exitAfterDefer). Returning a code lets the defer fire.
func run() int {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Require DATABASE_URL rather than default to localhost: a deployed worker that
	// silently connected to a local (empty) database would report healthy while
	// materializing nothing.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		slog.Error("position-materializer startup failed: missing configuration", "error", err)
		return 1
	}
	// The projection list is explicit configuration, never discovery: a projection is
	// materialized because an operator listed its wrapper, so a stray contract-shaped
	// view can never be picked up by accident (the per-view disjointness contract makes
	// an accidental extra writer a correctness hazard, not just noise).
	projectionsRaw, err := env.Require("POSITION_PROJECTIONS")
	if err != nil {
		slog.Error("position-materializer startup failed: missing configuration", "error", err)
		return 1
	}
	materializers, err := parseProjections(projectionsRaw)
	if err != nil {
		slog.Error("position-materializer startup failed: bad configuration", "env", "POSITION_PROJECTIONS", "error", err)
		return 1
	}

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
			return setupRunner(ctx, deps, materializers)
		},
	}); err != nil {
		slog.Error("position-materializer cronjob exited with error", "error", err)
		return 1
	}
	return 0
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

// parseProjections parses the comma-separated POSITION_PROJECTIONS value into
// materializer function names: one `materialize_<projection>()` wrapper per entry,
// so each projection's own pre-flight refusals run under the scheduler. Entries
// are validated as plain identifiers with the materialize_ prefix; anything else
// is a configuration error, never a value handed to SQL.
func parseProjections(raw string) ([]string, error) {
	var materializers []string
	for part := range strings.SplitSeq(raw, ",") {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		if !materializerName.MatchString(v) {
			return nil, fmt.Errorf("entry %q is not a materialize_<projection> function name", v)
		}
		if v == sharedMaterializer {
			return nil, fmt.Errorf("entry %q is the shared function; list the per-projection wrappers", v)
		}
		materializers = append(materializers, v)
	}
	return materializers, nil
}

var materializerName = regexp.MustCompile(`^materialize_[a-z][a-z0-9_]*$`)

const sharedMaterializer = "materialize_position_projection"

func setupRunner(ctx context.Context, deps temporal.Dependencies, materializers []string) (temporal.Runner, error) {
	telemetry, err := position_materializer.NewTelemetry()
	if err != nil {
		return nil, fmt.Errorf("creating position materializer telemetry: %w", err)
	}

	// Provenance is build_id on every appended row (ADR-0002), resolved the same way
	// every other cronjob resolves it: the registry maps this binary's git hash to an
	// id, inserting it on first sight.
	buildReg, err := buildregistry.New(ctx, deps.Pool)
	if err != nil {
		return nil, fmt.Errorf("registering build: %w", err)
	}

	repo := postgres.NewPositionMaterializerRepository(deps.Pool, deps.Logger)

	service, err := position_materializer.NewService(materializers, repo, int(buildReg.BuildID()), deps.Logger, telemetry)
	if err != nil {
		return nil, fmt.Errorf("creating position materializer service: %w", err)
	}

	return temporal.RunnerFunc(service.RunOnce), nil
}
