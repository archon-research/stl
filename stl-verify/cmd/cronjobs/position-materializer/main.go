// Package main implements a Temporal cronjob worker that materializes the
// position projections (VEC-402). On each scheduled run it calls one
// materialize_<projection>() wrapper per configured projection, which validates
// its inputs and appends through the shared materialize_position_projection().
//
// Every run re-projects each view's whole history and appends only unseen
// observation keys, so the first scheduled run is also the history bootstrap:
// deploy gated at replicas 0 and bump once the projection list is confirmed (see
// k8s/base/position-materializer). The dedicated stl_materialize role (VEC-562)
// replaces the interim credentials.
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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/writerrun"
	"github.com/archon-research/stl/stl-verify/internal/services/position_materializer"
)

func main() { os.Exit(run()) }

// run holds what main would otherwise do, so main is one statement. Splitting them is not
// cosmetic: os.Exit skips deferred calls, so `defer cancel()` beside an os.Exit in the same
// function silently never runs (exitAfterDefer). Returning a code lets the defer fire.
func run() int {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// DATABASE_URL is required: a worker on a local empty database would report healthy while writing nothing.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		slog.Error("position-materializer startup failed: missing configuration", "error", err)
		return 1
	}
	// A projection runs only when an operator lists its wrapper: the per-view disjointness
	// contract makes an extra writer a correctness hazard.
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
	}, cronjobConfig(serviceName, dbURL, materializers)); err != nil {
		slog.Error("position-materializer cronjob exited with error", "error", err)
		return 1
	}
	return 0
}

func cronjobConfig(serviceName, dbURL string, materializers []string) temporal.CronjobConfig {
	return temporal.CronjobConfig{
		Name:              serviceName,
		IntervalEnv:       "MATERIALIZE_INTERVAL",
		IntervalDefault:   "1h",
		IntervalOffsetEnv: "MATERIALIZE_SCHEDULE_OFFSET",
		ActivityTimeouts:  materializeActivityTimeouts,
		OpenDatabase: func(ctx context.Context) (*pgxpool.Pool, error) {
			// slog.Default() is read here, not above: the cronjob bootstrap installs its logger first.
			return postgres.OpenPool(ctx, materializerDBConfig(dbURL, slog.Default()))
		},
		Setup: func(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
			return setupRunner(ctx, deps, materializers)
		},
	}
}

// materializerDBConfig logs the server's WARNINGs, which is how the shared function reports a
// withheld or declined position.
func materializerDBConfig(dbURL string, logger *slog.Logger) postgres.DBConfig {
	cfg := postgres.DefaultDBConfig(dbURL)
	cfg.NoticeLogger = logger.With("component", "position-materializer")
	return cfg
}

// Build metadata, populated from VCS in init() (GitBranch is set at link time).
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.Populate(&GitCommit, &GitBranch, &BuildTime)
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

// materializeActivityTimeouts sizes a tick against the first run, the whole-history bootstrap. The
// schedule keeps the timeouts it was created with. The heartbeat carries Temporal's cancellation to the
// running query while the pod lives; a SIGKILLed pod's query keeps running behind the pooler.
var materializeActivityTimeouts = temporal.ActivityTimeouts{
	StartToClose:    6 * time.Hour,
	ScheduleToClose: 12 * time.Hour,
	MaximumAttempts: 2,
	Heartbeat:       time.Minute,
}

var materializerName = regexp.MustCompile(`^materialize_[a-z][a-z0-9_]*$`)

const sharedMaterializer = "materialize_position_projection"

func setupRunner(ctx context.Context, deps temporal.Dependencies, materializers []string) (temporal.Runner, error) {
	telemetry, err := position_materializer.NewTelemetry(materializers)
	if err != nil {
		return nil, fmt.Errorf("creating position materializer telemetry: %w", err)
	}

	// Provenance on every appended row: build_id for the code artefact (ADR-0002) and run_id
	// for this process start (ADR-0006 §2), resolved the way every other cronjob resolves them.
	buildReg, runID, err := writerrun.Open(ctx, deps.Pool)
	if err != nil {
		return nil, err
	}

	repo := postgres.NewPositionMaterializerRepository(deps.Pool, deps.Logger)

	service, err := position_materializer.NewService(materializers, repo, int(buildReg.BuildID()), int64(runID), deps.Logger, telemetry)
	if err != nil {
		return nil, fmt.Errorf("creating position materializer service: %w", err)
	}
	if err := service.CheckConfigured(ctx); err != nil {
		return nil, fmt.Errorf("position materializer startup check: %w", err)
	}

	return temporal.RunnerFunc(service.RunOnce), nil
}
