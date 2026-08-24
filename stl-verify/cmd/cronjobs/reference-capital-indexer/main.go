// Package main implements a Temporal cronjob worker that syncs prime capital stack
// data from approved upstream sources (Sky risk-capital API) on a schedule.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sky"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/skydata"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/reference_capital_indexer"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name:            "reference-capital-indexer",
		IntervalEnv:     "REFERENCE_CAPITAL_SYNC_INTERVAL",
		IntervalDefault: "15m",
		OpenDatabase:    postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"))),
		Setup:           setupRunner,
	})
	cancel()
	if err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func setupRunner(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	buildReg, err := buildregistry.New(ctx, deps.Pool)
	if err != nil {
		return nil, fmt.Errorf("registering build: %w", err)
	}

	txm, err := postgres.NewTxManager(deps.Pool, deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}

	// Empty falls through to the client's own default, so the URL is not
	// duplicated here where the two could drift apart.
	skyClient, err := sky.NewClient(sky.ClientConfig{
		BaseURL: env.Get("SKY_RISK_CAPITAL_URL", ""),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating sky client: %w", err)
	}

	// Empty falls through to the client's own default, as above.
	sheetClient, err := skydata.NewClient(skydata.ClientConfig{
		BaseURL: env.Get("SKY_DATA_URL", ""),
		Logger:  deps.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating sky-data client: %w", err)
	}

	trackedStars, err := trackedStarsFromContract()
	if err != nil {
		return nil, err
	}

	syncTelemetry, err := reference_capital_indexer.NewTelemetry()
	if err != nil {
		return nil, fmt.Errorf("creating telemetry: %w", err)
	}

	service := reference_capital_indexer.NewService(
		postgres.NewPrimeRepository(deps.Pool),
		postgres.NewPrimeCapitalStackRepository(deps.Pool, txm, deps.Logger),
		skyClient,
		postgres.NewPrimeBalanceSheetRepository(deps.Pool, txm, deps.Logger),
		sheetClient,
		trackedStars,
		int(buildReg.BuildID()),
		time.Now,
		syncTelemetry,
		deps.Logger,
	)

	return temporal.RunnerFunc(func(ctx context.Context) error {
		return service.Run(ctx)
	}), nil
}

// trackedStarsFromContract names the primes STL tracks, sorted so a cycle's
// upstream calls are issued in a stable order.
func trackedStarsFromContract() ([]string, error) {
	contract, err := axis_synome_contract.LoadDefaultContract()
	if err != nil {
		return nil, fmt.Errorf("loading axis-synome contract: %w", err)
	}

	almProxies := contract.GetAlmProxies()
	stars := make([]string, 0, len(almProxies))
	for star := range almProxies {
		stars = append(stars, star)
	}
	if len(stars) == 0 {
		return nil, fmt.Errorf("axis-synome contract names no primes")
	}
	sort.Strings(stars)
	return stars, nil
}
