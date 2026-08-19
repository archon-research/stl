// Package main backfills the reference balance-sheet history that predates
// STL's own observation of Sky's Star monitor.
//
// One-shot: the monitor publishes no history, so the capital-stack syncer can
// only accumulate forward from its first run, and this fills the year before
// it. Re-running is safe — rows are insert-only and conflict away within a
// build.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/skydata"
	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/balance_sheet_backfill"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("balance-sheet-backfill", flag.ContinueOnError)
	daysAgo := flags.Int("days-ago", 365, "How many days of history to request from the feed.")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing flags: %w", err)
	}

	logger := slog.Default()

	pool, err := postgres.PoolOpener(postgres.DefaultDBConfig(env.Get(
		"DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable",
	)))(ctx)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer pool.Close()

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		return fmt.Errorf("registering build: %w", err)
	}

	txm, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("creating tx manager: %w", err)
	}

	client, err := skydata.NewClient(skydata.ClientConfig{
		BaseURL: env.Get("SKY_DATA_URL", "https://sky.data.blockanalitica.com/internal"),
		Logger:  logger,
	})
	if err != nil {
		return fmt.Errorf("creating sky-data client: %w", err)
	}

	trackedStars, err := trackedStarsFromContract()
	if err != nil {
		return err
	}

	service := balance_sheet_backfill.NewService(
		postgres.NewPrimeRepository(pool),
		postgres.NewPrimeBalanceSheetRepository(pool, txm, logger),
		client,
		trackedStars,
		*daysAgo,
		int(buildReg.BuildID()),
		logger,
	)

	return service.Run(ctx)
}

// trackedStarsFromContract names the primes STL tracks, sorted for a stable
// request order.
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
