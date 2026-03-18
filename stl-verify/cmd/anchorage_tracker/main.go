package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	tracker "github.com/archon-research/stl/stl-verify/internal/services/anchorage_tracker"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

var (
	GitCommit string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		slog.Info("received signal", "signal", sig)
		cancel()
	}()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("anchorage-tracker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("anchorage-tracker", flag.ContinueOnError)

	dbURL := fs.String("db", env.Get("DATABASE_URL",
		"postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"),
		"PostgreSQL connection string")

	apiURL := fs.String("api-url", env.Get("ANCHORAGE_API_URL", ""),
		"Anchorage API base URL (e.g. https://api.anchorage.com)")

	apiKey := fs.String("api-key", env.Get("ANCHORAGE_API_KEY", ""),
		"Anchorage API key")

	pollIntervalStr := fs.String("poll-interval", env.Get("POLL_INTERVAL", "15m"),
		"Polling interval (e.g. 15m, 1h)")

	prime := fs.String("prime", env.Get("ANCHORAGE_PRIME", "spark"),
		"Prime name for these packages (e.g. spark, grove)")

	backfill := fs.Bool("backfill", false,
		"Backfill operations from the Anchorage API, then exit")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	if *apiURL == "" {
		return fmt.Errorf("anchorage API URL is required (use -api-url flag or ANCHORAGE_API_URL env var)")
	}
	if *apiKey == "" {
		return fmt.Errorf("anchorage API key is required (use -api-key flag or ANCHORAGE_API_KEY env var)")
	}

	logger.Info("starting anchorage tracker",
		"prime", *prime,
		"db", shared.MaskDBURL(*dbURL),
		"api_url", *apiURL,
		"backfill", *backfill,
		"git_commit", GitCommit,
		"build_time", BuildTime,
	)

	// Database
	dbPool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(*dbURL))
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer dbPool.Close()
	logger.Info("postgres connected")

	// Look up prime ID
	var primeID int64
	if err := dbPool.QueryRow(ctx, "SELECT id FROM prime WHERE name = $1", *prime).Scan(&primeID); err != nil {
		return fmt.Errorf("prime %q not found in database: %w", *prime, err)
	}
	logger.Info("resolved prime", "name", *prime, "id", primeID)

	// Dependencies
	client := tracker.NewClient(*apiURL, *apiKey)
	repo := postgres.NewAnchorageRepository(dbPool, logger)

	// Backfill mode: fetch all operations, store them, exit.
	// Poll interval is not required for backfill.
	if *backfill {
		svc := tracker.NewService(client, repo, repo, primeID, time.Minute, logger)
		n, err := svc.BackfillOperations(ctx)
		if err != nil {
			return fmt.Errorf("backfill: %w", err)
		}
		logger.Info("backfill finished", "operations", n)
		return nil
	}

	// Parse poll interval only for long-running mode.
	pollInterval, err := time.ParseDuration(*pollIntervalStr)
	if err != nil {
		return fmt.Errorf("parse poll interval: %w", err)
	}

	svc := tracker.NewService(client, repo, repo, primeID, pollInterval, logger)
	return lifecycle.Run(ctx, logger, svc)
}
