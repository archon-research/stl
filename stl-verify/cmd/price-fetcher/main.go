// Package main provides a CLI for fetching token prices from external APIs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coingecko"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/price_fetcher"
)

// Build-time variables - can be set via ldflags, otherwise populated from Go's build info.
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if GitCommit == "" {
					GitCommit = setting.Value
				}
			case "vcs.time":
				if BuildTime == "" {
					BuildTime = setting.Value
				}
			}
		}
	}
}

func main() {
	mode := flag.String("mode", "current", "Mode: 'historical' or 'current'")
	source := flag.String("source", "coingecko", "Price source: 'coingecko' (more coming)")
	fromDate := flag.String("from", "", "Start date (YYYY-MM-DD) for historical mode")
	toDate := flag.String("to", "", "End date (YYYY-MM-DD) for historical mode, default: yesterday")
	assets := flag.String("assets", "", "Comma-separated asset IDs (default: all enabled for source)")
	showVersion := flag.Bool("version", false, "Show version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("price-fetcher\n")
		fmt.Printf("  Commit:     %s\n", GitCommit)
		fmt.Printf("  Branch:     %s\n", GitBranch)
		fmt.Printf("  Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting price-fetcher",
		"commit", GitCommit,
		"mode", *mode,
		"source", *source,
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	if err := run(ctx, logger, *mode, *source, *fromDate, *toDate, *assets); err != nil {
		logger.Error("failed", "error", err)
		os.Exit(1)
	}

	logger.Info("completed successfully")
}

func run(ctx context.Context, logger *slog.Logger, mode, source, fromDate, toDate, assets string) error {
	provider, err := createProvider(source, logger)
	if err != nil {
		return fmt.Errorf("creating provider: %w", err)
	}

	postgresURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(postgresURL))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	priceRepo, err := postgres.NewPriceRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating price repository: %w", err)
	}

	chainID, err := getChainID()
	if err != nil {
		return err
	}

	service, err := price_fetcher.NewService(price_fetcher.ServiceConfig{
		ChainID: chainID,
		Logger:  logger,
	}, provider, priceRepo)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	assetIDs := parseAssetIDs(assets)

	switch mode {
	case "current":
		return runCurrentMode(ctx, service, assetIDs)
	case "historical":
		return runHistoricalMode(ctx, service, assetIDs, fromDate, toDate)
	default:
		return fmt.Errorf("unknown mode: %s (must be 'current' or 'historical')", mode)
	}
}

func createProvider(source string, logger *slog.Logger) (outbound.PriceProvider, error) {
	switch source {
	case "coingecko":
		apiKey := os.Getenv("COINGECKO_API_KEY")
		if apiKey == "" {
			return nil, fmt.Errorf("COINGECKO_API_KEY environment variable is required")
		}
		return coingecko.NewClient(coingecko.ClientConfig{
			APIKey: apiKey,
			Logger: logger,
		})
	default:
		return nil, fmt.Errorf("unknown source: %s (supported: coingecko)", source)
	}
}

func runCurrentMode(ctx context.Context, service *price_fetcher.Service, assetIDs []string) error {
	return service.FetchCurrentPrices(ctx, assetIDs)
}

func runHistoricalMode(ctx context.Context, service *price_fetcher.Service, assetIDs []string, fromDate, toDate string) error {
	if fromDate == "" {
		return fmt.Errorf("--from is required for historical mode")
	}

	from, err := time.Parse(time.DateOnly, fromDate)
	if err != nil {
		return fmt.Errorf("invalid --from date (must be YYYY-MM-DD): %w", err)
	}

	var to time.Time
	if toDate == "" {
		// Default to yesterday
		to = time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	} else {
		to, err = time.Parse(time.DateOnly, toDate)
		if err != nil {
			return fmt.Errorf("invalid --to date (must be YYYY-MM-DD): %w", err)
		}
	}

	// Set end of day for the 'to' date
	to = to.Add(24*time.Hour - time.Second)

	if from.After(to) {
		return fmt.Errorf("--from date must be before --to date")
	}

	return service.FetchHistoricalData(ctx, assetIDs, from, to)
}

func parseAssetIDs(assets string) []string {
	if assets == "" {
		return nil
	}
	ids := strings.Split(assets, ",")
	for i := range ids {
		ids[i] = strings.TrimSpace(ids[i])
	}
	return ids
}

func getChainID() (int, error) {
	chainIDStr := getEnv("CHAIN_ID", "1")
	chainID, err := strconv.Atoi(chainIDStr)
	if err != nil {
		return 0, fmt.Errorf("CHAIN_ID must be a valid integer: %w", err)
	}
	return chainID, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
