// Package main provides a CLI for validating blockchain data stored by the watcher.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/etherscan"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/data_validator"
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
	fromBlock := flag.Int64("from", 0, "Start block number (0 = min block in DB)")
	toBlock := flag.Int64("to", 0, "End block number (0 = max block in DB)")
	spotChecks := flag.Int("spot-checks", 10, "Number of random blocks to verify against Etherscan")
	skipReorgs := flag.Bool("skip-reorgs", false, "Skip reorg validation")
	skipIntegrity := flag.Bool("skip-integrity", false, "Skip chain integrity check")
	output := flag.String("output", "text", "Output format: 'text' or 'json'")
	showVersion := flag.Bool("version", false, "Show version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("data-validator\n")
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

	logger.Info("starting data-validator",
		"commit", GitCommit,
		"from", *fromBlock,
		"to", *toBlock,
		"spot_checks", *spotChecks,
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	exitCode := 0
	if err := run(ctx, logger, runConfig{
		fromBlock:     *fromBlock,
		toBlock:       *toBlock,
		spotChecks:    *spotChecks,
		skipReorgs:    *skipReorgs,
		skipIntegrity: *skipIntegrity,
		output:        *output,
	}); err != nil {
		logger.Error("validation failed", "error", err)
		exitCode = 1
	}

	os.Exit(exitCode)
}

type runConfig struct {
	fromBlock     int64
	toBlock       int64
	spotChecks    int
	skipReorgs    bool
	skipIntegrity bool
	output        string
}

func run(ctx context.Context, logger *slog.Logger, cfg runConfig) error {
	etherscanAPIKey := os.Getenv("ETHERSCAN_API_KEY")
	if etherscanAPIKey == "" {
		return fmt.Errorf("ETHERSCAN_API_KEY environment variable is required")
	}

	postgresURL := env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(postgresURL))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	blockStateRepo := postgres.NewBlockStateRepository(pool, logger)

	etherscanClient, err := etherscan.NewClient(etherscan.ClientConfig{
		APIKey: etherscanAPIKey,
		Logger: logger,
	})
	if err != nil {
		return fmt.Errorf("creating etherscan client: %w", err)
	}

	serviceConfig := data_validator.ServiceConfig{
		FromBlock:              cfg.fromBlock,
		ToBlock:                cfg.toBlock,
		SpotCheckCount:         cfg.spotChecks,
		ValidateReorgs:         !cfg.skipReorgs,
		ValidateChainIntegrity: !cfg.skipIntegrity,
		Logger:                 logger,
	}

	service, err := data_validator.NewService(serviceConfig, blockStateRepo, etherscanClient)
	if err != nil {
		return fmt.Errorf("creating service: %w", err)
	}

	report, err := service.Validate(ctx)
	if err != nil {
		return fmt.Errorf("running validation: %w", err)
	}

	if err := printReport(report, cfg.output); err != nil {
		return fmt.Errorf("printing report: %w", err)
	}

	if !report.Success() {
		return fmt.Errorf("validation failed: %d failures, %d errors", report.Failed, report.Errors)
	}

	return nil
}

func printReport(report *data_validator.Report, format string) error {
	switch format {
	case "json":
		jsonStr, err := report.FormatJSON()
		if err != nil {
			return err
		}
		fmt.Println(jsonStr)
	case "text":
		fmt.Print(report.FormatText())
	default:
		return fmt.Errorf("unknown output format: %s (supported: text, json)", format)
	}
	return nil
}
