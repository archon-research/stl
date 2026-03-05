// Package main provides the vault-debt-worker service.
// It polls on-chain debt for each prime agent vault every 15 minutes
// and writes append-only snapshots to Postgres.
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

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/ethereum"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/vault_debt"
)

// Build-time variables - can be set via ldflags, otherwise populated from Go's build info.
var (
	GitCommit string
	GitBranch string
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
		<-sigChan
		cancel()
	}()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("vault-debt-worker exited with error", "error", err)
		os.Exit(1)
	}
}

// run is the entry point for the vault-debt-worker service.
// It is extracted from main() to allow integration testing.
func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("vault-debt-worker", flag.ContinueOnError)
	dbURL := fs.String("db", env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"), "PostgreSQL connection string")
	vatAddr := fs.String("vat", env.Get("VAT_ADDRESS", "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"), "MCD Vat contract address")
	pollIntervalStr := fs.String("poll-interval", env.Get("POLL_INTERVAL", "15m"), "Debt polling interval (e.g. 15m, 1h)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	pollInterval, err := time.ParseDuration(*pollIntervalStr)
	if err != nil {
		return fmt.Errorf("invalid poll interval %q: %w", *pollIntervalStr, err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	logger.Info("starting vault-debt-worker",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
	)

	// OpenTelemetry
	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "vault-debt-worker",
		ServiceVersion: GitCommit,
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}
	defer shutdownOTEL(context.Background())

	// PostgreSQL
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(*dbURL))
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	// Alchemy HTTP client
	alchemyAPIKey := env.Get("ALCHEMY_API_KEY", "")
	alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")

	alchemyTelemetry, err := alchemy.NewTelemetry()
	if err != nil {
		logger.Warn("failed to create alchemy telemetry, continuing without instrumentation", "error", err)
	}

	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:   fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey),
		Logger:    logger,
		Telemetry: alchemyTelemetry,
	})
	if err != nil {
		return fmt.Errorf("alchemy client: %w", err)
	}
	logger.Info("alchemy client configured")

	// Vat caller
	if !common.IsHexAddress(*vatAddr) {
		return fmt.Errorf("invalid vat address: %q", *vatAddr)
	}
	vatCaller, err := ethereum.NewVatCaller(client, common.HexToAddress(*vatAddr))
	if err != nil {
		return fmt.Errorf("vat caller: %w", err)
	}
	logger.Info("vat caller configured", "vatAddress", *vatAddr)

	// Prime debt repository
	txm, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("tx manager: %w", err)
	}
	primeDebtRepo := postgres.NewPrimeDebtRepository(pool, txm, logger)

	// Vault debt service
	svc, err := vault_debt.NewVaultDebtService(
		vault_debt.Config{
			PollInterval: pollInterval,
			Logger:       logger,
		},
		vatCaller,
		primeDebtRepo,
	)
	if err != nil {
		return fmt.Errorf("vault debt service: %w", err)
	}

	logger.Info("starting vault debt service...", "pollInterval", pollInterval)
	if err := svc.Start(ctx); err != nil {
		return fmt.Errorf("start vault debt service: %w", err)
	}

	// Wait for shutdown
	<-ctx.Done()
	logger.Info("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	shutdownDone := make(chan struct{})
	var stopErr error
	go func() {
		defer close(shutdownDone)
		stopErr = svc.Stop()
	}()

	select {
	case <-shutdownDone:
		if stopErr != nil {
			return fmt.Errorf("stop vault debt service: %w", stopErr)
		}
		logger.Info("shutdown complete")
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown timed out")
	}

	return nil
}
