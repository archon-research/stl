// Package main provides the vault-debt-worker service.
// It polls on-chain debt for each prime agent vault every 15 minutes
// and writes append-only snapshots to Postgres.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
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

// maskRPCURL redacts the path (which typically contains API keys) from an RPC URL.
// Example: "https://eth-mainnet.g.alchemy.com/v2/abc123" → "https://eth-mainnet.g.alchemy.com/v2/***"
func maskRPCURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "***"
	}
	return fmt.Sprintf("%s://%s/***", u.Scheme, u.Host)
}

// run is the entry point for the vault-debt-worker service.
// It is extracted from main() to allow integration testing.
func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("vault-debt-worker", flag.ContinueOnError)
	dbURL := fs.String("db", env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"), "PostgreSQL connection string")
	vatAddr := fs.String("vat", env.Get("VAT_ADDRESS", "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"), "MCD Vat contract address")
	rpcURL := fs.String("rpc", env.Get("ETH_RPC_URL", ""), "Ethereum JSON-RPC endpoint (e.g. https://eth-mainnet.g.alchemy.com/v2/<key>)")
	pollIntervalStr := fs.String("poll-interval", env.Get("POLL_INTERVAL", "15m"), "Debt polling interval (e.g. 15m, 1h)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	if *rpcURL == "" {
		// Fallback: compose from legacy ALCHEMY_HTTP_URL + ALCHEMY_API_KEY env vars.
		alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
		alchemyAPIKey := env.Get("ALCHEMY_API_KEY", "")
		*rpcURL = fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)
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

	// Ethereum JSON-RPC client
	ethClient, err := ethclient.DialContext(ctx, *rpcURL)
	if err != nil {
		return fmt.Errorf("eth rpc dial: %w", err)
	}
	defer ethClient.Close()
	logger.Info("eth rpc client connected", "rpc", maskRPCURL(*rpcURL))

	// Multicaller
	mc, err := multicall.NewClient(ethClient, common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"))
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}

	// Vat caller (backed by multicall)
	if !common.IsHexAddress(*vatAddr) {
		return fmt.Errorf("invalid vat address: %q", *vatAddr)
	}
	vatCaller, err := blockchain.NewVatCaller(mc, common.HexToAddress(*vatAddr))
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
	// ethClient satisfies outbound.BlockQuerier (has BlockNumber(ctx) method).
	svc, err := vault_debt.NewVaultDebtService(
		vault_debt.Config{
			PollInterval: pollInterval,
			Logger:       logger,
		},
		vatCaller,
		primeDebtRepo,
		ethClient,
	)
	if err != nil {
		return fmt.Errorf("vault debt service: %w", err)
	}

	logger.Info("starting vault debt service...", "pollInterval", pollInterval)
	return lifecycle.Run(ctx, logger, svc)
}
