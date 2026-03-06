// Package main provides the vault-debt-worker service.
// It consumes block events from SQS and reads on-chain debt for each
// prime agent vault every N blocks, writing append-only snapshots to Postgres.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
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
	queueURL := fs.String("queue", env.Get("AWS_SQS_QUEUE_URL", ""), "SQS Queue URL")
	sweepBlocks := fs.Int("sweep-blocks", 75, "Read debt every N blocks")
	visibilityTimeout := fs.Int("visibility-timeout", 300, "SQS visibility timeout in seconds")
	waitTime := fs.Int("wait", 20, "SQS wait time in seconds (long polling)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	if *rpcURL == "" {
		// Fallback: compose from legacy ALCHEMY_HTTP_URL + ALCHEMY_API_KEY env vars.
		alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2")
		alchemyAPIKey := env.Get("ALCHEMY_API_KEY", "")
		*rpcURL = fmt.Sprintf("%s/%s", alchemyHTTPURL, alchemyAPIKey)
	}

	if *queueURL == "" {
		return fmt.Errorf("queue URL not provided (use -queue flag or AWS_SQS_QUEUE_URL env var)")
	}

	chainIDStr := env.Get("CHAIN_ID", "1")
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing CHAIN_ID %q: %w", chainIDStr, err)
	}

	if waitTimeStr := env.Get("SQS_WAIT_TIME", ""); waitTimeStr != "" {
		v, err := strconv.Atoi(waitTimeStr)
		if err != nil {
			return fmt.Errorf("parsing SQS_WAIT_TIME %q: %w", waitTimeStr, err)
		}
		*waitTime = v
	}
	if visTimeStr := env.Get("SQS_VISIBILITY_TIMEOUT", ""); visTimeStr != "" {
		v, err := strconv.Atoi(visTimeStr)
		if err != nil {
			return fmt.Errorf("parsing SQS_VISIBILITY_TIMEOUT %q: %w", visTimeStr, err)
		}
		*visibilityTimeout = v
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	logger.Info("starting vault-debt-worker",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
		"chainID", chainID,
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

	// AWS config
	awsRegion := env.Get("AWS_REGION", "eu-west-1")
	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(awsRegion),
	}
	if accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID"); accessKeyID != "" {
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		awsOpts = append(awsOpts, awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
				Source:          "StaticCredentials",
			}, nil
		})))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	// SQS
	sqsConsumer, err := sqsAdapter.NewConsumer(awsCfg, sqsAdapter.Config{
		QueueURL:          *queueURL,
		WaitTimeSeconds:   int32(*waitTime),
		VisibilityTimeout: int32(*visibilityTimeout),
		BaseEndpoint:      env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("creating SQS consumer: %w", err)
	}
	defer sqsConsumer.Close()

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
	svc, err := vault_debt.NewVaultDebtService(
		vault_debt.Config{
			SweepEveryNBlocks: *sweepBlocks,
			ChainID:           chainID,
			MaxMessages:       10,
			PollInterval:      100 * time.Millisecond,
			Logger:            logger,
		},
		vatCaller,
		primeDebtRepo,
		sqsConsumer,
		ethClient,
	)
	if err != nil {
		return fmt.Errorf("vault debt service: %w", err)
	}

	logger.Info("starting vault debt service...",
		"sweepEveryNBlocks", *sweepBlocks,
		"chainID", chainID,
	)

	if err := svc.Start(ctx); err != nil {
		return fmt.Errorf("starting service: %w", err)
	}

	// Block until context is cancelled
	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	var stopErr error
	go func() {
		defer close(done)
		stopErr = svc.Stop()
	}()

	select {
	case <-done:
		if stopErr != nil {
			return fmt.Errorf("stop: %w", stopErr)
		}
		logger.Info("shutdown complete")
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown timeout")
	}

	return nil
}
