// Package main consumes Ethereum block events from SQS, reads on-chain debt
// for each prime agent vault every N blocks, and writes append-only snapshots
// to Postgres.
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

	vatAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/prime_debt"
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
		slog.Error("prime-debt-indexer exited with error", "error", err)
		os.Exit(1)
	}
}

// maskRPCURL redacts the path (which typically contains API keys) from an RPC URL.
// Example: "https://eth-mainnet.g.alchemy.com/v2/abc123" → "https://eth-mainnet.g.alchemy.com/***"
func maskRPCURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "***"
	}
	return fmt.Sprintf("%s://%s/***", u.Scheme, u.Host)
}

// run is the entry point for the prime-debt-indexer.
// It is extracted from main() to allow integration testing.
func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("prime-debt-indexer", flag.ContinueOnError)
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

	logger.Info("starting prime-debt-indexer",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
		"chainID", chainID,
	)

	// OpenTelemetry
	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "prime-debt-indexer",
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
	vatCaller, err := vatAdapter.NewVatCaller(mc, common.HexToAddress(*vatAddr))
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
	svc, err := prime_debt.NewVaultDebtService(
		prime_debt.Config{
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
		return fmt.Errorf("prime debt indexer: %w", err)
	}

	logger.Info("starting prime debt indexer...",
		"sweepEveryNBlocks", *sweepBlocks,
		"chainID", chainID,
	)

	return lifecycle.Run(ctx, logger, svc)
}
