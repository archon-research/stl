// Package main consumes per-chain block events from SQS, reads Spark PSM3
// reserve state every N blocks pinned to the event's block, and writes
// append-only snapshots to Postgres.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	psm3Adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/psm3"
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
		slog.Error("psm3-indexer exited with error", "error", err)
		os.Exit(1)
	}
}

// run is the entry point for the psm3-indexer.
// It is extracted from main() to allow integration testing.
func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("psm3-indexer", flag.ContinueOnError)
	dbURL := fs.String("db", env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"), "PostgreSQL connection string")
	rpcURL := fs.String("rpc", env.Get("ETH_RPC_URL", ""), "Ethereum JSON-RPC endpoint (e.g. https://base-mainnet.g.alchemy.com/v2/<key>)")
	queueURL := fs.String("queue", env.Get("AWS_SQS_QUEUE_URL", ""), "SQS Queue URL")
	sweepBlocks := fs.Int("sweep-blocks", 300, "Read PSM3 state every N blocks")
	visibilityTimeout := fs.Int("visibility-timeout", 300, "SQS visibility timeout in seconds")
	waitTime := fs.Int("wait", 20, "SQS wait time in seconds (long polling)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	// Env vars are fallbacks only; an explicitly-set flag wins over its env var.
	setFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })

	if *rpcURL == "" {
		// Fallback: compose from ALCHEMY_HTTP_URL + ALCHEMY_API_KEY env vars.
		alchemyHTTPURL := env.Get("ALCHEMY_HTTP_URL", "")
		if alchemyHTTPURL == "" {
			return fmt.Errorf("RPC endpoint not provided (use -rpc flag, ETH_RPC_URL or ALCHEMY_HTTP_URL+ALCHEMY_API_KEY env vars)")
		}
		*rpcURL = fmt.Sprintf("%s/%s", alchemyHTTPURL, env.Get("ALCHEMY_API_KEY", ""))
	}

	if *queueURL == "" {
		return fmt.Errorf("queue URL not provided (use -queue flag or AWS_SQS_QUEUE_URL env var)")
	}

	chainIDStr := env.Get("CHAIN_ID", "")
	if chainIDStr == "" {
		return fmt.Errorf("CHAIN_ID env var not provided")
	}
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing CHAIN_ID %q: %w", chainIDStr, err)
	}

	if waitTimeStr := env.Get("SQS_WAIT_TIME", ""); waitTimeStr != "" && !setFlags["wait"] {
		v, err := strconv.Atoi(waitTimeStr)
		if err != nil {
			return fmt.Errorf("parsing SQS_WAIT_TIME %q: %w", waitTimeStr, err)
		}
		*waitTime = v
	}
	if visTimeStr := env.Get("SQS_VISIBILITY_TIMEOUT", ""); visTimeStr != "" && !setFlags["visibility-timeout"] {
		v, err := strconv.Atoi(visTimeStr)
		if err != nil {
			return fmt.Errorf("parsing SQS_VISIBILITY_TIMEOUT %q: %w", visTimeStr, err)
		}
		*visibilityTimeout = v
	}
	if sweepBlocksStr := env.Get("SWEEP_BLOCKS", ""); sweepBlocksStr != "" && !setFlags["sweep-blocks"] {
		v, err := strconv.Atoi(sweepBlocksStr)
		if err != nil {
			return fmt.Errorf("parsing SWEEP_BLOCKS %q: %w", sweepBlocksStr, err)
		}
		*sweepBlocks = v
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	// Per-chain PSM3 addresses, cross-checked against axis-synome so the two
	// registries cannot drift silently.
	psm3Cfg, err := psm3Adapter.PSM3ConfigForChain(chainID)
	if err != nil {
		return fmt.Errorf("psm3 config: %w", err)
	}
	axisSynome, err := axis_synome_contract.LoadDefaultContract()
	if err != nil {
		return fmt.Errorf("load axis-synome contract: %w", err)
	}
	if err := psm3Cfg.ValidateAgainstAxisSynome(axisSynome, chainID); err != nil {
		return fmt.Errorf("validate psm3 config against axis-synome: %w", err)
	}

	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{
		StaticCredentialsFromEnv: true,
	})
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

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		return fmt.Errorf("registering build: %w", err)
	}

	logger.Info("starting psm3-indexer",
		"commit", buildReg.GitHash(),
		"branch", GitBranch,
		"buildTime", BuildTime,
		"chainID", chainID,
		"psm3", psm3Cfg.PSM3.Hex(),
	)

	// OpenTelemetry
	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    "psm3-indexer",
		ServiceVersion: buildReg.GitHash(),
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}
	defer shutdownOTEL(context.Background())

	// Ethereum JSON-RPC client
	ethClient, err := rpchttp.DialEthereum(ctx, *rpcURL)
	if err != nil {
		return fmt.Errorf("eth rpc dial: %w", err)
	}
	defer ethClient.Close()
	logger.Info("eth rpc client connected", "rpc", rpchttp.MaskURL(*rpcURL))

	// Multicaller
	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}

	// PSM3 caller (backed by multicall)
	psm3Caller, err := psm3Adapter.NewPSM3Caller(mc, psm3Cfg)
	if err != nil {
		return fmt.Errorf("psm3 caller: %w", err)
	}

	// Reserves repository
	txm, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("tx manager: %w", err)
	}
	reservesRepo := postgres.NewPSM3ReservesRepository(txm, logger, buildReg.BuildID())

	// PSM3 service
	svc, err := psm3.NewService(
		psm3.Config{
			SweepEveryNBlocks: *sweepBlocks,
			ChainID:           chainID,
			PSM3Address:       psm3Cfg.PSM3,
			MaxMessages:       10,
			PollInterval:      100 * time.Millisecond,
			Logger:            logger,
		},
		psm3Caller,
		reservesRepo,
		sqsConsumer,
		ethClient,
	)
	if err != nil {
		return fmt.Errorf("psm3 indexer: %w", err)
	}

	logger.Info("starting psm3 indexer...",
		"sweepEveryNBlocks", *sweepBlocks,
		"chainID", chainID,
	)

	return lifecycle.Run(ctx, logger, svc)
}
