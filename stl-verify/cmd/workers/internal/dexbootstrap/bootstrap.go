package dexbootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cache"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/writerrun"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
)

// defaultRepoBatchSize is the batchSize passed to the repository constructors:
// a non-positive value tells them to use DefaultRepositoryConfig()'s sizes.
const defaultRepoBatchSize = 0

// BootstrapOptions specifies the worker-identity bits Bootstrap can't infer.
type BootstrapOptions struct {
	// ServiceName is the OTEL/logger service name, e.g. "curve-dex-worker".
	ServiceName string
	// MetricPrefix is passed to dextelemetry.NewTelemetry, e.g. "curve" → emits
	// curve_blocks_processed_total / curve_errors_total.
	MetricPrefix string
	// BuildTime is the compile-time-baked build timestamp the worker exposes
	// via the buildinfo package.
	BuildTime string
	// GitBranch is the ldflags-injected build branch, logged at startup so an
	// image built from a stray branch is spottable (mirrors the other indexers).
	GitBranch string
}

// Deps is the bundle of long-lived clients + repositories created by
// Bootstrap. The caller must invoke Close() (typically via defer) to release
// every resource in reverse order — skipping Close leaks Redis / Postgres
// connections + leaves OTEL exporters mid-flight.
type Deps struct {
	Logger        *slog.Logger
	SQSConsumer   outbound.SQSConsumer
	CacheReader   outbound.BlockCacheReader
	Multicaller   outbound.Multicaller
	PostgresPool  *pgxpool.Pool
	BuildRegistry *buildregistry.Registry
	RunID         buildregistry.RunID

	TxManager    outbound.TxManager
	ProtocolRepo outbound.ProtocolRepository
	TokenRepo    outbound.TokenRepository
	EventRepo    outbound.EventRepository

	DexTelemetry *dextelemetry.Telemetry

	// cleanups runs registered teardown functions in reverse order on Close().
	cleanups []func()
}

// Close releases every resource in reverse-registration order. Safe to call
// on a partially-initialised Deps from a Bootstrap error path.
func (d *Deps) Close() {
	for _, v := range slices.Backward(d.cleanups) {
		v()
	}
}

// CommonDeps projects the shared outbound ports into the service-layer
// dexconsumer.CommonDeps each DEX worker validates at startup. Mapping them here
// (rather than in each worker) keeps it the single, compiler-checked source of
// truth: a port added to Deps surfaces as a build error here, not a silently
// unmapped field at three call sites.
func (d *Deps) CommonDeps() dexconsumer.CommonDeps {
	return dexconsumer.CommonDeps{
		SQSConsumer:  d.SQSConsumer,
		CacheReader:  d.CacheReader,
		Multicaller:  d.Multicaller,
		TxManager:    d.TxManager,
		TokenRepo:    d.TokenRepo,
		ProtocolRepo: d.ProtocolRepo,
		EventRepo:    d.EventRepo,
	}
}

// Bootstrap performs the wire-up shared by every DEX worker: logger, OTEL init,
// AWS config, SQS consumer, Redis cache, S3 reader, multicall client, Postgres
// pool, build registry, dex telemetry, and the four shared repositories
// (txManager, protocolRepo, tokenRepo, eventRepo).
//
// The returned *Deps owns the lifecycle of everything it returns; on
// success the caller defers Close. On any error mid-setup the partially-
// initialised resources are closed before the error propagates.
func Bootstrap(ctx context.Context, cfg Config, opts BootstrapOptions) (*Deps, error) {
	if opts.ServiceName == "" {
		return nil, fmt.Errorf("dexbootstrap.Bootstrap: ServiceName required")
	}
	if opts.MetricPrefix == "" {
		return nil, fmt.Errorf("dexbootstrap.Bootstrap: MetricPrefix required")
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	d := &Deps{Logger: logger}

	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    opts.ServiceName,
		ServiceVersion: buildinfo.GitHash(),
		BuildTime:      opts.BuildTime,
		Logger:         logger,
	})
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("initializing telemetry: %w", err)
	}
	// OTEL shutdown takes a context; use background so a cancelled ctx during
	// signal-driven teardown doesn't truncate the final metric flush.
	d.cleanups = append(d.cleanups, func() { shutdownOTEL(context.Background()) })

	awsCfg, err := loadAWSConfig(ctx)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	sqsConsumer, err := sqsAdapter.NewConsumer(awsCfg, sqsAdapter.Config{
		QueueURL:          cfg.QueueURL,
		WaitTimeSeconds:   int32(cfg.WaitTime),
		VisibilityTimeout: int32(cfg.VisibilityTimeout),
		BaseEndpoint:      env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating SQS consumer: %w", err)
	}
	d.SQSConsumer = sqsConsumer
	d.cleanups = append(d.cleanups, func() {
		if err := sqsConsumer.Close(); err != nil {
			logger.Warn("closing SQS consumer", "error", err)
		}
	})

	blockCache, err := redisAdapter.NewBlockCache(redisAdapter.Config{
		Addr:      cfg.RedisAddr,
		Password:  env.Get("REDIS_PASSWORD", ""),
		DB:        0,
		TTL:       2 * 24 * time.Hour,
		KeyPrefix: "stl",
	}, logger)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating Redis cache: %w", err)
	}
	d.cleanups = append(d.cleanups, func() {
		if err := blockCache.Close(); err != nil {
			logger.Warn("closing Redis cache", "error", err)
		}
	})
	if err := blockCache.Ping(ctx); err != nil {
		d.Close()
		return nil, fmt.Errorf("connecting to Redis at %s: %w", cfg.RedisAddr, err)
	}
	logger.Info("Redis connected", "addr", cfg.RedisAddr)

	s3Reader := s3adapter.NewReaderFromEnv(awsCfg, logger)
	d.CacheReader, err = cache.NewReaderWithFallback(blockCache, s3Reader, cfg.ChainID, cfg.DeployEnv, cfg.S3Bucket, logger)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating cache reader: %w", err)
	}

	ethClient, err := rpchttp.DialEthereum(ctx, cfg.AlchemyURL)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("connecting to Ethereum node: %w", err)
	}
	d.cleanups = append(d.cleanups, func() { ethClient.Close() })
	logger.Info("Ethereum node connected")

	pool, err := postgres.OpenPool(ctx, postgres.WorkerDBConfig(cfg.DBURL))
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("opening database: %w", err)
	}
	d.PostgresPool = pool
	d.cleanups = append(d.cleanups, func() { pool.Close() })
	logger.Info("PostgreSQL connected")

	buildReg, runID, err := writerrun.Open(ctx, pool)
	if err != nil {
		d.Close()
		return nil, err
	}
	d.BuildRegistry = buildReg
	d.RunID = runID

	logger.Info("starting "+opts.ServiceName,
		"dex", cfg.Dex,
		"queue", cfg.QueueURL,
		"redis", cfg.RedisAddr,
		"chainID", cfg.ChainID,
		"commit", buildReg.GitHash(),
		"branch", opts.GitBranch)

	dexTel, err := dextelemetry.NewTelemetry(opts.MetricPrefix, cfg.ChainID)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating dex telemetry: %w", err)
	}
	d.DexTelemetry = dexTel

	// Resolving the chain name fails hard rather than emitting an empty chain
	// label on multicall_batch_size{chain}.
	chainName, err := entity.ChainName(cfg.ChainID)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("resolving chain name: %w", err)
	}
	mcTel, err := multicall.NewTelemetry(chainName)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating multicall telemetry: %w", err)
	}
	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3, multicall.WithTelemetry(mcTel))
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating multicall client: %w", err)
	}
	d.Multicaller = mc

	d.TxManager, err = postgres.NewTxManager(pool, logger)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating transaction manager: %w", err)
	}
	d.ProtocolRepo, err = postgres.NewProtocolRepository(pool, logger, buildReg.BuildID(), runID, defaultRepoBatchSize)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating protocol repository: %w", err)
	}
	d.TokenRepo, err = postgres.NewTokenRepository(pool, logger, defaultRepoBatchSize)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("creating token repository: %w", err)
	}
	d.EventRepo = postgres.NewEventRepository(logger, buildReg.BuildID(), runID)

	return d, nil
}

// loadAWSConfig delegates to the shared awsconfig.Load helper so DEX workers
// inherit the same eu-west-1 default and AKID-without-secret guard as every
// other worker. Pre-N8-1/N8-2 this function inlined its own version with a
// us-east-1 default and no guard — that divergence is exactly what the
// dedup pass was supposed to eliminate, and the shared helper closes it.
func loadAWSConfig(ctx context.Context) (aws.Config, error) {
	return awsconfig.Load(ctx, awsconfig.Options{StaticCredentialsFromEnv: true})
}
