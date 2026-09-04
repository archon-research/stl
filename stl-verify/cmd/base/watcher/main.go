// Package main provides a test application for the Watcher service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// Build-time variables - can be set via ldflags, otherwise populated from Go's build info
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

// defaultServiceName is the OTEL service.name used when no chain-specific
// override is provided via the environment (e.g. local dev, tests).
const defaultServiceName = "stl-watcher"

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	// Parse command-line flags
	enableTraces := flag.Bool("enable-traces", true, "Enable fetching execution traces (trace_block)")
	enableBlobs := flag.Bool("enable-blobs", false, "Enable fetching blob sidecars")
	parallelRPC := flag.Bool("parallel-rpc", true, "Use parallel goroutines for RPC calls instead of batching (faster but uses more credits)")
	pprofAddr := flag.String("pprof", "", "Enable pprof profiling server (e.g., ':6060')")
	traceFile := flag.String("trace", "", "Write execution trace to file")
	showVersion := flag.Bool("version", false, "Show version information and exit")
	flag.Parse()

	// Show version if requested
	if *showVersion {
		fmt.Printf("stl-watcher\n")
		fmt.Printf("  Commit:     %s\n", GitCommit)
		fmt.Printf("  Branch:     %s\n", GitBranch)
		fmt.Printf("  Build Time: %s\n", BuildTime)
		os.Exit(0)
	}

	ctx, stop := lifecycle.SignalContext(context.Background())

	// main holds no defers, so the os.Exit below cannot strand cleanup; every
	// deferred close lives in run and has already unwound by this point.
	err := run(ctx, cliOptions{
		enableTraces:      *enableTraces,
		enableBlobs:       *enableBlobs,
		parallelRPC:       *parallelRPC,
		pprofAddr:         *pprofAddr,
		traceFile:         *traceFile,
		onShutdownTimeout: lifecycle.ForceExitAfter(cleanupTimeout),
	})
	stop()
	if err != nil {
		slog.Error("stl-watcher exited with error", "error", err)
		os.Exit(1)
	}
}

// cleanupTimeout bounds the deferred closes that run after a shutdown timeout.
// pgxpool.Close blocks until every acquired connection is handed back, and the
// goroutines still holding them are the ones that just missed
// lifecycle.ShutdownTimeout. Together the two fit inside the pod's 60s
// terminationGracePeriodSeconds (k8s/base/watcher/deployment.yaml).
const cleanupTimeout = 15 * time.Second

type cliOptions struct {
	pprofAddr string
	traceFile string

	// onShutdownTimeout bounds the cleanup that follows a shutdown timeout.
	// main supplies the process-killing one; tests leave it nil, so a timeout
	// fails the test instead of taking the whole test binary down with it.
	onShutdownTimeout func()

	enableTraces bool
	enableBlobs  bool
	parallelRPC  bool
}

// watcherConfig is the env-driven configuration, read and validated before any
// connection is opened so a missing variable cannot cost a pool and a Redis
// handshake first.
type watcherConfig struct {
	alchemyAPIKey  string
	alchemyHTTPURL string
	alchemyWSURL   string
	chainName      string
	postgresURL    string
	redisAddr      string
	redisPassword  string
	snsEndpoint    string
	snsTopicARN    string
	chainID        int64
	enableBackfill bool
}

// dependencies are the outbound adapters the services are built from. The three
// that need closing (cache, eventSink, and the pool behind blockState) are
// opened and deferred by run; the rest hold no resource of their own.
type dependencies struct {
	subscriber *alchemy.Subscriber
	client     *alchemy.Client
	blockState *postgres.BlockStateRepository
	cache      *rediscache.BlockCache
	eventSink  *snsadapter.EventSink
	metrics    *shared.ServiceTelemetry
}

func run(ctx context.Context, opts cliOptions) (err error) {
	if opts.traceFile != "" {
		stopTrace, terr := startTrace(opts.traceFile)
		if terr != nil {
			return terr
		}
		defer func() { err = errors.Join(err, stopTrace()) }()
	}

	// Derived so an early return cancels anything run started, independently of
	// whether the parent context is still live.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	logger.Info("starting stl-watcher",
		"commit", GitCommit,
		"branch", GitBranch,
		"buildTime", BuildTime,
	)

	startPprofServer(opts.pprofAddr, logger)

	// ServiceName is resolved from the environment so that each per-chain
	// watcher deployment (arbitrum-watcher, base-watcher, etc.) reports a
	// distinct service.name in Prometheus/Tempo, instead of all collapsing
	// into a single "stl-watcher" time series.
	shutdownOTEL, err := telemetry.InitOTEL(ctx, telemetry.OTELConfig{
		ServiceName:    resolveServiceName(os.Getenv),
		ServiceVersion: GitCommit,
		BuildTime:      BuildTime,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("initializing telemetry: %w", err)
	}
	defer shutdownOTEL(context.Background())

	cfg, err := loadWatcherConfig()
	if err != nil {
		return err
	}

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.postgresURL))
	if err != nil {
		return fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected, block state tracking enabled")

	cache, err := openRedisCache(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := cache.Close(); err != nil {
			logger.Warn("failed to close Redis connection", "error", err)
		}
	}()
	logger.Info("Redis cache connected", "addr", cfg.redisAddr)

	eventSink, err := openEventSink(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := eventSink.Close(); err != nil {
			logger.Error("failed to close SNS event sink", "error", err)
		}
	}()
	logger.Info("SNS event sink created", "endpoint", cfg.snsEndpoint, "topic", cfg.snsTopicARN)

	deps, err := openDependencies(cfg, opts, pool, cache, eventSink, logger)
	if err != nil {
		return err
	}

	live, backfill, err := newServices(cfg, opts, deps, logger)
	if err != nil {
		return err
	}

	return serveUntilShutdown(ctx, live, backfill, opts.onShutdownTimeout, logger)
}

// startTrace begins an execution trace into path and returns the stop function
// that flushes and closes it.
func startTrace(path string) (func() error, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating trace file: %w", err)
	}
	if err := trace.Start(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("starting trace: %w", err)
	}
	return func() error {
		trace.Stop()
		// Close reports the flush failure that would otherwise leave a
		// silently truncated trace behind.
		if err := f.Close(); err != nil {
			return fmt.Errorf("closing trace file: %w", err)
		}
		return nil
	}, nil
}

// startPprofServer serves pprof on addr in the background, and is a no-op when
// addr is empty. Block and mutex profiling are global rates, so they are only
// turned on when someone is there to read them.
func startPprofServer(addr string, logger *slog.Logger) {
	if addr == "" {
		return
	}
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	runtime.SetCPUProfileRate(1)

	go func() {
		logger.Info("starting pprof server", "addr", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			logger.Error("pprof server failed", "error", err)
		}
	}()
}

func loadWatcherConfig() (watcherConfig, error) {
	apiKey, err := env.Require("ALCHEMY_API_KEY")
	if err != nil {
		return watcherConfig{}, err
	}
	chainIDStr, err := env.Require("CHAIN_ID")
	if err != nil {
		return watcherConfig{}, err
	}
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return watcherConfig{}, fmt.Errorf("CHAIN_ID must be a valid integer: %w", err)
	}
	// The chain name becomes the `chain` metric label; an unknown CHAIN_ID is a
	// misconfiguration that would silently emit an empty chain, so fail hard
	// like the parse above.
	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return watcherConfig{}, fmt.Errorf("resolving chain name for metrics: %w", err)
	}
	snsTopicARN, err := env.Require("AWS_SNS_TOPIC_ARN")
	if err != nil {
		return watcherConfig{}, err
	}
	return watcherConfig{
		alchemyAPIKey:  apiKey,
		alchemyHTTPURL: env.Get("ALCHEMY_HTTP_URL", "https://eth-mainnet.g.alchemy.com/v2"),
		alchemyWSURL:   env.Get("ALCHEMY_WS_URL", "wss://eth-mainnet.g.alchemy.com/v2"),
		chainName:      chainName,
		postgresURL:    env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"),
		redisAddr:      env.Get("REDIS_ADDR", "localhost:6379"),
		redisPassword:  env.Get("REDIS_PASSWORD", ""),
		snsEndpoint:    env.Get("AWS_SNS_ENDPOINT", "http://localhost:4566"),
		snsTopicARN:    snsTopicARN,
		chainID:        chainID,
		enableBackfill: env.Get("ENABLE_BACKFILL", "false") == "true",
	}, nil
}

func openRedisCache(ctx context.Context, cfg watcherConfig, logger *slog.Logger) (*rediscache.BlockCache, error) {
	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      cfg.redisAddr,
		Password:  cfg.redisPassword,
		DB:        0,
		TTL:       2 * time.Hour,
		KeyPrefix: "stl",
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("creating Redis cache: %w", err)
	}
	if err := cache.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connecting to Redis at %s: %w", cfg.redisAddr, err)
	}
	return cache, nil
}

func openEventSink(ctx context.Context, cfg watcherConfig, logger *slog.Logger) (*snsadapter.EventSink, error) {
	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{
		StaticCredentialsFromEnv: true,
	})
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
		logger.Info("using static AWS credentials from environment")
	} else {
		logger.Info("using default AWS credential chain (IAM role / instance profile)")
	}

	// Custom endpoint so the same binary talks to LocalStack in tests.
	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		if cfg.snsEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.snsEndpoint)
		}
	})

	eventSink, err := snsadapter.NewEventSink(snsClient, snsadapter.Config{
		TopicARN: cfg.snsTopicARN,
		Logger:   logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating SNS event sink: %w", err)
	}
	return eventSink, nil
}

// openDependencies builds the adapters that need no closing on top of the three
// run already owns.
func openDependencies(
	cfg watcherConfig,
	opts cliOptions,
	pool *pgxpool.Pool,
	cache *rediscache.BlockCache,
	eventSink *snsadapter.EventSink,
	logger *slog.Logger,
) (dependencies, error) {
	// Instrument construction fails on a bad instrument definition, not on a
	// transient condition, so continuing here would mean running blind forever.
	alchemyTelemetry, err := alchemy.NewTelemetry(cfg.chainName)
	if err != nil {
		return dependencies{}, fmt.Errorf("creating alchemy telemetry: %w", err)
	}

	subscriber, err := alchemy.NewSubscriber(alchemy.SubscriberConfig{
		WebSocketURL:      fmt.Sprintf("%s/%s", cfg.alchemyWSURL, cfg.alchemyAPIKey),
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        30 * time.Second,
		PingInterval:      30 * time.Second,
		PongTimeout:       10 * time.Second,
		ReadTimeout:       60 * time.Second,
		ChannelBufferSize: 100,
		HealthTimeout:     30 * time.Second,
		Logger:            logger,
		Telemetry:         alchemyTelemetry,
	})
	if err != nil {
		return dependencies{}, fmt.Errorf("creating subscriber: %w", err)
	}

	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:      fmt.Sprintf("%s/%s", cfg.alchemyHTTPURL, cfg.alchemyAPIKey),
		EnableTraces: opts.enableTraces,
		EnableBlobs:  opts.enableBlobs,
		ParallelRPC:  opts.parallelRPC,
		Logger:       logger,
		Telemetry:    alchemyTelemetry,
	})
	if err != nil {
		return dependencies{}, fmt.Errorf("creating client: %w", err)
	}
	logger.Info("alchemy client configured",
		"enableTraces", opts.enableTraces,
		"enableBlobs", opts.enableBlobs,
		"parallelRPC", opts.parallelRPC,
		"chainID", cfg.chainID,
	)

	// One recorder shared by live and backfill, wiring ReorgRecorder
	// (LiveConfig.Metrics) and BackfillRecorder (BackfillConfig.Metrics) onto
	// the OTel global meter provider initialised above.
	serviceTelemetry, err := shared.NewServiceTelemetry()
	if err != nil {
		return dependencies{}, fmt.Errorf("creating service telemetry: %w", err)
	}

	return dependencies{
		subscriber: subscriber,
		client:     client,
		blockState: postgres.NewBlockStateRepository(pool, cfg.chainID, logger),
		cache:      cache,
		eventSink:  eventSink,
		metrics:    serviceTelemetry,
	}, nil
}

// newServices returns the live service and, when ENABLE_BACKFILL is set, the
// backfill service. A nil backfill service means the feature is off.
func newServices(cfg watcherConfig, opts cliOptions, deps dependencies, logger *slog.Logger) (*live_data.LiveService, *backfill_gaps.BackfillService, error) {
	live, err := live_data.NewLiveService(
		live_data.LiveConfig{
			ChainID:            cfg.chainID,
			FinalityBlockCount: 64,
			EnableTraces:       opts.enableTraces,
			EnableBlobs:        opts.enableBlobs,
			Logger:             logger,
			Metrics:            deps.metrics,
		},
		deps.subscriber,
		deps.client,
		deps.blockState,
		deps.cache,
		deps.eventSink,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating live service: %w", err)
	}

	if !cfg.enableBackfill {
		return live, nil, nil
	}

	backfillConfig, err := loadBackfillConfig(cfg.chainID, opts.enableTraces, opts.enableBlobs, logger, deps.metrics)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid backfill config: %w", err)
	}
	logger.Info("backfill config",
		"chainID", backfillConfig.ChainID,
		"batchSize", backfillConfig.BatchSize,
		"pollInterval", backfillConfig.PollInterval,
	)

	backfill, err := backfill_gaps.NewBackfillService(
		backfillConfig,
		deps.client,
		deps.blockState,
		deps.cache,
		deps.eventSink,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating backfill service: %w", err)
	}
	return live, backfill, nil
}

// serveUntilShutdown runs the services until ctx is cancelled. On a shutdown
// timeout it calls onShutdownTimeout before returning, because run's deferred
// closes wait on the same goroutines that just missed the deadline — returning
// the error is not by itself enough to bound process exit.
func serveUntilShutdown(
	ctx context.Context,
	live *live_data.LiveService,
	backfill *backfill_gaps.BackfillService,
	onShutdownTimeout func(),
	logger *slog.Logger,
) error {
	services := []lifecycle.Service{live}
	if backfill != nil {
		services = append(services, backfill)
	}
	logger.Info("starting services...", "backfill", backfill != nil)

	err := lifecycle.Run(ctx, logger, services...)
	if errors.Is(err, lifecycle.ErrShutdownTimedOut) && onShutdownTimeout != nil {
		onShutdownTimeout()
	}
	return err
}

// resolveServiceName returns the OTEL service.name for this watcher process.
//
// In k8s, each per-chain watcher Deployment injects SERVICE_NAME via the
// downward API (sourced from the pod's `app` label, e.g. "arbitrum-watcher"),
// so each chain reports a distinct service.name in Prometheus and Tempo.
//
// Resolution order:
//  1. SERVICE_NAME           (explicit, set by k8s downward API)
//  2. OTEL_SERVICE_NAME      (standard OTEL env var, honoured if a user sets it)
//  3. defaultServiceName     ("stl-watcher") for local dev / tests
//
// Whitespace is trimmed; empty values are treated as unset.
func resolveServiceName(getenv func(string) string) string {
	for _, key := range []string{"SERVICE_NAME", "OTEL_SERVICE_NAME"} {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
	}
	return defaultServiceName
}

// loadBackfillConfig reads the env-driven backfill knobs. Defaults preserve the
// historic 10 blocks / 30s behaviour for any chain that doesn't override them.
// Non-positive values are rejected: time.NewTicker panics on d <= 0, and a
// negative BatchSize would feed back into SQL LIMIT and gap-fill arithmetic.
//
// Env vars:
//   - BACKFILL_BATCH_SIZE      (int,      default 10)
//   - BACKFILL_POLL_INTERVAL   (duration, default 30s)
//   - BACKFILL_RETRY_MIN_AGE   (duration, default 30s)
func loadBackfillConfig(chainID int64, enableTraces, enableBlobs bool, logger *slog.Logger, metrics *shared.ServiceTelemetry) (backfill_gaps.BackfillConfig, error) {
	batchSize, err := env.GetInt("BACKFILL_BATCH_SIZE", 10)
	if err != nil {
		return backfill_gaps.BackfillConfig{}, err
	}
	if batchSize <= 0 {
		return backfill_gaps.BackfillConfig{}, fmt.Errorf("BACKFILL_BATCH_SIZE must be > 0, got %d", batchSize)
	}
	pollInterval, err := env.GetDuration("BACKFILL_POLL_INTERVAL", 30*time.Second)
	if err != nil {
		return backfill_gaps.BackfillConfig{}, err
	}
	if pollInterval <= 0 {
		return backfill_gaps.BackfillConfig{}, fmt.Errorf("BACKFILL_POLL_INTERVAL must be > 0, got %s", pollInterval)
	}
	retryMinAge, err := env.GetDuration("BACKFILL_RETRY_MIN_AGE", 30*time.Second)
	if err != nil {
		return backfill_gaps.BackfillConfig{}, err
	}
	if retryMinAge <= 0 {
		return backfill_gaps.BackfillConfig{}, fmt.Errorf("BACKFILL_RETRY_MIN_AGE must be > 0, got %s", retryMinAge)
	}
	return backfill_gaps.BackfillConfig{
		ChainID:      chainID,
		BatchSize:    batchSize,
		PollInterval: pollInterval,
		RetryMinAge:  retryMinAge,
		EnableTraces: enableTraces,
		EnableBlobs:  enableBlobs,
		Logger:       logger,
		Metrics:      metrics,
	}, nil
}
