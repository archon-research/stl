// Package main implements an on-demand Temporal worker that re-publishes
// already-mined blocks under a new version, so every consumer of the chain's
// block feed appends them as a correction.
//
// # What it heals
//
// A height whose only published version is a losing fork (ARCT-379): the
// canonical broadcast was dropped as a stale fork, the next block's reorg commit
// orphaned the height without replacing it, and nothing re-fetched it. S3 holds
// only that height's _0_ objects and every indexer holds its events at
// block_version 0. Nothing else in the pipeline can repair it — the watcher's
// retryBlockPublish only reaches non-orphaned block_states rows, which the
// 30-day retention has already dropped, and raw-block-bulk-downloader repairs S3
// alone, without telling the indexers.
//
// # How to start a run
//
// This carries no schedule: the worker idles on its task queue, so deploying it
// never starts a run. An operator supplies the heights and the target version:
//
//	temporal workflow start --namespace vector \
//	  --task-queue block-republisher --type BlockRepublish \
//	  --workflow-id block-republisher-<date> \
//	  --input '{"blocks":[25395651,25087888],"version":1}'
//
// The chain comes from CHAIN_ID, not from the input. version defaults to 1 and
// must be at least 1: version 0 is the slot holding the data being corrected.
//
// # What it does not write
//
// block_states. Its assign_block_version trigger overwrites whatever version the
// caller supplies with MAX(version)+1 over the rows surviving at that height —
// after the 30-day retention there are none, so it would hand back 0 and
// re-stamp the losing fork's own slot. Writing there would also hand the
// watcher's gap filler an unpublished row to re-publish and pin its backfill
// watermark behind the repaired height. The durable record of a republish is
// therefore the SNS event, the <number>_<version>_* objects the raw-data-backup
// worker writes from it, and the block_version rows the indexers append.
//
// # Idempotency
//
// A repeat of the same (chain, number, hash, version) is a no-op: inside SNS
// FIFO's five-minute deduplication window the event never reaches the queues,
// and outside it every append-only consumer re-derives the same rows from
// identical data.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	snsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/services/block_republish"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := run(ctx)
	cancel()
	if err != nil {
		slog.Error("block-republisher exited with error", "error", err)
		os.Exit(1)
	}
}

// Build metadata, populated from VCS in init() (GitBranch is set at link time).
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

const (
	// taskQueueName is the Temporal task queue an operator starts a run on, and
	// also the OTel service name the vector-cronjobs alerts select by.
	taskQueueName = "block-republisher"

	// workflowTypeName is what an operator types into the Temporal UI's "Workflow
	// Type" field, so it is registered explicitly rather than derived from the Go
	// function name — a rename must not invalidate the runbook or muscle memory.
	workflowTypeName = "BlockRepublish"
)

// run declares no OpenDatabase: this worker touches no Postgres.
func run(ctx context.Context) error {
	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:     taskQueueName,
		Register: register,
	})
}

func register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	activities, err := newRepublishActivities(ctx, deps.Logger, cfg)
	if err != nil {
		return fmt.Errorf("wiring the republish activity: %w", err)
	}

	r.RegisterWorkflowWithOptions(republishWorkflow, workflow.RegisterOptions{Name: workflowTypeName})
	r.RegisterActivityWithOptions(activities.RepublishBlock, activity.RegisterOptions{Name: republishActivityName})
	return nil
}

func newRepublishActivities(ctx context.Context, logger *slog.Logger, cfg config) (*republishActivities, error) {
	chainName, err := entity.ChainName(cfg.chainID)
	if err != nil {
		return nil, fmt.Errorf("resolving the chain name for telemetry: %w", err)
	}

	client, err := newChainClient(chainName, cfg, logger)
	if err != nil {
		return nil, err
	}
	cache, err := openBlockCache(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}
	sink, err := openEventSink(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}

	service, err := block_republish.NewService(block_republish.Config{
		ChainID:      cfg.chainID,
		EnableTraces: cfg.enableTraces,
		EnableBlobs:  cfg.enableBlobs,
		Logger:       logger,
	}, client, cache, sink)
	if err != nil {
		return nil, fmt.Errorf("creating the block republish service: %w", err)
	}

	logger.Info("block-republisher configured",
		"chainID", cfg.chainID, "chain", chainName, "environment", cfg.deployEnv,
		"topic", cfg.snsTopicARN, "redis", cfg.redisAddr,
		"enableTraces", cfg.enableTraces, "enableBlobs", cfg.enableBlobs)

	return &republishActivities{service: service}, nil
}

// newChainClient builds the same batched RPC client the watcher fetches a block
// with, so a republished payload is byte-for-byte the shape a live one has.
func newChainClient(chainName string, cfg config, logger *slog.Logger) (*alchemy.Client, error) {
	telemetry, err := alchemy.NewTelemetry(chainName)
	if err != nil {
		return nil, fmt.Errorf("creating alchemy telemetry: %w", err)
	}
	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:      cfg.rpcURL,
		EnableTraces: cfg.enableTraces,
		EnableBlobs:  cfg.enableBlobs,
		// Batched, not parallel: a repair run has no latency budget, and it shares
		// the Alchemy key with the live watcher.
		ParallelRPC: false,
		Logger:      logger,
		Telemetry:   telemetry,
	})
	if err != nil {
		return nil, fmt.Errorf("creating the RPC client: %w", err)
	}
	return client, nil
}

// openBlockCache dials Redis at startup rather than on the first run, so an
// unreachable cache shows up as a worker that will not start instead of as a run
// an operator has to babysit.
func openBlockCache(ctx context.Context, cfg config, logger *slog.Logger) (*rediscache.BlockCache, error) {
	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      cfg.redisAddr,
		Password:  cfg.redisPassword,
		DB:        0,
		TTL:       cacheTTL,
		KeyPrefix: cfg.redisKeyPrefix,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("creating the Redis cache: %w", err)
	}
	if err := cache.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connecting to Redis at %s: %w", cfg.redisAddr, err)
	}
	return cache, nil
}

// publishTimeout bounds one SNS publish. The activity's own StartToClose is
// minutes, sized for a slow archive read, so without this a wedged publish would
// look like a slow block.
const publishTimeout = 30 * time.Second

func openEventSink(ctx context.Context, cfg config, logger *slog.Logger) (*snsadapter.EventSink, error) {
	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{StaticCredentialsFromEnv: true})
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	// Custom endpoint so the same binary talks to LocalStack in kind and in tests.
	snsClient := awssns.NewFromConfig(awsCfg, func(o *awssns.Options) {
		if cfg.snsEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.snsEndpoint)
		}
	})

	sink, err := snsadapter.NewEventSink(snsClient, snsadapter.Config{
		TopicARN:       cfg.snsTopicARN,
		PublishTimeout: publishTimeout,
		Logger:         logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating the SNS event sink: %w", err)
	}
	return sink, nil
}
