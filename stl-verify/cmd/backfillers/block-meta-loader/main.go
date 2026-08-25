// Command block-meta-loader fills the block_meta dimension for ONE chain from that chain's S3
// raw-block archive (the authoritative block-header timestamp). It is a run-to-completion job:
// it exits once no referenced block is missing from block_meta. Run one per chain, out of band.
//
// Env: DATABASE_URL (write role), CHAIN_ID, S3_BUCKET (that chain's raw-block bucket), DEPLOY_ENV.
// Optional: AWS_REGION (default eu-west-1), AWS_ENDPOINT_URL (LocalStack), BATCH_SIZE, LOG_LEVEL.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/block_meta_loader"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)
	if err := run(context.Background(), logger); err != nil {
		logger.Error("block-meta-loader failed", "error", err)
		os.Exit(1)
	}
	logger.Info("block-meta-loader completed successfully")
}

func run(parent context.Context, logger *slog.Logger) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return fmt.Errorf("S3_BUCKET is required")
	}
	deployEnv := os.Getenv("DEPLOY_ENV")
	chainID, err := strconv.ParseInt(os.Getenv("CHAIN_ID"), 10, 64)
	if err != nil {
		return fmt.Errorf("CHAIN_ID: %w", err)
	}
	batchSize := 0
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if batchSize, err = strconv.Atoi(v); err != nil {
			return fmt.Errorf("BATCH_SIZE: %w", err)
		}
	}

	// Guard against pointing at the wrong chain's archive (same check raw-data-backup uses at startup).
	if err := chainutil.ValidateS3BucketForChain(chainID, bucket, deployEnv); err != nil {
		return err
	}

	// Graceful shutdown: SIGINT/SIGTERM cancels the context, which the loader checks between batches.
	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "eu-west-1"
	}
	awsOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(awsRegion)}
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		awsOpts = append(awsOpts, awsconfig.WithBaseEndpoint(endpoint))
		logger.Info("using custom AWS endpoint", "url", endpoint)
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	// A custom endpoint (LocalStack) needs path-style addressing; virtual-hosted URLs won't resolve.
	var reader *s3adapter.Reader
	if os.Getenv("AWS_ENDPOINT_URL") != "" {
		reader = s3adapter.NewReaderWithOptions(awsCfg, logger, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	} else {
		reader = s3adapter.NewReader(awsCfg, logger)
	}

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(dsn))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	repo, err := postgres.NewBlockMetaRepository(pool, logger)
	if err != nil {
		return fmt.Errorf("creating block_meta repository: %w", err)
	}

	svc, err := block_meta_loader.New(block_meta_loader.Config{
		ChainID:   chainID,
		Bucket:    bucket,
		BatchSize: batchSize,
	}, repo, reader, logger)
	if err != nil {
		return err
	}

	logger.Info("starting block-meta-loader", "chain", chainID, "bucket", bucket)
	loaded, err := svc.Run(ctx)
	if err != nil {
		return err
	}
	logger.Info("block-meta-loader done", "chain", chainID, "rows", loaded)
	return nil
}
