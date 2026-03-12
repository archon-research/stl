// Command data-export transforms and exports block data from the staging backup S3 bucket
// to a stress-test destination bucket.
//
// This is not a simple copy: staging keys are gzip-compressed and use a versioned key format
// ({partition}/{blockNumber}_{version}_{dataType}.json.gz). The mock blockchain server expects
// plain JSON in a flat key structure ({prefix}/{blockNumber}/{dataType}.json).
// data-export decompresses and re-keys the data into the format the mock server can load.
//
// Usage (local dev — reads from staging, writes to LocalStack):
//
//	go run ./cmd/stress-test/data-export \
//	  --env local \
//	  --src-bucket stl-staging-backup \
//	  --dest-bucket stress-test-data \
//	  --dest-prefix blocks/chain-1 \
//	  --dest-endpoint http://localhost:4566 \
//	  --start-block 21000000 --block-count 500
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/testutil/dataexport"
)

func main() {
	if err := Main(); err != nil {
		slog.Error("data-export failed", "error", err)
		os.Exit(1)
	}
}

// cfg holds the parsed CLI configuration.
type cfg struct {
	env          string
	srcBucket    string
	destBucket   string
	destPrefix   string
	destEndpoint string
	startBlock   int64
	blockCount   int
	version      int
	region       string
}

// Main is the testable entry point.
func Main() error {
	c := cfg{}
	flag.StringVar(&c.env, "env", "", "deployment environment: \"staging\" (real AWS source) or \"local\" (LocalStack destination)")
	flag.StringVar(&c.srcBucket, "src-bucket", "", "source staging backup bucket")
	flag.StringVar(&c.destBucket, "dest-bucket", "", "destination stress-test bucket")
	flag.StringVar(&c.destPrefix, "dest-prefix", "blocks/chain-1", "key prefix for destination bucket")
	flag.StringVar(&c.destEndpoint, "dest-endpoint", "", "LocalStack endpoint (required when --env=local, e.g. http://localhost:4566)")
	flag.Int64Var(&c.startBlock, "start-block", 0, "first block number to export")
	flag.IntVar(&c.blockCount, "block-count", 100, "number of blocks to export")
	flag.IntVar(&c.version, "version", 0, "block data version (default 0)")
	flag.StringVar(&c.region, "region", "eu-west-1", "AWS region")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return run(ctx, logger, c)
}

func run(ctx context.Context, logger *slog.Logger, c cfg) error {
	if c.env != "staging" && c.env != "local" {
		return fmt.Errorf("--env must be \"staging\" or \"local\", got %q", c.env)
	}
	if c.env == "local" && c.destEndpoint == "" {
		return fmt.Errorf("--dest-endpoint is required when --env=local")
	}
	if c.env != "local" && c.destEndpoint != "" {
		return fmt.Errorf("--dest-endpoint is only valid when --env=local; refusing to use LocalStack endpoint against %q environment", c.env)
	}
	if c.srcBucket == "" {
		return fmt.Errorf("--src-bucket is required")
	}
	if c.destBucket == "" {
		return fmt.Errorf("--dest-bucket is required")
	}
	if c.blockCount <= 0 {
		return fmt.Errorf("--block-count must be positive, got %d", c.blockCount)
	}

	// Read config: uses the default credential chain (staging credentials from environment).
	srcCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(c.region))
	if err != nil {
		return fmt.Errorf("loading source AWS config: %w", err)
	}

	// Write config: LocalStack when env=local, otherwise real AWS.
	destCfg, destOptFns, err := buildDestConfig(ctx, c)
	if err != nil {
		return fmt.Errorf("building destination config: %w", err)
	}

	reader := s3adapter.NewReader(srcCfg, logger)
	writer := s3adapter.NewWriterWithOptions(destCfg, logger, destOptFns...)

	exporter := dataexport.NewS3Exporter(
		reader, writer,
		c.srcBucket, c.destBucket, c.destPrefix,
		logger,
	)

	logger.Info("starting export",
		"src_bucket", c.srcBucket,
		"dest_bucket", c.destBucket,
		"dest_prefix", c.destPrefix,
		"start_block", c.startBlock,
		"block_count", c.blockCount,
		"version", c.version,
		"localstack", c.env == "local",
	)

	if err := exporter.ExportRange(ctx, c.startBlock, c.blockCount, c.version); err != nil {
		return fmt.Errorf("export failed: %w", err)
	}

	logger.Info("export complete",
		"blocks_exported", c.blockCount,
		"start_block", c.startBlock,
	)
	return nil
}

// buildDestConfig returns an AWS config and S3 option functions for the destination.
// When env is "local", configures for LocalStack with dummy credentials and path-style addressing.
func buildDestConfig(ctx context.Context, c cfg) (aws.Config, []func(*awss3.Options), error) {
	if c.env == "local" {
		// LocalStack: dummy credentials, custom endpoint, path-style addressing.
		destCfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(c.region),
			config.WithBaseEndpoint(c.destEndpoint),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		)
		if err != nil {
			return aws.Config{}, nil, fmt.Errorf("loading localstack dest config: %w", err)
		}
		optFns := []func(*awss3.Options){
			func(o *awss3.Options) { o.UsePathStyle = true },
		}
		return destCfg, optFns, nil
	}

	// Real AWS: use default credential chain.
	destCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(c.region))
	if err != nil {
		return aws.Config{}, nil, fmt.Errorf("loading dest config: %w", err)
	}
	return destCfg, nil, nil
}
