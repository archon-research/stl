// Command block-meta-loader fills the block_meta dimension for ONE chain from that chain's S3
// raw-block archive (the authoritative block-header timestamp). Run one per chain, out of band.
//
// Env: DATABASE_URL (write role), CHAIN_ID, S3_BUCKET (that chain's raw-block bucket), DEPLOY_ENV.
//
// STATUS: first-draft scaffold — wiring mirrors cmd/backfillers/morpho-vault-indexer. CI is the
// compile/lint gate (Go is not in the dev env). TODOs inline.
package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/services/block_meta_loader"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(context.Background(), logger); err != nil {
		logger.Error("block-meta-loader failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	dsn := os.Getenv("DATABASE_URL")
	bucket := os.Getenv("S3_BUCKET")
	env := os.Getenv("DEPLOY_ENV")
	chainID, err := strconv.ParseInt(os.Getenv("CHAIN_ID"), 10, 64)
	if err != nil {
		return err
	}
	// Guard against pointing at the wrong chain's archive (same check raw-data-backup uses at startup).
	if err := chainutil.ValidateS3BucketForChain(chainID, bucket, env); err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	reader := s3.NewReader(awsCfg, logger)

	svc, err := block_meta_loader.New(block_meta_loader.Config{
		ChainID: chainID,
		Bucket:  bucket,
	}, pool, reader, logger)
	if err != nil {
		return err
	}
	loaded, err := svc.Run(ctx)
	if err != nil {
		return err
	}
	logger.Info("block-meta-loader done", "chain", chainID, "rows", loaded)
	return nil
}
