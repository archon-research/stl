package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/gziputil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.BlockCacheReader = (*BlockCacheReaderWithFallback)(nil)

// BlockCacheReaderWithFallback implements BlockCacheReader by first checking Redis and
// falling back to S3 on a cache miss. Chain isolation is achieved via the
// bucket name — each chain has its own dedicated bucket, so S3 keys do not
// include the chain ID.
type BlockCacheReaderWithFallback struct {
	redis   outbound.BlockCacheReader
	s3      outbound.S3Reader
	chainID int64
	bucket  string
	logger  *slog.Logger
}

// NewReaderWithFallback creates a BlockCacheReaderWithFallback. It validates that all
// dependencies are non-nil, that the bucket is non-empty, and that the bucket
// name has the correct prefix for the given chain ID and environment
// (format: stl-sentinel{environment}-{chainName}-raw).
func NewReaderWithFallback(redis outbound.BlockCacheReader, s3 outbound.S3Reader, chainID int64, environment string, bucket string, logger *slog.Logger) (*BlockCacheReaderWithFallback, error) {
	if redis == nil {
		return nil, fmt.Errorf("redis reader must not be nil")
	}
	if s3 == nil {
		return nil, fmt.Errorf("s3 reader must not be nil")
	}
	if bucket == "" {
		return nil, fmt.Errorf("s3 bucket name must not be empty")
	}
	if err := chainutil.ValidateS3BucketForChain(chainID, bucket, environment); err != nil {
		return nil, fmt.Errorf("invalid s3 bucket: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BlockCacheReaderWithFallback{
		redis:   redis,
		s3:      s3,
		chainID: chainID,
		bucket:  bucket,
		logger:  logger,
	}, nil
}

// redisGetter defines the function signature for Redis get operations, allowing getWithFallback to be generic over different data types.
type redisGetter func(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error)

// GetBlock retrieves the full block with transactions.
// It checks Redis first; on a miss it falls back to S3.
// Returns nil, nil if the data is not found in either store.
func (c *BlockCacheReaderWithFallback) GetBlock(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error) {
	return c.getWithFallback(ctx, chainID, blockNumber, version, c.redis.GetBlock, s3key.Block)
}

// GetReceipts retrieves transaction receipts for a block.
// It checks Redis first; on a miss it falls back to S3.
// Returns nil, nil if the data is not found in either store.
func (c *BlockCacheReaderWithFallback) GetReceipts(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error) {
	return c.getWithFallback(ctx, chainID, blockNumber, version, c.redis.GetReceipts, s3key.Receipts)
}

// GetTraces retrieves execution traces for a block.
// It checks Redis first; on a miss it falls back to S3.
// Returns nil, nil if the data is not found in either store.
func (c *BlockCacheReaderWithFallback) GetTraces(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error) {
	return c.getWithFallback(ctx, chainID, blockNumber, version, c.redis.GetTraces, s3key.Traces)
}

// GetBlobs retrieves blob sidecars for a block.
// It checks Redis first; on a miss it falls back to S3.
// Returns nil, nil if the data is not found in either store.
func (c *BlockCacheReaderWithFallback) GetBlobs(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error) {
	return c.getWithFallback(ctx, chainID, blockNumber, version, c.redis.GetBlobs, s3key.Blobs)
}

// Close closes the underlying Redis connection.
func (c *BlockCacheReaderWithFallback) Close() error {
	return c.redis.Close()
}

func (c *BlockCacheReaderWithFallback) getWithFallback(ctx context.Context, chainID, blockNumber int64, version int, fetch redisGetter, dataType s3key.DataType) (json.RawMessage, error) {
	if chainID != c.chainID {
		return nil, fmt.Errorf("chainID mismatch: reader configured for chain %d, got %d", c.chainID, chainID)
	}
	data, err := fetch(ctx, chainID, blockNumber, version)
	if err != nil {
		return nil, fmt.Errorf("redis %s: %w", dataType, err)
	}
	if data != nil {
		return data, nil
	}
	return c.getFromS3(ctx, blockNumber, version, dataType)
}

func (c *BlockCacheReaderWithFallback) getFromS3(ctx context.Context, blockNumber int64, version int, dataType s3key.DataType) (json.RawMessage, error) {
	key := s3key.Build(blockNumber, version, dataType)

	s3Ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	reader, err := c.s3.StreamFile(s3Ctx, c.bucket, key)
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get %s from s3 (key=%s): %w", dataType, key, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read s3 data: %w", err)
	}

	data, err = gziputil.Decompress(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress s3 data (key=%s): %w", key, err)
	}

	c.logger.Debug("fallback: fetched from s3", "key", key, "size", len(data))

	return data, nil
}
