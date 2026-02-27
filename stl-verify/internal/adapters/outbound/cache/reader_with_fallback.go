package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.BlockCacheReader = (*BlockCacheReaderWithFallback)(nil)

// BlockCacheReaderWithFallback implements BlockCacheReader by first checking Redis and
// falling back to S3 on a cache miss. Chain isolation is achieved via the
// bucket name — each chain has its own dedicated bucket, so S3 keys do not
// include the chain ID.
type BlockCacheReaderWithFallback struct {
	redis  outbound.BlockCacheReader
	s3     outbound.S3Reader
	bucket string
	logger *slog.Logger
}

// NewReaderWithFallback creates a BlockCacheReaderWithFallback. It returns an error if
// bucket is empty, because an empty bucket name would silently read from the
// wrong place in production.
func NewReaderWithFallback(redis outbound.BlockCacheReader, s3 outbound.S3Reader, bucket string, logger *slog.Logger) (*BlockCacheReaderWithFallback, error) {
	if bucket == "" {
		return nil, fmt.Errorf("s3 bucket name must not be empty")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BlockCacheReaderWithFallback{
		redis:  redis,
		s3:     s3,
		bucket: bucket,
		logger: logger,
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

	reader, err := c.s3.StreamFile(ctx, c.bucket, key)
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

	c.logger.Debug("fallback: fetched from s3", "key", key, "size", len(data))

	return data, nil
}
