// Package redis provides a Redis implementation of the BlockCache port.
//
// This adapter stores block data in Redis with configurable TTL for
// automatic expiration. It uses a key format of chainID:blockNumber:version:dataType
// to organize cached data.
//
// Data is compressed using gzip before storing to reduce network transfer time
// and Redis memory usage. Decompression is handled transparently on read.
package redis

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that BlockCache implements outbound.BlockCache
var _ outbound.BlockCache = (*BlockCache)(nil)

// Config holds Redis cache configuration.
type Config struct {
	// Addr is the Redis server address (e.g., "localhost:6379")
	Addr string
	// Password for Redis authentication (empty for no auth)
	Password string
	// DB is the Redis database number (0-15)
	DB int
	// TTL is how long cached data lives before expiring
	TTL time.Duration
	// KeyPrefix is prepended to all cache keys
	KeyPrefix string
}

// ConfigDefaults returns sensible defaults for Redis cache configuration.
func ConfigDefaults() Config {
	return Config{
		Addr:      "localhost:6379",
		Password:  "",
		DB:        0,
		TTL:       24 * time.Hour,
		KeyPrefix: "stl",
	}
}

// BlockCache is a Redis implementation of the outbound.BlockCache port.
type BlockCache struct {
	client    *redis.Client
	ttl       time.Duration
	keyPrefix string
	logger    *slog.Logger
}

// NewBlockCache creates a new Redis block cache.
func NewBlockCache(cfg Config, logger *slog.Logger) (*BlockCache, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis address is required")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "redis-cache")

	return &BlockCache{
		client:    client,
		ttl:       cfg.TTL,
		keyPrefix: cfg.KeyPrefix,
		logger:    logger,
	}, nil
}

// Ping checks the Redis connection.
func (c *BlockCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close closes the Redis connection.
func (c *BlockCache) Close() error {
	return c.client.Close()
}

// SetBlockData stores all block data types in a single pipelined operation.
// This is more efficient than calling SetBlock, SetReceipts, SetTraces, SetBlobs separately
// as it batches all commands into a single network round-trip.
// Data is compressed using gzip before storing.
func (c *BlockCache) SetBlockData(ctx context.Context, chainID, blockNumber int64, version int, data outbound.BlockDataInput) error {
	// Compress all data
	blockCompressed, err := compress(data.Block)
	if err != nil {
		return fmt.Errorf("failed to compress block: %w", err)
	}
	receiptsCompressed, err := compress(data.Receipts)
	if err != nil {
		return fmt.Errorf("failed to compress receipts: %w", err)
	}
	tracesCompressed, err := compress(data.Traces)
	if err != nil {
		return fmt.Errorf("failed to compress traces: %w", err)
	}

	pipe := c.client.Pipeline()

	// Queue all SET commands with compressed data
	pipe.Set(ctx, c.key(chainID, blockNumber, version, "block"), blockCompressed, c.ttl)
	pipe.Set(ctx, c.key(chainID, blockNumber, version, "receipts"), receiptsCompressed, c.ttl)
	pipe.Set(ctx, c.key(chainID, blockNumber, version, "traces"), tracesCompressed, c.ttl)

	if data.Blobs != nil {
		blobsCompressed, err := compress(data.Blobs)
		if err != nil {
			return fmt.Errorf("failed to compress blobs: %w", err)
		}
		pipe.Set(ctx, c.key(chainID, blockNumber, version, "blobs"), blobsCompressed, c.ttl)
	}

	// Execute all commands in one round-trip
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to pipeline cache block data: %w", err)
	}
	return nil
}

// key generates a cache key in the format prefix:chainID:blockNumber:version:dataType
func (c *BlockCache) key(chainID, blockNumber int64, version int, dataType string) string {
	return fmt.Sprintf("%s:%d:%d:%d:%s", c.keyPrefix, chainID, blockNumber, version, dataType)
}

// compress compresses data using gzip level 1 (fastest).
// Returns the compressed data or an error.
func compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip writer: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, fmt.Errorf("failed to write compressed data: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}
	return buf.Bytes(), nil
}

// isGzipped checks if data is gzip-compressed by looking for the gzip magic bytes.
// Gzip data always starts with 0x1f 0x8b.
func isGzipped(data []byte) bool {
	return len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

// decompress decompresses gzip data if compressed, otherwise returns data as-is.
// This provides backward compatibility with uncompressed data in the cache.
func decompress(data []byte) ([]byte, error) {
	if !isGzipped(data) {
		// Data is not compressed, return as-is (backward compatibility)
		return data, nil
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

// SetBlock caches block data (compressed).
func (c *BlockCache) SetBlock(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	compressed, err := compress(data)
	if err != nil {
		return fmt.Errorf("failed to compress block: %w", err)
	}
	key := c.key(chainID, blockNumber, version, "block")
	if err := c.client.Set(ctx, key, compressed, c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache block: %w", err)
	}
	return nil
}

// SetReceipts caches receipt data (compressed).
func (c *BlockCache) SetReceipts(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	compressed, err := compress(data)
	if err != nil {
		return fmt.Errorf("failed to compress receipts: %w", err)
	}
	key := c.key(chainID, blockNumber, version, "receipts")
	if err := c.client.Set(ctx, key, compressed, c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache receipts: %w", err)
	}
	return nil
}

// SetTraces caches trace data (compressed).
func (c *BlockCache) SetTraces(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	compressed, err := compress(data)
	if err != nil {
		return fmt.Errorf("failed to compress traces: %w", err)
	}
	key := c.key(chainID, blockNumber, version, "traces")
	if err := c.client.Set(ctx, key, compressed, c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache traces: %w", err)
	}
	return nil
}

// SetBlobs caches blob data (compressed).
func (c *BlockCache) SetBlobs(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	compressed, err := compress(data)
	if err != nil {
		return fmt.Errorf("failed to compress blobs: %w", err)
	}
	key := c.key(chainID, blockNumber, version, "blobs")
	if err := c.client.Set(ctx, key, compressed, c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache blobs: %w", err)
	}
	return nil
}

// GetBlock retrieves cached block data (decompressed).
func (c *BlockCache) GetBlock(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	key := c.key(chainID, blockNumber, version, "block")
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	decompressed, err := decompress(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress block: %w", err)
	}
	return decompressed, nil
}

// GetReceipts retrieves cached receipt data (decompressed).
func (c *BlockCache) GetReceipts(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	key := c.key(chainID, blockNumber, version, "receipts")
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get receipts: %w", err)
	}
	decompressed, err := decompress(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress receipts: %w", err)
	}
	return decompressed, nil
}

// GetTraces retrieves cached trace data (decompressed).
func (c *BlockCache) GetTraces(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	key := c.key(chainID, blockNumber, version, "traces")
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get traces: %w", err)
	}
	decompressed, err := decompress(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress traces: %w", err)
	}
	return decompressed, nil
}

// GetBlobs retrieves cached blob data (decompressed).
func (c *BlockCache) GetBlobs(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	key := c.key(chainID, blockNumber, version, "blobs")
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get blobs: %w", err)
	}
	decompressed, err := decompress(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress blobs: %w", err)
	}
	return decompressed, nil
}

// DeleteBlock removes all cached data for a block.
func (c *BlockCache) DeleteBlock(ctx context.Context, chainID, blockNumber int64, version int) error {
	keys := []string{
		c.key(chainID, blockNumber, version, "block"),
		c.key(chainID, blockNumber, version, "receipts"),
		c.key(chainID, blockNumber, version, "traces"),
		c.key(chainID, blockNumber, version, "blobs"),
	}
	if err := c.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("failed to delete block cache: %w", err)
	}
	return nil
}
