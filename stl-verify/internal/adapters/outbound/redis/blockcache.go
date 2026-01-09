// Package redis provides a Redis implementation of the BlockCache port.
//
// This adapter stores block data in Redis with configurable TTL for
// automatic expiration. It uses a key format of chainID:blockNumber:version:dataType
// to organize cached data.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
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

// key generates a cache key in the format prefix:chainID:blockNumber:version:dataType
func (c *BlockCache) key(chainID, blockNumber int64, version int, dataType string) string {
	return fmt.Sprintf("%s:%d:%d:%d:%s", c.keyPrefix, chainID, blockNumber, version, dataType)
}

// SetBlock caches block data.
func (c *BlockCache) SetBlock(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	key := c.key(chainID, blockNumber, version, "block")
	if err := c.client.Set(ctx, key, []byte(data), c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache block: %w", err)
	}
	return nil
}

// SetReceipts caches receipt data.
func (c *BlockCache) SetReceipts(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	key := c.key(chainID, blockNumber, version, "receipts")
	if err := c.client.Set(ctx, key, []byte(data), c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache receipts: %w", err)
	}
	return nil
}

// SetTraces caches trace data.
func (c *BlockCache) SetTraces(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	key := c.key(chainID, blockNumber, version, "traces")
	if err := c.client.Set(ctx, key, []byte(data), c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache traces: %w", err)
	}
	return nil
}

// SetBlobs caches blob data.
func (c *BlockCache) SetBlobs(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	key := c.key(chainID, blockNumber, version, "blobs")
	if err := c.client.Set(ctx, key, []byte(data), c.ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache blobs: %w", err)
	}
	return nil
}

// GetBlock retrieves cached block data.
func (c *BlockCache) GetBlock(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	key := c.key(chainID, blockNumber, version, "block")
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	return data, nil
}

// GetReceipts retrieves cached receipt data.
func (c *BlockCache) GetReceipts(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	key := c.key(chainID, blockNumber, version, "receipts")
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get receipts: %w", err)
	}
	return data, nil
}

// GetTraces retrieves cached trace data.
func (c *BlockCache) GetTraces(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	key := c.key(chainID, blockNumber, version, "traces")
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get traces: %w", err)
	}
	return data, nil
}

// GetBlobs retrieves cached blob data.
func (c *BlockCache) GetBlobs(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	key := c.key(chainID, blockNumber, version, "blobs")
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get blobs: %w", err)
	}
	return data, nil
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
