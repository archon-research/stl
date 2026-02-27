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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const tracerName = "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"

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
	// MaxRetries is the maximum number of retry attempts for transient failures (default: 3)
	MaxRetries int
	// RetryBackoff is the initial backoff duration between retries (default: 100ms)
	// Subsequent retries use exponential backoff: backoff * 2^attempt
	RetryBackoff time.Duration
}

// ConfigDefaults returns sensible defaults for Redis cache configuration.
func ConfigDefaults() Config {
	return Config{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		TTL:          24 * time.Hour,
		KeyPrefix:    "stl",
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
	}
}

// BlockCache is a Redis implementation of the outbound.BlockCache port.
type BlockCache struct {
	client       *redis.Client
	ttl          time.Duration
	keyPrefix    string
	maxRetries   int
	retryBackoff time.Duration
	logger       *slog.Logger
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

	// Apply defaults for retry config
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	retryBackoff := cfg.RetryBackoff
	if retryBackoff <= 0 {
		retryBackoff = 100 * time.Millisecond
	}

	return &BlockCache{
		client:       client,
		ttl:          cfg.TTL,
		keyPrefix:    cfg.KeyPrefix,
		maxRetries:   maxRetries,
		retryBackoff: retryBackoff,
		logger:       logger,
	}, nil
}

// isRetryableError checks if an error should trigger a retry.
// Retries on any error except context cancellation (shutdown signal).
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Don't retry on context cancellation (shutdown)
	if errors.Is(err, context.Canceled) {
		return false
	}
	return true
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
// Transient failures are automatically retried.
func (c *BlockCache) SetBlockData(ctx context.Context, chainID, blockNumber int64, version int, data outbound.BlockDataInput) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "redis.SetBlockData",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "redis"),
			attribute.String("db.operation", "PIPELINE"),
			attribute.Int64("block.number", blockNumber),
			attribute.Int64("chain.id", chainID),
			attribute.Int("block.version", version),
		),
	)
	defer span.End()

	// Compress non-nil data in parallel (do this once, outside the retry loop)
	var blockCompressed, receiptsCompressed, tracesCompressed, blobsCompressed []byte
	var compressErr error
	var errMu sync.Mutex
	var wg sync.WaitGroup

	// Helper to set first error encountered
	setErr := func(err error) {
		errMu.Lock()
		if compressErr == nil {
			compressErr = err
		}
		errMu.Unlock()
	}

	if data.Block != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			compressed, err := compress(data.Block)
			if err != nil {
				setErr(fmt.Errorf("failed to compress block: %w", err))
				return
			}
			blockCompressed = compressed
		}()
	}
	if data.Receipts != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			compressed, err := compress(data.Receipts)
			if err != nil {
				setErr(fmt.Errorf("failed to compress receipts: %w", err))
				return
			}
			receiptsCompressed = compressed
		}()
	}
	if data.Traces != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			compressed, err := compress(data.Traces)
			if err != nil {
				setErr(fmt.Errorf("failed to compress traces: %w", err))
				return
			}
			tracesCompressed = compressed
		}()
	}
	if data.Blobs != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			compressed, err := compress(data.Blobs)
			if err != nil {
				setErr(fmt.Errorf("failed to compress blobs: %w", err))
				return
			}
			blobsCompressed = compressed
		}()
	}

	wg.Wait()

	if compressErr != nil {
		span.RecordError(compressErr)
		span.SetStatus(codes.Error, "failed to compress data")
		return compressErr
	}

	totalCommands := 0
	if blockCompressed != nil {
		totalCommands++
	}
	if receiptsCompressed != nil {
		totalCommands++
	}
	if tracesCompressed != nil {
		totalCommands++
	}
	if blobsCompressed != nil {
		totalCommands++
	}
	span.SetAttributes(attribute.Int("redis.pipeline_commands", totalCommands))

	cfg := retry.Config{
		MaxRetries:     c.maxRetries,
		InitialBackoff: c.retryBackoff,
		MaxBackoff:     c.retryBackoff * 8, // Cap at 8x initial
		BackoffFactor:  2.0,
		Jitter:         true,
	}

	onRetry := func(attempt int, retryErr error, backoff time.Duration) {
		c.logger.Warn("retrying Redis pipeline",
			"attempt", attempt,
			"block", blockNumber,
			"backoff", backoff,
			"error", retryErr)
		span.AddEvent("retry_attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", retryErr.Error()),
		))
	}

	err := retry.DoVoid(ctx, cfg, isRetryableError, onRetry, func() error {
		pipe := c.client.Pipeline()
		if blockCompressed != nil {
			pipe.Set(ctx, c.key(chainID, blockNumber, version, "block"), blockCompressed, c.ttl)
		}
		if receiptsCompressed != nil {
			pipe.Set(ctx, c.key(chainID, blockNumber, version, "receipts"), receiptsCompressed, c.ttl)
		}
		if tracesCompressed != nil {
			pipe.Set(ctx, c.key(chainID, blockNumber, version, "traces"), tracesCompressed, c.ttl)
		}
		if blobsCompressed != nil {
			pipe.Set(ctx, c.key(chainID, blockNumber, version, "blobs"), blobsCompressed, c.ttl)
		}
		_, err := pipe.Exec(ctx)
		return err
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to cache block data")
		return fmt.Errorf("failed to cache block data: %w", err)
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
// Transient failures are automatically retried.
func (c *BlockCache) SetBlock(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	return c.setWithRetry(ctx, chainID, blockNumber, version, "block", data)
}

// setWithRetry is a helper that handles compression, retry logic, and tracing for individual set operations.
func (c *BlockCache) setWithRetry(ctx context.Context, chainID, blockNumber int64, version int, dataType string, data json.RawMessage) error {
	tracer := otel.Tracer(tracerName)
	key := c.key(chainID, blockNumber, version, dataType)
	ctx, span := tracer.Start(ctx, "redis.Set",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "redis"),
			attribute.String("db.operation", "SET"),
			attribute.String("redis.key", key),
			attribute.String("redis.data_type", dataType),
			attribute.Int64("block.number", blockNumber),
		),
	)
	defer span.End()

	// Compress data outside retry loop
	compressed, err := compress(data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("failed to compress %s", dataType))
		return fmt.Errorf("failed to compress %s: %w", dataType, err)
	}

	cfg := retry.Config{
		MaxRetries:     c.maxRetries,
		InitialBackoff: c.retryBackoff,
		MaxBackoff:     c.retryBackoff * 8,
		BackoffFactor:  2.0,
		Jitter:         true,
	}

	onRetry := func(attempt int, err error, backoff time.Duration) {
		c.logger.Warn("retrying Redis SET",
			"dataType", dataType,
			"attempt", attempt,
			"block", blockNumber,
			"backoff", backoff,
			"error", err)
		span.AddEvent("retry_attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", err.Error()),
		))
	}

	err = retry.DoVoid(ctx, cfg, isRetryableError, onRetry, func() error {
		return c.client.Set(ctx, key, compressed, c.ttl).Err()
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("failed to cache %s", dataType))
		return fmt.Errorf("failed to cache %s: %w", dataType, err)
	}
	return nil
}

// SetReceipts caches receipt data (compressed).
// Transient failures are automatically retried.
func (c *BlockCache) SetReceipts(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	return c.setWithRetry(ctx, chainID, blockNumber, version, "receipts", data)
}

// SetTraces caches trace data (compressed).
// Transient failures are automatically retried.
func (c *BlockCache) SetTraces(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	return c.setWithRetry(ctx, chainID, blockNumber, version, "traces", data)
}

// SetBlobs caches blob data (compressed).
// Transient failures are automatically retried.
func (c *BlockCache) SetBlobs(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	return c.setWithRetry(ctx, chainID, blockNumber, version, "blobs", data)
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
