// Package main provides a CLI tool for bulk downloading Ethereum block data.
// It fetches blocks, receipts, and traces from Alchemy and writes directly to S3.
//
// Usage:
//
//	./bulk-download \
//	  --alchemy-urls="https://eth-mainnet.g.alchemy.com/v2/KEY1,https://...KEY2" \
//	  --start-block=16000000 \
//	  --end-block=21000000 \
//	  --bucket=my-bucket \
//	  --workers-per-key=2
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/aws/aws-sdk-go-v2/config"
)

// BlockRangeSize is the number of blocks per S3 partition.
const BlockRangeSize = 1000

// Config holds the CLI configuration.
type Config struct {
	AlchemyURLs   []string
	StartBlock    int64
	EndBlock      int64
	Bucket        string
	WorkersPerKey int
	BatchSize     int
	DryRun        bool
}

// Stats tracks download progress.
type Stats struct {
	blocksProcessed atomic.Int64
	blocksSkipped   atomic.Int64
	blocksFailed    atomic.Int64
	bytesWritten    atomic.Int64
	startTime       time.Time
}

// PartitionCache caches the list of existing keys per S3 partition.
// This avoids repeated S3 ListObjects calls for blocks in the same partition.
type PartitionCache struct {
	mu        sync.RWMutex
	cache     map[string]map[string]struct{} // partition -> set of keys
	s3Reader  outbound.S3Reader
	bucket    string
	logger    *slog.Logger
	hitCount  atomic.Int64
	missCount atomic.Int64
}

// NewPartitionCache creates a new partition cache.
func NewPartitionCache(s3Reader outbound.S3Reader, bucket string, logger *slog.Logger) *PartitionCache {
	return &PartitionCache{
		cache:    make(map[string]map[string]struct{}),
		s3Reader: s3Reader,
		bucket:   bucket,
		logger:   logger,
	}
}

// loadPartition loads all keys for a partition from S3.
func (pc *PartitionCache) loadPartition(ctx context.Context, partition string) (map[string]struct{}, error) {
	// Check if already cached
	pc.mu.RLock()
	if keys, ok := pc.cache[partition]; ok {
		pc.mu.RUnlock()
		pc.hitCount.Add(1)
		return keys, nil
	}
	pc.mu.RUnlock()

	pc.missCount.Add(1)

	// List all keys in the partition
	prefix := partition + "/"
	keyList, err := pc.s3Reader.ListPrefix(ctx, pc.bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list partition %s: %w", partition, err)
	}

	// Build set of keys
	keySet := make(map[string]struct{}, len(keyList))
	for _, key := range keyList {
		keySet[key] = struct{}{}
	}

	// Cache the result
	pc.mu.Lock()
	pc.cache[partition] = keySet
	pc.mu.Unlock()

	pc.logger.Debug("loaded partition from S3",
		"partition", partition,
		"keyCount", len(keySet),
	)

	return keySet, nil
}

// BlockExists checks if all required data types exist for a block.
func (pc *PartitionCache) BlockExists(ctx context.Context, blockNum int64) (bool, error) {
	partition := getPartition(blockNum)

	keySet, err := pc.loadPartition(ctx, partition)
	if err != nil {
		return false, err
	}

	// Check for all required data types
	dataTypes := []string{"block", "receipts", "traces"}
	for _, dataType := range dataTypes {
		key := fmt.Sprintf("%s/%d_1_%s.json.gz", partition, blockNum, dataType)
		if _, exists := keySet[key]; !exists {
			return false, nil
		}
	}

	return true, nil
}

// MarkWritten adds a key to the cache after writing.
func (pc *PartitionCache) MarkWritten(blockNum int64, dataType string) {
	partition := getPartition(blockNum)
	key := fmt.Sprintf("%s/%d_1_%s.json.gz", partition, blockNum, dataType)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if keySet, ok := pc.cache[partition]; ok {
		keySet[key] = struct{}{}
	}
}

// GetStats returns cache hit/miss statistics.
func (pc *PartitionCache) GetStats() (hits, misses int64) {
	return pc.hitCount.Load(), pc.missCount.Load()
}

func main() {
	cfg := parseFlags()

	// Set up structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Validate configuration
	if err := validateConfig(cfg); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	// Set up context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	// Run the downloader
	if err := run(ctx, cfg, logger); err != nil {
		if ctx.Err() != nil {
			logger.Info("shutdown complete")
			os.Exit(0)
		}
		logger.Error("download failed", "error", err)
		os.Exit(1)
	}

	logger.Info("download complete")
}

func parseFlags() Config {
	var cfg Config
	var alchemyURLsStr string

	flag.StringVar(&alchemyURLsStr, "alchemy-urls", "", "Comma-separated list of Alchemy HTTP URLs (required)")
	flag.Int64Var(&cfg.StartBlock, "start-block", 0, "Starting block number (required)")
	flag.Int64Var(&cfg.EndBlock, "end-block", 0, "Ending block number (required)")
	flag.StringVar(&cfg.Bucket, "bucket", "", "S3 bucket name (required)")
	flag.IntVar(&cfg.WorkersPerKey, "workers-per-key", 2, "Number of workers per Alchemy key")
	flag.IntVar(&cfg.BatchSize, "batch-size", 10, "Number of blocks per RPC batch")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Print what would be done without downloading")
	flag.Parse()

	if alchemyURLsStr != "" {
		cfg.AlchemyURLs = strings.Split(alchemyURLsStr, ",")
		// Trim whitespace from each URL
		for i := range cfg.AlchemyURLs {
			cfg.AlchemyURLs[i] = strings.TrimSpace(cfg.AlchemyURLs[i])
		}
	}

	return cfg
}

func validateConfig(cfg Config) error {
	if len(cfg.AlchemyURLs) == 0 {
		return fmt.Errorf("--alchemy-urls is required")
	}
	if cfg.StartBlock == 0 {
		return fmt.Errorf("--start-block is required")
	}
	if cfg.EndBlock == 0 {
		return fmt.Errorf("--end-block is required")
	}
	if cfg.EndBlock < cfg.StartBlock {
		return fmt.Errorf("--end-block must be >= --start-block")
	}
	if cfg.Bucket == "" {
		return fmt.Errorf("--bucket is required")
	}
	return nil
}

func run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	// Initialize Alchemy clients (one per URL)
	clients := make([]*alchemy.Client, 0, len(cfg.AlchemyURLs))
	for i, url := range cfg.AlchemyURLs {
		client, err := alchemy.NewClient(alchemy.ClientConfig{
			HTTPURL:        url,
			Timeout:        60 * time.Second,
			MaxRetries:     5,
			InitialBackoff: 500 * time.Millisecond,
			MaxBackoff:     30 * time.Second,
			BackoffFactor:  2.0,
			Logger:         logger.With("client", i),
		})
		if err != nil {
			return fmt.Errorf("failed to create Alchemy client %d: %w", i, err)
		}
		clients = append(clients, client)
	}
	logger.Info("initialized Alchemy clients", "count", len(clients))

	// Initialize S3 writer
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Writer := s3.NewWriter(awsCfg, logger)
	s3Reader := s3.NewReader(awsCfg, logger)
	logger.Info("initialized S3 writer and reader", "bucket", cfg.Bucket)

	// Initialize partition cache
	partitionCache := NewPartitionCache(s3Reader, cfg.Bucket, logger)

	// Calculate total blocks and workers
	totalBlocks := cfg.EndBlock - cfg.StartBlock + 1
	totalWorkers := len(clients) * cfg.WorkersPerKey

	// Calculate number of partitions
	startPartition := cfg.StartBlock / BlockRangeSize
	endPartition := cfg.EndBlock / BlockRangeSize
	totalPartitions := endPartition - startPartition + 1

	logger.Info("starting bulk download",
		"startBlock", cfg.StartBlock,
		"endBlock", cfg.EndBlock,
		"totalBlocks", totalBlocks,
		"totalPartitions", totalPartitions,
		"alchemyKeys", len(clients),
		"workersPerKey", cfg.WorkersPerKey,
		"totalWorkers", totalWorkers,
		"batchSize", cfg.BatchSize,
	)

	if cfg.DryRun {
		logger.Info("dry run mode - no data will be downloaded")
		return nil
	}

	// Create stats tracker
	stats := &Stats{startTime: time.Now()}

	// Start progress reporter
	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go reportProgress(progressCtx, stats, totalBlocks, partitionCache, logger)

	// Create work channel
	workCh := make(chan int64, totalWorkers*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < totalWorkers; i++ {
		clientIdx := i % len(clients)
		wg.Add(1)
		go func(workerID int, client *alchemy.Client) {
			defer wg.Done()
			worker(ctx, workerID, client, s3Writer, partitionCache, cfg, workCh, stats, logger)
		}(i, clients[clientIdx])
	}

	// Feed work to workers
	go func() {
		defer close(workCh)
		for blockNum := cfg.StartBlock; blockNum <= cfg.EndBlock; blockNum += int64(cfg.BatchSize) {
			select {
			case <-ctx.Done():
				return
			case workCh <- blockNum:
			}
		}
	}()

	// Wait for workers to finish
	wg.Wait()

	// Final stats
	elapsed := time.Since(stats.startTime)
	hits, misses := partitionCache.GetStats()
	logger.Info("download complete",
		"processed", stats.blocksProcessed.Load(),
		"skipped", stats.blocksSkipped.Load(),
		"failed", stats.blocksFailed.Load(),
		"bytesWritten", formatBytes(stats.bytesWritten.Load()),
		"elapsed", elapsed.Round(time.Second),
		"blocksPerSecond", fmt.Sprintf("%.1f", float64(stats.blocksProcessed.Load()+stats.blocksSkipped.Load())/elapsed.Seconds()),
		"cacheHits", hits,
		"cacheMisses", misses,
	)

	return nil
}

// worker processes batches of blocks from the work channel.
func worker(
	ctx context.Context,
	workerID int,
	client *alchemy.Client,
	s3Writer outbound.S3Writer,
	partitionCache *PartitionCache,
	cfg Config,
	workCh <-chan int64,
	stats *Stats,
	logger *slog.Logger,
) {
	logger = logger.With("worker", workerID)

	for batchStart := range workCh {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batchEnd := batchStart + int64(cfg.BatchSize) - 1
		if batchEnd > cfg.EndBlock {
			batchEnd = cfg.EndBlock
		}

		if err := processBatch(ctx, client, s3Writer, partitionCache, cfg, batchStart, batchEnd, stats, logger); err != nil {
			logger.Warn("batch failed", "from", batchStart, "to", batchEnd, "error", err)
		}
	}
}

// processBatch fetches and stores a batch of blocks.
func processBatch(
	ctx context.Context,
	client *alchemy.Client,
	s3Writer outbound.S3Writer,
	partitionCache *PartitionCache,
	cfg Config,
	batchStart, batchEnd int64,
	stats *Stats,
	logger *slog.Logger,
) error {
	// Build block numbers for this batch
	blockNums := make([]int64, 0, batchEnd-batchStart+1)
	for blockNum := batchStart; blockNum <= batchEnd; blockNum++ {
		blockNums = append(blockNums, blockNum)
	}

	// Check which blocks already exist in S3 using partition cache
	blocksToFetch := make([]int64, 0, len(blockNums))
	for _, blockNum := range blockNums {
		exists, err := partitionCache.BlockExists(ctx, blockNum)
		if err != nil {
			logger.Warn("failed to check S3 existence", "block", blockNum, "error", err)
			blocksToFetch = append(blocksToFetch, blockNum)
			continue
		}
		if exists {
			stats.blocksSkipped.Add(1)
		} else {
			blocksToFetch = append(blocksToFetch, blockNum)
		}
	}

	if len(blocksToFetch) == 0 {
		return nil
	}

	// Fetch blocks from Alchemy
	blockDataList, err := client.GetBlocksBatch(ctx, blocksToFetch, true)
	if err != nil {
		for range blocksToFetch {
			stats.blocksFailed.Add(1)
		}
		return fmt.Errorf("failed to fetch batch: %w", err)
	}

	// Write each block's data to S3
	for _, bd := range blockDataList {
		if err := writeBlockToS3(ctx, s3Writer, partitionCache, cfg.Bucket, bd, stats); err != nil {
			logger.Warn("failed to write block", "block", bd.BlockNumber, "error", err)
			stats.blocksFailed.Add(1)
			continue
		}
		stats.blocksProcessed.Add(1)
	}

	return nil
}

// writeBlockToS3 writes all data types for a block to S3.
func writeBlockToS3(ctx context.Context, s3Writer outbound.S3Writer, partitionCache *PartitionCache, bucket string, bd outbound.BlockData, stats *Stats) error {
	partition := getPartition(bd.BlockNumber)

	// Write block data
	if bd.Block != nil {
		key := fmt.Sprintf("%s/%d_1_block.json.gz", partition, bd.BlockNumber)
		written, err := s3Writer.WriteFileIfNotExists(ctx, bucket, key, bytes.NewReader(bd.Block), true)
		if err != nil {
			return fmt.Errorf("failed to write block: %w", err)
		}
		if written {
			stats.bytesWritten.Add(int64(len(bd.Block)))
			partitionCache.MarkWritten(bd.BlockNumber, "block")
		}
	} else if bd.BlockErr != nil {
		return fmt.Errorf("block fetch error: %w", bd.BlockErr)
	}

	// Write receipts
	if bd.Receipts != nil {
		key := fmt.Sprintf("%s/%d_1_receipts.json.gz", partition, bd.BlockNumber)
		written, err := s3Writer.WriteFileIfNotExists(ctx, bucket, key, bytes.NewReader(bd.Receipts), true)
		if err != nil {
			return fmt.Errorf("failed to write receipts: %w", err)
		}
		if written {
			stats.bytesWritten.Add(int64(len(bd.Receipts)))
			partitionCache.MarkWritten(bd.BlockNumber, "receipts")
		}
	} else if bd.ReceiptsErr != nil {
		return fmt.Errorf("receipts fetch error: %w", bd.ReceiptsErr)
	}

	// Write traces
	if bd.Traces != nil {
		key := fmt.Sprintf("%s/%d_1_traces.json.gz", partition, bd.BlockNumber)
		written, err := s3Writer.WriteFileIfNotExists(ctx, bucket, key, bytes.NewReader(bd.Traces), true)
		if err != nil {
			return fmt.Errorf("failed to write traces: %w", err)
		}
		if written {
			stats.bytesWritten.Add(int64(len(bd.Traces)))
			partitionCache.MarkWritten(bd.BlockNumber, "traces")
		}
	} else if bd.TracesErr != nil {
		return fmt.Errorf("traces fetch error: %w", bd.TracesErr)
	}

	return nil
}

// getPartition returns the partition string for a block number.
func getPartition(blockNumber int64) string {
	partitionIndex := blockNumber / BlockRangeSize
	start := partitionIndex * BlockRangeSize
	end := start + BlockRangeSize - 1
	return fmt.Sprintf("%d-%d", start, end)
}

// reportProgress logs download progress periodically.
func reportProgress(ctx context.Context, stats *Stats, totalBlocks int64, partitionCache *PartitionCache, logger *slog.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processed := stats.blocksProcessed.Load()
			skipped := stats.blocksSkipped.Load()
			failed := stats.blocksFailed.Load()
			completed := processed + skipped + failed

			elapsed := time.Since(stats.startTime)
			blocksPerSec := float64(completed) / elapsed.Seconds()

			remaining := totalBlocks - completed
			var eta time.Duration
			if blocksPerSec > 0 {
				eta = time.Duration(float64(remaining)/blocksPerSec) * time.Second
			}

			pct := float64(completed) / float64(totalBlocks) * 100

			hits, misses := partitionCache.GetStats()

			logger.Info("progress",
				"completed", completed,
				"total", totalBlocks,
				"percent", fmt.Sprintf("%.1f%%", pct),
				"processed", processed,
				"skipped", skipped,
				"failed", failed,
				"blocksPerSec", fmt.Sprintf("%.1f", blocksPerSec),
				"eta", eta.Round(time.Second),
				"bytesWritten", formatBytes(stats.bytesWritten.Load()),
				"cacheHitRate", fmt.Sprintf("%.1f%%", float64(hits)/float64(hits+misses+1)*100),
			)
		}
	}
}

// formatBytes formats bytes as a human-readable string.
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
