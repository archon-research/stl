// Package main provides a CLI tool for bulk downloading Ethereum block data.
// It fetches blocks, receipts, and traces from a local Erigon node and writes to S3.
//
// Architecture:
//   - RPC workers fetch block+receipt data from Erigon
//   - As blocks complete, they're immediately queued for trace fetching (pipelined)
//   - S3 uploads happen asynchronously in a separate upload pool
//   - This decouples RPC fetching from S3 I/O for maximum throughput
//
// Usage:
//
//	./bulk-download \
//	  --rpc-url=http://localhost:8545 \
//	  --start-block=16000000 \
//	  --end-block=21000000 \
//	  --bucket=stl-sentinelstaging-ethereum-raw-89d540d0
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/aws/aws-sdk-go-v2/config"
)

// Default settings optimized for local Erigon node
const (
	DefaultBlockBatchSize      = 2   // Small batches maximize parallelism
	DefaultTraceBatchSize      = 2   // Small batches maximize parallelism
	DefaultBlockReceiptWorkers = 200 // High parallelism for RPC calls
	DefaultTraceWorkers        = 100 // High parallelism for trace fetching
	DefaultUploadWorkers       = 64  // S3 uploads are fast, but need enough workers
	DefaultTimeout             = 120 * time.Second
	DefaultMaxRetries          = 3
)

// Config holds the CLI configuration.
type Config struct {
	RPCURL     string
	StartBlock int64
	EndBlock   int64
	Bucket     string
	Region     string

	BlockReceiptWorkers int
	TraceWorkers        int
	UploadWorkers       int
	BlockBatchSize      int
	TraceBatchSize      int
}

// Stats tracks download progress and timing metrics.
type Stats struct {
	blocksProcessed   atomic.Int64
	blocksSkipped     atomic.Int64
	blocksFailed      atomic.Int64
	blockBytesWritten atomic.Int64

	tracesProcessed   atomic.Int64
	tracesSkipped     atomic.Int64
	tracesFailed      atomic.Int64
	traceBytesWritten atomic.Int64

	uploadsQueued    atomic.Int64
	uploadsCompleted atomic.Int64
	uploadsFailed    atomic.Int64

	// Timing metrics (in nanoseconds)
	rpcBlockTime atomic.Int64 // Time spent in RPC calls for blocks+receipts
	rpcTraceTime atomic.Int64 // Time spent in RPC calls for traces
	s3UploadTime atomic.Int64 // Time spent uploading to S3
	s3CheckTime  atomic.Int64 // Time spent checking S3 for existing files

	// Counts for averaging
	rpcBlockCalls atomic.Int64
	rpcTraceCalls atomic.Int64
	s3UploadCalls atomic.Int64
	s3CheckCalls  atomic.Int64

	startTime time.Time
}

// UploadJob represents an S3 upload to be performed asynchronously.
type UploadJob struct {
	Bucket   string
	Key      string
	Data     []byte // Raw uncompressed data (S3 writer handles compression)
	DataType string // "block", "receipts", or "traces"
	BlockNum int64
}

// PartitionCache caches the list of existing keys per S3 partition.
type PartitionCache struct {
	mu        sync.RWMutex
	cache     map[string]map[string]struct{}
	s3Reader  outbound.S3Reader
	bucket    string
	logger    *slog.Logger
	hitCount  atomic.Int64
	missCount atomic.Int64
}

func NewPartitionCache(s3Reader outbound.S3Reader, bucket string, logger *slog.Logger) *PartitionCache {
	return &PartitionCache{
		cache:    make(map[string]map[string]struct{}),
		s3Reader: s3Reader,
		bucket:   bucket,
		logger:   logger,
	}
}

// ensurePartitionLoaded loads a partition into the cache if not already present.
func (pc *PartitionCache) ensurePartitionLoaded(ctx context.Context, partition string) error {
	pc.mu.RLock()
	_, ok := pc.cache[partition]
	pc.mu.RUnlock()

	if ok {
		pc.hitCount.Add(1)
		return nil
	}

	pc.missCount.Add(1)

	prefix := partition + "/"
	keyList, err := pc.s3Reader.ListPrefix(ctx, pc.bucket, prefix)
	if err != nil {
		return fmt.Errorf("failed to list partition %s: %w", partition, err)
	}

	keySet := make(map[string]struct{}, len(keyList))
	for _, key := range keyList {
		keySet[key] = struct{}{}
	}

	pc.mu.Lock()
	// Double-check in case another goroutine loaded it while we were fetching
	if _, ok := pc.cache[partition]; !ok {
		pc.cache[partition] = keySet
		pc.logger.Debug("loaded partition from S3", "partition", partition, "keyCount", len(keySet))
	}
	pc.mu.Unlock()

	return nil
}

func (pc *PartitionCache) HasBlockAndReceipts(ctx context.Context, blockNum int64) (bool, error) {
	partition := partition.GetPartition(blockNum)
	if err := pc.ensurePartitionLoaded(ctx, partition); err != nil {
		return false, err
	}

	blockKey := fmt.Sprintf("%s/%d_1_block.json.gz", partition, blockNum)
	receiptsKey := fmt.Sprintf("%s/%d_1_receipts.json.gz", partition, blockNum)

	pc.mu.RLock()
	defer pc.mu.RUnlock()

	keySet, ok := pc.cache[partition]
	if !ok {
		return false, nil
	}

	_, hasBlock := keySet[blockKey]
	_, hasReceipts := keySet[receiptsKey]
	return hasBlock && hasReceipts, nil
}

// HasAllData checks if block, receipts, AND traces all exist for a block.
// Use this to determine if a block can be completely skipped.
func (pc *PartitionCache) HasAllData(ctx context.Context, blockNum int64) (bool, error) {
	partition := partition.GetPartition(blockNum)
	if err := pc.ensurePartitionLoaded(ctx, partition); err != nil {
		return false, err
	}

	blockKey := fmt.Sprintf("%s/%d_1_block.json.gz", partition, blockNum)
	receiptsKey := fmt.Sprintf("%s/%d_1_receipts.json.gz", partition, blockNum)
	tracesKey := fmt.Sprintf("%s/%d_1_traces.json.gz", partition, blockNum)

	pc.mu.RLock()
	defer pc.mu.RUnlock()

	keySet, ok := pc.cache[partition]
	if !ok {
		return false, nil
	}

	_, hasBlock := keySet[blockKey]
	_, hasReceipts := keySet[receiptsKey]
	_, hasTraces := keySet[tracesKey]
	return hasBlock && hasReceipts && hasTraces, nil
}

func (pc *PartitionCache) HasTraces(ctx context.Context, blockNum int64) (bool, error) {
	partition := partition.GetPartition(blockNum)
	if err := pc.ensurePartitionLoaded(ctx, partition); err != nil {
		return false, err
	}

	tracesKey := fmt.Sprintf("%s/%d_1_traces.json.gz", partition, blockNum)

	pc.mu.RLock()
	defer pc.mu.RUnlock()

	keySet, ok := pc.cache[partition]
	if !ok {
		return false, nil
	}

	_, exists := keySet[tracesKey]
	return exists, nil
}

func (pc *PartitionCache) MarkWritten(blockNum int64, dataType string) {
	partition := partition.GetPartition(blockNum)
	key := fmt.Sprintf("%s/%d_1_%s.json.gz", partition, blockNum, dataType)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if keySet, ok := pc.cache[partition]; ok {
		keySet[key] = struct{}{}
	}
}

func (pc *PartitionCache) GetStats() (hits, misses int64) {
	return pc.hitCount.Load(), pc.missCount.Load()
}

func main() {
	cfg := parseFlags()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if err := validateConfig(cfg); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

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

	flag.StringVar(&cfg.RPCURL, "rpc-url", "http://localhost:8545", "RPC endpoint URL")
	flag.Int64Var(&cfg.StartBlock, "start-block", 0, "Starting block number (required)")
	flag.Int64Var(&cfg.EndBlock, "end-block", 0, "Ending block number (required)")
	flag.StringVar(&cfg.Bucket, "bucket", "", "S3 bucket name (required)")
	flag.StringVar(&cfg.Region, "region", "", "AWS region (e.g., eu-west-1)")
	flag.IntVar(&cfg.BlockReceiptWorkers, "block-workers", DefaultBlockReceiptWorkers, "Block+receipt worker count")
	flag.IntVar(&cfg.TraceWorkers, "trace-workers", DefaultTraceWorkers, "Trace worker count")
	flag.IntVar(&cfg.UploadWorkers, "upload-workers", DefaultUploadWorkers, "S3 upload worker count")
	flag.IntVar(&cfg.BlockBatchSize, "block-batch-size", DefaultBlockBatchSize, "Blocks per batch for block+receipt fetching")
	flag.IntVar(&cfg.TraceBatchSize, "trace-batch-size", DefaultTraceBatchSize, "Blocks per batch for trace fetching")
	flag.Parse()

	return cfg
}

func validateConfig(cfg Config) error {
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
	// Calculate total RPC workers for connection pooling
	totalRPCWorkers := cfg.BlockReceiptWorkers + cfg.TraceWorkers

	// Create a custom HTTP client for RPC with connection limits.
	// This prevents connection exhaustion when running many workers.
	rpcHTTPClient := &http.Client{
		Timeout: DefaultTimeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          totalRPCWorkers * 2,
			MaxIdleConnsPerHost:   totalRPCWorkers,
			MaxConnsPerHost:       totalRPCWorkers,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	// Create RPC client
	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:        cfg.RPCURL,
		Timeout:        DefaultTimeout,
		MaxRetries:     DefaultMaxRetries,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		BackoffFactor:  2.0,
		Logger:         logger,
		HTTPClient:     rpcHTTPClient,
	})
	if err != nil {
		return fmt.Errorf("failed to create RPC client: %w", err)
	}

	// Initialize S3
	var loadOpts []func(*config.LoadOptions) error
	if cfg.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(cfg.Region))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create a custom HTTP client with connection limits to prevent exhaustion.
	// With many workers, the default transport creates too many connections.
	// This client is shared between S3 reader and writer.
	s3HTTPClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          cfg.UploadWorkers * 2, // Allow some headroom
			MaxIdleConnsPerHost:   cfg.UploadWorkers,     // Match upload workers
			MaxConnsPerHost:       cfg.UploadWorkers,     // Limit concurrent connections
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
		Timeout: 60 * time.Second, // Overall request timeout
	}

	s3Writer := s3.NewWriterWithHTTPClient(awsCfg, s3HTTPClient, logger)
	s3Reader := s3.NewReaderWithHTTPClient(awsCfg, s3HTTPClient, logger)
	partitionCache := NewPartitionCache(s3Reader, cfg.Bucket, logger)

	totalBlocks := cfg.EndBlock - cfg.StartBlock + 1

	logger.Info("starting pipelined bulk download",
		"rpcURL", cfg.RPCURL,
		"startBlock", cfg.StartBlock,
		"endBlock", cfg.EndBlock,
		"totalBlocks", totalBlocks,
		"blockWorkers", cfg.BlockReceiptWorkers,
		"traceWorkers", cfg.TraceWorkers,
		"uploadWorkers", cfg.UploadWorkers,
		"blockBatchSize", cfg.BlockBatchSize,
		"traceBatchSize", cfg.TraceBatchSize,
		"bucket", cfg.Bucket,
	)

	stats := &Stats{startTime: time.Now()}

	// Start progress reporter
	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	go reportProgress(progressCtx, stats, totalBlocks, partitionCache, logger)

	// Create channels for pipelined processing
	blockWorkCh := make(chan int64, cfg.BlockReceiptWorkers*2) // Block batch start numbers
	traceWorkCh := make(chan []int64, cfg.TraceWorkers*2)      // Batches of blocks needing traces
	uploadCh := make(chan UploadJob, cfg.UploadWorkers*4)      // S3 upload jobs
	traceCollectorCh := make(chan int64, 10000)                // Blocks ready for trace fetching

	var traceWg sync.WaitGroup

	// Start S3 upload workers
	var uploadWg sync.WaitGroup
	for i := 0; i < cfg.UploadWorkers; i++ {
		uploadWg.Add(1)
		go func(workerID int) {
			defer uploadWg.Done()
			uploadWorker(ctx, workerID, s3Writer, partitionCache, uploadCh, stats, logger)
		}(i)
	}

	// Start trace collector - batches individual blocks into trace batches
	traceWg.Add(1)
	go func() {
		defer traceWg.Done()
		defer close(traceWorkCh)
		traceCollector(ctx, cfg, traceCollectorCh, traceWorkCh, partitionCache, stats, logger)
	}()

	// Start block+receipt workers
	var blockWg sync.WaitGroup
	for i := 0; i < cfg.BlockReceiptWorkers; i++ {
		blockWg.Add(1)
		go func(workerID int) {
			defer blockWg.Done()
			blockReceiptWorker(ctx, workerID, client, partitionCache, cfg, blockWorkCh, traceCollectorCh, uploadCh, stats, logger)
		}(i)
	}

	// Start trace workers
	for i := 0; i < cfg.TraceWorkers; i++ {
		traceWg.Add(1)
		go func(workerID int) {
			defer traceWg.Done()
			traceWorker(ctx, workerID, client, partitionCache, cfg.Bucket, traceWorkCh, uploadCh, stats, logger)
		}(i)
	}

	// Feed block work
	go func() {
		defer close(blockWorkCh)
		for blockNum := cfg.StartBlock; blockNum <= cfg.EndBlock; blockNum += int64(cfg.BlockBatchSize) {
			select {
			case <-ctx.Done():
				return
			case blockWorkCh <- blockNum:
			}
		}
	}()

	// Wait for block workers to finish, then close traceCollectorCh
	go func() {
		blockWg.Wait()
		close(traceCollectorCh)
	}()

	// Wait for trace collector and trace workers to finish
	traceWg.Wait()

	// Close upload channel and wait for uploads to complete
	close(uploadCh)
	uploadWg.Wait()

	// Final stats
	elapsed := time.Since(stats.startTime)
	hits, misses := partitionCache.GetStats()

	// Calculate average times
	avgRpcBlock := time.Duration(0)
	if calls := stats.rpcBlockCalls.Load(); calls > 0 {
		avgRpcBlock = time.Duration(stats.rpcBlockTime.Load() / calls)
	}
	avgRpcTrace := time.Duration(0)
	if calls := stats.rpcTraceCalls.Load(); calls > 0 {
		avgRpcTrace = time.Duration(stats.rpcTraceTime.Load() / calls)
	}
	avgUpload := time.Duration(0)
	if calls := stats.s3UploadCalls.Load(); calls > 0 {
		avgUpload = time.Duration(stats.s3UploadTime.Load() / calls)
	}
	avgS3Check := time.Duration(0)
	if calls := stats.s3CheckCalls.Load(); calls > 0 {
		avgS3Check = time.Duration(stats.s3CheckTime.Load() / calls)
	}

	logger.Info("download complete",
		"blocksProcessed", stats.blocksProcessed.Load(),
		"blocksSkipped", stats.blocksSkipped.Load(),
		"blocksFailed", stats.blocksFailed.Load(),
		"tracesProcessed", stats.tracesProcessed.Load(),
		"tracesSkipped", stats.tracesSkipped.Load(),
		"tracesFailed", stats.tracesFailed.Load(),
		"uploadsCompleted", stats.uploadsCompleted.Load(),
		"uploadsFailed", stats.uploadsFailed.Load(),
		"totalBytesWritten", formatBytes(stats.blockBytesWritten.Load()+stats.traceBytesWritten.Load()),
		"blocksPerSec", fmt.Sprintf("%.1f", float64(stats.blocksProcessed.Load()+stats.blocksSkipped.Load())/elapsed.Seconds()),
		"elapsed", elapsed.Round(time.Second),
		"cacheHits", hits,
		"cacheMisses", misses,
	)

	// Timing breakdown
	logger.Info("timing breakdown",
		"avgRpcBlockBatch", avgRpcBlock.Round(time.Millisecond),
		"avgRpcTraceBatch", avgRpcTrace.Round(time.Millisecond),
		"avgS3Upload", avgUpload.Round(time.Millisecond),
		"avgS3Check", avgS3Check.Round(time.Millisecond),
		"totalRpcBlockTime", time.Duration(stats.rpcBlockTime.Load()).Round(time.Second),
		"totalRpcTraceTime", time.Duration(stats.rpcTraceTime.Load()).Round(time.Second),
		"totalS3UploadTime", time.Duration(stats.s3UploadTime.Load()).Round(time.Second),
		"totalS3CheckTime", time.Duration(stats.s3CheckTime.Load()).Round(time.Second),
	)

	return nil
}

// blockReceiptWorker fetches blocks and receipts, queues S3 uploads, and signals trace collection.
func blockReceiptWorker(
	ctx context.Context,
	workerID int,
	client *alchemy.Client,
	partitionCache *PartitionCache,
	cfg Config,
	workCh <-chan int64,
	traceCollectorCh chan<- int64,
	uploadCh chan<- UploadJob,
	stats *Stats,
	logger *slog.Logger,
) {
	logger = logger.With("worker", workerID, "type", "block")

	for batchStart := range workCh {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batchEnd := batchStart + int64(cfg.BlockBatchSize) - 1
		if batchEnd > cfg.EndBlock {
			batchEnd = cfg.EndBlock
		}

		// Check what exists and build fetch list
		blocksToFetch := make([]int64, 0, batchEnd-batchStart+1)

		// In dry-run mode, skip S3 checks and fetch all blocks
		if cfg.DryRun {
			for blockNum := batchStart; blockNum <= batchEnd; blockNum++ {
				blocksToFetch = append(blocksToFetch, blockNum)
			}
		} else {
			s3CheckStart := time.Now()
			for blockNum := batchStart; blockNum <= batchEnd; blockNum++ {
				// Check if ALL data (block, receipts, traces) exists
				hasAllData, err := partitionCache.HasAllData(ctx, blockNum)
				if err != nil {
					logger.Warn("failed to check S3", "block", blockNum, "error", err)
					blocksToFetch = append(blocksToFetch, blockNum)
					continue
				}

				if hasAllData {
					// All 3 files exist - completely skip this block
					stats.blocksSkipped.Add(1)
					stats.tracesSkipped.Add(1)
				} else {
					// Check if just block+receipts exist (need traces only)
					hasBlockAndReceipts, err := partitionCache.HasBlockAndReceipts(ctx, blockNum)
					if err != nil {
						logger.Warn("failed to check S3", "block", blockNum, "error", err)
						blocksToFetch = append(blocksToFetch, blockNum)
						continue
					}

					if hasBlockAndReceipts {
						// Block+receipts exist but traces missing - signal for trace fetch only
						stats.blocksSkipped.Add(1)
						select {
						case traceCollectorCh <- blockNum:
						case <-ctx.Done():
							return
						}
					} else {
						// Need to fetch block+receipts (and traces)
						blocksToFetch = append(blocksToFetch, blockNum)
					}
				}
			}
			stats.s3CheckTime.Add(time.Since(s3CheckStart).Nanoseconds())
			stats.s3CheckCalls.Add(1)
		}

		if len(blocksToFetch) == 0 {
			continue
		}

		// Fetch blocks and receipts
		rpcStart := time.Now()
		results, err := client.GetBlocksAndReceiptsBatch(ctx, blocksToFetch, true)
		stats.rpcBlockTime.Add(time.Since(rpcStart).Nanoseconds())
		stats.rpcBlockCalls.Add(1)
		if err != nil {
			logger.Warn("batch fetch failed", "from", batchStart, "to", batchEnd, "error", err)
			for range blocksToFetch {
				stats.blocksFailed.Add(1)
			}
			continue
		}

		// Queue uploads and signal for traces
		for _, r := range results {
			if r.BlockErr != nil || r.ReceiptsErr != nil {
				logger.Warn("block data has errors", "block", r.BlockNumber, "blockErr", r.BlockErr, "receiptsErr", r.ReceiptsErr)
				stats.blocksFailed.Add(1)
				continue
			}

			partition := partition.GetPartition(r.BlockNumber)

			// Queue block upload
			if r.Block != nil {
				select {
				case uploadCh <- UploadJob{
					Bucket:   cfg.Bucket,
					Key:      fmt.Sprintf("%s/%d_1_block.json.gz", partition, r.BlockNumber),
					Data:     r.Block,
					DataType: "block",
					BlockNum: r.BlockNumber,
				}:
					stats.uploadsQueued.Add(1)
				case <-ctx.Done():
					return
				}
			}

			// Queue receipts upload
			if r.Receipts != nil {
				select {
				case uploadCh <- UploadJob{
					Bucket:   cfg.Bucket,
					Key:      fmt.Sprintf("%s/%d_1_receipts.json.gz", partition, r.BlockNumber),
					Data:     r.Receipts,
					DataType: "receipts",
					BlockNum: r.BlockNumber,
				}:
					stats.uploadsQueued.Add(1)
				case <-ctx.Done():
					return
				}
			}

			stats.blocksProcessed.Add(1)

			// Signal for trace fetching
			select {
			case traceCollectorCh <- r.BlockNumber:
			case <-ctx.Done():
				return
			}
		}
	}

	// Signal that this worker is done sending to trace collector
	// (handled by closing traceCollectorCh after all workers done)
}

// traceCollector batches blocks for trace fetching.
func traceCollector(
	ctx context.Context,
	cfg Config,
	inCh <-chan int64,
	outCh chan<- []int64,
	partitionCache *PartitionCache,
	stats *Stats,
	logger *slog.Logger,
) {
	batch := make([]int64, 0, cfg.TraceBatchSize)
	flushTimer := time.NewTimer(500 * time.Millisecond)
	defer flushTimer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// In dry-run mode, process all blocks without S3 checks
		var toProcess []int64
		if cfg.DryRun {
			toProcess = batch
		} else {
			// Filter out blocks that already have traces
			toProcess = make([]int64, 0, len(batch))
			for _, blockNum := range batch {
				hasTraces, err := partitionCache.HasTraces(ctx, blockNum)
				if err != nil {
					logger.Warn("failed to check traces", "block", blockNum, "error", err)
					toProcess = append(toProcess, blockNum)
					continue
				}
				if hasTraces {
					stats.tracesSkipped.Add(1)
				} else {
					toProcess = append(toProcess, blockNum)
				}
			}
		}

		if len(toProcess) > 0 {
			select {
			case outCh <- toProcess:
			case <-ctx.Done():
				return
			}
		}
		batch = make([]int64, 0, cfg.TraceBatchSize)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case blockNum, ok := <-inCh:
			if !ok {
				// Channel closed, flush remaining
				flush()
				return
			}
			batch = append(batch, blockNum)
			if len(batch) >= cfg.TraceBatchSize {
				flush()
				flushTimer.Reset(500 * time.Millisecond)
			}
		case <-flushTimer.C:
			flush()
			flushTimer.Reset(500 * time.Millisecond)
		}
	}
}

// traceWorker fetches traces and queues S3 uploads.
func traceWorker(
	ctx context.Context,
	workerID int,
	client *alchemy.Client,
	partitionCache *PartitionCache,
	bucket string,
	workCh <-chan []int64,
	uploadCh chan<- UploadJob,
	stats *Stats,
	logger *slog.Logger,
) {
	logger = logger.With("worker", workerID, "type", "trace")

	for batch := range workCh {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rpcStart := time.Now()
		traces, errs := client.GetTracesBatch(ctx, batch)
		stats.rpcTraceTime.Add(time.Since(rpcStart).Nanoseconds())
		stats.rpcTraceCalls.Add(1)

		for _, blockNum := range batch {
			if err, hasErr := errs[blockNum]; hasErr {
				logger.Warn("trace fetch failed", "block", blockNum, "error", err)
				stats.tracesFailed.Add(1)
				continue
			}

			traceData, ok := traces[blockNum]
			if !ok {
				logger.Warn("missing trace data", "block", blockNum)
				stats.tracesFailed.Add(1)
				continue
			}

			partition := partition.GetPartition(blockNum)
			select {
			case uploadCh <- UploadJob{
				Bucket:   bucket,
				Key:      fmt.Sprintf("%s/%d_1_traces.json.gz", partition, blockNum),
				Data:     traceData,
				DataType: "traces",
				BlockNum: blockNum,
			}:
				stats.uploadsQueued.Add(1)
			case <-ctx.Done():
				return
			}

			stats.tracesProcessed.Add(1)
		}
	}
}

// uploadWorker handles async S3 uploads.
func uploadWorker(
	ctx context.Context,
	workerID int,
	s3Writer outbound.S3Writer,
	partitionCache *PartitionCache,
	workCh <-chan UploadJob,
	stats *Stats,
	dryRun bool,
	logger *slog.Logger,
) {
	logger = logger.With("worker", workerID, "type", "upload")

	for job := range workCh {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// In dry-run mode, skip actual S3 upload but still count the bytes
		if dryRun {
			switch job.DataType {
			case "block", "receipts":
				stats.blockBytesWritten.Add(int64(len(job.Data)))
			case "traces":
				stats.traceBytesWritten.Add(int64(len(job.Data)))
			}
			stats.uploadsCompleted.Add(1)
			continue
		}

		// Write data with gzip compression (S3 writer handles compression and sets Content-Encoding)
		uploadStart := time.Now()
		written, err := s3Writer.WriteFileIfNotExists(ctx, job.Bucket, job.Key, bytes.NewReader(job.Data), true)
		stats.s3UploadTime.Add(time.Since(uploadStart).Nanoseconds())
		stats.s3UploadCalls.Add(1)
		if err != nil {
			logger.Warn("upload failed", "key", job.Key, "error", err)
			stats.uploadsFailed.Add(1)
			continue
		}

		if written {
			switch job.DataType {
			case "block", "receipts":
				stats.blockBytesWritten.Add(int64(len(job.Data)))
			case "traces":
				stats.traceBytesWritten.Add(int64(len(job.Data)))
			}
			partitionCache.MarkWritten(job.BlockNum, job.DataType)
		}
		stats.uploadsCompleted.Add(1)
	}
}

func reportProgress(ctx context.Context, stats *Stats, totalBlocks int64, partitionCache *PartitionCache, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second) // More frequent updates
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			blocksProcessed := stats.blocksProcessed.Load()
			blocksSkipped := stats.blocksSkipped.Load()
			blocksFailed := stats.blocksFailed.Load()
			blocksTotal := blocksProcessed + blocksSkipped + blocksFailed

			tracesProcessed := stats.tracesProcessed.Load()
			tracesSkipped := stats.tracesSkipped.Load()
			tracesFailed := stats.tracesFailed.Load()

			uploadsQueued := stats.uploadsQueued.Load()
			uploadsCompleted := stats.uploadsCompleted.Load()
			uploadsFailed := stats.uploadsFailed.Load()
			uploadsPending := uploadsQueued - uploadsCompleted - uploadsFailed

			elapsed := time.Since(stats.startTime)
			blocksPerSec := float64(blocksTotal) / elapsed.Seconds()
			tracesPerSec := float64(tracesProcessed+tracesSkipped+tracesFailed) / elapsed.Seconds()

			pct := float64(blocksTotal) / float64(totalBlocks) * 100

			hits, misses := partitionCache.GetStats()
			hitRate := float64(0)
			if hits+misses > 0 {
				hitRate = float64(hits) / float64(hits+misses) * 100
			}

			totalBytes := stats.blockBytesWritten.Load() + stats.traceBytesWritten.Load()

			// Calculate current average times
			avgRpcBlock := time.Duration(0)
			if calls := stats.rpcBlockCalls.Load(); calls > 0 {
				avgRpcBlock = time.Duration(stats.rpcBlockTime.Load() / calls)
			}
			avgRpcTrace := time.Duration(0)
			if calls := stats.rpcTraceCalls.Load(); calls > 0 {
				avgRpcTrace = time.Duration(stats.rpcTraceTime.Load() / calls)
			}
			avgUpload := time.Duration(0)
			if calls := stats.s3UploadCalls.Load(); calls > 0 {
				avgUpload = time.Duration(stats.s3UploadTime.Load() / calls)
			}

			logger.Info("progress",
				"pct", fmt.Sprintf("%.1f%%", pct),
				"blocks", fmt.Sprintf("%d/%d/%d", blocksProcessed, blocksSkipped, blocksFailed),
				"traces", fmt.Sprintf("%d/%d/%d", tracesProcessed, tracesSkipped, tracesFailed),
				"uploads", fmt.Sprintf("%d pending", uploadsPending),
				"blk/s", fmt.Sprintf("%.1f", blocksPerSec),
				"trc/s", fmt.Sprintf("%.1f", tracesPerSec),
				"bytes", formatBytes(totalBytes),
				"avgRpcBlk", avgRpcBlock.Round(time.Millisecond),
				"avgRpcTrc", avgRpcTrace.Round(time.Millisecond),
				"avgUpload", avgUpload.Round(time.Millisecond),
				"cache", fmt.Sprintf("%.0f%%", hitRate),
				"elapsed", elapsed.Round(time.Second),
			)
		}
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
