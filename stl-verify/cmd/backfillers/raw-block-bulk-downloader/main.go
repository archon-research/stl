// Package main provides a CLI tool for bulk downloading Ethereum block data.
// It fetches blocks, receipts, and traces from a local Erigon node and writes to S3.
//
// Architecture:
//   - RPC workers fetch block+receipt data from Erigon
//   - As blocks complete, they're immediately queued for trace fetching (pipelined)
//   - S3 uploads happen asynchronously in a separate upload pool
//   - This decouples RPC fetching from S3 I/O for maximum throughput
//
// The archive's contract is highest-version-wins, so every height is weighed
// against what is already there: a first archive lands at version 0, an archived
// version whose hash the chain no longer recognises is corrected at the next free
// version, and nothing is ever overwritten. See planBlock.
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
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
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
	DryRun     bool

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

	planFresh     atomic.Int64
	planSkip      atomic.Int64
	planFill      atomic.Int64
	planRepublish atomic.Int64

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

func (s *Stats) recordPlan(action blockAction) {
	switch action {
	case actionFresh:
		s.planFresh.Add(1)
	case actionSkip:
		s.planSkip.Add(1)
	case actionFill:
		s.planFill.Add(1)
	case actionRepublish:
		s.planRepublish.Add(1)
	}
}

// UploadJob represents an S3 upload to be performed asynchronously.
type UploadJob struct {
	Bucket   string
	Key      string
	Data     []byte // Raw uncompressed data (S3 writer handles compression)
	DataType s3key.DataType
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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	err := run(ctx, cfg, logger)
	shuttingDown := ctx.Err() != nil
	cancel()
	if err != nil {
		if shuttingDown {
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
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Log every block's decision and upload nothing")
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
	rpcClient, err := createRPCClient(cfg, logger)
	if err != nil {
		return err
	}

	s3Writer, s3Reader, err := createS3Clients(ctx, cfg, logger)
	if err != nil {
		return err
	}

	partitionCache := NewPartitionCache(s3Reader, cfg.Bucket, logger)
	stats := &Stats{startTime: time.Now()}
	planner := &blockPlanner{cache: partitionCache, reader: s3Reader, bucket: cfg.Bucket, stats: stats}

	logStartupInfo(cfg, logger)

	stopProgressReporter := startProgressReporter(ctx, stats, cfg.EndBlock-cfg.StartBlock+1, partitionCache, logger)
	defer stopProgressReporter()

	pipeline := newPipeline(cfg)
	pipeline.startUploadWorkers(ctx, s3Writer, stats, logger)
	pipeline.startTraceCollector(ctx, cfg)
	pipeline.startBlockReceiptWorkers(ctx, rpcClient, planner, cfg, stats, logger)
	pipeline.startTraceWorkers(ctx, rpcClient, cfg.Bucket, stats, logger)
	pipeline.feedBlockWork(ctx, cfg.StartBlock, cfg.EndBlock, cfg.BlockBatchSize)

	pipeline.waitForCompletion()

	logFinalStats(stats, partitionCache, logger)
	return failureError(stats)
}

// failureError reports the holes a run left behind: an operator reading only the
// exit code would take the summary line for a complete archive.
func failureError(stats *Stats) error {
	blocks, traces, uploads := stats.blocksFailed.Load(), stats.tracesFailed.Load(), stats.uploadsFailed.Load()
	if blocks == 0 && traces == 0 && uploads == 0 {
		return nil
	}
	return fmt.Errorf("archive incomplete: %d blocks, %d traces and %d uploads failed; re-run the same range", blocks, traces, uploads)
}

func createRPCClient(cfg Config, logger *slog.Logger) (*alchemy.Client, error) {
	totalRPCWorkers := cfg.BlockReceiptWorkers + cfg.TraceWorkers

	httpClient := &http.Client{
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

	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:        cfg.RPCURL,
		Timeout:        DefaultTimeout,
		MaxRetries:     DefaultMaxRetries,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		BackoffFactor:  2.0,
		Logger:         logger,
		HTTPClient:     httpClient,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}
	return client, nil
}

func createS3Clients(ctx context.Context, cfg Config, logger *slog.Logger) (*s3.Writer, *s3.Reader, error) {
	var loadOpts []func(*config.LoadOptions) error
	if cfg.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(cfg.Region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	httpClient := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          cfg.UploadWorkers * 2,
			MaxIdleConnsPerHost:   cfg.UploadWorkers,
			MaxConnsPerHost:       cfg.UploadWorkers,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
	}

	endpoint := s3.EndpointOptionsFromEnv()
	writer := s3.NewWriterWithHTTPClient(awsCfg, httpClient, logger, endpoint...)
	reader := s3.NewReaderWithHTTPClient(awsCfg, httpClient, logger, endpoint...)
	return writer, reader, nil
}

func logStartupInfo(cfg Config, logger *slog.Logger) {
	logger.Info("starting pipelined bulk download",
		"rpcURL", cfg.RPCURL,
		"startBlock", cfg.StartBlock,
		"endBlock", cfg.EndBlock,
		"totalBlocks", cfg.EndBlock-cfg.StartBlock+1,
		"blockWorkers", cfg.BlockReceiptWorkers,
		"traceWorkers", cfg.TraceWorkers,
		"uploadWorkers", cfg.UploadWorkers,
		"blockBatchSize", cfg.BlockBatchSize,
		"traceBatchSize", cfg.TraceBatchSize,
		"bucket", cfg.Bucket,
		"dryRun", cfg.DryRun,
	)
}

func startProgressReporter(ctx context.Context, stats *Stats, totalBlocks int64, cache *PartitionCache, logger *slog.Logger) func() {
	progressCtx, cancel := context.WithCancel(ctx)
	go reportProgress(progressCtx, stats, totalBlocks, cache, logger)
	return cancel
}

func logFinalStats(stats *Stats, cache *PartitionCache, logger *slog.Logger) {
	elapsed := time.Since(stats.startTime)
	hits, misses := cache.GetStats()

	logger.Info("download complete",
		"blocksProcessed", stats.blocksProcessed.Load(),
		"blocksSkipped", stats.blocksSkipped.Load(),
		"blocksFailed", stats.blocksFailed.Load(),
		"tracesProcessed", stats.tracesProcessed.Load(),
		"tracesSkipped", stats.tracesSkipped.Load(),
		"tracesFailed", stats.tracesFailed.Load(),
		"uploadsCompleted", stats.uploadsCompleted.Load(),
		"uploadsFailed", stats.uploadsFailed.Load(),
		"planFresh", stats.planFresh.Load(),
		"planSkip", stats.planSkip.Load(),
		"planFill", stats.planFill.Load(),
		"planRepublish", stats.planRepublish.Load(),
		"totalBytesWritten", formatBytes(stats.blockBytesWritten.Load()+stats.traceBytesWritten.Load()),
		"blocksPerSec", fmt.Sprintf("%.1f", float64(stats.blocksProcessed.Load()+stats.blocksSkipped.Load())/elapsed.Seconds()),
		"elapsed", elapsed.Round(time.Second),
		"cacheHits", hits,
		"cacheMisses", misses,
	)

	logTimingBreakdown(stats, logger)
}

func logTimingBreakdown(stats *Stats, logger *slog.Logger) {
	avgRpcBlock := avgDuration(stats.rpcBlockTime.Load(), stats.rpcBlockCalls.Load())
	avgRpcTrace := avgDuration(stats.rpcTraceTime.Load(), stats.rpcTraceCalls.Load())
	avgUpload := avgDuration(stats.s3UploadTime.Load(), stats.s3UploadCalls.Load())
	avgS3Check := avgDuration(stats.s3CheckTime.Load(), stats.s3CheckCalls.Load())

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
}

func avgDuration(totalNanos, count int64) time.Duration {
	if count == 0 {
		return 0
	}
	return time.Duration(totalNanos / count)
}

// pipeline coordinates the concurrent worker pools for bulk downloading.
type pipeline struct {
	blockWorkCh      chan int64
	traceWorkCh      chan []traceRequest
	uploadCh         chan UploadJob
	traceCollectorCh chan traceRequest

	uploadWorkers int
	traceWorkers  int
	blockWorkers  int

	uploadWg sync.WaitGroup
	blockWg  sync.WaitGroup
	traceWg  sync.WaitGroup
}

func newPipeline(cfg Config) *pipeline {
	return &pipeline{
		blockWorkCh:      make(chan int64, cfg.BlockReceiptWorkers*2),
		traceWorkCh:      make(chan []traceRequest, cfg.TraceWorkers*2),
		uploadCh:         make(chan UploadJob, cfg.UploadWorkers*4),
		traceCollectorCh: make(chan traceRequest, 10000),
		uploadWorkers:    cfg.UploadWorkers,
		traceWorkers:     cfg.TraceWorkers,
		blockWorkers:     cfg.BlockReceiptWorkers,
	}
}

func (p *pipeline) startUploadWorkers(ctx context.Context, writer outbound.S3Writer, stats *Stats, logger *slog.Logger) {
	for i := 0; i < p.uploadWorkers; i++ {
		p.uploadWg.Add(1)
		go func(workerID int) {
			defer p.uploadWg.Done()
			uploadWorker(ctx, workerID, writer, p.uploadCh, stats, logger)
		}(i)
	}
}

func (p *pipeline) startTraceCollector(ctx context.Context, cfg Config) {
	p.traceWg.Go(func() {
		defer close(p.traceWorkCh)
		traceCollector(ctx, cfg, p.traceCollectorCh, p.traceWorkCh)
	})
}

func (p *pipeline) startBlockReceiptWorkers(ctx context.Context, client *alchemy.Client, planner *blockPlanner, cfg Config, stats *Stats, logger *slog.Logger) {
	for i := 0; i < p.blockWorkers; i++ {
		p.blockWg.Add(1)
		go func(workerID int) {
			defer p.blockWg.Done()
			blockReceiptWorker(ctx, workerID, client, planner, cfg, p.blockWorkCh, p.traceCollectorCh, p.uploadCh, stats, logger)
		}(i)
	}
}

func (p *pipeline) startTraceWorkers(ctx context.Context, client *alchemy.Client, bucket string, stats *Stats, logger *slog.Logger) {
	for i := 0; i < p.traceWorkers; i++ {
		p.traceWg.Add(1)
		go func(workerID int) {
			defer p.traceWg.Done()
			traceWorker(ctx, workerID, client, bucket, p.traceWorkCh, p.uploadCh, stats, logger)
		}(i)
	}
}

func (p *pipeline) feedBlockWork(ctx context.Context, startBlock, endBlock int64, batchSize int) {
	go func() {
		defer close(p.blockWorkCh)
		for blockNum := startBlock; blockNum <= endBlock; blockNum += int64(batchSize) {
			select {
			case <-ctx.Done():
				return
			case p.blockWorkCh <- blockNum:
			}
		}
	}()

	go func() {
		p.blockWg.Wait()
		close(p.traceCollectorCh)
	}()
}

func (p *pipeline) waitForCompletion() {
	p.traceWg.Wait()
	close(p.uploadCh)
	p.uploadWg.Wait()
}

// blockReceiptWorker fetches blocks and receipts, then acts on each height's plan.
func blockReceiptWorker(
	ctx context.Context,
	workerID int,
	client *alchemy.Client,
	planner *blockPlanner,
	cfg Config,
	workCh <-chan int64,
	traceCh chan<- traceRequest,
	uploadCh chan<- UploadJob,
	stats *Stats,
	logger *slog.Logger,
) {
	logger = logger.With("worker", workerID, "type", "block")

	for batchStart := range workCh {
		if ctx.Err() != nil {
			return
		}

		batchEnd := min(batchStart+int64(cfg.BlockBatchSize)-1, cfg.EndBlock)
		results, err := fetchBlocksAndReceipts(ctx, client, batchStart, batchEnd, stats)
		if err != nil {
			logger.Warn("batch fetch failed", "from", batchStart, "to", batchEnd, "error", err)
			stats.blocksFailed.Add(batchEnd - batchStart + 1)
			continue
		}

		for _, r := range results {
			if err := applyBlockPlan(ctx, r, planner, cfg, uploadCh, traceCh, stats, logger); err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Warn("block left unarchived", "block", r.BlockNumber, "error", err)
				stats.blocksFailed.Add(1)
			}
		}
	}
}

// fetchBlocksAndReceipts fetches one inclusive range of blocks with their receipts.
func fetchBlocksAndReceipts(ctx context.Context, client *alchemy.Client, from, to int64, stats *Stats) ([]outbound.BlockData, error) {
	blockNums := make([]int64, 0, to-from+1)
	for blockNum := from; blockNum <= to; blockNum++ {
		blockNums = append(blockNums, blockNum)
	}

	rpcStart := time.Now()
	results, err := client.GetBlocksAndReceiptsBatch(ctx, blockNums, true)
	stats.rpcBlockTime.Add(time.Since(rpcStart).Nanoseconds())
	stats.rpcBlockCalls.Add(1)
	return results, err
}

// traceCollector batches the heights whose traces are still to fetch.
func traceCollector(ctx context.Context, cfg Config, inCh <-chan traceRequest, outCh chan<- []traceRequest) {
	batch := make([]traceRequest, 0, cfg.TraceBatchSize)
	flushTimer := time.NewTimer(500 * time.Millisecond)
	defer flushTimer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		select {
		case outCh <- batch:
		case <-ctx.Done():
			return
		}
		batch = make([]traceRequest, 0, cfg.TraceBatchSize)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-inCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, req)
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

// traceWorker fetches traces and queues them for upload at the version the plan chose.
func traceWorker(
	ctx context.Context,
	workerID int,
	client *alchemy.Client,
	bucket string,
	workCh <-chan []traceRequest,
	uploadCh chan<- UploadJob,
	stats *Stats,
	logger *slog.Logger,
) {
	logger = logger.With("worker", workerID, "type", "trace")

	for batch := range workCh {
		if ctx.Err() != nil {
			return
		}

		blockNums := make([]int64, len(batch))
		for i, req := range batch {
			blockNums[i] = req.BlockNum
		}

		rpcStart := time.Now()
		traces, errs := client.GetTracesBatch(ctx, blockNums)
		stats.rpcTraceTime.Add(time.Since(rpcStart).Nanoseconds())
		stats.rpcTraceCalls.Add(1)

		for _, req := range batch {
			if err, hasErr := errs[req.BlockNum]; hasErr {
				logger.Warn("trace fetch failed", "block", req.BlockNum, "error", err)
				stats.tracesFailed.Add(1)
				continue
			}

			traceData, ok := traces[req.BlockNum]
			if !ok {
				logger.Warn("missing trace data", "block", req.BlockNum)
				stats.tracesFailed.Add(1)
				continue
			}

			select {
			case uploadCh <- UploadJob{
				Bucket:   bucket,
				Key:      s3key.Build(req.BlockNum, req.Version, s3key.Traces),
				Data:     traceData,
				DataType: s3key.Traces,
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
	workCh <-chan UploadJob,
	stats *Stats,
	logger *slog.Logger,
) {
	logger = logger.With("worker", workerID, "type", "upload")

	for job := range workCh {
		select {
		case <-ctx.Done():
			return
		default:
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
			case s3key.Block, s3key.Receipts:
				stats.blockBytesWritten.Add(int64(len(job.Data)))
			case s3key.Traces:
				stats.traceBytesWritten.Add(int64(len(job.Data)))
			}
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
				"plans", fmt.Sprintf("fresh %d / skip %d / fill %d / republish %d",
					stats.planFresh.Load(), stats.planSkip.Load(), stats.planFill.Load(), stats.planRepublish.Load()),
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
