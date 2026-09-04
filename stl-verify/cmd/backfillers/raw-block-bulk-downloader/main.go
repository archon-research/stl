// Package main provides a CLI tool for bulk downloading block data from any
// chain the repo archives. It fetches blocks, receipts, and traces from a node
// and writes to that chain's raw bucket.
//
// Architecture:
//   - RPC workers fetch block+receipt data from the node
//   - As blocks complete, they're immediately queued for trace fetching (pipelined)
//   - S3 uploads happen asynchronously in a separate upload pool
//   - This decouples RPC fetching from S3 I/O for maximum throughput
//
// --chain-id decides which data types the archive holds (only Ethereum's carries
// traces) and is checked against the bucket's own chain, so an L2 run reports
// neither missing traces nor another chain's heights.
//
// The archive's contract is highest-version-wins, so every height is weighed
// against what is already there: a first archive lands at version 0, an archived
// version whose hash the chain no longer recognises is corrected at the next free
// version, and nothing is ever overwritten. See planBlock. A run refuses a range
// reaching past the node's finalized head, where a height that loses its fork
// could never be corrected; --allow-unfinalized overrides that.
//
// With --dry-run it writes nothing and is the audit for ARCT-379 holes; --report
// leaves every non-skip decision in a file, one JSON object per line, where a
// "republish" row is a height whose only archived version is a losing fork.
//
// Usage:
//
//	./bulk-download \
//	  --chain-id=1 \
//	  --rpc-url=http://localhost:8545 \
//	  --start-block=16000000 \
//	  --end-block=21000000 \
//	  --bucket=stl-sentinelstaging-ethereum-raw-89d540d0
package main

import (
	"bytes"
	"context"
	"errors"
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
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
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

func run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	logStartupInfo(cfg, logger)

	// Before the node and the archive are touched: a report path that cannot be
	// created is the whole output of an audit, and finding that out an hour in
	// is an audit not run.
	report, err := newDecisionReport(cfg.ReportPath)
	if err != nil {
		return err
	}

	rpcClient, err := createRPCClient(cfg, logger)
	if err != nil {
		return err
	}

	if err := guardFinality(ctx, rpcClient, cfg, logger); err != nil {
		return err
	}

	s3Writer, s3Reader, err := createS3Clients(ctx, cfg, logger)
	if err != nil {
		return err
	}
	if err := checkArchiveReachable(ctx, s3Reader, cfg.Bucket); err != nil {
		return err
	}

	types, err := chainDataTypes(cfg.ChainID)
	if err != nil {
		return err
	}

	partitionCache := NewPartitionCache(s3Reader, cfg.Bucket, logger)
	stats := &Stats{startTime: time.Now()}
	planner := &blockPlanner{cache: partitionCache, reader: s3Reader, bucket: cfg.Bucket, types: types, stats: stats}

	stopProgressReporter := startProgressReporter(ctx, stats, cfg.EndBlock-cfg.StartBlock+1, partitionCache, logger)
	defer stopProgressReporter()

	pipeline := newPipeline(cfg)
	archiver := blockArchiver{
		client:   rpcClient,
		planner:  planner,
		report:   report,
		cfg:      cfg,
		uploadCh: pipeline.uploadCh,
		traceCh:  pipeline.traceCollectorCh,
		stats:    stats,
		logger:   logger,
	}
	pipeline.startUploadWorkers(ctx, s3Writer, stats, logger)
	pipeline.startTraceCollector(ctx, cfg)
	pipeline.startBlockReceiptWorkers(ctx, archiver)
	pipeline.startTraceWorkers(ctx, rpcClient, cfg.Bucket, stats, logger)
	pipeline.feedBlockWork(ctx, cfg.StartBlock, cfg.EndBlock, cfg.BlockBatchSize)

	pipeline.waitForCompletion()

	logFinalStats(stats, partitionCache, logger)
	return errors.Join(failureError(stats), report.close())
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

	// Reader and writer share this pool, so it holds both roles at once: sized
	// to the uploaders alone, the block workers' ranged GETs queue behind PUTs.
	conns := cfg.BlockReceiptWorkers + cfg.UploadWorkers
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          conns * 2,
			MaxIdleConnsPerHost:   conns,
			MaxConnsPerHost:       conns,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
	}

	options := append([]func(*awss3.Options){func(o *awss3.Options) { o.HTTPClient = httpClient }},
		s3.EndpointOptionsFromEnv()...)
	return s3.NewWriterWithOptions(awsCfg, logger, options...), s3.NewReaderWithOptions(awsCfg, logger, options...), nil
}

// checkArchiveReachable fails the run before any worker starts. Without it a
// bucket that is not there, or one this run may not list, is discovered once per
// partition — and the run walks the whole range before saying so.
func checkArchiveReachable(ctx context.Context, reader *s3.Reader, bucket string) error {
	if err := reader.HeadBucket(ctx, bucket); err != nil {
		return fmt.Errorf("cannot reach the archive bucket %s; it must exist and this run needs s3:ListBucket on it: %w",
			bucket, err)
	}
	return nil
}

func logStartupInfo(cfg Config, logger *slog.Logger) {
	logger.Info("starting pipelined bulk download",
		"chainID", cfg.ChainID,
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
		"report", cfg.ReportPath,
		"allowUnfinalized", cfg.AllowUnfinalized,
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

func (p *pipeline) startBlockReceiptWorkers(ctx context.Context, archiver blockArchiver) {
	for i := 0; i < p.blockWorkers; i++ {
		p.blockWg.Add(1)
		go func(workerID int) {
			defer p.blockWg.Done()
			worker := archiver
			worker.logger = archiver.logger.With("worker", workerID, "type", "block")
			worker.archiveBatches(ctx, p.blockWorkCh)
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

// blockArchiver archives the batches one block worker is handed.
type blockArchiver struct {
	client   *alchemy.Client
	planner  *blockPlanner
	report   *decisionReport
	cfg      Config
	uploadCh chan<- UploadJob
	traceCh  chan<- traceRequest
	stats    *Stats
	logger   *slog.Logger
}

func (a blockArchiver) archiveBatches(ctx context.Context, workCh <-chan int64) {
	for batchStart := range workCh {
		if ctx.Err() != nil {
			return
		}
		a.archiveBatch(ctx, batchStart, min(batchStart+int64(a.cfg.BlockBatchSize)-1, a.cfg.EndBlock))
	}
}

// archiveBatch archives one inclusive range of heights: it plans each of them
// first, then pays for the full block and receipts of the heights a plan writes.
func (a blockArchiver) archiveBatch(ctx context.Context, from, to int64) {
	planned, err := a.planBatch(ctx, from, to)
	if err != nil {
		a.failBatch(from, to, "batch planning failed", err)
		return
	}

	payloads, err := a.fetchPlanned(ctx, planned)
	if err != nil {
		a.failBatch(from, to, "batch fetch failed", err)
		return
	}

	for _, height := range planned {
		if err := a.archiveHeight(ctx, height, payloads[height.BlockNumber]); err != nil {
			if ctx.Err() != nil {
				return
			}
			a.logger.Warn("block left unarchived", "block", height.BlockNumber, "error", err)
			a.stats.blocksFailed.Add(1)
		}
	}
}

func (a blockArchiver) failBatch(from, to int64, msg string, err error) {
	a.logger.Warn(msg, "from", from, "to", to, "error", err)
	a.stats.blocksFailed.Add(to - from + 1)
}

// plannedHeight is one height's decision, or the failure that stopped it before
// a decision was reached.
type plannedHeight struct {
	BlockNumber int64
	Decision    blockDecision
	Err         error
}

// planBatch decides every height in the range. One the archive already holds a
// version of is planned from its header, not the megabyte its block and receipts
// weigh; an untouched one is fresh whatever the chain says, so it costs no read.
func (a blockArchiver) planBatch(ctx context.Context, from, to int64) ([]plannedHeight, error) {
	planned := make([]plannedHeight, to-from+1)
	states := make(map[int64]archiveState, to-from+1)
	var archived []int64

	for blockNum := from; blockNum <= to; blockNum++ {
		planned[blockNum-from] = plannedHeight{BlockNumber: blockNum}
		state, err := a.planner.topVersion(ctx, blockNum)
		switch {
		case err != nil:
			planned[blockNum-from].Err = err
		case state.Version == noArchive:
			planned[blockNum-from].Decision = a.planner.fresh(blockNum)
		default:
			states[blockNum] = state
			archived = append(archived, blockNum)
		}
	}

	// GetBlockHeadersBatch answers one entry per requested height, in order, with
	// BlockErr set where a response is missing, so every archived height lands.
	headers, err := a.fetchHeaders(ctx, archived)
	if err != nil {
		return nil, err
	}
	for _, header := range headers {
		planned[header.BlockNumber-from] = a.planArchived(ctx, header, states[header.BlockNumber])
	}
	return planned, nil
}

func (a blockArchiver) planArchived(ctx context.Context, header outbound.BlockData, state archiveState) plannedHeight {
	if header.BlockErr != nil {
		return plannedHeight{
			BlockNumber: header.BlockNumber,
			Err:         fmt.Errorf("fetching the header of block %d: %w", header.BlockNumber, header.BlockErr),
		}
	}

	decision, err := a.planner.decide(ctx, header.BlockNumber, state, header.Block)
	if err != nil {
		return plannedHeight{BlockNumber: header.BlockNumber, Err: err}
	}
	return plannedHeight{BlockNumber: header.BlockNumber, Decision: decision}
}

// archiveHeight acts on one height's decision, or surfaces the failure that
// stopped it from reaching one.
func (a blockArchiver) archiveHeight(ctx context.Context, height plannedHeight, payload outbound.BlockData) error {
	if height.Err != nil {
		return height.Err
	}
	return a.applyDecision(ctx, height.Decision, payload)
}

// fetchHeaders fetches the headers the archived heights are planned from.
func (a blockArchiver) fetchHeaders(ctx context.Context, blockNums []int64) ([]outbound.BlockData, error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	rpcStart := time.Now()
	headers, err := a.client.GetBlockHeadersBatch(ctx, blockNums)
	a.stats.rpcBlockTime.Add(time.Since(rpcStart).Nanoseconds())
	a.stats.rpcBlockCalls.Add(1)
	return headers, err
}

// fetchPlanned fetches the full blocks and receipts the plans write. A dry run
// writes nothing, so it fetches nothing.
func (a blockArchiver) fetchPlanned(ctx context.Context, planned []plannedHeight) (map[int64]outbound.BlockData, error) {
	if a.cfg.DryRun {
		return nil, nil
	}

	blockNums := make([]int64, 0, len(planned))
	for _, height := range planned {
		if height.Err == nil && needsPayloads(height.Decision.Plan) {
			blockNums = append(blockNums, height.BlockNumber)
		}
	}
	if len(blockNums) == 0 {
		return nil, nil
	}

	rpcStart := time.Now()
	results, err := a.client.GetBlocksAndReceiptsBatch(ctx, blockNums, true)
	a.stats.rpcBlockTime.Add(time.Since(rpcStart).Nanoseconds())
	a.stats.rpcBlockCalls.Add(1)
	if err != nil {
		return nil, err
	}

	payloads := make(map[int64]outbound.BlockData, len(results))
	for _, result := range results {
		payloads[result.BlockNumber] = result
	}
	return payloads, nil
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
