package backfill_gaps

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

const (
	// tracerName is the instrumentation name for this service.
	tracerName = "github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
)

// BackfillConfig holds configuration for the BackfillService.
type BackfillConfig struct {
	// ChainID is the blockchain chain ID (e.g., 1 for Ethereum mainnet).
	ChainID int64

	// BatchSize is how many blocks to fetch in a single batched RPC call.
	BatchSize int

	// PollInterval is how often to check for gaps when running continuously.
	PollInterval time.Duration

	// Logger is the structured logger.
	Logger *slog.Logger
}

// BackfillConfigDefaults returns default configuration.
func BackfillConfigDefaults() BackfillConfig {
	return BackfillConfig{
		ChainID:      1,
		BatchSize:    10,
		PollInterval: 30 * time.Second,
		Logger:       slog.Default(),
	}
}

// BackfillService finds and fills gaps in the block state table.
// It operates independently of LiveService, using the database as the source of truth.
type BackfillService struct {
	config BackfillConfig

	client    outbound.BlockchainClient
	stateRepo outbound.BlockStateRepository
	cache     outbound.BlockCache
	eventSink outbound.EventSink

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *slog.Logger
}

// NewBackfillService creates a new BackfillService.
func NewBackfillService(
	config BackfillConfig,
	client outbound.BlockchainClient,
	stateRepo outbound.BlockStateRepository,
	cache outbound.BlockCache,
	eventSink outbound.EventSink,
) (*BackfillService, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	}
	if stateRepo == nil {
		return nil, fmt.Errorf("stateRepo is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("cache is required")
	}
	if eventSink == nil {
		return nil, fmt.Errorf("eventSink is required")
	}

	// Apply defaults
	defaults := BackfillConfigDefaults()
	if config.ChainID == 0 {
		config.ChainID = defaults.ChainID
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &BackfillService{
		config:    config,
		client:    client,
		stateRepo: stateRepo,
		cache:     cache,
		eventSink: eventSink,
		logger:    config.Logger.With("component", "backfill-service"),
	}, nil
}

// Start begins the backfill service, periodically checking for and filling gaps.
func (s *BackfillService) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Add(1)
	go s.run()

	s.logger.Info("backfill service started", "chainID", s.config.ChainID, "pollInterval", s.config.PollInterval)
	return nil
}

// Stop stops the backfill service.
func (s *BackfillService) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("backfill service stopped")
	return nil
}

// RunOnce performs a single backfill pass - finds gaps and fills them.
// Useful for testing or one-shot backfill operations.
func (s *BackfillService) RunOnce(ctx context.Context) error {
	s.ctx = ctx
	return s.findAndFillGaps()
}

// run is the main loop that periodically checks for gaps.
func (s *BackfillService) run() {
	defer s.wg.Done()

	// Run immediately on start
	if err := s.findAndFillGaps(); err != nil {
		s.logger.Warn("initial backfill failed", "error", err)
	}

	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.findAndFillGaps(); err != nil {
				s.logger.Warn("backfill pass failed", "error", err)
			}
		}
	}
}

// findAndFillGaps queries the state repo for gaps and fills them.
func (s *BackfillService) findAndFillGaps() error {
	start := time.Now()

	// Start span for the backfill pass
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(s.ctx, "backfill.findAndFillGaps",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		span.SetAttributes(attribute.Int64("backfill.duration_ms", time.Since(start).Milliseconds()))
		span.End()
	}()

	// Get the highest block we've processed
	maxBlock, err := s.stateRepo.GetMaxBlockNumber(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get max block number")
		return fmt.Errorf("failed to get max block number: %w", err)
	}
	if maxBlock == 0 {
		s.logger.Debug("no blocks in state repo, nothing to backfill")
		return nil
	}

	// Get the lowest block we have
	minBlock, err := s.stateRepo.GetMinBlockNumber(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get min block number")
		return fmt.Errorf("failed to get min block number: %w", err)
	}

	// Find gaps in our block sequence
	gaps, err := s.stateRepo.FindGaps(ctx, minBlock, maxBlock)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to find block gaps")
		return fmt.Errorf("failed to find block gaps: %w", err)
	}

	if len(gaps) == 0 {
		s.logger.Debug("no gaps to backfill", "minBlock", minBlock, "maxBlock", maxBlock)
		span.SetAttributes(
			attribute.Int("backfill.gaps_count", 0),
			attribute.Int64("backfill.min_block", minBlock),
			attribute.Int64("backfill.max_block", maxBlock),
		)
		return nil
	}

	// Calculate total missing blocks
	totalMissing := int64(0)
	for _, gap := range gaps {
		totalMissing += gap.To - gap.From + 1
	}

	span.SetAttributes(
		attribute.Int("backfill.gaps_count", len(gaps)),
		attribute.Int64("backfill.total_missing", totalMissing),
		attribute.Int64("backfill.min_block", minBlock),
		attribute.Int64("backfill.max_block", maxBlock),
	)

	s.logger.Info("found gaps to backfill",
		"gaps", len(gaps),
		"totalMissing", totalMissing,
		"minBlock", minBlock,
		"maxBlock", maxBlock)

	// Process each gap
	for _, gap := range gaps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := s.fillGapWithContext(ctx, gap); err != nil {
			s.logger.Warn("failed to fill gap", "from", gap.From, "to", gap.To, "error", err)
			// Continue with other gaps
		}
	}

	// After filling gaps, advance the watermark to the highest contiguous block.
	// We find the new contiguous range by checking if there are still gaps.
	if err := s.advanceWatermark(ctx); err != nil {
		s.logger.Warn("failed to advance watermark", "error", err)
	}

	return nil
}

// fillGap fills a single gap range using batched RPC calls.
func (s *BackfillService) fillGap(gap outbound.BlockRange) error {
	batchSize := s.config.BatchSize
	total := gap.To - gap.From + 1

	s.logger.Info("starting gap backfill", "from", gap.From, "to", gap.To, "total", total)

	for batchStart := gap.From; batchStart <= gap.To; batchStart += int64(batchSize) {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}

		batchEnd := batchStart + int64(batchSize) - 1
		if batchEnd > gap.To {
			batchEnd = gap.To
		}

		if err := s.processBatch(batchStart, batchEnd); err != nil {
			s.logger.Warn("batch failed", "from", batchStart, "to", batchEnd, "error", err)
			// Continue with next batch
		}
	}

	s.logger.Info("gap backfill complete", "from", gap.From, "to", gap.To)
	return nil
}

// fillGapWithContext fills a gap with tracing context propagation.
func (s *BackfillService) fillGapWithContext(ctx context.Context, gap outbound.BlockRange) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "backfill.fillGap",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("gap.from", gap.From),
			attribute.Int64("gap.to", gap.To),
			attribute.Int64("gap.size", gap.To-gap.From+1),
		),
	)
	defer span.End()

	// Temporarily override context for batch processing
	oldCtx := s.ctx
	s.ctx = ctx
	defer func() { s.ctx = oldCtx }()

	err := s.fillGap(gap)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "gap fill failed")
	}
	return err
}

// processBatch fetches and processes a batch of blocks.
func (s *BackfillService) processBatch(from, to int64) error {
	ctx := s.ctx

	// Build block numbers for this batch
	blockNums := make([]int64, 0, to-from+1)
	for blockNum := from; blockNum <= to; blockNum++ {
		blockNums = append(blockNums, blockNum)
	}

	// Fetch all data for this batch in a single RPC call
	batchStart := time.Now()
	blockDataList, err := s.client.GetBlocksBatch(ctx, blockNums, true)
	if err != nil {
		return fmt.Errorf("failed to fetch batch: %w", err)
	}
	s.logger.Debug("batch fetched", "from", from, "to", to, "fetchMs", time.Since(batchStart).Milliseconds())

	// Process each block in the batch
	for _, bd := range blockDataList {
		if err := s.processBlockData(bd); err != nil {
			s.logger.Warn("failed to process block", "block", bd.BlockNumber, "error", err)
			// Continue with other blocks
		}
	}

	return nil
}

// processBlockData processes a single block's data.
func (s *BackfillService) processBlockData(bd outbound.BlockData) error {
	ctx := s.ctx
	blockNum := bd.BlockNumber

	if bd.Block == nil {
		return fmt.Errorf("missing block data")
	}

	var header outbound.BlockHeader
	if err := shared.ParseBlockHeader(bd.Block, &header); err != nil {
		return fmt.Errorf("failed to parse block header: %w", err)
	}

	// Check if block already exists in DB (idempotency)
	existing, err := s.stateRepo.GetBlockByHash(ctx, header.Hash)
	if err != nil {
		s.logger.Warn("failed to check for existing block", "block", blockNum, "error", err)
	} else if existing != nil {
		s.logger.Debug("block already exists, skipping", "block", blockNum)
		return nil
	}

	receivedAt := time.Now()

	// Save block state to DB
	state := outbound.BlockState{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
		ReceivedAt: receivedAt.Unix(),
		IsOrphaned: false,
	}
	if err := s.stateRepo.SaveBlock(ctx, state); err != nil {
		return fmt.Errorf("failed to save block state: %w", err)
	}

	// Cache and publish the data
	s.cacheAndPublishBlockData(bd, header, receivedAt)

	return nil
}

// cacheAndPublishBlockData caches pre-fetched data and publishes events.
func (s *BackfillService) cacheAndPublishBlockData(bd outbound.BlockData, header outbound.BlockHeader, receivedAt time.Time) {
	chainID := s.config.ChainID
	blockNum := bd.BlockNumber
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, _ := shared.ParseBlockNumber(header.Timestamp)

	// Get version before we save: count of existing blocks at this number
	version, err := s.stateRepo.GetBlockVersionCount(s.ctx, blockNum)
	if err != nil {
		s.logger.Warn("failed to get block version count", "block", blockNum, "error", err)
		version = 0 // Default to 0 on error
	}

	var wg sync.WaitGroup

	// Cache and publish block
	if bd.Block != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.cache.SetBlock(s.ctx, chainID, blockNum, version, bd.Block); err != nil {
				s.logger.Warn("failed to cache block", "block", blockNum, "error", err)
				return
			}
			event := outbound.BlockEvent{
				ChainID:        chainID,
				BlockNumber:    blockNum,
				Version:        version,
				BlockHash:      blockHash,
				ParentHash:     parentHash,
				BlockTimestamp: blockTimestamp,
				ReceivedAt:     receivedAt,
				CacheKey:       shared.CacheKey(chainID, blockNum, version, "block"),
				IsBackfill:     true,
			}
			if err := s.eventSink.Publish(s.ctx, event); err != nil {
				s.logger.Warn("failed to publish block event", "block", blockNum, "error", err)
			}
		}()
	}

	// Cache and publish receipts
	if bd.Receipts != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.cache.SetReceipts(s.ctx, chainID, blockNum, version, bd.Receipts); err != nil {
				s.logger.Warn("failed to cache receipts", "block", blockNum, "error", err)
				return
			}
			event := outbound.ReceiptsEvent{
				ChainID:     chainID,
				BlockNumber: blockNum,
				Version:     version,
				BlockHash:   blockHash,
				ReceivedAt:  receivedAt,
				CacheKey:    shared.CacheKey(chainID, blockNum, version, "receipts"),
				IsBackfill:  true,
			}
			if err := s.eventSink.Publish(s.ctx, event); err != nil {
				s.logger.Warn("failed to publish receipts event", "block", blockNum, "error", err)
			}
		}()
	}

	// Cache and publish traces
	if bd.Traces != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.cache.SetTraces(s.ctx, chainID, blockNum, version, bd.Traces); err != nil {
				s.logger.Warn("failed to cache traces", "block", blockNum, "error", err)
				return
			}
			event := outbound.TracesEvent{
				ChainID:     chainID,
				BlockNumber: blockNum,
				Version:     version,
				BlockHash:   blockHash,
				ReceivedAt:  receivedAt,
				CacheKey:    shared.CacheKey(chainID, blockNum, version, "traces"),
				IsBackfill:  true,
			}
			if err := s.eventSink.Publish(s.ctx, event); err != nil {
				s.logger.Warn("failed to publish traces event", "block", blockNum, "error", err)
			}
		}()
	}

	// Cache and publish blobs
	if bd.Blobs != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.cache.SetBlobs(s.ctx, chainID, blockNum, version, bd.Blobs); err != nil {
				s.logger.Warn("failed to cache blobs", "block", blockNum, "error", err)
				return
			}
			event := outbound.BlobsEvent{
				ChainID:     chainID,
				BlockNumber: blockNum,
				Version:     version,
				BlockHash:   blockHash,
				ReceivedAt:  receivedAt,
				CacheKey:    shared.CacheKey(chainID, blockNum, version, "blobs"),
				IsBackfill:  true,
			}
			if err := s.eventSink.Publish(s.ctx, event); err != nil {
				s.logger.Warn("failed to publish blobs event", "block", blockNum, "error", err)
			}
		}()
	}

	wg.Wait()
}

// advanceWatermark updates the backfill watermark to the highest contiguous block.
// This allows future FindGaps calls to skip already-verified ranges.
func (s *BackfillService) advanceWatermark(ctx context.Context) error {
	// Get current watermark
	currentWatermark, err := s.stateRepo.GetBackfillWatermark(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current watermark: %w", err)
	}

	// Get min and max block numbers
	minBlock, err := s.stateRepo.GetMinBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get min block: %w", err)
	}
	maxBlock, err := s.stateRepo.GetMaxBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get max block: %w", err)
	}

	if maxBlock == 0 {
		return nil
	}

	// Start checking from the block after the current watermark
	startBlock := currentWatermark + 1
	if startBlock < minBlock {
		startBlock = minBlock
	}

	// Find the first gap after the current watermark
	// We temporarily bypass the watermark check by querying directly
	gaps, err := s.findGapsFromBlock(ctx, startBlock, maxBlock)
	if err != nil {
		return fmt.Errorf("failed to find gaps for watermark: %w", err)
	}

	var newWatermark int64
	if len(gaps) == 0 {
		// No gaps - advance watermark to maxBlock
		newWatermark = maxBlock
	} else {
		// Advance watermark to just before the first gap
		newWatermark = gaps[0].From - 1
	}

	// Only advance if we have a higher watermark
	if newWatermark > currentWatermark {
		if err := s.stateRepo.SetBackfillWatermark(ctx, newWatermark); err != nil {
			return fmt.Errorf("failed to set watermark: %w", err)
		}
		s.logger.Info("advanced backfill watermark",
			"from", currentWatermark,
			"to", newWatermark,
			"blocksVerified", newWatermark-currentWatermark)
	}

	return nil
}

// findGapsFromBlock finds gaps starting from a specific block, bypassing the watermark.
// Used internally for watermark advancement.
func (s *BackfillService) findGapsFromBlock(ctx context.Context, fromBlock, toBlock int64) ([]outbound.BlockRange, error) {
	// We need a direct query here since the repository's FindGaps uses the watermark.
	// For now, we'll use a simplified approach: check if fromBlock exists, and if there
	// are any gaps in the range. This is called infrequently (once per backfill pass).

	// Get the first block in the range
	firstBlock, err := s.getFirstBlockInRange(ctx, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	// If the first block in range is not fromBlock, we have a gap at the start
	var gaps []outbound.BlockRange
	if firstBlock > fromBlock {
		gaps = append(gaps, outbound.BlockRange{From: fromBlock, To: firstBlock - 1})
	}

	// For finding remaining gaps, we still use the repository method
	// but it will be efficient since we're starting from a recent point
	repoGaps, err := s.stateRepo.FindGaps(ctx, firstBlock, toBlock)
	if err != nil {
		return nil, err
	}

	return append(gaps, repoGaps...), nil
}

// getFirstBlockInRange returns the first block number that exists in the given range.
func (s *BackfillService) getFirstBlockInRange(ctx context.Context, fromBlock, toBlock int64) (int64, error) {
	// Check if fromBlock exists
	block, err := s.stateRepo.GetBlockByNumber(ctx, fromBlock)
	if err != nil {
		return 0, err
	}
	if block != nil {
		return fromBlock, nil
	}

	// fromBlock doesn't exist, so the gap starts at fromBlock
	// Return toBlock + 1 to indicate no blocks exist in range
	return toBlock + 1, nil
}
