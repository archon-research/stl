package backfill_gaps

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
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

	// BoundaryCheckDepth is how many recent blocks to verify against RPC before backfilling.
	// This detects reorgs that happened while the service was down.
	BoundaryCheckDepth int

	// EnableBlobs enables caching blob sidecars (post-Dencun blocks on supported nodes).
	EnableBlobs bool

	// Logger is the structured logger.
	Logger *slog.Logger
}

// BackfillConfigDefaults returns default configuration.
func BackfillConfigDefaults() BackfillConfig {
	return BackfillConfig{
		ChainID:            1,
		BatchSize:          10,
		PollInterval:       30 * time.Second,
		BoundaryCheckDepth: 10,
		Logger:             slog.Default(),
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
	// BoundaryCheckDepth: 0 = use default, > 0 = use value, < 0 = disabled
	if config.BoundaryCheckDepth == 0 {
		config.BoundaryCheckDepth = defaults.BoundaryCheckDepth
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

	// Verify our boundary blocks match the canonical chain on RPC.
	// This detects reorgs that happened while the service was down.
	staleBlocks, err := s.verifyBoundaryBlocks(ctx)
	if err != nil {
		span.RecordError(err)
		s.logger.Warn("boundary verification failed", "error", err)
		// Continue anyway - we'll catch linkage issues per-block
	} else if len(staleBlocks) > 0 {
		// We have stale blocks - recover from the reorg before filling gaps
		span.SetAttributes(attribute.Int("backfill.stale_blocks_detected", len(staleBlocks)))
		s.logger.Warn("detected stale blocks from reorg during downtime",
			"staleCount", len(staleBlocks),
			"oldestStale", staleBlocks[len(staleBlocks)-1].Number,
			"newestStale", staleBlocks[0].Number)

		if err := s.recoverFromStaleChain(ctx, staleBlocks); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to recover from stale chain")
			return fmt.Errorf("failed to recover from stale chain: %w", err)
		}

		// After orphaning stale blocks, we need to backfill up to the current chain head.
		// Get maxBlock from RPC since our DB's maxBlock is now lower (orphaned blocks excluded).
		rpcHead, err := s.client.GetCurrentBlockNumber(ctx)
		if err != nil {
			s.logger.Warn("failed to get current block from RPC, using DB maxBlock", "error", err)
		} else if rpcHead > maxBlock {
			maxBlock = rpcHead
			s.logger.Info("using RPC head as maxBlock after reorg recovery",
				"rpcHead", rpcHead)
		}
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

		if err := s.fillGapWithTracing(ctx, gap); err != nil {
			s.logger.Warn("failed to fill gap", "from", gap.From, "to", gap.To, "error", err)
			// Continue with other gaps
		}
	}

	// Retry blocks that were saved but not fully published (cache/publish failed).
	// This ensures eventual consistency for blocks that had transient failures.
	if err := s.retryIncompletePublishes(ctx); err != nil {
		s.logger.Warn("failed to retry incomplete publishes", "error", err)
		// Continue - this will be retried on the next pass
	}

	// After filling gaps, advance the watermark to the highest contiguous block.
	// We find the new contiguous range by checking if there are still gaps.
	if err := s.advanceWatermark(ctx); err != nil {
		s.logger.Warn("failed to advance watermark", "error", err)
	}

	return nil
}

// fillGap fills a single gap range using batched RPC calls.
func (s *BackfillService) fillGap(ctx context.Context, gap outbound.BlockRange) error {
	batchSize := s.config.BatchSize
	total := gap.To - gap.From + 1

	s.logger.Info("starting gap backfill", "from", gap.From, "to", gap.To, "total", total)

	for batchStart := gap.From; batchStart <= gap.To; batchStart += int64(batchSize) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchEnd := batchStart + int64(batchSize) - 1
		if batchEnd > gap.To {
			batchEnd = gap.To
		}

		if err := s.processBatch(ctx, batchStart, batchEnd); err != nil {
			s.logger.Warn("batch failed", "from", batchStart, "to", batchEnd, "error", err)
			// Continue with next batch
		}
	}

	s.logger.Info("gap backfill complete", "from", gap.From, "to", gap.To)
	return nil
}

// fillGapWithTracing fills a gap with tracing context propagation.
func (s *BackfillService) fillGapWithTracing(ctx context.Context, gap outbound.BlockRange) error {
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

	err := s.fillGap(ctx, gap)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "gap fill failed")
	}
	return err
}

// processBatch fetches and processes a batch of blocks.
func (s *BackfillService) processBatch(ctx context.Context, from, to int64) error {
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
		if err := s.processBlockData(ctx, bd); err != nil {
			s.logger.Warn("failed to process block", "block", bd.BlockNumber, "error", err)
			// Continue with other blocks
		}
	}

	return nil
}

// processBlockData processes a single block's data.
func (s *BackfillService) processBlockData(ctx context.Context, bd outbound.BlockData) error {
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
		return fmt.Errorf("failed to check for existing block %d: %w", blockNum, err)
	}
	if existing != nil {
		s.logger.Debug("block already exists, skipping", "block", blockNum)
		return nil
	}

	// Check if a DIFFERENT block already exists at this height (live service may have
	// already processed a newer canonical block while we were fetching).
	// This prevents the race: backfill fetches stale block A, live saves canonical block B,
	// then backfill saves A with higher version - breaking "highest version = canonical".
	existingAtHeight, err := s.stateRepo.GetBlockByNumber(ctx, blockNum)
	if err != nil {
		s.logger.Warn("failed to check existing block at height", "block", blockNum, "error", err)
	} else if existingAtHeight != nil && existingAtHeight.Hash != header.Hash {
		s.logger.Debug("different block already exists at this height, skipping stale block",
			"block", blockNum,
			"existingHash", truncateHash(existingAtHeight.Hash),
			"fetchedHash", truncateHash(header.Hash))
		return nil
	}

	// Validate that fetched block matches the expected chain.
	// This prevents caching/publishing data for a reorged block that doesn't
	// link correctly with blocks we've already processed.
	if err := s.validateBlockLinkage(ctx, blockNum, header.Hash, header.ParentHash); err != nil {
		s.logger.Warn("block linkage validation failed, skipping",
			"block", blockNum,
			"hash", header.Hash,
			"error", err)
		return fmt.Errorf("block linkage validation failed: %w", err)
	}

	receivedAt := time.Now()

	// Parse block timestamp for deterministic created_at
	blockTimestamp, err := hexutil.ParseInt64(header.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to parse block timestamp for block %d: %w", blockNum, err)
	}

	// Save block state to DB - version is assigned atomically to prevent race conditions
	state := outbound.BlockState{
		Number:         blockNum,
		Hash:           header.Hash,
		ParentHash:     header.ParentHash,
		ReceivedAt:     receivedAt.Unix(),
		BlockTimestamp: blockTimestamp,
		IsOrphaned:     false,
	}

	version, err := s.stateRepo.SaveBlock(ctx, state)
	if err != nil {
		return fmt.Errorf("failed to save block state: %w", err)
	}

	// Re-validate linkage AFTER save to catch reorgs that happened between check and save.
	// This closes the TOCTOU (Time-of-Check to Time-of-Use) window.
	if err := s.validateBlockLinkage(ctx, blockNum, header.Hash, header.ParentHash); err != nil {
		s.logger.Warn("post-save linkage validation failed (race condition detected), orphaning block",
			"block", blockNum,
			"hash", truncateHash(header.Hash),
			"error", err)

		// Mark the invalid block we just saved as orphaned so it doesn't break the chain
		if orphanErr := s.stateRepo.MarkBlockOrphaned(ctx, header.Hash); orphanErr != nil {
			s.logger.Error("failed to orphan invalid block after race detected",
				"block", blockNum, "error", orphanErr)
		}

		return fmt.Errorf("post-save linkage validation failed: %w", err)
	}

	// Cache and publish the data
	if err := s.cacheAndPublishBlockData(ctx, bd, header, version, receivedAt); err != nil {
		// Block is saved to DB but cache/publish failed.
		// Return error so caller knows to retry. The block will be picked up
		// by retryIncompletePublishes() on the next backfill pass.
		return fmt.Errorf("cache/publish failed for block %d: %w", blockNum, err)
	}

	return nil
}

// validateBlockLinkage checks that the fetched block links correctly with
// existing blocks in our database. This prevents saving/caching/publishing
// blocks from a reorged chain that don't match our canonical chain.
//
// Validation rules:
// 1. If block N+1 exists, fetched block N's hash must equal N+1's parent_hash
// 2. If block N-1 exists, fetched block N's parent_hash must equal N-1's hash
func (s *BackfillService) validateBlockLinkage(ctx context.Context, blockNum int64, blockHash, parentHash string) error {
	// Check forward linkage: does block N+1 exist and expect this block as parent?
	nextBlock, err := s.stateRepo.GetBlockByNumber(ctx, blockNum+1)
	if err != nil {
		s.logger.Debug("failed to check next block for linkage", "block", blockNum+1, "error", err)
	} else if nextBlock != nil {
		if nextBlock.ParentHash != blockHash {
			return fmt.Errorf("hash %s does not match next block's parent_hash %s",
				truncateHash(blockHash), truncateHash(nextBlock.ParentHash))
		}
	}

	// Check backward linkage: does our parent_hash match block N-1's hash?
	if blockNum > 1 {
		prevBlock, err := s.stateRepo.GetBlockByNumber(ctx, blockNum-1)
		if err != nil {
			s.logger.Debug("failed to check previous block for linkage", "block", blockNum-1, "error", err)
		} else if prevBlock != nil {
			if prevBlock.Hash != parentHash {
				return fmt.Errorf("parent_hash %s does not match previous block's hash %s",
					truncateHash(parentHash), truncateHash(prevBlock.Hash))
			}
		}
	}

	return nil
}

// truncateHash returns a shortened version of a hash for logging.
func truncateHash(hash string) string {
	if len(hash) > 18 {
		return hash[:10] + "..." + hash[len(hash)-6:]
	}
	return hash
}

// verifyBoundaryBlocks checks that our most recent blocks match the canonical chain on RPC.
// This detects reorgs that happened while the service was down.
//
// Returns a slice of stale blocks (blocks in our DB that no longer exist on the canonical chain).
// The slice is ordered from most recent to oldest.
// Returns empty slice if all boundary blocks are valid or if boundary checking is disabled.
func (s *BackfillService) verifyBoundaryBlocks(ctx context.Context) ([]outbound.BlockState, error) {
	depth := s.config.BoundaryCheckDepth
	if depth <= 0 {
		// Boundary checking is disabled
		return nil, nil
	}

	// Get our most recent N blocks from DB
	recentBlocks, err := s.stateRepo.GetRecentBlocks(ctx, depth)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent blocks: %w", err)
	}

	if len(recentBlocks) == 0 {
		return nil, nil
	}

	var staleBlocks []outbound.BlockState

	// Check each block against RPC - if the hash doesn't match CANONICAL chain, the block is stale
	for _, dbBlock := range recentBlocks {
		// Query RPC for the CANONICAL block at this height.
		// We use GetBlockByNumber because GetBlockByHash might return "Uncle" blocks
		// that are present on the node but not canonical.
		blockJSON, err := s.client.GetBlockByNumber(ctx, dbBlock.Number, false)
		if err != nil {
			// RPC error - log and continue checking other blocks
			s.logger.Warn("failed to verify block against RPC",
				"block", dbBlock.Number,
				"hash", truncateHash(dbBlock.Hash),
				"error", err)
			continue
		}

		var header outbound.BlockHeader
		if err := shared.ParseBlockHeader(blockJSON, &header); err != nil {
			s.logger.Warn("failed to parse RPC block for verification",
				"block", dbBlock.Number, "error", err)
			continue
		}

		// Compare Hash: Canonical (RPC) vs Database
		// Note: Case-insensitive comparison is safer for hex strings
		if header.Hash != dbBlock.Hash {
			// Block hash doesn't match canonical chain - it's stale!
			s.logger.Info("detected stale block (hash mismatch with canonical chain)",
				"block", dbBlock.Number,
				"dbHash", truncateHash(dbBlock.Hash),
				"canonicalHash", truncateHash(header.Hash))
			staleBlocks = append(staleBlocks, dbBlock)
		}
	}

	return staleBlocks, nil
}

// recoverFromStaleChain handles recovery when we detect blocks that are no longer
// on the canonical chain (due to a reorg that happened while we were offline).
//
// This method marks stale blocks as orphaned AND immediately fetches and saves
// the canonical replacement blocks. This ensures the highest version at each
// block height is always the canonical block.
func (s *BackfillService) recoverFromStaleChain(ctx context.Context, staleBlocks []outbound.BlockState) error {
	if len(staleBlocks) == 0 {
		return nil
	}

	// Sort blocks by height (ascending) to ensure deterministic recovery
	sort.Slice(staleBlocks, func(i, j int) bool {
		return staleBlocks[i].Number < staleBlocks[j].Number
	})

	lowestStale := staleBlocks[0].Number
	s.logger.Info("recovering from stale chain",
		"lowestStaleBlock", lowestStale,
		"staleBlockCount", len(staleBlocks))

	// Phase 1: Orphan ALL stale blocks first.
	// This is crucial! We must clear the "next" references in the DB before inserting new blocks,
	// otherwise validateBlockLinkage will fail when checking against the yet-to-be-orphaned 'next' block.
	for _, staleBlock := range staleBlocks {
		if err := s.stateRepo.MarkBlockOrphaned(ctx, staleBlock.Hash); err != nil {
			return fmt.Errorf("failed to mark block %d as orphaned: %w", staleBlock.Number, err)
		}
		s.logger.Info("marked stale block as orphaned",
			"block", staleBlock.Number,
			"hash", truncateHash(staleBlock.Hash))
	}

	// Phase 2: Fetch and save canonical replacements
	for _, staleBlock := range staleBlocks {
		// Immediately fetch the canonical block at this height
		blockDataList, err := s.client.GetBlocksBatch(ctx, []int64{staleBlock.Number}, true)
		if err != nil || len(blockDataList) == 0 {
			s.logger.Warn("failed to fetch canonical replacement block",
				"block", staleBlock.Number,
				"error", err)
			// Continue - gap-filling will pick this up later
			continue
		}

		// Process and save the canonical block (assigns new version)
		if err := s.processBlockData(ctx, blockDataList[0]); err != nil {
			s.logger.Warn("failed to process canonical replacement block",
				"block", staleBlock.Number,
				"error", err)
			// Continue - gap-filling will pick this up later
			continue
		}

		s.logger.Info("replaced stale block with canonical block",
			"block", staleBlock.Number)
	}

	s.logger.Info("stale chain recovery complete",
		"staleBlockCount", len(staleBlocks))

	return nil
}

// cacheAndPublishBlockData caches all pre-fetched data types and publishes a block event.
// All data must be cached successfully before the event is published.
// Returns an error if any step fails, allowing the caller to handle retry logic.
func (s *BackfillService) cacheAndPublishBlockData(ctx context.Context, bd outbound.BlockData, header outbound.BlockHeader, version int, receivedAt time.Time) error {
	chainID := s.config.ChainID
	blockNum := bd.BlockNumber
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, err := hexutil.ParseInt64(header.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to parse block timestamp for block %d: %w", blockNum, err)
	}

	// Validate all required data before caching
	if bd.Block == nil {
		return fmt.Errorf("missing block data for block %d", blockNum)
	}
	if bd.ReceiptsErr != nil {
		return fmt.Errorf("receipts fetch failed for block %d: %w", blockNum, bd.ReceiptsErr)
	}
	if bd.Receipts == nil {
		return fmt.Errorf("missing receipts data for block %d (no error reported)", blockNum)
	}
	if bd.TracesErr != nil {
		return fmt.Errorf("traces fetch failed for block %d: %w", blockNum, bd.TracesErr)
	}
	if bd.Traces == nil {
		return fmt.Errorf("missing traces data for block %d (no error reported)", blockNum)
	}

	// Validate blobs if enabled
	var blobs json.RawMessage
	if s.config.EnableBlobs {
		if bd.BlobsErr != nil {
			return fmt.Errorf("blobs fetch failed for block %d: %w", blockNum, bd.BlobsErr)
		}
		if bd.Blobs == nil {
			return fmt.Errorf("missing blobs data for block %d (no error reported)", blockNum)
		}
		blobs = bd.Blobs
	}

	// Cache all block data in a single pipelined operation with retry
	cacheInput := outbound.BlockDataInput{
		Block:    bd.Block,
		Receipts: bd.Receipts,
		Traces:   bd.Traces,
		Blobs:    blobs,
	}
	if err := s.cache.SetBlockData(ctx, chainID, blockNum, version, cacheInput); err != nil {
		return fmt.Errorf("failed to cache block data for block %d: %w", blockNum, err)
	}

	// All data cached successfully - now publish the block event
	event := outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNum,
		Version:        version,
		BlockHash:      blockHash,
		ParentHash:     parentHash,
		BlockTimestamp: blockTimestamp,
		ReceivedAt:     receivedAt,
		IsBackfill:     true,
	}
	if err := s.eventSink.Publish(ctx, event); err != nil {
		return fmt.Errorf("failed to publish block event for block %d: %w", blockNum, err)
	}

	// Mark block publish complete for crash recovery tracking.
	// If this fails, we return an error because:
	// 1. The block will appear as "incomplete" in the DB
	// 2. Retry logic will attempt to republish, potentially causing duplicates
	// 3. It's better to surface this failure than silently leave inconsistent state
	if err := s.stateRepo.MarkPublishComplete(ctx, blockHash); err != nil {
		return fmt.Errorf("failed to mark block %d publish complete (block was published but tracking failed): %w", blockNum, err)
	}

	return nil
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
		// Verify chain integrity before advancing watermark
		// This ensures eventual consistency of the parent_hash chain
		if err := s.stateRepo.VerifyChainIntegrity(ctx, currentWatermark, newWatermark); err != nil {
			s.logger.Error("chain integrity violation detected, not advancing watermark",
				"error", err,
				"from", currentWatermark,
				"to", newWatermark)
			return fmt.Errorf("chain integrity check failed: %w", err)
		}

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

// retryIncompletePublishes finds blocks that were saved to DB but not fully
// cached/published, and retries them. This ensures eventual consistency for
// blocks that had transient failures during cache or publish operations.
func (s *BackfillService) retryIncompletePublishes(ctx context.Context) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "backfill.retryIncompletePublishes",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	// Get blocks with incomplete publishes (limit to batch size to avoid overwhelming the system)
	incompleteBlocks, err := s.stateRepo.GetBlocksWithIncompletePublish(ctx, s.config.BatchSize)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to get blocks with incomplete publish: %w", err)
	}

	if len(incompleteBlocks) == 0 {
		return nil
	}

	span.SetAttributes(attribute.Int("backfill.incomplete_blocks", len(incompleteBlocks)))
	s.logger.Info("retrying incomplete publishes", "count", len(incompleteBlocks))

	// Fetch and republish each incomplete block
	for _, block := range incompleteBlocks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := s.retryBlockPublish(ctx, block); err != nil {
			s.logger.Warn("failed to retry block publish",
				"block", block.Number,
				"hash", truncateHash(block.Hash),
				"error", err)
			// Continue with other blocks
		}
	}

	return nil
}

// retryBlockPublish fetches and republishes a single block that had incomplete publish.
func (s *BackfillService) retryBlockPublish(ctx context.Context, block outbound.BlockState) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "backfill.retryBlockPublish",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", block.Number),
			attribute.String("block.hash", truncateHash(block.Hash)),
		),
	)
	defer span.End()

	// Fetch block data by hash to ensure we get the exact block we have in DB
	bd, err := s.client.GetBlockDataByHash(ctx, block.Number, block.Hash, true)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to fetch block data: %w", err)
	}

	// Parse header
	var header outbound.BlockHeader
	if err := shared.ParseBlockHeader(bd.Block, &header); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to parse block header: %w", err)
	}

	// Retry cache and publish using the version from the block state
	receivedAt := time.Unix(block.ReceivedAt, 0)
	if err := s.cacheAndPublishBlockData(ctx, bd, header, block.Version, receivedAt); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to cache/publish: %w", err)
	}

	s.logger.Info("successfully retried block publish",
		"block", block.Number,
		"hash", truncateHash(block.Hash))

	return nil
}
