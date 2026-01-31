package live_data

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	// tracerName is the instrumentation name for this service.
	tracerName = "github.com/archon-research/stl/stl-verify/internal/services/live_data"
)

// LiveConfig holds configuration for the LiveService.
type LiveConfig struct {
	// ChainID is the blockchain chain ID (e.g., 1 for Ethereum mainnet).
	ChainID int64

	// FinalityBlockCount is how many blocks behind the tip before considered finalized.
	FinalityBlockCount int

	// MaxUnfinalizedBlocks is the max number of unfinalized blocks to keep in memory.
	MaxUnfinalizedBlocks int

	// EnableBlobs enables fetching blob sidecars (post-Dencun blocks on supported nodes).
	EnableBlobs bool

	// Logger is the structured logger.
	Logger *slog.Logger

	// Metrics is the metrics recorder for telemetry (optional).
	Metrics outbound.ReorgRecorder
}

// LiveConfigDefaults returns default configuration.
func LiveConfigDefaults() LiveConfig {
	return LiveConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 200,
		Logger:               slog.Default(),
	}
}

// LightBlock represents a minimal block for chain tracking.
type LightBlock struct {
	Number     int64
	Hash       string
	ParentHash string
}

// prefetchResult holds the result of a prefetched RPC call.
// This enables pipelining: we can start fetching the next block's data
// while still processing the current block's cache/publish operations.
type prefetchResult struct {
	header    outbound.BlockHeader
	blockNum  int64
	blockData outbound.BlockData
	err       error
	fetchedAt time.Time
}

// LiveService handles live block subscription, reorg detection, and event publishing.
// It does NOT handle backfilling - that's the responsibility of BackfillService.
type LiveService struct {
	config LiveConfig

	subscriber outbound.BlockSubscriber
	client     outbound.BlockchainClient
	stateRepo  outbound.BlockStateRepository
	cache      outbound.BlockCache
	eventSink  outbound.EventSink
	metrics    outbound.ReorgRecorder

	// In-memory chain state for reorg detection (single-goroutine access)
	unfinalizedBlocks []LightBlock
	finalizedBlock    *LightBlock

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewLiveService creates a new LiveService.
func NewLiveService(
	config LiveConfig,
	subscriber outbound.BlockSubscriber,
	client outbound.BlockchainClient,
	stateRepo outbound.BlockStateRepository,
	cache outbound.BlockCache,
	eventSink outbound.EventSink,
) (*LiveService, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber is required")
	}
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
	defaults := LiveConfigDefaults()
	if config.ChainID == 0 {
		config.ChainID = defaults.ChainID
	}
	if config.FinalityBlockCount == 0 {
		config.FinalityBlockCount = defaults.FinalityBlockCount
	}
	if config.MaxUnfinalizedBlocks == 0 {
		config.MaxUnfinalizedBlocks = defaults.MaxUnfinalizedBlocks
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &LiveService{
		config:     config,
		subscriber: subscriber,
		client:     client,
		stateRepo:  stateRepo,
		cache:      cache,
		eventSink:  eventSink,
		metrics:    config.Metrics,
		logger:     config.Logger.With("component", "live-service"),
	}, nil
}

// Start begins watching for new blocks.
func (s *LiveService) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Restore in-memory chain state from DB
	if err := s.restoreInMemoryChain(); err != nil {
		return fmt.Errorf("failed to restore chain from DB: %w", err)
	}

	// Subscribe to new block headers
	headers, err := s.subscriber.Subscribe(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Process incoming block headers
	go s.processHeaders(headers)

	s.logger.Info("live service started", "chainID", s.config.ChainID)
	return nil
}

// Stop stops the live service.
func (s *LiveService) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return s.subscriber.Unsubscribe()
}

// processHeaders processes incoming block headers using a prefetch pipeline.
// This overlaps expensive RPC fetches with state operations for better throughput.
//
// Pipeline design:
//
//	Header arrives → Start prefetch (async) → Do state ops → Wait for prefetch → Cache/Publish
//
// Without prefetch:  [state: 30ms] → [RPC: 150ms] → [cache: 50ms] = 230ms total
// With prefetch:     [state: 30ms ─────────────────] → [cache: 50ms] = 80ms total
//
// The prefetch completes during state ops, so we only wait ~120ms (150ms - 30ms).
func (s *LiveService) processHeaders(headers <-chan outbound.BlockHeader) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case header, ok := <-headers:
			if !ok {
				return
			}

			receivedAt := time.Now()

			// Process the block with prefetch
			if err := s.processBlockWithPrefetch(header, receivedAt); err != nil {
				blockNum, _ := hexutil.ParseInt64(header.Number)
				s.logger.Warn("failed to process live block",
					"block", blockNum,
					"hash", header.Hash,
					"parentHash", header.ParentHash,
					"error", err)
			}
		}
	}
}

// startPrefetch starts an async RPC fetch for a block's data.
// The provided context should contain trace information so RPC spans are linked to the parent trace.
// Returns a channel that will receive the result when complete.
func (s *LiveService) startPrefetch(ctx context.Context, header outbound.BlockHeader, blockNum int64) <-chan prefetchResult {
	resultCh := make(chan prefetchResult, 1)

	go func() {
		start := time.Now()

		// Fetch all data in a single batched HTTP request
		// Use the provided context which carries trace information
		bd, err := s.client.GetBlockDataByHash(ctx, blockNum, header.Hash, true)

		resultCh <- prefetchResult{
			header:    header,
			blockNum:  blockNum,
			blockData: bd,
			err:       err,
			fetchedAt: time.Now(),
		}
		close(resultCh)

		s.logger.Debug("prefetch completed", "block", blockNum, "duration", time.Since(start))
	}()

	return resultCh
}

// processBlockWithPrefetch processes a block with RPC data being fetched concurrently.
// State operations run while the prefetch happens in the background, then we wait
// for the prefetch and proceed to cache/publish.
//
// Timeline:
//
//	t=0ms:  Start prefetch (async), begin state ops
//	t=30ms: State ops complete, wait for prefetch
//	t=0-150ms: Prefetch running in background
//	t=150ms: Prefetch complete (waited ~120ms if state ops took 30ms)
//	t=200ms: Cache/publish complete
func (s *LiveService) processBlockWithPrefetch(header outbound.BlockHeader, receivedAt time.Time) error {
	blockNum, err := hexutil.ParseInt64(header.Number)
	if err != nil {
		return fmt.Errorf("failed to parse block number: %w", err)
	}

	start := time.Now()

	// Start span for the entire block processing FIRST
	// This ensures the prefetch RPC calls are children of this span
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(s.ctx, "live.processBlock",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", header.Hash),
			attribute.String("block.parent_hash", header.ParentHash),
			attribute.Bool("block.prefetched", true),
		),
	)

	// Create a cancellable context for the prefetch
	// This allows us to cancel the prefetch if we return early (e.g., duplicate block)
	prefetchCtx, cancelPrefetch := context.WithCancel(ctx)

	// Start prefetching RPC data immediately with the traced context
	// This ensures Alchemy RPC spans are linked to this parent span
	prefetchCh := s.startPrefetch(prefetchCtx, header, blockNum)

	// Ensure we always clean up: cancel prefetch context and end span
	defer func() {
		cancelPrefetch()
		span.SetAttributes(attribute.Int64("block.duration_ms", time.Since(start).Milliseconds()))
		span.End()
		s.logger.Info("processBlock completed", "block", blockNum, "duration", time.Since(start))
	}()

	// === PHASE 1: State Operations (runs while prefetch is in progress) ===

	// Normalize hashes once at the entry point
	normalizedHash := normalizeHash(header.Hash)
	normalizedParentHash := normalizeHash(header.ParentHash)

	block := LightBlock{
		Number:     blockNum,
		Hash:       normalizedHash,
		ParentHash: normalizedParentHash,
	}

	// Check if we've already processed this block
	isDuplicate, err := s.isDuplicateBlock(ctx, normalizedHash, blockNum)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to check for duplicate block")
		return fmt.Errorf("failed to check for duplicate block: %w", err)
	}
	if isDuplicate {
		span.SetAttributes(attribute.Bool("block.duplicate", true))
		return nil
	}

	// Detect reorg BEFORE adding to chain
	isReorg, reorgDepth, commonAncestor, reorgEvent, err := s.detectReorg(ctx, block, receivedAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "reorg detection failed")
		return fmt.Errorf("reorg detection failed: %w", err)
	}
	if isReorg {
		span.SetAttributes(
			attribute.Bool("block.reorg", true),
			attribute.Int("block.reorg_depth", reorgDepth),
			attribute.Int64("block.common_ancestor", commonAncestor),
		)
		s.logger.Warn("reorg detected", "block", blockNum, "depth", reorgDepth, "commonAncestor", commonAncestor)
		if s.metrics != nil {
			s.metrics.RecordReorg(ctx, reorgDepth, commonAncestor, blockNum)
		}
	}

	// Build block state for DB
	state := outbound.BlockState{
		Number:     block.Number,
		Hash:       block.Hash,
		ParentHash: block.ParentHash,
		ReceivedAt: receivedAt.Unix(),
		IsOrphaned: false,
	}

	// Save block state to DB
	var version int
	if isReorg && reorgEvent != nil {
		version, err = s.stateRepo.HandleReorgAtomic(ctx, commonAncestor, *reorgEvent, state)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to handle reorg atomically")
			return fmt.Errorf("failed to handle reorg atomically: %w", err)
		}
		s.pruneReorgedBlocks(ctx, commonAncestor)
	} else {
		version, err = s.stateRepo.SaveBlock(ctx, state)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to save block state")
			return fmt.Errorf("failed to save block state: %w", err)
		}
	}

	// Add block to in-memory chain AFTER successful DB save
	s.addBlock(ctx, block)

	stateOpsDuration := time.Since(start)
	span.SetAttributes(attribute.Int64("block.state_ops_ms", stateOpsDuration.Milliseconds()))

	// === PHASE 2: Wait for Prefetch (should be mostly done by now) ===

	prefetchWaitStart := time.Now()
	var prefetch prefetchResult
	select {
	case prefetch = <-prefetchCh:
		// Got the prefetched data
	case <-ctx.Done():
		return ctx.Err()
	}
	prefetchWaitDuration := time.Since(prefetchWaitStart)
	span.SetAttributes(attribute.Int64("block.prefetch_wait_ms", prefetchWaitDuration.Milliseconds()))

	if prefetch.err != nil {
		span.RecordError(prefetch.err)
		span.SetStatus(codes.Error, "prefetch failed")
		return fmt.Errorf("failed to fetch block data for block %d: %w", blockNum, prefetch.err)
	}

	// === PHASE 3: Cache and Publish ===

	if err := s.cacheAndPublishBlockData(ctx, header, blockNum, version, receivedAt, isReorg, prefetch.blockData); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to cache and publish block data")
		return fmt.Errorf("failed to cache and publish block data: %w", err)
	}

	// Update finalized block pointer
	s.updateFinalizedBlock(blockNum)

	return nil
}

// isDuplicateBlock checks if a block has already been processed.
// It first does a quick in-memory check, then falls back to DB lookup.
func (s *LiveService) isDuplicateBlock(ctx context.Context, hash string, blockNum int64) (bool, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.isDuplicateBlock",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", hash),
		),
	)
	defer span.End()

	// Quick in-memory check
	inMemory := slices.ContainsFunc(s.unfinalizedBlocks, func(b LightBlock) bool {
		return b.Hash == hash
	})
	if inMemory {
		span.SetAttributes(attribute.Bool("duplicate.in_memory", true))
		s.logger.Debug("duplicate block, skipping", "block", blockNum)
		return true, nil
	}

	// Also check DB for duplicates (backfill may have processed this block)
	existing, err := s.stateRepo.GetBlockByHash(ctx, hash)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to check DB for duplicate")
		return false, fmt.Errorf("failed to check DB for duplicate: %w", err)
	}
	if existing != nil {
		span.SetAttributes(attribute.Bool("duplicate.in_db", true))
		s.logger.Debug("block already in DB, skipping", "block", blockNum)
		return true, nil
	}

	span.SetAttributes(attribute.Bool("duplicate", false))
	return false, nil
}

// addBlock adds a block to the in-memory unfinalized chain.
func (s *LiveService) addBlock(ctx context.Context, block LightBlock) {
	tracer := otel.Tracer(tracerName)
	_, span := tracer.Start(ctx, "live.addBlock",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", block.Number),
			attribute.String("block.hash", block.Hash),
			attribute.Int("chain.size_before", len(s.unfinalizedBlocks)),
		),
	)
	defer span.End()

	// Sorted insert by block number
	insertIdx := len(s.unfinalizedBlocks)
	for i, b := range s.unfinalizedBlocks {
		if b.Number >= block.Number {
			insertIdx = i
			break
		}
	}

	s.unfinalizedBlocks = slices.Insert(s.unfinalizedBlocks, insertIdx, block)

	// Trim to max size
	trimmed := false
	if len(s.unfinalizedBlocks) > s.config.MaxUnfinalizedBlocks {
		s.unfinalizedBlocks = s.unfinalizedBlocks[1:]
		trimmed = true
	}

	span.SetAttributes(
		attribute.Int("chain.size_after", len(s.unfinalizedBlocks)),
		attribute.Bool("chain.trimmed", trimmed),
	)
}

// pruneReorgedBlocks removes blocks above the common ancestor from the in-memory chain.
// This should only be called AFTER the reorg has been successfully persisted to the database.
func (s *LiveService) pruneReorgedBlocks(ctx context.Context, commonAncestor int64) {
	tracer := otel.Tracer(tracerName)
	_, span := tracer.Start(ctx, "live.pruneReorgedBlocks",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("reorg.common_ancestor", commonAncestor),
			attribute.Int("chain.size_before", len(s.unfinalizedBlocks)),
		),
	)
	defer span.End()

	prunedCount := 0
	newChain := make([]LightBlock, 0, len(s.unfinalizedBlocks))
	for _, b := range s.unfinalizedBlocks {
		if b.Number <= commonAncestor {
			newChain = append(newChain, b)
		} else {
			prunedCount++
		}
	}
	s.unfinalizedBlocks = newChain

	span.SetAttributes(
		attribute.Int("chain.size_after", len(s.unfinalizedBlocks)),
		attribute.Int("chain.pruned_count", prunedCount),
	)
}

// detectReorg detects chain reorganizations using Ponder-style parent hash chain validation.
// Returns: isReorg, reorgDepth, commonAncestor, reorgEvent (if reorg), error
func (s *LiveService) detectReorg(ctx context.Context, block LightBlock, receivedAt time.Time) (bool, int, int64, *outbound.ReorgEvent, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.detectReorg",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", block.Number),
			attribute.String("block.hash", block.Hash),
			attribute.String("block.parent_hash", block.ParentHash),
			attribute.Int("chain.unfinalized_count", len(s.unfinalizedBlocks)),
		),
	)
	defer span.End()

	if len(s.unfinalizedBlocks) == 0 {
		span.SetAttributes(attribute.Bool("reorg.detected", false))
		return false, 0, 0, nil, nil
	}

	latestBlock := s.unfinalizedBlocks[len(s.unfinalizedBlocks)-1]
	span.SetAttributes(
		attribute.Int64("chain.latest_block", latestBlock.Number),
		attribute.String("chain.latest_hash", latestBlock.Hash),
	)

	// Block number decreased - definite reorg
	if block.Number <= latestBlock.Number {
		return s.handleReorg(ctx, block, receivedAt)
	}

	// Block is exactly one ahead - check parent hash
	if block.Number == latestBlock.Number+1 {
		if block.ParentHash == latestBlock.Hash {
			span.SetAttributes(attribute.Bool("reorg.detected", false))
			return false, 0, 0, nil, nil
		}
		// Parent hash mismatch - reorg
		return s.handleReorg(ctx, block, receivedAt)
	}

	// Gap in blocks - will be backfilled by BackfillService
	span.SetAttributes(
		attribute.Bool("reorg.detected", false),
		attribute.Bool("block.gap", true),
	)
	return false, 0, 0, nil, nil
}

// handleReorg processes a detected reorg and returns the reorg event for atomic handling.
// The actual database operations (save reorg event, mark orphans, save new block) are
// done atomically in processBlock via HandleReorgAtomic.
func (s *LiveService) handleReorg(ctx context.Context, block LightBlock, receivedAt time.Time) (bool, int, int64, *outbound.ReorgEvent, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.handleReorg",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", block.Number),
			attribute.String("block.hash", block.Hash),
		),
	)
	defer span.End()

	// Walk back to find common ancestor
	// Start with the incoming block (already normalized), then walk to its parent each iteration
	walkBlock := block

	var commonAncestor int64 = -1
	walkCount := 0
	for walkCount = 0; walkCount < s.config.FinalityBlockCount; walkCount++ {
		// Check if parent matches our chain
		for _, b := range s.unfinalizedBlocks {
			if b.Hash == walkBlock.ParentHash {
				commonAncestor = b.Number
				break
			}
		}
		if commonAncestor >= 0 {
			break
		}

		// Check finality boundary
		if s.finalizedBlock != nil && walkBlock.Number <= s.finalizedBlock.Number {
			err := fmt.Errorf("block %d is at or below finalized block %d (likely late arrival after pruning)",
				walkBlock.Number, s.finalizedBlock.Number)
			span.RecordError(err)
			span.SetStatus(codes.Error, "block below finality")
			return false, 0, 0, nil, err
		}

		// Fetch parent from network
		parentHeader, err := s.client.GetBlockByHash(ctx, walkBlock.ParentHash, false)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to fetch parent block")
			return false, 0, 0, nil, fmt.Errorf("failed to fetch parent block %s during reorg walk: %w", walkBlock.ParentHash, err)
		}

		// Walk to parent block to continue searching for common ancestor
		// Normalize RPC response at the point of ingestion
		parentNum, err := hexutil.ParseInt64(parentHeader.Number)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to parse parent block number")
			return false, 0, 0, nil, fmt.Errorf("failed to parse parent block number %q: %w", parentHeader.Number, err)
		}
		walkBlock = LightBlock{
			Number:     parentNum,
			ParentHash: normalizeHash(parentHeader.ParentHash),
		}
	}

	span.SetAttributes(attribute.Int("reorg.walk_count", walkCount))

	if commonAncestor < 0 {
		err := fmt.Errorf("no common ancestor found after walking %d blocks (chain diverged beyond finality window)", s.config.FinalityBlockCount)
		span.RecordError(err)
		span.SetStatus(codes.Error, "no common ancestor found")
		return false, 0, 0, nil, err
	}

	// Collect all blocks that will be orphaned (blocks > commonAncestor)
	orphanedBlocks := make([]LightBlock, 0)
	for i := len(s.unfinalizedBlocks) - 1; i >= 0; i-- {
		if s.unfinalizedBlocks[i].Number > commonAncestor {
			orphanedBlocks = append(orphanedBlocks, s.unfinalizedBlocks[i])
		}
	}
	reorgDepth := len(orphanedBlocks)

	span.SetAttributes(
		attribute.Bool("reorg.detected", true),
		attribute.Int("reorg.depth", reorgDepth),
		attribute.Int64("reorg.common_ancestor", commonAncestor),
		attribute.Int("reorg.orphaned_count", len(orphanedBlocks)),
	)

	// NOTE: We do NOT prune the in-memory chain here.
	// The pruning happens in processBlock AFTER successful HandleReorgAtomic.
	// This ensures that if the DB operation fails, the in-memory chain is not modified,
	// maintaining consistency between memory and database state.

	// Build reorg event to be saved atomically with the new block
	// We always create a reorg event when a reorg is detected, even if depth is 0
	// (depth can be 0 when the incoming block is ahead of our chain but on a different fork)
	var reorgEvent *outbound.ReorgEvent
	if len(orphanedBlocks) > 0 {
		// Use the first orphaned block (closest to incoming block number) for OldHash
		reorgEvent = &outbound.ReorgEvent{
			DetectedAt:  receivedAt,
			BlockNumber: block.Number,
			OldHash:     orphanedBlocks[0].Hash, // Most recent orphaned block
			NewHash:     block.Hash,
			Depth:       reorgDepth,
		}
	} else {
		// No blocks orphaned, but we still detected a chain divergence
		// This can happen when the incoming block is ahead but on a different fork
		// We still need to record this as a reorg event
		reorgEvent = &outbound.ReorgEvent{
			DetectedAt:  receivedAt,
			BlockNumber: block.Number,
			OldHash:     "", // No block orphaned at this exact number
			NewHash:     block.Hash,
			Depth:       0,
		}
	}

	return true, reorgDepth, commonAncestor, reorgEvent, nil
}

// updateFinalizedBlock updates the finalized block pointer.
func (s *LiveService) updateFinalizedBlock(currentBlockNum int64) {
	finalizedNum := currentBlockNum - int64(s.config.FinalityBlockCount)
	if finalizedNum <= 0 {
		return
	}

	for i := range s.unfinalizedBlocks {
		if s.unfinalizedBlocks[i].Number == finalizedNum {
			s.finalizedBlock = &s.unfinalizedBlocks[i]
			break
		}
	}

	// Remove blocks before finality buffer
	cutoff := finalizedNum - int64(s.config.FinalityBlockCount/2)
	if cutoff > 0 {
		newChain := make([]LightBlock, 0)
		for _, b := range s.unfinalizedBlocks {
			if b.Number >= cutoff {
				newChain = append(newChain, b)
			}
		}
		s.unfinalizedBlocks = newChain
	}
}

// restoreInMemoryChain restores the in-memory chain state from the database.
func (s *LiveService) restoreInMemoryChain() error {
	recentBlocks, err := s.stateRepo.GetRecentBlocks(s.ctx, s.config.MaxUnfinalizedBlocks)
	if err != nil {
		return fmt.Errorf("failed to get recent blocks: %w", err)
	}

	if len(recentBlocks) == 0 {
		return nil
	}

	// GetRecentBlocks returns blocks in ascending order (oldest first)
	// Trust database data - it was normalized when originally stored
	s.unfinalizedBlocks = make([]LightBlock, 0, len(recentBlocks))
	for _, b := range recentBlocks {
		s.unfinalizedBlocks = append(s.unfinalizedBlocks, LightBlock{
			Number:     b.Number,
			Hash:       b.Hash,
			ParentHash: b.ParentHash,
		})
	}

	if len(s.unfinalizedBlocks) > 0 {
		tip := s.unfinalizedBlocks[len(s.unfinalizedBlocks)-1]
		finalizedNum := tip.Number - int64(s.config.FinalityBlockCount)
		for i := range s.unfinalizedBlocks {
			if s.unfinalizedBlocks[i].Number <= finalizedNum {
				s.finalizedBlock = &s.unfinalizedBlocks[i]
			}
		}
	}

	s.logger.Info("restored chain from DB", "blockCount", len(s.unfinalizedBlocks))
	return nil
}

func (s *LiveService) publishBlockEvent(ctx context.Context, chainID, blockNum int64, version int, blockHash, parentHash string, blockTimestamp int64, receivedAt time.Time, isReorg bool) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.publishBlockEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", blockHash),
			attribute.Int64("chain.id", chainID),
			attribute.Int("block.version", version),
			attribute.Bool("block.is_reorg", isReorg),
		),
	)
	defer span.End()

	event := outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNum,
		Version:        version,
		BlockHash:      blockHash,
		ParentHash:     parentHash,
		BlockTimestamp: blockTimestamp,
		ReceivedAt:     receivedAt,
		IsReorg:        isReorg,
		IsBackfill:     false,
	}

	snsStart := time.Now()
	if err := s.eventSink.Publish(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish block event")
		return fmt.Errorf("failed to publish block event for block %d: %w", blockNum, err)
	}
	snsDuration := time.Since(snsStart)
	span.SetAttributes(attribute.Int64("sns.duration_ms", snsDuration.Milliseconds()))

	// Mark block publish as complete in DB for crash recovery
	dbStart := time.Now()
	if err := s.stateRepo.MarkPublishComplete(ctx, blockHash); err != nil {
		s.logger.Warn("failed to mark block publish complete", "block", blockNum, "error", err)
		// Note: We don't fail here as the publish already succeeded
	}
	dbDuration := time.Since(dbStart)
	span.SetAttributes(attribute.Int64("db.mark_complete_duration_ms", dbDuration.Milliseconds()))

	s.logger.Debug("published block event", "block", blockNum, "sns_ms", snsDuration.Milliseconds(), "db_mark_ms", dbDuration.Milliseconds())

	return nil
}

// cacheAndPublishBlockData caches prefetched block data and publishes the event.
// This is the optimized version that skips the RPC fetch (data already prefetched).
func (s *LiveService) cacheAndPublishBlockData(ctx context.Context, header outbound.BlockHeader, blockNum int64, version int, receivedAt time.Time, isReorg bool, bd outbound.BlockData) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.cacheAndPublishBlockData",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", header.Hash),
			attribute.Int("block.version", version),
		),
	)
	defer span.End()

	chainID := s.config.ChainID
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, err := hexutil.ParseInt64(header.Timestamp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to parse block timestamp")
		return fmt.Errorf("failed to parse block timestamp %q for block %d: %w", header.Timestamp, blockNum, err)
	}

	// Check for fetch errors before caching
	if bd.BlockErr != nil {
		span.RecordError(bd.BlockErr)
		span.SetStatus(codes.Error, "block fetch error")
		return fmt.Errorf("failed to fetch block %d: %w", blockNum, bd.BlockErr)
	}
	if bd.ReceiptsErr != nil {
		span.RecordError(bd.ReceiptsErr)
		span.SetStatus(codes.Error, "receipts fetch error")
		return fmt.Errorf("failed to fetch receipts for block %d: %w", blockNum, bd.ReceiptsErr)
	}
	if bd.TracesErr != nil {
		span.RecordError(bd.TracesErr)
		span.SetStatus(codes.Error, "traces fetch error")
		return fmt.Errorf("failed to fetch traces for block %d: %w", blockNum, bd.TracesErr)
	}
	if s.config.EnableBlobs && bd.BlobsErr != nil {
		span.RecordError(bd.BlobsErr)
		span.SetStatus(codes.Error, "blobs fetch error")
		return fmt.Errorf("failed to fetch blobs for block %d: %w", blockNum, bd.BlobsErr)
	}

	// Verify all required data is present (defensive check against nil data without error)
	if bd.Block == nil {
		err := fmt.Errorf("missing block data for block %d (no error reported)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing block data")
		return err
	}
	if bd.Receipts == nil {
		err := fmt.Errorf("missing receipts data for block %d (no error reported)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing receipts data")
		return err
	}
	if bd.Traces == nil {
		err := fmt.Errorf("missing traces data for block %d (no error reported)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing traces data")
		return err
	}
	if s.config.EnableBlobs && bd.Blobs == nil {
		err := fmt.Errorf("missing blobs data for block %d (no error reported)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing blobs data")
		return err
	}

	// Cache all data types - create a child span for the cache operation
	cacheCtx, cacheSpan := tracer.Start(ctx, "live.cacheBlockData",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.Int64("chain.id", chainID),
			attribute.Int("block.version", version),
			attribute.Bool("blobs.enabled", s.config.EnableBlobs),
		),
	)
	cacheStart := time.Now()

	// Cache all data types in a single pipelined operation (single network round-trip)
	cacheInput := outbound.BlockDataInput{
		Block:    bd.Block,
		Receipts: bd.Receipts,
		Traces:   bd.Traces,
	}
	if s.config.EnableBlobs {
		cacheInput.Blobs = bd.Blobs
	}

	if err := s.cache.SetBlockData(cacheCtx, chainID, blockNum, version, cacheInput); err != nil {
		cacheSpan.RecordError(err)
		cacheSpan.SetStatus(codes.Error, "failed to cache block data")
		cacheSpan.End()
		return fmt.Errorf("failed to cache block data for block %d: %w", blockNum, err)
	}

	cacheDuration := time.Since(cacheStart)
	cacheSpan.SetAttributes(attribute.Int64("cache.duration_ms", cacheDuration.Milliseconds()))
	cacheSpan.End()
	s.logger.Debug("cached all block data", "block", blockNum, "cache_ms", cacheDuration.Milliseconds())

	// All data cached successfully - now publish the block event
	return s.publishBlockEvent(ctx, chainID, blockNum, version, blockHash, parentHash, blockTimestamp, receivedAt, isReorg)
}

// normalizeHash normalizes a hex hash to lowercase for consistent comparisons.
// Ethereum hashes are case-insensitive (0xAAA == 0xaaa), but Go string comparison
// is case-sensitive. Normalizing to lowercase prevents false mismatches.
func normalizeHash(hash string) string {
	return strings.ToLower(hash)
}
