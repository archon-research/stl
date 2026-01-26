package live_data

import (
	"context"
	"errors"
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
			blockNum, err := hexutil.ParseInt64(header.Number)
			if err != nil {
				s.logger.Warn("failed to parse block number", "error", err)
				continue
			}

			// Start prefetching RPC data immediately (runs async while we do state ops)
			prefetchCh := s.startPrefetch(header, blockNum)

			// Process the block - state ops run while prefetch happens in background
			if err := s.processBlockWithPrefetch(header, blockNum, receivedAt, prefetchCh); err != nil {
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
// Returns a channel that will receive the result when complete.
func (s *LiveService) startPrefetch(header outbound.BlockHeader, blockNum int64) <-chan prefetchResult {
	resultCh := make(chan prefetchResult, 1)

	go func() {
		start := time.Now()
		ctx := s.ctx // Use service context for cancellation

		// Fetch all data in a single batched HTTP request
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

// stateOpsResult holds the results from processStateOperations.
type stateOpsResult struct {
	version int
	isReorg bool
	skip    bool // true if block was duplicate and should be skipped
}

// processStateOperations handles dedup checking, reorg detection, and block state persistence.
// This runs concurrently with data prefetching to minimize total processing time.
// Returns skip=true if the block is a duplicate and should not be processed further.
func (s *LiveService) processStateOperations(ctx context.Context, span trace.Span, header outbound.BlockHeader, blockNum int64, receivedAt time.Time) (stateOpsResult, error) {
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
		return stateOpsResult{}, fmt.Errorf("failed to check for duplicate block: %w", err)
	}
	if isDuplicate {
		span.SetAttributes(attribute.Bool("block.duplicate", true))
		return stateOpsResult{skip: true}, nil
	}

	// Detect reorg BEFORE adding to chain
	isReorg, reorgDepth, commonAncestor, reorgEvent, err := s.detectReorg(block, receivedAt)
	if err != nil {
		return stateOpsResult{}, fmt.Errorf("reorg detection failed: %w", err)
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
			return stateOpsResult{}, fmt.Errorf("failed to handle reorg atomically: %w", err)
		}
		s.pruneReorgedBlocks(commonAncestor)
	} else {
		version, err = s.stateRepo.SaveBlock(ctx, state)
		if err != nil {
			return stateOpsResult{}, fmt.Errorf("failed to save block state: %w", err)
		}
	}

	// Add block to in-memory chain AFTER successful DB save
	s.addBlock(block)

	return stateOpsResult{version: version, isReorg: isReorg}, nil
}

// waitForPrefetch waits for the prefetch goroutine to complete and returns the result.
func (s *LiveService) waitForPrefetch(ctx context.Context, span trace.Span, prefetchCh <-chan prefetchResult, blockNum int64) (outbound.BlockData, error) {
	prefetchWaitStart := time.Now()
	var prefetch prefetchResult
	select {
	case prefetch = <-prefetchCh:
		// Got the prefetched data
		prefetchWaitDuration := time.Since(prefetchWaitStart)
		span.SetAttributes(attribute.Int64("block.prefetch_wait_ms", prefetchWaitDuration.Milliseconds()))
	case <-ctx.Done():
		return outbound.BlockData{}, ctx.Err()
	}

	if prefetch.err != nil {
		return outbound.BlockData{}, fmt.Errorf("failed to fetch block data for block %d: %w", blockNum, prefetch.err)
	}

	return prefetch.blockData, nil
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
func (s *LiveService) processBlockWithPrefetch(header outbound.BlockHeader, blockNum int64, receivedAt time.Time, prefetchCh <-chan prefetchResult) error {
	start := time.Now()

	// Start span for the entire block processing
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
	defer func() {
		span.SetAttributes(attribute.Int64("block.duration_ms", time.Since(start).Milliseconds()))
		span.End()
		s.logger.Info("processBlock completed", "block", blockNum, "duration", time.Since(start))
	}()

	// State operations: dedup check, reorg detection, DB save (runs while prefetch is in progress)
	stateResult, err := s.processStateOperations(ctx, span, header, blockNum, receivedAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if stateResult.skip {
		return nil
	}
	span.SetAttributes(attribute.Int64("block.state_ops_ms", time.Since(start).Milliseconds()))

	// Wait for prefetch to complete (should be mostly done by now)
	blockData, err := s.waitForPrefetch(ctx, span, prefetchCh, blockNum)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "prefetch failed")
		return err
	}

	// Cache and publish block data
	if err := s.cacheAndPublishBlockData(ctx, header, blockNum, stateResult.version, receivedAt, stateResult.isReorg, blockData); err != nil {
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
	// Quick in-memory check
	inMemory := slices.ContainsFunc(s.unfinalizedBlocks, func(b LightBlock) bool {
		return b.Hash == hash
	})
	if inMemory {
		s.logger.Debug("duplicate block, skipping", "block", blockNum)
		return true, nil
	}

	// Also check DB for duplicates (backfill may have processed this block)
	existing, err := s.stateRepo.GetBlockByHash(ctx, hash)
	if err != nil {
		return false, fmt.Errorf("failed to check DB for duplicate: %w", err)
	}
	if existing != nil {
		s.logger.Debug("block already in DB, skipping", "block", blockNum)
		return true, nil
	}

	return false, nil
}

// processBlock handles a single block: dedup, reorg detection, state tracking, data fetching, publishing.
func (s *LiveService) processBlock(header outbound.BlockHeader, receivedAt time.Time) error {
	start := time.Now()
	blockNum, err := hexutil.ParseInt64(header.Number)
	if err != nil {
		return fmt.Errorf("failed to parse block number: %w", err)
	}

	// Start span for the entire block processing
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(s.ctx, "live.processBlock",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", header.Hash),
			attribute.String("block.parent_hash", header.ParentHash),
		),
	)
	defer func() {
		span.SetAttributes(attribute.Int64("block.duration_ms", time.Since(start).Milliseconds()))
		span.End()
		s.logger.Info("processBlock completed", "block", blockNum, "duration", time.Since(start))
	}()

	// State operations: dedup check, reorg detection, DB save
	stateResult, err := s.processStateOperations(ctx, span, header, blockNum, receivedAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if stateResult.skip {
		return nil
	}

	// Fetch all data types concurrently, cache, and publish events
	if err := s.fetchAndPublishBlockData(ctx, header, blockNum, stateResult.version, receivedAt, stateResult.isReorg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to fetch and publish block data")
		return fmt.Errorf("failed to fetch and publish block data: %w", err)
	}

	// Update finalized block pointer after successful publishing
	s.updateFinalizedBlock(blockNum)

	return nil
}

// addBlock adds a block to the in-memory unfinalized chain.
func (s *LiveService) addBlock(block LightBlock) {
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
	if len(s.unfinalizedBlocks) > s.config.MaxUnfinalizedBlocks {
		s.unfinalizedBlocks = s.unfinalizedBlocks[1:]
	}
}

// pruneReorgedBlocks removes blocks above the common ancestor from the in-memory chain.
// This should only be called AFTER the reorg has been successfully persisted to the database.
func (s *LiveService) pruneReorgedBlocks(commonAncestor int64) {
	newChain := make([]LightBlock, 0, len(s.unfinalizedBlocks))
	for _, b := range s.unfinalizedBlocks {
		if b.Number <= commonAncestor {
			newChain = append(newChain, b)
		}
	}
	s.unfinalizedBlocks = newChain
}

// detectReorg detects chain reorganizations using Ponder-style parent hash chain validation.
// Returns: isReorg, reorgDepth, commonAncestor, reorgEvent (if reorg), error
func (s *LiveService) detectReorg(block LightBlock, receivedAt time.Time) (bool, int, int64, *outbound.ReorgEvent, error) {
	if len(s.unfinalizedBlocks) == 0 {
		return false, 0, 0, nil, nil
	}

	latestBlock := s.unfinalizedBlocks[len(s.unfinalizedBlocks)-1]

	// Block number decreased - definite reorg
	if block.Number <= latestBlock.Number {
		return s.handleReorg(block, receivedAt)
	}

	// Block is exactly one ahead - check parent hash
	if block.Number == latestBlock.Number+1 {
		if block.ParentHash == latestBlock.Hash {
			return false, 0, 0, nil, nil
		}
		// Parent hash mismatch - reorg
		return s.handleReorg(block, receivedAt)
	}

	// Gap in blocks - will be backfilled by BackfillService
	return false, 0, 0, nil, nil
}

// handleReorg processes a detected reorg and returns the reorg event for atomic handling.
// The actual database operations (save reorg event, mark orphans, save new block) are
// done atomically in processBlock via HandleReorgAtomic.
func (s *LiveService) handleReorg(block LightBlock, receivedAt time.Time) (bool, int, int64, *outbound.ReorgEvent, error) {
	ctx := s.ctx

	// Walk back to find common ancestor
	// Start with the incoming block (already normalized), then walk to its parent each iteration
	walkBlock := block

	var commonAncestor int64 = -1
	for walkCount := 0; walkCount < s.config.FinalityBlockCount; walkCount++ {
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
			return false, 0, 0, nil, fmt.Errorf("block %d is at or below finalized block %d (likely late arrival after pruning)",
				walkBlock.Number, s.finalizedBlock.Number)
		}

		// Fetch parent from network
		parentHeader, err := s.client.GetBlockByHash(ctx, walkBlock.ParentHash, false)
		if err != nil {
			return false, 0, 0, nil, fmt.Errorf("failed to fetch parent block %s during reorg walk: %w", walkBlock.ParentHash, err)
		}

		// Walk to parent block to continue searching for common ancestor
		// Normalize RPC response at the point of ingestion
		parentNum, err := hexutil.ParseInt64(parentHeader.Number)
		if err != nil {
			return false, 0, 0, nil, fmt.Errorf("failed to parse parent block number %q: %w", parentHeader.Number, err)
		}
		walkBlock = LightBlock{
			Number:     parentNum,
			ParentHash: normalizeHash(parentHeader.ParentHash),
		}
	}

	if commonAncestor < 0 {
		return false, 0, 0, nil, fmt.Errorf("no common ancestor found after walking %d blocks (chain diverged beyond finality window)", s.config.FinalityBlockCount)
	}

	// Collect all blocks that will be orphaned (blocks > commonAncestor)
	orphanedBlocks := make([]LightBlock, 0)
	for i := len(s.unfinalizedBlocks) - 1; i >= 0; i-- {
		if s.unfinalizedBlocks[i].Number > commonAncestor {
			orphanedBlocks = append(orphanedBlocks, s.unfinalizedBlocks[i])
		}
	}
	reorgDepth := len(orphanedBlocks)

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

// fetchAndPublishBlockData fetches all data types in a single batched RPC call by hash,
// caches them in parallel, and publishes a single block event only after all data has been successfully cached.
// Fetching by hash is TOCTOU-safe - it ensures we get data for the exact block we received.
func (s *LiveService) fetchAndPublishBlockData(ctx context.Context, header outbound.BlockHeader, blockNum int64, version int, receivedAt time.Time, isReorg bool) error {
	chainID := s.config.ChainID
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, err := hexutil.ParseInt64(header.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to parse block timestamp %q for block %d: %w", header.Timestamp, blockNum, err)
	}

	start := time.Now()

	// Fetch all data in a single batched HTTP request (by hash for TOCTOU safety)
	bd, err := s.client.GetBlockDataByHash(ctx, blockNum, blockHash, true)
	if err != nil {
		return fmt.Errorf("failed to fetch block data for block %d: %w", blockNum, err)
	}

	s.logger.Debug("fetched block data", "block", blockNum, "duration", time.Since(start))

	// Check for fetch errors before caching
	if bd.BlockErr != nil {
		return fmt.Errorf("failed to fetch block %d: %w", blockNum, bd.BlockErr)
	}
	if bd.ReceiptsErr != nil {
		return fmt.Errorf("failed to fetch receipts for block %d: %w", blockNum, bd.ReceiptsErr)
	}
	if bd.TracesErr != nil {
		return fmt.Errorf("failed to fetch traces for block %d: %w", blockNum, bd.TracesErr)
	}
	if s.config.EnableBlobs && bd.BlobsErr != nil {
		return fmt.Errorf("failed to fetch blobs for block %d: %w", blockNum, bd.BlobsErr)
	}

	// Cache all data types in parallel
	numWorkers := 3
	if s.config.EnableBlobs {
		numWorkers = 4
	}
	errCh := make(chan error, numWorkers)

	go func() {
		if err := s.cache.SetBlock(ctx, chainID, blockNum, version, bd.Block); err != nil {
			errCh <- fmt.Errorf("failed to cache block %d: %w", blockNum, err)
		} else {
			errCh <- nil
		}
	}()

	go func() {
		if err := s.cache.SetReceipts(ctx, chainID, blockNum, version, bd.Receipts); err != nil {
			errCh <- fmt.Errorf("failed to cache receipts for block %d: %w", blockNum, err)
		} else {
			errCh <- nil
		}
	}()

	go func() {
		if err := s.cache.SetTraces(ctx, chainID, blockNum, version, bd.Traces); err != nil {
			errCh <- fmt.Errorf("failed to cache traces for block %d: %w", blockNum, err)
		} else {
			errCh <- nil
		}
	}()

	if s.config.EnableBlobs {
		go func() {
			if err := s.cache.SetBlobs(ctx, chainID, blockNum, version, bd.Blobs); err != nil {
				errCh <- fmt.Errorf("failed to cache blobs for block %d: %w", blockNum, err)
			} else {
				errCh <- nil
			}
		}()
	}

	// Wait for all caching to complete
	var errs []error
	for i := 0; i < numWorkers; i++ {
		if err := <-errCh; err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	s.logger.Debug("cached all block data", "block", blockNum, "duration", time.Since(start))

	// All data cached successfully - now publish the block event
	return s.publishBlockEvent(ctx, chainID, blockNum, version, blockHash, parentHash, blockTimestamp, receivedAt, isReorg)
}

func (s *LiveService) publishBlockEvent(ctx context.Context, chainID, blockNum int64, version int, blockHash, parentHash string, blockTimestamp int64, receivedAt time.Time, isReorg bool) error {
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
		return fmt.Errorf("failed to publish block event for block %d: %w", blockNum, err)
	}
	snsDuration := time.Since(snsStart)

	// Mark block publish as complete in DB for crash recovery
	dbStart := time.Now()
	if err := s.stateRepo.MarkPublishComplete(ctx, blockHash, outbound.PublishTypeBlock); err != nil {
		s.logger.Warn("failed to mark block publish complete", "block", blockNum, "error", err)
	}
	dbDuration := time.Since(dbStart)

	s.logger.Debug("published block event", "block", blockNum, "sns_ms", snsDuration.Milliseconds(), "db_mark_ms", dbDuration.Milliseconds())

	return nil
}

// cacheAndPublishBlockData caches prefetched block data and publishes the event.
// This is the optimized version that skips the RPC fetch (data already prefetched).
func (s *LiveService) cacheAndPublishBlockData(ctx context.Context, header outbound.BlockHeader, blockNum int64, version int, receivedAt time.Time, isReorg bool, bd outbound.BlockData) error {
	chainID := s.config.ChainID
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, err := hexutil.ParseInt64(header.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to parse block timestamp %q for block %d: %w", header.Timestamp, blockNum, err)
	}

	cacheStart := time.Now()

	// Check for fetch errors before caching
	if bd.BlockErr != nil {
		return fmt.Errorf("failed to fetch block %d: %w", blockNum, bd.BlockErr)
	}
	if bd.ReceiptsErr != nil {
		return fmt.Errorf("failed to fetch receipts for block %d: %w", blockNum, bd.ReceiptsErr)
	}
	if bd.TracesErr != nil {
		return fmt.Errorf("failed to fetch traces for block %d: %w", blockNum, bd.TracesErr)
	}
	if s.config.EnableBlobs && bd.BlobsErr != nil {
		return fmt.Errorf("failed to fetch blobs for block %d: %w", blockNum, bd.BlobsErr)
	}

	// Cache all data types in a single pipelined operation (single network round-trip)
	cacheInput := outbound.BlockDataInput{
		Block:    bd.Block,
		Receipts: bd.Receipts,
		Traces:   bd.Traces,
	}
	if s.config.EnableBlobs {
		cacheInput.Blobs = bd.Blobs
	}

	if err := s.cache.SetBlockData(ctx, chainID, blockNum, version, cacheInput); err != nil {
		return fmt.Errorf("failed to cache block data for block %d: %w", blockNum, err)
	}

	cacheDuration := time.Since(cacheStart)
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
