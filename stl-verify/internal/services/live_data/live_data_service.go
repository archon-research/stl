package live_data

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

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

	// DisableBlobs disables fetching blob sidecars (useful for pre-Dencun blocks or unsupported nodes).
	DisableBlobs bool

	// Logger is the structured logger.
	Logger *slog.Logger

	// Metrics is the metrics recorder for telemetry (optional).
	Metrics outbound.MetricsRecorder
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

// LiveService handles live block subscription, reorg detection, and event publishing.
// It does NOT handle backfilling - that's the responsibility of BackfillService.
type LiveService struct {
	config LiveConfig

	subscriber outbound.BlockSubscriber
	client     outbound.BlockchainClient
	stateRepo  outbound.BlockStateRepository
	cache      outbound.BlockCache
	eventSink  outbound.EventSink
	metrics    outbound.MetricsRecorder

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
	s.restoreInMemoryChain()

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

// processHeaders processes incoming block headers.
func (s *LiveService) processHeaders(headers <-chan outbound.BlockHeader) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case header, ok := <-headers:
			if !ok {
				return
			}
			if err := s.processBlock(header, time.Now()); err != nil {
				blockNum, parseErr := parseBlockNumber(header.Number)
				if parseErr != nil {
					err = errors.Join(parseErr)
				}
				s.logger.Warn("failed to process live block",
					"block", blockNum,
					"hash", header.Hash,
					"parentHash", header.ParentHash,
					"error", err)
			}
		}
	}
}

// isDuplicateBlock checks if a block has already been processed.
// It first does a quick in-memory check, then falls back to DB lookup.
func (s *LiveService) isDuplicateBlock(ctx context.Context, hash string, blockNum int64) bool {
	// Quick in-memory check
	inMemory := slices.ContainsFunc(s.unfinalizedBlocks, func(b LightBlock) bool {
		return b.Hash == hash
	})
	if inMemory {
		s.logger.Debug("duplicate block, skipping", "block", blockNum)
		return true
	}

	// Also check DB for duplicates (backfill may have processed this block)
	existing, err := s.stateRepo.GetBlockByHash(ctx, hash)
	if err != nil {
		s.logger.Warn("failed to check DB for duplicate", "error", err)
	} else if existing != nil {
		s.logger.Debug("block already in DB, skipping", "block", blockNum)
		return true
	}

	return false
}

// processBlock handles a single block: dedup, reorg detection, state tracking, data fetching, publishing.
func (s *LiveService) processBlock(header outbound.BlockHeader, receivedAt time.Time) error {
	start := time.Now()
	blockNum, err := parseBlockNumber(header.Number)
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

	block := LightBlock{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
	}

	// Check if we've already processed this block
	if s.isDuplicateBlock(ctx, header.Hash, blockNum) {
		span.SetAttributes(attribute.Bool("block.duplicate", true))
		return nil
	}

	// Detect reorg BEFORE adding to chain
	isReorg, reorgDepth, commonAncestor, reorgEvent, err := s.detectReorg(header, blockNum, receivedAt)
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

	// Add block to in-memory chain
	s.addBlock(block)

	// Build block state for DB
	state := outbound.BlockState{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
		ReceivedAt: receivedAt.Unix(),
		IsOrphaned: false,
	}

	// Save block state to DB - use atomic reorg handling if this is a reorg
	var version int
	if isReorg && reorgEvent != nil {
		// Atomically: save reorg event + mark orphans + save new block
		version, err = s.stateRepo.HandleReorgAtomic(ctx, *reorgEvent, state)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to handle reorg atomically")
			return fmt.Errorf("failed to handle reorg atomically: %w", err)
		}
	} else {
		// Normal block save
		version, err = s.stateRepo.SaveBlock(ctx, state)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to save block state")
			return fmt.Errorf("failed to save block state: %w", err)
		}
	}

	// Fetch all data types concurrently, cache, and publish events
	if err := s.fetchAndPublishBlockData(ctx, header, blockNum, version, receivedAt, isReorg); err != nil {
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

// detectReorg detects chain reorganizations using Ponder-style parent hash chain validation.
// Returns: isReorg, reorgDepth, commonAncestor, reorgEvent (if reorg), error
func (s *LiveService) detectReorg(header outbound.BlockHeader, incomingBlockNum int64, receivedAt time.Time) (bool, int, int64, *outbound.ReorgEvent, error) {
	if len(s.unfinalizedBlocks) == 0 {
		return false, 0, 0, nil, nil
	}

	latestBlock := s.unfinalizedBlocks[len(s.unfinalizedBlocks)-1]

	// Block number decreased - definite reorg
	if incomingBlockNum <= latestBlock.Number {
		return s.handleReorg(header, incomingBlockNum, receivedAt)
	}

	// Block is exactly one ahead - check parent hash
	if incomingBlockNum == latestBlock.Number+1 {
		if header.ParentHash == latestBlock.Hash {
			return false, 0, 0, nil, nil
		}
		// Parent hash mismatch - reorg
		return s.handleReorg(header, incomingBlockNum, receivedAt)
	}

	// Gap in blocks - will be backfilled by BackfillService
	return false, 0, 0, nil, nil
}

// handleReorg processes a detected reorg and returns the reorg event for atomic handling.
// The actual database operations (save reorg event, mark orphans, save new block) are
// done atomically in processBlock via HandleReorgAtomic.
func (s *LiveService) handleReorg(header outbound.BlockHeader, blockNum int64, receivedAt time.Time) (bool, int, int64, *outbound.ReorgEvent, error) {
	ctx := s.ctx

	// Walk back to find common ancestor
	// Start with the incoming block, then walk to its parent each iteration
	walkBlock := LightBlock{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
	}

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
		parentNum, _ := parseBlockNumber(parentHeader.Number)
		walkBlock = LightBlock{
			Number:     parentNum,
			Hash:       parentHeader.Hash,
			ParentHash: parentHeader.ParentHash,
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

	// Prune reorged blocks from in-memory chain
	newChain := make([]LightBlock, 0)
	for _, b := range s.unfinalizedBlocks {
		if b.Number <= commonAncestor {
			newChain = append(newChain, b)
		}
	}
	s.unfinalizedBlocks = newChain

	// Build reorg event to be saved atomically with the new block
	// We always create a reorg event when a reorg is detected, even if depth is 0
	// (depth can be 0 when the incoming block is ahead of our chain but on a different fork)
	var reorgEvent *outbound.ReorgEvent
	if len(orphanedBlocks) > 0 {
		// Use the first orphaned block (closest to incoming block number) for OldHash
		reorgEvent = &outbound.ReorgEvent{
			DetectedAt:  receivedAt,
			BlockNumber: blockNum,
			OldHash:     orphanedBlocks[0].Hash, // Most recent orphaned block
			NewHash:     header.Hash,
			Depth:       reorgDepth,
		}
	} else {
		// No blocks orphaned, but we still detected a chain divergence
		// This can happen when the incoming block is ahead but on a different fork
		// We still need to record this as a reorg event
		reorgEvent = &outbound.ReorgEvent{
			DetectedAt:  receivedAt,
			BlockNumber: blockNum,
			OldHash:     "", // No block orphaned at this exact number
			NewHash:     header.Hash,
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
func (s *LiveService) restoreInMemoryChain() {
	recentBlocks, err := s.stateRepo.GetRecentBlocks(s.ctx, s.config.MaxUnfinalizedBlocks)
	if err != nil {
		s.logger.Warn("failed to restore chain from DB", "error", err)
		return
	}

	if len(recentBlocks) == 0 {
		return
	}

	// GetRecentBlocks returns blocks in ascending order (oldest first)
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
}

// fetchAndPublishBlockData fetches all data types concurrently and publishes events.
func (s *LiveService) fetchAndPublishBlockData(ctx context.Context, header outbound.BlockHeader, blockNum int64, version int, receivedAt time.Time, isReorg bool) error {
	chainID := s.config.ChainID
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, _ := parseBlockNumber(header.Timestamp)

	numWorkers := 3
	if !s.config.DisableBlobs {
		numWorkers = 4
	}
	errCh := make(chan error, numWorkers)

	// Fetch and publish block
	go func() {
		errCh <- s.fetchCacheAndPublishBlock(ctx, chainID, blockNum, version, blockHash, parentHash, blockTimestamp, receivedAt, isReorg)
	}()

	// Fetch and publish receipts
	go func() {
		errCh <- s.fetchCacheAndPublishReceipts(ctx, chainID, blockNum, version, blockHash, receivedAt, isReorg)
	}()

	// Fetch and publish traces
	go func() {
		errCh <- s.fetchCacheAndPublishTraces(ctx, chainID, blockNum, version, blockHash, receivedAt, isReorg)
	}()

	// Fetch and publish blobs (if enabled)
	if !s.config.DisableBlobs {
		go func() {
			errCh <- s.fetchCacheAndPublishBlobs(ctx, chainID, blockNum, version, blockHash, receivedAt, isReorg)
		}()
	}

	// Collect errors from all workers
	var errs []error
	for i := 0; i < numWorkers; i++ {
		if err := <-errCh; err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *LiveService) fetchCacheAndPublishBlock(ctx context.Context, chainID, blockNum int64, version int, blockHash, parentHash string, blockTimestamp int64, receivedAt time.Time, isReorg bool) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("fetchCacheAndPublishBlock completed", "block", blockNum, "duration", time.Since(start))
	}()

	// Fetch by hash to prevent TOCTOU race condition.
	// If we fetched by number, a reorg between receiving the header and fetching
	// could cause us to cache data for the wrong block.
	data, err := s.client.GetFullBlockByHash(ctx, blockHash, true)
	if err != nil {
		return fmt.Errorf("failed to fetch block %d by hash %s: %w", blockNum, blockHash, err)
	}

	if err := s.cache.SetBlock(ctx, chainID, blockNum, version, data); err != nil {
		return fmt.Errorf("failed to cache block %d: %w", blockNum, err)
	}

	event := outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNum,
		Version:        version,
		BlockHash:      blockHash,
		ParentHash:     parentHash,
		BlockTimestamp: blockTimestamp,
		ReceivedAt:     receivedAt,
		CacheKey:       cacheKey(chainID, blockNum, version, "block"),
		IsReorg:        isReorg,
		IsBackfill:     false,
	}
	if err := s.eventSink.Publish(ctx, event); err != nil {
		return fmt.Errorf("failed to publish block event for block %d: %w", blockNum, err)
	}
	return nil
}

func (s *LiveService) fetchCacheAndPublishReceipts(ctx context.Context, chainID, blockNum int64, version int, blockHash string, receivedAt time.Time, isReorg bool) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("fetchCacheAndPublishReceipts completed", "block", blockNum, "duration", time.Since(start))
	}()

	data, err := s.client.GetBlockReceipts(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to fetch receipts for block %d: %w", blockNum, err)
	}

	if err := s.cache.SetReceipts(ctx, chainID, blockNum, version, data); err != nil {
		return fmt.Errorf("failed to cache receipts for block %d: %w", blockNum, err)
	}

	event := outbound.ReceiptsEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		Version:     version,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		CacheKey:    cacheKey(chainID, blockNum, version, "receipts"),
		IsReorg:     isReorg,
		IsBackfill:  false,
	}
	if err := s.eventSink.Publish(ctx, event); err != nil {
		return fmt.Errorf("failed to publish receipts event for block %d: %w", blockNum, err)
	}
	return nil
}

func (s *LiveService) fetchCacheAndPublishTraces(ctx context.Context, chainID, blockNum int64, version int, blockHash string, receivedAt time.Time, isReorg bool) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("fetchCacheAndPublishTraces completed", "block", blockNum, "duration", time.Since(start))
	}()

	data, err := s.client.GetBlockTraces(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to fetch traces for block %d: %w", blockNum, err)
	}

	if err := s.cache.SetTraces(ctx, chainID, blockNum, version, data); err != nil {
		return fmt.Errorf("failed to cache traces for block %d: %w", blockNum, err)
	}

	event := outbound.TracesEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		Version:     version,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		CacheKey:    cacheKey(chainID, blockNum, version, "traces"),
		IsReorg:     isReorg,
		IsBackfill:  false,
	}
	if err := s.eventSink.Publish(ctx, event); err != nil {
		return fmt.Errorf("failed to publish traces event for block %d: %w", blockNum, err)
	}
	return nil
}

func (s *LiveService) fetchCacheAndPublishBlobs(ctx context.Context, chainID, blockNum int64, version int, blockHash string, receivedAt time.Time, isReorg bool) error {
	start := time.Now()
	defer func() {
		s.logger.Debug("fetchCacheAndPublishBlobs completed", "block", blockNum, "duration", time.Since(start))
	}()

	data, err := s.client.GetBlobSidecars(ctx, blockNum)
	if err != nil {
		return fmt.Errorf("failed to fetch blobs for block %d: %w", blockNum, err)
	}

	if err := s.cache.SetBlobs(ctx, chainID, blockNum, version, data); err != nil {
		return fmt.Errorf("failed to cache blobs for block %d: %w", blockNum, err)
	}

	event := outbound.BlobsEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		Version:     version,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		CacheKey:    cacheKey(chainID, blockNum, version, "blobs"),
		IsReorg:     isReorg,
		IsBackfill:  false,
	}
	if err := s.eventSink.Publish(ctx, event); err != nil {
		return fmt.Errorf("failed to publish blobs event for block %d: %w", blockNum, err)
	}
	return nil
}

// Utility functions
// parseBlockNumber parses a hex-encoded block number string to int64.
func parseBlockNumber(hexNum string) (int64, error) {
	hexNum = strings.TrimPrefix(hexNum, "0x")
	return strconv.ParseInt(hexNum, 16, 64)
}

// cacheKey generates the cache key for a given data type.
// Format: {chainID}:{blockNumber}:{version}:{dataType}
// The version is incremented each time the watcher sees the same block after a reorg.
// This ensures data will be eventually correct after reorgs.
func cacheKey(chainID, blockNumber int64, version int, dataType string) string {
	return fmt.Sprintf("stl:%d:%d:%d:%s", chainID, blockNumber, version, dataType)
}
