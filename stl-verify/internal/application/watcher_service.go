package application

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// WatcherConfig holds configuration for the WatcherService.
type WatcherConfig struct {
	// ChainID is the blockchain chain ID (e.g., 1 for Ethereum mainnet).
	ChainID int64

	// FinalityBlockCount is how many blocks behind the tip before considered finalized.
	FinalityBlockCount int

	// MaxUnfinalizedBlocks is the max number of unfinalized blocks to keep in memory.
	MaxUnfinalizedBlocks int

	// BlockRetention is how many blocks to keep in the state repository.
	BlockRetention int

	// BackfillBatchSize is how many blocks to fetch in a single batched RPC call.
	BackfillBatchSize int

	// Logger is the structured logger.
	Logger *slog.Logger
}

// WatcherConfigDefaults returns default configuration.
func WatcherConfigDefaults() WatcherConfig {
	return WatcherConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 200,
		BlockRetention:       1000,
		BackfillBatchSize:    10,
		Logger:               slog.Default(),
	}
}

// LightBlock represents a minimal block for chain tracking.
type LightBlock struct {
	Number     int64
	Hash       string
	ParentHash string
}

// backfillRequest represents a range of blocks to backfill.
type backfillRequest struct {
	from int64
	to   int64
}

// WatcherService orchestrates block watching, reorg detection, data fetching, and event publishing.
type WatcherService struct {
	config WatcherConfig

	subscriber outbound.BlockSubscriber
	client     outbound.BlockchainClient
	stateRepo  outbound.BlockStateRepository
	cache      outbound.BlockCache
	eventSink  outbound.EventSink

	// In-memory chain state for reorg detection
	chainMu           sync.RWMutex
	unfinalizedBlocks []LightBlock
	finalizedBlock    *LightBlock

	// Background backfill
	backfillCh chan backfillRequest
	backfillWg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewWatcherService creates a new WatcherService.
func NewWatcherService(
	config WatcherConfig,
	subscriber outbound.BlockSubscriber,
	client outbound.BlockchainClient,
	stateRepo outbound.BlockStateRepository,
	cache outbound.BlockCache,
	eventSink outbound.EventSink,
) (*WatcherService, error) {
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
	defaults := WatcherConfigDefaults()
	if config.ChainID == 0 {
		config.ChainID = defaults.ChainID
	}
	if config.FinalityBlockCount == 0 {
		config.FinalityBlockCount = defaults.FinalityBlockCount
	}
	if config.MaxUnfinalizedBlocks == 0 {
		config.MaxUnfinalizedBlocks = defaults.MaxUnfinalizedBlocks
	}
	if config.BlockRetention == 0 {
		config.BlockRetention = defaults.BlockRetention
	}
	if config.BackfillBatchSize == 0 {
		config.BackfillBatchSize = defaults.BackfillBatchSize
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &WatcherService{
		config:     config,
		subscriber: subscriber,
		client:     client,
		stateRepo:  stateRepo,
		cache:      cache,
		eventSink:  eventSink,
		backfillCh: make(chan backfillRequest, 100),
		logger:     config.Logger.With("component", "watcher-service"),
	}, nil
}

// Start begins watching for new blocks.
func (w *WatcherService) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)

	// Restore chain state from DB
	w.restoreChainFromDB()

	// Start background backfill worker
	w.backfillWg.Add(1)
	go w.backfillWorker()

	// Subscribe to new block headers
	headers, err := w.subscriber.Subscribe(w.ctx)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Process incoming block headers
	go w.processHeaders(headers)

	w.logger.Info("watcher service started", "chainID", w.config.ChainID)
	return nil
}

// Stop stops the watcher service.
func (w *WatcherService) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}
	// Wait for backfill worker to finish
	w.backfillWg.Wait()
	return w.subscriber.Unsubscribe()
}

// OnReconnect handles reconnection by queuing missed blocks for background backfill.
// This should be called by the subscriber's reconnect callback.
func (w *WatcherService) OnReconnect() {
	w.queueBackfill()
}

// processHeaders processes incoming block headers.
func (w *WatcherService) processHeaders(headers <-chan outbound.BlockHeader) {
	for {
		select {
		case <-w.ctx.Done():
			return
		case header, ok := <-headers:
			if !ok {
				return
			}
			if err := w.processBlock(header, time.Now(), false); err != nil {
				blockNum, _ := parseBlockNumber(header.Number)
				w.logger.Warn("failed to process live block",
					"block", blockNum,
					"hash", header.Hash,
					"parentHash", header.ParentHash,
					"error", err)
			}
		}
	}
}

// processBlock handles a single block: dedup, reorg detection, state tracking, data fetching, publishing.
func (w *WatcherService) processBlock(header outbound.BlockHeader, receivedAt time.Time, isBackfill bool) error {
	blockNum, err := parseBlockNumber(header.Number)
	if err != nil {
		return fmt.Errorf("failed to parse block number: %w", err)
	}

	ctx := w.ctx

	block := LightBlock{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
	}

	// First check if duplicate (quick check under read lock)
	w.chainMu.RLock()
	isDup := false
	for _, b := range w.unfinalizedBlocks {
		if b.Hash == block.Hash {
			isDup = true
			break
		}
	}
	w.chainMu.RUnlock()

	if isDup {
		w.logger.Debug("duplicate block, skipping", "block", blockNum)
		return nil
	}

	// Detect reorg BEFORE adding to chain
	isReorg, reorgDepth, commonAncestor, err := w.detectReorg(header, blockNum, receivedAt)
	if err != nil {
		return fmt.Errorf("reorg detection failed: %w", err)
	}
	if isReorg {
		w.logger.Warn("reorg detected", "block", blockNum, "depth", reorgDepth, "commonAncestor", commonAncestor)
	}

	// Now atomically add to chain (may fail if added by backfill in between)
	if !w.tryAddBlock(block) {
		w.logger.Debug("block added by concurrent process, skipping", "block", blockNum)
		return nil
	}

	// Save block state to DB
	state := outbound.BlockState{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
		ReceivedAt: receivedAt.Unix(),
		IsOrphaned: false,
	}
	if err := w.stateRepo.SaveBlock(ctx, state); err != nil {
		return fmt.Errorf("failed to save block state: %w", err)
	}

	// Update finalized block pointer
	w.chainMu.Lock()
	w.updateFinalizedBlock(blockNum)
	w.chainMu.Unlock()

	// Fetch all data types concurrently, cache, and publish events
	w.fetchAndPublishBlockData(header, blockNum, receivedAt, isReorg, isBackfill)

	return nil
}

// isDuplicate checks if we've already processed this block.
// Note: This has a TOCTOU race with addToUnfinalizedChain. For full atomicity,
// use tryAddBlock which combines check and add under a single lock.
func (w *WatcherService) isDuplicate(hash string, blockNum int64) bool {
	// Check in-memory chain first
	w.chainMu.RLock()
	for _, b := range w.unfinalizedBlocks {
		if b.Hash == hash {
			w.chainMu.RUnlock()
			return true
		}
	}
	w.chainMu.RUnlock()

	// Check DB
	existing, err := w.stateRepo.GetBlockByHash(w.ctx, hash)
	if err != nil {
		w.logger.Warn("failed to check duplicate", "error", err)
		return false
	}
	return existing != nil
}

// tryAddBlock atomically checks for duplicate and adds to unfinalized chain.
// Returns false if block already exists (duplicate).
// Caller must still save to DB and handle reorg detection separately.
func (w *WatcherService) tryAddBlock(block LightBlock) bool {
	w.chainMu.Lock()
	defer w.chainMu.Unlock()

	// Check for duplicate by hash
	for _, b := range w.unfinalizedBlocks {
		if b.Hash == block.Hash {
			return false // Already exists
		}
	}

	// Add to chain (sorted insert)
	insertIdx := len(w.unfinalizedBlocks)
	for i, b := range w.unfinalizedBlocks {
		if b.Number > block.Number {
			insertIdx = i
			break
		} else if b.Number == block.Number {
			insertIdx = i + 1
			break
		}
	}

	w.unfinalizedBlocks = append(w.unfinalizedBlocks, LightBlock{})
	copy(w.unfinalizedBlocks[insertIdx+1:], w.unfinalizedBlocks[insertIdx:])
	w.unfinalizedBlocks[insertIdx] = block

	if len(w.unfinalizedBlocks) > w.config.MaxUnfinalizedBlocks {
		w.unfinalizedBlocks = w.unfinalizedBlocks[1:]
	}

	return true
}

// detectReorg detects chain reorganizations using Ponder-style parent hash chain validation.
func (w *WatcherService) detectReorg(header outbound.BlockHeader, incomingBlockNum int64, receivedAt time.Time) (bool, int, int64, error) {
	w.chainMu.Lock()
	defer w.chainMu.Unlock()

	if len(w.unfinalizedBlocks) == 0 {
		return false, 0, 0, nil
	}

	latestBlock := w.unfinalizedBlocks[len(w.unfinalizedBlocks)-1]

	// Block number decreased - definite reorg
	if incomingBlockNum <= latestBlock.Number {
		return w.handleReorg(header, incomingBlockNum, receivedAt)
	}

	// Block is exactly one ahead - check parent hash
	if incomingBlockNum == latestBlock.Number+1 {
		if header.ParentHash == latestBlock.Hash {
			return false, 0, 0, nil
		}
		// Parent hash mismatch - reorg
		return w.handleReorg(header, incomingBlockNum, receivedAt)
	}

	// Gap in blocks - will be backfilled
	return false, 0, 0, nil
}

// handleReorg processes a detected reorg.
func (w *WatcherService) handleReorg(header outbound.BlockHeader, blockNum int64, receivedAt time.Time) (bool, int, int64, error) {
	ctx := w.ctx

	// Find reorged blocks
	reorgedBlocks := make([]LightBlock, 0)
	for i := len(w.unfinalizedBlocks) - 1; i >= 0; i-- {
		if w.unfinalizedBlocks[i].Number >= blockNum {
			reorgedBlocks = append(reorgedBlocks, w.unfinalizedBlocks[i])
		}
	}

	// Walk back to find common ancestor
	remoteBlock := LightBlock{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
	}

	var commonAncestor int64 = -1
	for walkCount := 0; walkCount < w.config.FinalityBlockCount; walkCount++ {
		// Check if parent matches our chain
		for _, b := range w.unfinalizedBlocks {
			if b.Hash == remoteBlock.ParentHash {
				commonAncestor = b.Number
				break
			}
		}
		if commonAncestor >= 0 {
			break
		}

		// Check finality boundary
		if w.finalizedBlock != nil && remoteBlock.Number <= w.finalizedBlock.Number {
			return false, 0, 0, fmt.Errorf("block %d is at or below finalized block %d (likely late arrival after pruning)",
				remoteBlock.Number, w.finalizedBlock.Number)
		}

		// Fetch parent from network
		parentHeader, err := w.client.GetBlockByHash(ctx, remoteBlock.ParentHash, false)
		if err != nil {
			break
		}

		parentNum, _ := parseBlockNumber(parentHeader.Number)
		remoteBlock = LightBlock{
			Number:     parentNum,
			Hash:       parentHeader.Hash,
			ParentHash: parentHeader.ParentHash,
		}
	}

	if commonAncestor < 0 {
		commonAncestor = blockNum - 1
	}

	reorgDepth := len(reorgedBlocks)

	// Prune reorged blocks
	newChain := make([]LightBlock, 0)
	for _, b := range w.unfinalizedBlocks {
		if b.Number <= commonAncestor {
			newChain = append(newChain, b)
		}
	}
	w.unfinalizedBlocks = newChain

	// Record reorg event
	if reorgDepth > 0 && len(reorgedBlocks) > 0 {
		reorgEvent := outbound.ReorgEvent{
			DetectedAt:  receivedAt,
			BlockNumber: blockNum,
			OldHash:     reorgedBlocks[len(reorgedBlocks)-1].Hash,
			NewHash:     header.Hash,
			Depth:       reorgDepth,
		}
		if err := w.stateRepo.SaveReorgEvent(ctx, reorgEvent); err != nil {
			w.logger.Warn("failed to save reorg event", "error", err)
		}
		if err := w.stateRepo.MarkBlocksOrphanedAfter(ctx, commonAncestor); err != nil {
			w.logger.Warn("failed to mark orphaned blocks", "error", err)
		}
	}

	return true, reorgDepth, commonAncestor, nil
}

// addToUnfinalizedChain adds a block to the chain in sorted order, skipping duplicates.
// Caller must hold chainMu lock.
func (w *WatcherService) addToUnfinalizedChain(block LightBlock) {
	// Check for duplicate by hash
	for _, b := range w.unfinalizedBlocks {
		if b.Hash == block.Hash {
			return // Already exists
		}
	}

	// Find insertion point to maintain sorted order by block number
	insertIdx := len(w.unfinalizedBlocks)
	for i, b := range w.unfinalizedBlocks {
		if b.Number > block.Number {
			insertIdx = i
			break
		} else if b.Number == block.Number {
			// Same block number but different hash - this is a fork/reorg scenario
			// Keep both for now, reorg handling will clean up
			insertIdx = i + 1
			break
		}
	}

	// Insert at the correct position
	w.unfinalizedBlocks = append(w.unfinalizedBlocks, LightBlock{})
	copy(w.unfinalizedBlocks[insertIdx+1:], w.unfinalizedBlocks[insertIdx:])
	w.unfinalizedBlocks[insertIdx] = block

	// Trim if too long
	if len(w.unfinalizedBlocks) > w.config.MaxUnfinalizedBlocks {
		w.unfinalizedBlocks = w.unfinalizedBlocks[1:]
	}
}

// updateFinalizedBlock updates the finalized block pointer.
func (w *WatcherService) updateFinalizedBlock(currentBlockNum int64) {
	finalizedNum := currentBlockNum - int64(w.config.FinalityBlockCount)
	if finalizedNum <= 0 {
		return
	}

	for i := range w.unfinalizedBlocks {
		if w.unfinalizedBlocks[i].Number == finalizedNum {
			w.finalizedBlock = &w.unfinalizedBlocks[i]
			break
		}
	}

	// Remove blocks before finality buffer
	cutoff := finalizedNum - int64(w.config.FinalityBlockCount/2)
	if cutoff > 0 {
		newChain := make([]LightBlock, 0)
		for _, b := range w.unfinalizedBlocks {
			if b.Number >= cutoff {
				newChain = append(newChain, b)
			}
		}
		w.unfinalizedBlocks = newChain
	}
}

// restoreChainFromDB restores the in-memory chain from the database.
func (w *WatcherService) restoreChainFromDB() {
	recentBlocks, err := w.stateRepo.GetRecentBlocks(w.ctx, w.config.MaxUnfinalizedBlocks)
	if err != nil {
		w.logger.Warn("failed to restore chain from DB", "error", err)
		return
	}

	if len(recentBlocks) == 0 {
		return
	}

	w.chainMu.Lock()
	defer w.chainMu.Unlock()

	w.unfinalizedBlocks = make([]LightBlock, 0, len(recentBlocks))
	for i := len(recentBlocks) - 1; i >= 0; i-- {
		b := recentBlocks[i]
		w.unfinalizedBlocks = append(w.unfinalizedBlocks, LightBlock{
			Number:     b.Number,
			Hash:       b.Hash,
			ParentHash: b.ParentHash,
		})
	}

	if len(w.unfinalizedBlocks) > 0 {
		tip := w.unfinalizedBlocks[len(w.unfinalizedBlocks)-1]
		finalizedNum := tip.Number - int64(w.config.FinalityBlockCount)
		for i := range w.unfinalizedBlocks {
			if w.unfinalizedBlocks[i].Number <= finalizedNum {
				w.finalizedBlock = &w.unfinalizedBlocks[i]
			}
		}
	}

	w.logger.Info("restored chain from DB", "blockCount", len(w.unfinalizedBlocks))
}

// queueBackfill determines missed blocks and queues them for background processing.
func (w *WatcherService) queueBackfill() {
	w.restoreChainFromDB()

	lastBlock, err := w.stateRepo.GetLastBlock(w.ctx)
	if err != nil || lastBlock == nil {
		return
	}

	currentBlockNum, err := w.client.GetCurrentBlockNumber(w.ctx)
	if err != nil {
		w.logger.Warn("failed to get current block number", "error", err)
		return
	}

	missedCount := currentBlockNum - lastBlock.Number
	if missedCount <= 0 {
		return
	}

	w.logger.Info("queuing backfill", "from", lastBlock.Number+1, "to", currentBlockNum, "count", missedCount)

	// Queue the backfill request - non-blocking
	select {
	case w.backfillCh <- backfillRequest{from: lastBlock.Number + 1, to: currentBlockNum}:
	default:
		w.logger.Warn("backfill queue full, skipping", "from", lastBlock.Number+1, "to", currentBlockNum)
	}
}

// backfillWorker processes backfill requests in the background using batched RPC calls.
func (w *WatcherService) backfillWorker() {
	defer w.backfillWg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		case req, ok := <-w.backfillCh:
			if !ok {
				return
			}
			w.processBackfillRequest(req)
		}
	}
}

// processBackfillRequest processes a single backfill request using batched RPC calls.
func (w *WatcherService) processBackfillRequest(req backfillRequest) {
	batchSize := w.config.BackfillBatchSize
	total := req.to - req.from + 1

	w.logger.Info("starting backfill", "from", req.from, "to", req.to, "total", total, "batchSize", batchSize)

	for batchStart := req.from; batchStart <= req.to; batchStart += int64(batchSize) {
		// Check if context is cancelled
		select {
		case <-w.ctx.Done():
			w.logger.Info("backfill cancelled")
			return
		default:
		}

		batchEnd := batchStart + int64(batchSize) - 1
		if batchEnd > req.to {
			batchEnd = req.to
		}

		// Build block numbers for this batch
		blockNums := make([]int64, 0, batchEnd-batchStart+1)
		for blockNum := batchStart; blockNum <= batchEnd; blockNum++ {
			blockNums = append(blockNums, blockNum)
		}

		// Fetch all data for this batch in a single RPC call
		batchFetchStart := time.Now()
		blockDataList, err := w.client.GetBlocksBatch(w.ctx, blockNums, true)
		batchFetchDuration := time.Since(batchFetchStart)
		if err != nil {
			w.logger.Warn("failed to fetch batch", "from", batchStart, "to", batchEnd, "error", err)
			continue
		}
		w.logger.Debug("backfill batch fetched", "from", batchStart, "to", batchEnd, "fetchMs", batchFetchDuration.Milliseconds())

		// Process each block in the batch
		for _, bd := range blockDataList {
			blockStart := time.Now()

			if bd.Block == nil {
				w.logger.Warn("missing block data in batch", "block", bd.BlockNumber)
				continue
			}

			var header outbound.BlockHeader
			if err := parseBlockHeader(bd.Block, &header); err != nil {
				w.logger.Warn("failed to parse block header", "block", bd.BlockNumber, "error", err)
				continue
			}

			// Update chain state (reorg detection) - skip if already processed by live
			processed, err := w.processBlockHeader(header, time.Now(), true)
			if err != nil {
				w.logger.Warn("failed to process backfill block header", "block", bd.BlockNumber, "error", err)
				continue
			}
			if !processed {
				w.logger.Debug("backfill block already processed by live, skipping", "block", bd.BlockNumber)
				continue
			}

			// Cache and publish the pre-fetched data
			w.cacheAndPublishBatchData(bd, header, time.Now())

			w.logger.Debug("backfill block complete", "block", bd.BlockNumber, "durationMs", time.Since(blockStart).Milliseconds())
		}

		w.logger.Debug("backfill batch complete", "from", batchStart, "to", batchEnd, "totalMs", time.Since(batchFetchStart).Milliseconds())
	}

	w.logger.Info("backfill complete", "from", req.from, "to", req.to, "total", total)
}

// processBlockHeader processes a block header for chain state (without fetching data).
// Returns true if the block was processed, false if it was a duplicate.
// Backfill does NOT do reorg detection since it's just filling gaps.
func (w *WatcherService) processBlockHeader(header outbound.BlockHeader, receivedAt time.Time, isBackfill bool) (bool, error) {
	blockNum, err := parseBlockNumber(header.Number)
	if err != nil {
		return false, fmt.Errorf("invalid block number: %w", err)
	}

	// Atomically check duplicate and reserve slot in chain
	block := LightBlock{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
	}
	if !w.tryAddBlock(block) {
		return false, nil // Duplicate - already processed by live or earlier backfill
	}

	// Save block state to database
	state := outbound.BlockState{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
		ReceivedAt: receivedAt.Unix(),
	}
	if err := w.stateRepo.SaveBlock(w.ctx, state); err != nil {
		return false, fmt.Errorf("failed to save block state: %w", err)
	}

	// Update finalized block pointer
	w.chainMu.Lock()
	w.updateFinalizedBlock(blockNum)
	w.chainMu.Unlock()

	return true, nil
}

// cacheAndPublishBatchData caches pre-fetched data and publishes events.
func (w *WatcherService) cacheAndPublishBatchData(bd outbound.BlockData, header outbound.BlockHeader, receivedAt time.Time) {
	chainID := w.config.ChainID
	blockNum := bd.BlockNumber
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, _ := parseBlockNumber(header.Timestamp)

	var wg sync.WaitGroup

	// Cache and publish block
	if bd.Block != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.cache.SetBlock(w.ctx, chainID, blockNum, bd.Block); err != nil {
				w.logger.Warn("failed to cache block", "block", blockNum, "error", err)
				return
			}
			event := outbound.BlockEvent{
				ChainID:        chainID,
				BlockNumber:    blockNum,
				BlockHash:      blockHash,
				ParentHash:     parentHash,
				BlockTimestamp: blockTimestamp,
				ReceivedAt:     receivedAt,
				CacheKey:       cacheKey(chainID, blockNum, "block"),
				IsBackfill:     true,
			}
			if err := w.eventSink.Publish(w.ctx, event); err != nil {
				w.logger.Warn("failed to publish block event", "block", blockNum, "error", err)
			}
		}()
	}

	// Cache and publish receipts
	if bd.Receipts != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.cache.SetReceipts(w.ctx, chainID, blockNum, bd.Receipts); err != nil {
				w.logger.Warn("failed to cache receipts", "block", blockNum, "error", err)
				return
			}
			event := outbound.ReceiptsEvent{
				ChainID:     chainID,
				BlockNumber: blockNum,
				BlockHash:   blockHash,
				ReceivedAt:  receivedAt,
				CacheKey:    cacheKey(chainID, blockNum, "receipts"),
				IsBackfill:  true,
			}
			if err := w.eventSink.Publish(w.ctx, event); err != nil {
				w.logger.Warn("failed to publish receipts event", "block", blockNum, "error", err)
			}
		}()
	}

	// Cache and publish traces
	if bd.Traces != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.cache.SetTraces(w.ctx, chainID, blockNum, bd.Traces); err != nil {
				w.logger.Warn("failed to cache traces", "block", blockNum, "error", err)
				return
			}
			event := outbound.TracesEvent{
				ChainID:     chainID,
				BlockNumber: blockNum,
				BlockHash:   blockHash,
				ReceivedAt:  receivedAt,
				CacheKey:    cacheKey(chainID, blockNum, "traces"),
				IsBackfill:  true,
			}
			if err := w.eventSink.Publish(w.ctx, event); err != nil {
				w.logger.Warn("failed to publish traces event", "block", blockNum, "error", err)
			}
		}()
	}

	// Cache and publish blobs
	if bd.Blobs != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.cache.SetBlobs(w.ctx, chainID, blockNum, bd.Blobs); err != nil {
				w.logger.Warn("failed to cache blobs", "block", blockNum, "error", err)
				return
			}
			event := outbound.BlobsEvent{
				ChainID:     chainID,
				BlockNumber: blockNum,
				BlockHash:   blockHash,
				ReceivedAt:  receivedAt,
				CacheKey:    cacheKey(chainID, blockNum, "blobs"),
				IsBackfill:  true,
			}
			if err := w.eventSink.Publish(w.ctx, event); err != nil {
				w.logger.Warn("failed to publish blobs event", "block", blockNum, "error", err)
			}
		}()
	}

	wg.Wait()
}

// fetchAndPublishBlockData fetches all data types concurrently and publishes events.
func (w *WatcherService) fetchAndPublishBlockData(header outbound.BlockHeader, blockNum int64, receivedAt time.Time, isReorg, isBackfill bool) {
	chainID := w.config.ChainID
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, _ := parseBlockNumber(header.Timestamp)

	var wg sync.WaitGroup
	wg.Add(4)

	// Fetch and publish block
	go func() {
		defer wg.Done()
		w.fetchCacheAndPublishBlock(chainID, blockNum, blockHash, parentHash, blockTimestamp, receivedAt, isReorg, isBackfill)
	}()

	// Fetch and publish receipts
	go func() {
		defer wg.Done()
		w.fetchCacheAndPublishReceipts(chainID, blockNum, blockHash, receivedAt, isReorg, isBackfill)
	}()

	// Fetch and publish traces
	go func() {
		defer wg.Done()
		w.fetchCacheAndPublishTraces(chainID, blockNum, blockHash, receivedAt, isReorg, isBackfill)
	}()

	// Fetch and publish blobs
	go func() {
		defer wg.Done()
		w.fetchCacheAndPublishBlobs(chainID, blockNum, blockHash, receivedAt, isReorg, isBackfill)
	}()

	wg.Wait()
}

func (w *WatcherService) fetchCacheAndPublishBlock(chainID, blockNum int64, blockHash, parentHash string, blockTimestamp int64, receivedAt time.Time, isReorg, isBackfill bool) {
	ctx := w.ctx

	data, err := w.client.GetBlockByNumber(ctx, blockNum, true)
	if err != nil {
		w.logger.Warn("failed to fetch block", "block", blockNum, "error", err)
		return
	}

	if err := w.cache.SetBlock(ctx, chainID, blockNum, data); err != nil {
		w.logger.Warn("failed to cache block", "block", blockNum, "error", err)
		return
	}

	event := outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNum,
		BlockHash:      blockHash,
		ParentHash:     parentHash,
		BlockTimestamp: blockTimestamp,
		ReceivedAt:     receivedAt,
		CacheKey:       cacheKey(chainID, blockNum, "block"),
		IsReorg:        isReorg,
		IsBackfill:     isBackfill,
	}
	if err := w.eventSink.Publish(ctx, event); err != nil {
		w.logger.Warn("failed to publish block event", "block", blockNum, "error", err)
	}
}

func (w *WatcherService) fetchCacheAndPublishReceipts(chainID, blockNum int64, blockHash string, receivedAt time.Time, isReorg, isBackfill bool) {
	ctx := w.ctx

	data, err := w.client.GetBlockReceipts(ctx, blockNum)
	if err != nil {
		w.logger.Warn("failed to fetch receipts", "block", blockNum, "error", err)
		return
	}

	if err := w.cache.SetReceipts(ctx, chainID, blockNum, data); err != nil {
		w.logger.Warn("failed to cache receipts", "block", blockNum, "error", err)
		return
	}

	event := outbound.ReceiptsEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		CacheKey:    cacheKey(chainID, blockNum, "receipts"),
		IsReorg:     isReorg,
		IsBackfill:  isBackfill,
	}
	if err := w.eventSink.Publish(ctx, event); err != nil {
		w.logger.Warn("failed to publish receipts event", "block", blockNum, "error", err)
	}
}

func (w *WatcherService) fetchCacheAndPublishTraces(chainID, blockNum int64, blockHash string, receivedAt time.Time, isReorg, isBackfill bool) {
	ctx := w.ctx

	data, err := w.client.GetBlockTraces(ctx, blockNum)
	if err != nil {
		w.logger.Warn("failed to fetch traces", "block", blockNum, "error", err)
		return
	}

	if err := w.cache.SetTraces(ctx, chainID, blockNum, data); err != nil {
		w.logger.Warn("failed to cache traces", "block", blockNum, "error", err)
		return
	}

	event := outbound.TracesEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		CacheKey:    cacheKey(chainID, blockNum, "traces"),
		IsReorg:     isReorg,
		IsBackfill:  isBackfill,
	}
	if err := w.eventSink.Publish(ctx, event); err != nil {
		w.logger.Warn("failed to publish traces event", "block", blockNum, "error", err)
	}
}

func (w *WatcherService) fetchCacheAndPublishBlobs(chainID, blockNum int64, blockHash string, receivedAt time.Time, isReorg, isBackfill bool) {
	ctx := w.ctx

	data, err := w.client.GetBlobSidecars(ctx, blockNum)
	if err != nil {
		w.logger.Warn("failed to fetch blobs", "block", blockNum, "error", err)
		return
	}

	if err := w.cache.SetBlobs(ctx, chainID, blockNum, data); err != nil {
		w.logger.Warn("failed to cache blobs", "block", blockNum, "error", err)
		return
	}

	event := outbound.BlobsEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		CacheKey:    cacheKey(chainID, blockNum, "blobs"),
		IsReorg:     isReorg,
		IsBackfill:  isBackfill,
	}
	if err := w.eventSink.Publish(ctx, event); err != nil {
		w.logger.Warn("failed to publish blobs event", "block", blockNum, "error", err)
	}
}

func parseBlockNumber(hexNum string) (int64, error) {
	hexNum = strings.TrimPrefix(hexNum, "0x")
	return strconv.ParseInt(hexNum, 16, 64)
}

func parseBlockHeader(data []byte, header *outbound.BlockHeader) error {
	return json.Unmarshal(data, header)
}

// cacheKey generates the cache key for a given data type.
// Format: {chainID}:{blockNumber}:{dataType}
func cacheKey(chainID, blockNumber int64, dataType string) string {
	return fmt.Sprintf("%d:%d:%s", chainID, blockNumber, dataType)
}
