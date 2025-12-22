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

	// Logger is the structured logger.
	Logger *slog.Logger
}

// WatcherConfigDefaults returns default configuration.
func WatcherConfigDefaults() WatcherConfig {
	return WatcherConfig{
		ChainID:              1,
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 128,
		BlockRetention:       1000,
		Logger:               slog.Default(),
	}
}

// LightBlock represents a minimal block for chain tracking.
type LightBlock struct {
	Number     int64
	Hash       string
	ParentHash string
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
		logger:     config.Logger.With("component", "watcher-service"),
	}, nil
}

// Start begins watching for new blocks.
func (w *WatcherService) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)

	// Restore chain state from DB
	w.restoreChainFromDB()

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
	return w.subscriber.Unsubscribe()
}

// OnReconnect handles reconnection by backfilling missed blocks.
// This should be called by the subscriber's reconnect callback.
func (w *WatcherService) OnReconnect() {
	w.backfillMissedBlocks()
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
				w.logger.Warn("failed to process block", "error", err)
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

	// Check for duplicate
	if w.isDuplicate(header.Hash, blockNum) {
		w.logger.Debug("duplicate block, skipping", "block", blockNum)
		return nil
	}

	// Detect reorg
	isReorg, reorgDepth, commonAncestor, err := w.detectReorg(header, blockNum, receivedAt)
	if err != nil {
		return fmt.Errorf("reorg detection failed: %w", err)
	}
	if isReorg {
		w.logger.Warn("reorg detected", "block", blockNum, "depth", reorgDepth, "commonAncestor", commonAncestor)
	}

	// Save block state
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

	// Update in-memory chain
	w.chainMu.Lock()
	w.addToUnfinalizedChain(LightBlock{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
	})
	w.updateFinalizedBlock(blockNum)
	w.chainMu.Unlock()

	// Prune old blocks periodically
	if blockNum%100 == 0 {
		pruneThreshold := blockNum - int64(w.config.BlockRetention)
		if pruneThreshold > 0 {
			if err := w.stateRepo.PruneOldBlocks(ctx, pruneThreshold); err != nil {
				w.logger.Warn("failed to prune old blocks", "error", err)
			}
		}
	}

	// Fetch all data types concurrently, cache, and publish events
	w.fetchAndPublishBlockData(header, blockNum, receivedAt, isReorg, isBackfill)

	return nil
}

// isDuplicate checks if we've already processed this block.
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

// detectReorg detects chain reorganizations using Ponder-style parent hash chain validation.
func (w *WatcherService) detectReorg(header outbound.BlockHeader, blockNum int64, receivedAt time.Time) (bool, int, int64, error) {
	w.chainMu.Lock()
	defer w.chainMu.Unlock()

	if len(w.unfinalizedBlocks) == 0 {
		return false, 0, 0, nil
	}

	latestBlock := w.unfinalizedBlocks[len(w.unfinalizedBlocks)-1]

	// Block number decreased - definite reorg
	if blockNum <= latestBlock.Number {
		return w.handleReorg(header, blockNum, receivedAt)
	}

	// Block is exactly one ahead - check parent hash
	if blockNum == latestBlock.Number+1 {
		if header.ParentHash == latestBlock.Hash {
			return false, 0, 0, nil
		}
		// Parent hash mismatch - reorg
		return w.handleReorg(header, blockNum, receivedAt)
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
			return false, 0, 0, fmt.Errorf("unrecoverable reorg: beyond finalized block %d", w.finalizedBlock.Number)
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

// addToUnfinalizedChain adds a block to the chain.
func (w *WatcherService) addToUnfinalizedChain(block LightBlock) {
	w.unfinalizedBlocks = append(w.unfinalizedBlocks, block)
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

// backfillMissedBlocks fetches blocks missed during disconnection.
func (w *WatcherService) backfillMissedBlocks() {
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

	w.logger.Info("backfilling missed blocks", "from", lastBlock.Number+1, "to", currentBlockNum)

	for blockNum := lastBlock.Number + 1; blockNum <= currentBlockNum; blockNum++ {
		blockData, err := w.client.GetBlockByNumber(w.ctx, blockNum, false)
		if err != nil {
			w.logger.Warn("failed to fetch block for backfill", "block", blockNum, "error", err)
			continue
		}

		var header outbound.BlockHeader
		if err := parseBlockHeader(blockData, &header); err != nil {
			w.logger.Warn("failed to parse block header", "block", blockNum, "error", err)
			continue
		}

		if err := w.processBlock(header, time.Now(), true); err != nil {
			w.logger.Warn("failed to process backfill block", "block", blockNum, "error", err)
		}
	}

	w.logger.Info("backfill complete", "count", missedCount)
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
