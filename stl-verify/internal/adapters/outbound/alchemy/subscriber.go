// Package alchemy provides an adapter for Alchemy's WebSocket API.
package alchemy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Subscriber implements outbound.BlockSubscriber
var _ outbound.BlockSubscriber = (*Subscriber)(nil)

// NewSubscriber creates a new Alchemy WebSocket subscriber with automatic reconnection.
// Returns an error if required configuration is missing (BlockStateRepo, WebSocketURL, HTTPURL).
func NewSubscriber(config Config) (*Subscriber, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	config.applyDefaults()
	return &Subscriber{
		config:  config,
		done:    make(chan struct{}),
		headers: make(chan outbound.BlockHeader, config.ChannelBufferSize),
	}, nil
}

// Subscribe starts listening for new block headers via Alchemy's eth_newHeads subscription.
// The subscription automatically reconnects if the connection is lost.
func (s *Subscriber) Subscribe(ctx context.Context) (<-chan outbound.BlockHeader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("subscriber is closed")
	}

	// Create a cancellable context for the subscription
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Start the connection manager goroutine
	go s.connectionManager()

	return s.headers, nil
}

// connectionManager manages the WebSocket connection with automatic reconnection.
func (s *Subscriber) connectionManager() {
	backoff := s.config.InitialBackoff
	logger := s.config.Logger.With("component", "alchemy-subscriber")
	isFirstConnect := true

	for {
		select {
		case <-s.done:
			return
		case <-s.ctx.Done():
			return
		default:
		}

		err := s.connectAndSubscribe()
		if err != nil {
			logger.Warn("failed to connect", "error", err, "backoff", backoff)

			select {
			case <-s.done:
				return
			case <-s.ctx.Done():
				return
			case <-time.After(backoff):
			}

			// Increase backoff for next attempt
			backoff = time.Duration(float64(backoff) * s.config.BackoffFactor)
			if backoff > s.config.MaxBackoff {
				backoff = s.config.MaxBackoff
			}
			continue
		}

		// Reset backoff on successful connection
		backoff = s.config.InitialBackoff
		logger.Info("connected to Alchemy WebSocket")

		// Restore chain state and backfill missed blocks
		if isFirstConnect {
			// On first connect, just restore chain state from DB
			s.restoreChainFromDB(logger)
		} else {
			// On reconnect, restore and backfill missed blocks
			s.backfillMissedBlocks(logger)
		}
		isFirstConnect = false

		// Run the read loop until disconnection
		s.readLoop(logger)

		logger.Warn("WebSocket connection was killed by Alchemy, reconnecting...")
	}
}

// connectAndSubscribe establishes the WebSocket connection and subscribes to newHeads.
func (s *Subscriber) connectAndSubscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Connect to WebSocket
	conn, _, err := websocket.DefaultDialer.DialContext(s.ctx, s.config.WebSocketURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Alchemy WebSocket: %w", err)
	}

	// Fix #4: Set initial read deadline to prevent goroutine leaks
	if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
		conn.Close()
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Set up pong handler for connection health monitoring
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
	})

	s.conn = conn

	// Send eth_subscribe request for newHeads
	subscribeReq := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_subscribe",
		Params:  []interface{}{"newHeads"},
	}

	if err := conn.WriteJSON(subscribeReq); err != nil {
		conn.Close()
		s.conn = nil
		return fmt.Errorf("failed to send subscription request: %w", err)
	}

	// Read subscription confirmation
	var response jsonRPCResponse
	if err := conn.ReadJSON(&response); err != nil {
		conn.Close()
		s.conn = nil
		return fmt.Errorf("failed to read subscription response: %w", err)
	}

	if response.Error != nil {
		conn.Close()
		s.conn = nil
		return fmt.Errorf("subscription failed: %s", response.Error.Message)
	}

	return nil
}

// readLoop continuously reads block headers from the WebSocket connection.
// It also sends periodic pings to keep the connection alive.
func (s *Subscriber) readLoop(logger *slog.Logger) {
	pingTicker := time.NewTicker(s.config.PingInterval)
	defer pingTicker.Stop()

	// Channel to signal read errors
	readErr := make(chan error, 1)
	blockChan := make(chan outbound.BlockHeader, 10)

	go func() {
		for {
			s.mu.RLock()
			conn := s.conn
			s.mu.RUnlock()

			if conn == nil {
				readErr <- errors.New("connection is nil")
				return
			}

			// Fix #4: Extend read deadline on each read attempt
			if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
				readErr <- fmt.Errorf("failed to set read deadline: %w", err)
				return
			}

			var response jsonRPCResponse
			if err := conn.ReadJSON(&response); err != nil {
				readErr <- err
				return
			}

			// Record the time we received the message from the WebSocket
			receivedAt := time.Now()

			// Check if this is a subscription notification
			if response.Method == "eth_subscription" && response.Params != nil {
				var params subscriptionParams
				if err := json.Unmarshal(response.Params, &params); err != nil {
					logger.Warn("failed to parse subscription params", "error", err)
					continue
				}

				// Process block through deduplication and reorg detection
				header, err := s.processBlock(params.Result, receivedAt, logger)
				if err != nil {
					logger.Warn("failed to process block", "error", err)
					continue
				}
				if header == nil {
					// Block was deduplicated
					continue
				}

				select {
				case blockChan <- *header:
				case <-s.done:
					return
				case <-s.ctx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case <-s.done:
			s.closeConnection()
			return
		case <-s.ctx.Done():
			s.closeConnection()
			return
		case err := <-readErr:
			logger.Warn("read error", "error", err)
			s.closeConnection()
			return
		case header := <-blockChan:
			// Forward to consumer channel with backpressure handling
			select {
			case s.headers <- header:
				// Fix #7: Update last block time for health checks
				s.lastBlockTime.Store(time.Now().Unix())

				blockNum, _ := parseBlockNumber(header.Number)
				logger.Debug("block header forwarded",
					"block", blockNum,
					"hash", truncateHash(header.Hash),
					"isReorg", header.IsReorg,
					"isBackfill", header.IsBackfill,
				)
			default:
				// Channel full - log warning but don't block
				blockNum, _ := parseBlockNumber(header.Number)
				logger.Warn("block header channel full, dropping block",
					"block", blockNum,
					"hash", truncateHash(header.Hash),
				)
			}
		case <-pingTicker.C:
			s.mu.RLock()
			conn := s.conn
			s.mu.RUnlock()

			if conn != nil {
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.config.PongTimeout)); err != nil {
					logger.Warn("ping failed", "error", err)
					s.closeConnection()
					return
				}
			}
		}
	}
}

// processBlock handles deduplication and reorg detection for a received block.
// Uses Ponder-style parent hash chain validation for accurate reorg detection.
// Returns nil if the block should be skipped (duplicate).
func (s *Subscriber) processBlock(header outbound.BlockHeader, receivedAt time.Time, logger *slog.Logger) (*outbound.BlockHeader, error) {
	blockNum, err := parseBlockNumber(header.Number)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block number: %w", err)
	}

	ctx := s.ctx

	// Check for duplicate by hash (both in-memory and DB)
	s.chainMu.RLock()
	latestBlock := s.getLatestUnfinalizedBlock()
	s.chainMu.RUnlock()

	if latestBlock != nil && latestBlock.Hash == header.Hash {
		logger.Debug("duplicate block received (in-memory), skipping", "block", blockNum, "hash", truncateHash(header.Hash))
		return nil, nil
	}

	existingByHash, err := s.config.BlockStateRepo.GetBlockByHash(ctx, header.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to check for duplicate: %w", err)
	}
	if existingByHash != nil {
		logger.Debug("duplicate block received (DB), skipping", "block", blockNum, "hash", truncateHash(header.Hash))
		return nil, nil
	}

	// Ponder-style reorg detection using parent hash chain validation
	reorgDetected, reorgDepth, commonAncestor, err := s.detectAndHandleReorg(header, blockNum, receivedAt, logger)
	if err != nil {
		return nil, fmt.Errorf("reorg detection failed: %w", err)
	}
	if reorgDetected {
		header.IsReorg = true
		logger.Warn("chain reorganization detected",
			"block", blockNum,
			"newHash", truncateHash(header.Hash),
			"depth", reorgDepth,
			"commonAncestor", commonAncestor,
		)
	}

	// Save block state to DB
	state := outbound.BlockState{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
		ReceivedAt: receivedAt.Unix(),
		IsOrphaned: false,
	}
	if err := s.config.BlockStateRepo.SaveBlock(ctx, state); err != nil {
		return nil, fmt.Errorf("failed to save block state: %w", err)
	}

	// Add to in-memory unfinalized chain
	s.chainMu.Lock()
	s.addToUnfinalizedChain(LightBlock{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
	})
	s.updateFinalizedBlock(blockNum, logger)
	s.chainMu.Unlock()

	// Prune old blocks periodically
	pruneThreshold := blockNum - int64(s.config.BlockRetention)
	if pruneThreshold > 0 && blockNum%100 == 0 {
		if err := s.config.BlockStateRepo.PruneOldBlocks(ctx, pruneThreshold); err != nil {
			logger.Warn("failed to prune old blocks", "error", err)
		}
	}

	// Fetch all data types concurrently, cache, and publish events
	s.fetchAndPublishBlockData(ctx, header, blockNum, receivedAt, logger)

	return &header, nil
}

// detectAndHandleReorg performs Ponder-style reorg detection using parent hash chain validation.
// Returns: reorgDetected, reorgDepth, commonAncestorBlockNumber, error
func (s *Subscriber) detectAndHandleReorg(header outbound.BlockHeader, blockNum int64, receivedAt time.Time, logger *slog.Logger) (bool, int, int64, error) {
	s.chainMu.Lock()
	defer s.chainMu.Unlock()

	latestBlock := s.getLatestUnfinalizedBlock()
	if latestBlock == nil {
		// No blocks in chain yet, no reorg possible
		return false, 0, 0, nil
	}

	// Case 1: Block number decreased or stayed same - definite reorg
	if blockNum <= latestBlock.Number {
		return s.handleReorg(header, blockNum, receivedAt, logger)
	}

	// Case 2: Block is exactly one ahead - check parent hash chain
	if blockNum == latestBlock.Number+1 {
		if header.ParentHash == latestBlock.Hash {
			// Happy path: block extends our chain
			return false, 0, 0, nil
		}
		// Parent hash mismatch - reorg even though block number increased!
		logger.Debug("parent hash mismatch detected",
			"expectedParent", truncateHash(latestBlock.Hash),
			"actualParent", truncateHash(header.ParentHash),
		)
		return s.handleReorg(header, blockNum, receivedAt, logger)
	}

	// Case 3: Gap in blocks - we missed some, will be backfilled
	// For now, just add to chain without reorg detection
	// Backfill will validate the chain
	return false, 0, 0, nil
}

// handleReorg processes a detected reorg by finding the common ancestor.
// Must be called with chainMu held.
func (s *Subscriber) handleReorg(header outbound.BlockHeader, blockNum int64, receivedAt time.Time, logger *slog.Logger) (bool, int, int64, error) {
	ctx := s.ctx

	// Find blocks that will be reorged out
	reorgedBlocks := make([]LightBlock, 0)
	for i := len(s.unfinalizedBlocks) - 1; i >= 0; i-- {
		block := s.unfinalizedBlocks[i]
		if block.Number >= blockNum {
			reorgedBlocks = append(reorgedBlocks, block)
		}
	}

	// Walk back through remote chain to find common ancestor
	remoteBlock := LightBlock{
		Number:     blockNum,
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
	}

	var commonAncestor int64 = -1
	maxWalkBack := s.config.FinalityBlockCount

	for walkCount := 0; walkCount < maxWalkBack; walkCount++ {
		// Check if remote block's parent matches any of our chain
		parentBlock := s.findBlockByHash(remoteBlock.ParentHash)
		if parentBlock != nil {
			commonAncestor = parentBlock.Number
			break
		}

		// Check if we've gone past our finalized block (unrecoverable reorg)
		if s.finalizedBlock != nil && remoteBlock.Number <= s.finalizedBlock.Number {
			return false, 0, 0, fmt.Errorf("unrecoverable reorg: extends beyond finalized block %d", s.finalizedBlock.Number)
		}

		// Fetch parent block from network to continue walking back
		parentHeader, err := s.getBlockByHash(ctx, remoteBlock.ParentHash)
		if err != nil {
			logger.Warn("failed to fetch parent block during reorg walk", "hash", truncateHash(remoteBlock.ParentHash), "error", err)
			// Fall back to DB-based detection
			break
		}

		parentNum, _ := parseBlockNumber(parentHeader.Number)
		remoteBlock = LightBlock{
			Number:     parentNum,
			Hash:       parentHeader.Hash,
			ParentHash: parentHeader.ParentHash,
		}

		// Add to reorged blocks if it replaces one of ours
		for i := len(s.unfinalizedBlocks) - 1; i >= 0; i-- {
			if s.unfinalizedBlocks[i].Number == parentNum {
				reorgedBlocks = append(reorgedBlocks, s.unfinalizedBlocks[i])
				break
			}
		}
	}

	// If we couldn't find common ancestor via walk, use the incoming block's number - 1
	if commonAncestor < 0 {
		commonAncestor = blockNum - 1
	}

	reorgDepth := len(reorgedBlocks)

	// Prune reorged blocks from our chain
	newChain := make([]LightBlock, 0, len(s.unfinalizedBlocks))
	for _, block := range s.unfinalizedBlocks {
		if block.Number <= commonAncestor {
			newChain = append(newChain, block)
		}
	}
	s.unfinalizedBlocks = newChain

	// Mark orphaned blocks in DB and save reorg event
	if reorgDepth > 0 {
		// Save reorg event for historical tracking
		oldestReorged := reorgedBlocks[len(reorgedBlocks)-1]
		reorgEvent := outbound.ReorgEvent{
			DetectedAt:  receivedAt,
			BlockNumber: blockNum,
			OldHash:     oldestReorged.Hash,
			NewHash:     header.Hash,
			Depth:       reorgDepth,
		}
		if err := s.config.BlockStateRepo.SaveReorgEvent(ctx, reorgEvent); err != nil {
			logger.Warn("failed to save reorg event", "error", err)
		}

		// Mark orphaned blocks in DB
		if err := s.config.BlockStateRepo.MarkBlocksOrphanedAfter(ctx, commonAncestor); err != nil {
			logger.Warn("failed to mark orphaned blocks", "error", err)
		}
	}

	return true, reorgDepth, commonAncestor, nil
}

// getLatestUnfinalizedBlock returns the tip of our unfinalized chain.
// Must be called with chainMu held (read or write).
func (s *Subscriber) getLatestUnfinalizedBlock() *LightBlock {
	if len(s.unfinalizedBlocks) == 0 {
		return nil
	}
	return &s.unfinalizedBlocks[len(s.unfinalizedBlocks)-1]
}

// findBlockByHash searches the unfinalized chain for a block with the given hash.
// Must be called with chainMu held (read or write).
func (s *Subscriber) findBlockByHash(hash string) *LightBlock {
	for i := range s.unfinalizedBlocks {
		if s.unfinalizedBlocks[i].Hash == hash {
			return &s.unfinalizedBlocks[i]
		}
	}
	return nil
}

// addToUnfinalizedChain adds a block to the unfinalized chain.
// Must be called with chainMu held (write).
func (s *Subscriber) addToUnfinalizedChain(block LightBlock) {
	s.unfinalizedBlocks = append(s.unfinalizedBlocks, block)

	// Trim if exceeds max size
	if len(s.unfinalizedBlocks) > s.config.MaxUnfinalizedBlocks {
		trimCount := len(s.unfinalizedBlocks) - s.config.MaxUnfinalizedBlocks
		s.unfinalizedBlocks = s.unfinalizedBlocks[trimCount:]
	}
}

// updateFinalizedBlock updates the finalized block based on the current tip.
// Must be called with chainMu held (write).
func (s *Subscriber) updateFinalizedBlock(currentBlockNum int64, logger *slog.Logger) {
	finalizedNum := currentBlockNum - int64(s.config.FinalityBlockCount)
	if finalizedNum <= 0 {
		return
	}

	// Find and set the finalized block
	for i := range s.unfinalizedBlocks {
		if s.unfinalizedBlocks[i].Number == finalizedNum {
			if s.finalizedBlock == nil || s.finalizedBlock.Number < finalizedNum {
				s.finalizedBlock = &s.unfinalizedBlocks[i]
				logger.Debug("block finalized", "block", finalizedNum, "hash", truncateHash(s.finalizedBlock.Hash))
			}
			break
		}
	}

	// Remove finalized blocks from unfinalized chain (keep some buffer)
	bufferBlocks := s.config.FinalityBlockCount / 2
	cutoff := finalizedNum - int64(bufferBlocks)
	if cutoff > 0 {
		newChain := make([]LightBlock, 0, len(s.unfinalizedBlocks))
		for _, block := range s.unfinalizedBlocks {
			if block.Number >= cutoff {
				newChain = append(newChain, block)
			}
		}
		s.unfinalizedBlocks = newChain
	}
}

// getBlockByHash fetches a block by its hash using HTTP RPC.
func (s *Subscriber) getBlockByHash(ctx context.Context, hash string) (*outbound.BlockHeader, error) {
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockByHash",
		Params:  []interface{}{hash, false},
	}

	resp, err := s.httpRPCCall(ctx, reqBody)
	if err != nil {
		return nil, err
	}

	if resp.Result == nil || string(resp.Result) == "null" {
		return nil, fmt.Errorf("block not found: %s", hash)
	}

	var header outbound.BlockHeader
	if err := json.Unmarshal(resp.Result, &header); err != nil {
		return nil, fmt.Errorf("failed to parse block: %w", err)
	}

	return &header, nil
}

// restoreChainFromDB restores the in-memory unfinalized chain from the database.
// Called on startup/reconnection to resume chain state.
func (s *Subscriber) restoreChainFromDB(logger *slog.Logger) {
	ctx := s.ctx

	// Load recent blocks from DB into memory
	recentBlocks, err := s.config.BlockStateRepo.GetRecentBlocks(ctx, s.config.MaxUnfinalizedBlocks)
	if err != nil {
		logger.Warn("failed to load recent blocks from DB", "error", err)
		return
	}

	if len(recentBlocks) == 0 {
		return
	}

	s.chainMu.Lock()
	defer s.chainMu.Unlock()

	// Clear existing chain
	s.unfinalizedBlocks = make([]LightBlock, 0, len(recentBlocks))

	// Add blocks in ascending order (recentBlocks is descending)
	for i := len(recentBlocks) - 1; i >= 0; i-- {
		block := recentBlocks[i]
		s.unfinalizedBlocks = append(s.unfinalizedBlocks, LightBlock{
			Number:     block.Number,
			Hash:       block.Hash,
			ParentHash: block.ParentHash,
		})
	}

	// Set finalized block
	if len(s.unfinalizedBlocks) > 0 {
		tip := s.unfinalizedBlocks[len(s.unfinalizedBlocks)-1]
		finalizedNum := tip.Number - int64(s.config.FinalityBlockCount)
		for i := range s.unfinalizedBlocks {
			if s.unfinalizedBlocks[i].Number <= finalizedNum {
				s.finalizedBlock = &s.unfinalizedBlocks[i]
			}
		}
	}

	logger.Info("restored chain from DB",
		"blockCount", len(s.unfinalizedBlocks),
		"tip", s.unfinalizedBlocks[len(s.unfinalizedBlocks)-1].Number,
		"finalizedBlock", func() int64 {
			if s.finalizedBlock != nil {
				return s.finalizedBlock.Number
			}
			return -1
		}(),
	)
}

// backfillMissedBlocks fetches any blocks missed during disconnection.
func (s *Subscriber) backfillMissedBlocks(logger *slog.Logger) {
	// First restore chain state from DB
	s.restoreChainFromDB(logger)

	ctx := s.ctx

	// Get last known block
	lastBlock, err := s.config.BlockStateRepo.GetLastBlock(ctx)
	if err != nil {
		logger.Warn("failed to get last block for backfill", "error", err)
		return
	}
	if lastBlock == nil {
		logger.Debug("no previous blocks, skipping backfill")
		return
	}

	// Get current block number from network
	currentBlockNum, err := s.getCurrentBlockNumber(ctx)
	if err != nil {
		logger.Warn("failed to get current block number for backfill", "error", err)
		return
	}

	missedCount := currentBlockNum - lastBlock.Number
	if missedCount <= 0 {
		logger.Debug("no blocks missed during disconnection")
		return
	}

	logger.Info("backfilling missed blocks", "from", lastBlock.Number+1, "to", currentBlockNum, "count", missedCount)

	// Backfill blocks
	for blockNum := lastBlock.Number + 1; blockNum <= currentBlockNum; blockNum++ {
		header, err := s.getBlockByNumber(ctx, blockNum)
		if err != nil {
			logger.Warn("failed to fetch block for backfill", "block", blockNum, "error", err)
			continue
		}

		header.IsBackfill = true

		// Process through deduplication/reorg logic
		processed, err := s.processBlock(*header, time.Now(), logger)
		if err != nil {
			logger.Warn("failed to process backfill block", "block", blockNum, "error", err)
			continue
		}
		if processed == nil {
			continue
		}

		// Send to channel
		select {
		case s.headers <- *processed:
			s.lastBlockTime.Store(time.Now().Unix())
		case <-s.done:
			return
		case <-s.ctx.Done():
			return
		default:
			logger.Warn("channel full during backfill, skipping block", "block", blockNum)
		}
	}

	logger.Info("backfill complete", "count", missedCount)
}

// getCurrentBlockNumber fetches the current block number from the network.
func (s *Subscriber) getCurrentBlockNumber(ctx context.Context) (int64, error) {
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_blockNumber",
		Params:  []interface{}{},
	}

	resp, err := s.httpRPCCall(ctx, reqBody)
	if err != nil {
		return 0, err
	}

	var result string
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return 0, fmt.Errorf("failed to parse block number: %w", err)
	}

	return parseBlockNumber(result)
}

// getBlockByNumber fetches a block by its number.
func (s *Subscriber) getBlockByNumber(ctx context.Context, blockNum int64) (*outbound.BlockHeader, error) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{hexNum, false},
	}

	resp, err := s.httpRPCCall(ctx, reqBody)
	if err != nil {
		return nil, err
	}

	var header outbound.BlockHeader
	if err := json.Unmarshal(resp.Result, &header); err != nil {
		return nil, fmt.Errorf("failed to parse block: %w", err)
	}

	return &header, nil
}

// httpRPCCall makes an HTTP JSON-RPC call to the Alchemy API.
func (s *Subscriber) httpRPCCall(ctx context.Context, req jsonRPCRequest) (*jsonRPCResponse, error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.config.HTTPURL, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer httpResp.Body.Close()

	respBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(respBytes, &rpcResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return &rpcResp, nil
}

// fetchAndPublishBlockData fetches all data types concurrently, writes to cache, and publishes events.
// Each data type is fetched, cached, and published independently.
func (s *Subscriber) fetchAndPublishBlockData(ctx context.Context, header outbound.BlockHeader, blockNum int64, receivedAt time.Time, logger *slog.Logger) {
	if s.config.BlockCache == nil || s.config.EventSink == nil || s.config.HTTPURL == "" {
		return
	}

	chainID := s.config.ChainID
	blockHash := header.Hash
	parentHash := header.ParentHash
	isReorg := header.IsReorg
	isBackfill := header.IsBackfill

	// Parse block timestamp
	blockTimestamp, _ := parseBlockNumber(header.Timestamp)

	var wg sync.WaitGroup
	wg.Add(4)

	// Fetch and publish block (full block with transactions)
	go func() {
		defer wg.Done()
		s.fetchCacheAndPublishBlock(ctx, chainID, blockNum, blockHash, parentHash, blockTimestamp, receivedAt, isReorg, isBackfill, logger)
	}()

	// Fetch and publish receipts
	go func() {
		defer wg.Done()
		s.fetchCacheAndPublishReceipts(ctx, chainID, blockNum, blockHash, receivedAt, isReorg, isBackfill, logger)
	}()

	// Fetch and publish traces
	go func() {
		defer wg.Done()
		s.fetchCacheAndPublishTraces(ctx, chainID, blockNum, blockHash, receivedAt, isReorg, isBackfill, logger)
	}()

	// Fetch and publish blobs
	go func() {
		defer wg.Done()
		s.fetchCacheAndPublishBlobs(ctx, chainID, blockNum, blockHash, receivedAt, isReorg, isBackfill, logger)
	}()

	wg.Wait()
}

// fetchCacheAndPublishBlock fetches full block, caches it, then publishes event.
func (s *Subscriber) fetchCacheAndPublishBlock(ctx context.Context, chainID, blockNum int64, blockHash, parentHash string, blockTimestamp int64, receivedAt time.Time, isReorg, isBackfill bool, logger *slog.Logger) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{hexNum, true}, // true = include full transactions
	}

	resp, err := s.httpRPCCall(ctx, reqBody)
	if err != nil {
		logger.Warn("failed to fetch block", "block", blockNum, "error", err)
		return
	}

	// Write to cache first
	if err := s.config.BlockCache.SetBlock(ctx, chainID, blockNum, resp.Result); err != nil {
		logger.Warn("failed to cache block", "block", blockNum, "error", err)
		return
	}

	// Then publish event
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
	if err := s.config.EventSink.Publish(ctx, event); err != nil {
		logger.Warn("failed to publish block event", "block", blockNum, "error", err)
		return
	}

	logger.Debug("block cached and published", "block", blockNum, "hash", truncateHash(blockHash))
}

// fetchCacheAndPublishReceipts fetches receipts, caches them, then publishes event.
func (s *Subscriber) fetchCacheAndPublishReceipts(ctx context.Context, chainID, blockNum int64, blockHash string, receivedAt time.Time, isReorg, isBackfill bool, logger *slog.Logger) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlockReceipts",
		Params:  []interface{}{hexNum},
	}

	resp, err := s.httpRPCCall(ctx, reqBody)
	if err != nil {
		logger.Warn("failed to fetch receipts", "block", blockNum, "error", err)
		return
	}

	// Write to cache first
	if err := s.config.BlockCache.SetReceipts(ctx, chainID, blockNum, resp.Result); err != nil {
		logger.Warn("failed to cache receipts", "block", blockNum, "error", err)
		return
	}

	// Then publish event
	event := outbound.ReceiptsEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		IsReorg:     isReorg,
		IsBackfill:  isBackfill,
	}
	if err := s.config.EventSink.Publish(ctx, event); err != nil {
		logger.Warn("failed to publish receipts event", "block", blockNum, "error", err)
		return
	}

	logger.Debug("receipts cached and published", "block", blockNum)
}

// fetchCacheAndPublishTraces fetches traces, caches them, then publishes event.
func (s *Subscriber) fetchCacheAndPublishTraces(ctx context.Context, chainID, blockNum int64, blockHash string, receivedAt time.Time, isReorg, isBackfill bool, logger *slog.Logger) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "trace_block",
		Params:  []interface{}{hexNum},
	}

	resp, err := s.httpRPCCall(ctx, reqBody)
	if err != nil {
		logger.Warn("failed to fetch traces", "block", blockNum, "error", err)
		return
	}

	// Write to cache first
	if err := s.config.BlockCache.SetTraces(ctx, chainID, blockNum, resp.Result); err != nil {
		logger.Warn("failed to cache traces", "block", blockNum, "error", err)
		return
	}

	// Then publish event
	event := outbound.TracesEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		IsReorg:     isReorg,
		IsBackfill:  isBackfill,
	}
	if err := s.config.EventSink.Publish(ctx, event); err != nil {
		logger.Warn("failed to publish traces event", "block", blockNum, "error", err)
		return
	}

	logger.Debug("traces cached and published", "block", blockNum)
}

// fetchCacheAndPublishBlobs fetches blob sidecars, caches them, then publishes event.
func (s *Subscriber) fetchCacheAndPublishBlobs(ctx context.Context, chainID, blockNum int64, blockHash string, receivedAt time.Time, isReorg, isBackfill bool, logger *slog.Logger) {
	hexNum := fmt.Sprintf("0x%x", blockNum)
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_getBlobSidecars",
		Params:  []interface{}{hexNum},
	}

	resp, err := s.httpRPCCall(ctx, reqBody)
	if err != nil {
		logger.Warn("failed to fetch blobs", "block", blockNum, "error", err)
		return
	}

	// Write to cache first
	if err := s.config.BlockCache.SetBlobs(ctx, chainID, blockNum, resp.Result); err != nil {
		logger.Warn("failed to cache blobs", "block", blockNum, "error", err)
		return
	}

	// Then publish event
	event := outbound.BlobsEvent{
		ChainID:     chainID,
		BlockNumber: blockNum,
		BlockHash:   blockHash,
		ReceivedAt:  receivedAt,
		IsReorg:     isReorg,
		IsBackfill:  isBackfill,
	}
	if err := s.config.EventSink.Publish(ctx, event); err != nil {
		logger.Warn("failed to publish blobs event", "block", blockNum, "error", err)
		return
	}

	logger.Debug("blobs cached and published", "block", blockNum)
}

// closeConnection safely closes the current WebSocket connection.
func (s *Subscriber) closeConnection() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// Unsubscribe stops the subscription and closes the WebSocket connection.
func (s *Subscriber) Unsubscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.done)

	// Cancel the context to stop the connection manager
	if s.cancel != nil {
		s.cancel()
	}

	// Close the headers channel
	close(s.headers)

	if s.conn != nil {
		// Send unsubscribe request (best effort)
		unsubscribeReq := jsonRPCRequest{
			JSONRPC: "2.0",
			ID:      2,
			Method:  "eth_unsubscribe",
			Params:  []interface{}{},
		}
		_ = s.conn.WriteJSON(unsubscribeReq)

		return s.conn.Close()
	}

	return nil
}

// HealthCheck verifies the WebSocket connection is operational and receiving blocks.
func (s *Subscriber) HealthCheck(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errors.New("subscriber is closed")
	}

	// Check if we've received a block recently
	lastBlockTime := s.lastBlockTime.Load()
	if lastBlockTime > 0 {
		timeSinceLastBlock := time.Since(time.Unix(lastBlockTime, 0))
		if timeSinceLastBlock > s.config.HealthTimeout {
			return fmt.Errorf("no blocks received for %v (threshold: %v)", timeSinceLastBlock, s.config.HealthTimeout)
		}
	}

	if s.conn == nil {
		// Not yet connected, try a temporary connection
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, s.config.WebSocketURL, nil)
		if err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}
		conn.Close()
		return nil
	}

	// Send a ping to verify connection
	if err := s.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// parseBlockNumber parses a hex block number string to int64.
func parseBlockNumber(hexNum string) (int64, error) {
	hexNum = strings.TrimPrefix(hexNum, "0x")
	return strconv.ParseInt(hexNum, 16, 64)
}

// truncateHash shortens a hash for logging purposes.
func truncateHash(hash string) string {
	if len(hash) <= 14 {
		return hash
	}
	return fmt.Sprintf("%s...%s", hash[:8], hash[len(hash)-6:])
}
