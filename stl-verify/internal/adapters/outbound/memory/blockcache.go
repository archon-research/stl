// cache.go provides an in-memory implementation of BlockCache.
//
// This adapter caches block data (blocks, receipts, traces, blobs) in memory
// for testing and development. Data is keyed by chainID:blockNumber:dataType.
//
// All operations are thread-safe. Data is lost on process restart.
// For production use, consider a Redis or other persistent cache implementation.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that BlockCache implements outbound.BlockCache
var _ outbound.BlockCache = (*BlockCache)(nil)

// BlockCache is an in-memory implementation of the BlockCache port for testing.
type BlockCache struct {
	mu     sync.RWMutex
	blocks map[string]json.RawMessage
	closed bool
}

// NewBlockCache creates a new in-memory block cache for testing.
func NewBlockCache() *BlockCache {
	return &BlockCache{
		blocks: make(map[string]json.RawMessage),
	}
}

func (c *BlockCache) key(chainID int64, blockNumber int64, version int, dataType string) string {
	return fmt.Sprintf("%d:%d:%d:%s", chainID, blockNumber, version, dataType)
}

// SetBlock stores the full block with transactions.
func (c *BlockCache) SetBlock(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks[c.key(chainID, blockNumber, version, "block")] = data
	return nil
}

// SetReceipts stores transaction receipts for a block.
func (c *BlockCache) SetReceipts(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks[c.key(chainID, blockNumber, version, "receipts")] = data
	return nil
}

// SetTraces stores execution traces for a block.
func (c *BlockCache) SetTraces(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks[c.key(chainID, blockNumber, version, "traces")] = data
	return nil
}

// SetBlobs stores blob sidecars for a block.
func (c *BlockCache) SetBlobs(ctx context.Context, chainID int64, blockNumber int64, version int, data json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks[c.key(chainID, blockNumber, version, "blobs")] = data
	return nil
}

// SetBlockData stores all block data types in a single operation.
// For the in-memory cache, this is equivalent to calling SetBlock, SetReceipts, SetTraces, SetBlobs separately.
// Only non-nil data is stored.
func (c *BlockCache) SetBlockData(ctx context.Context, chainID int64, blockNumber int64, version int, data outbound.BlockDataInput) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if data.Block != nil {
		c.blocks[c.key(chainID, blockNumber, version, "block")] = data.Block
	}
	if data.Receipts != nil {
		c.blocks[c.key(chainID, blockNumber, version, "receipts")] = data.Receipts
	}
	if data.Traces != nil {
		c.blocks[c.key(chainID, blockNumber, version, "traces")] = data.Traces
	}
	if data.Blobs != nil {
		c.blocks[c.key(chainID, blockNumber, version, "blobs")] = data.Blobs
	}
	return nil
}

// DeleteBlock removes all cached data for a block at a specific version.
func (c *BlockCache) DeleteBlock(ctx context.Context, chainID int64, blockNumber int64, version int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.blocks, c.key(chainID, blockNumber, version, "block"))
	delete(c.blocks, c.key(chainID, blockNumber, version, "receipts"))
	delete(c.blocks, c.key(chainID, blockNumber, version, "traces"))
	delete(c.blocks, c.key(chainID, blockNumber, version, "blobs"))
	return nil
}

// Close marks the cache as closed.
func (c *BlockCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// GetAllKeys returns all cache keys (for testing).
func (c *BlockCache) GetAllKeys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.blocks))
	for k := range c.blocks {
		keys = append(keys, k)
	}
	return keys
}

// GetEntryCount returns the number of cached entries (for testing).
func (c *BlockCache) GetEntryCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.blocks)
}

// Clear removes all cached data (for testing).
func (c *BlockCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks = make(map[string]json.RawMessage)
}

// GetBlock retrieves cached block data.
func (c *BlockCache) GetBlock(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blocks[c.key(chainID, blockNumber, version, "block")], nil
}

// GetReceipts retrieves cached receipts data.
func (c *BlockCache) GetReceipts(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blocks[c.key(chainID, blockNumber, version, "receipts")], nil
}

// GetTraces retrieves cached traces data.
func (c *BlockCache) GetTraces(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blocks[c.key(chainID, blockNumber, version, "traces")], nil
}

// GetBlobs retrieves cached blobs data.
func (c *BlockCache) GetBlobs(ctx context.Context, chainID int64, blockNumber int64, version int) (json.RawMessage, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blocks[c.key(chainID, blockNumber, version, "blobs")], nil
}
