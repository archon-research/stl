// blockstate.go provides an in-memory implementation of BlockStateRepository.
//
// This adapter is designed for testing and development purposes. It stores:
//   - Block states keyed by hash with O(1) lookup
//   - Reorg events for chain reorganization tracking
//   - Backfill watermark for gap detection
//
// All operations are thread-safe using sync.RWMutex. Data is lost on process restart.
// For production use, see the postgres adapter.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that BlockStateRepository implements outbound.BlockStateRepository
var _ outbound.BlockStateRepository = (*BlockStateRepository)(nil)

// BlockStateRepository is an in-memory implementation for testing.
type BlockStateRepository struct {
	mu                sync.RWMutex
	blocks            map[string]outbound.BlockState // keyed by hash
	reorgEvents       []outbound.ReorgEvent
	backfillWatermark int64
}

// NewBlockStateRepository creates a new in-memory block state repository.
func NewBlockStateRepository() *BlockStateRepository {
	return &BlockStateRepository{
		blocks:      make(map[string]outbound.BlockState),
		reorgEvents: make([]outbound.ReorgEvent, 0),
	}
}

// SaveBlock persists a block's state with atomic version assignment.
// Returns the assigned version number.
func (r *BlockStateRepository) SaveBlock(ctx context.Context, state outbound.BlockState) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Calculate the next version atomically while holding the lock
	maxVersion := -1
	for _, b := range r.blocks {
		if b.Number == state.Number && b.Version > maxVersion {
			maxVersion = b.Version
		}
	}
	version := maxVersion + 1
	state.Version = version

	r.blocks[state.Hash] = state
	return version, nil
}

// GetLastBlock retrieves the most recently saved canonical block state.
func (r *BlockStateRepository) GetLastBlock(ctx context.Context) (*outbound.BlockState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var latest *outbound.BlockState
	for _, b := range r.blocks {
		if b.IsOrphaned {
			continue
		}
		if latest == nil || b.Number > latest.Number {
			bc := b // copy
			latest = &bc
		}
	}
	return latest, nil
}

// GetBlockByNumber retrieves a canonical block state by its number.
func (r *BlockStateRepository) GetBlockByNumber(ctx context.Context, number int64) (*outbound.BlockState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, b := range r.blocks {
		if b.Number == number && !b.IsOrphaned {
			bc := b
			return &bc, nil
		}
	}
	return nil, nil
}

// GetBlockByHash retrieves a block state by its hash.
func (r *BlockStateRepository) GetBlockByHash(ctx context.Context, hash string) (*outbound.BlockState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if b, ok := r.blocks[hash]; ok {
		bc := b
		return &bc, nil
	}
	return nil, nil
}

// GetBlockVersionCount returns the next version number for blocks at a given number.
// If no blocks exist at that number, returns 0. Otherwise returns MAX(version) + 1.
func (r *BlockStateRepository) GetBlockVersionCount(ctx context.Context, number int64) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	maxVersion := -1
	for _, b := range r.blocks {
		if b.Number == number && b.Version > maxVersion {
			maxVersion = b.Version
		}
	}
	return maxVersion + 1, nil
}

// GetRecentBlocks retrieves the N most recent canonical blocks.
func (r *BlockStateRepository) GetRecentBlocks(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	canonical := make([]outbound.BlockState, 0)
	for _, b := range r.blocks {
		if !b.IsOrphaned {
			canonical = append(canonical, b)
		}
	}

	// Sort by number descending
	sort.Slice(canonical, func(i, j int) bool {
		return canonical[i].Number > canonical[j].Number
	})

	if len(canonical) > limit {
		canonical = canonical[:limit]
	}

	// Reverse to ascending order
	for i, j := 0, len(canonical)-1; i < j; i, j = i+1, j-1 {
		canonical[i], canonical[j] = canonical[j], canonical[i]
	}

	return canonical, nil
}

// MarkBlockOrphaned marks a block as orphaned during a reorg.
func (r *BlockStateRepository) MarkBlockOrphaned(ctx context.Context, hash string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if b, ok := r.blocks[hash]; ok {
		b.IsOrphaned = true
		r.blocks[hash] = b
	}
	return nil
}

// HandleReorgAtomic atomically performs all reorg-related operations.
// In the memory implementation, this is naturally atomic since we hold the lock.
func (r *BlockStateRepository) HandleReorgAtomic(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if block already exists (idempotency)
	if existing, ok := r.blocks[newBlock.Hash]; ok {
		return existing.Version, nil
	}

	// 1. Save reorg event
	r.reorgEvents = append(r.reorgEvents, event)

	// 2. Mark old blocks as orphaned
	for hash, b := range r.blocks {
		if b.Number > commonAncestor && !b.IsOrphaned {
			b.IsOrphaned = true
			r.blocks[hash] = b
		}
	}

	// 3. Calculate version for new block
	version := 0
	for _, b := range r.blocks {
		if b.Number == newBlock.Number {
			if b.Version >= version {
				version = b.Version + 1
			}
		}
	}

	// 4. Save new block
	newBlock.Version = version
	r.blocks[newBlock.Hash] = newBlock

	return version, nil
}

// GetBlockCount returns the total number of blocks (for testing).
func (r *BlockStateRepository) GetBlockCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.blocks)
}

// GetCanonicalBlockCount returns the number of non-orphaned blocks (for testing).
func (r *BlockStateRepository) GetCanonicalBlockCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, b := range r.blocks {
		if !b.IsOrphaned {
			count++
		}
	}
	return count
}

// GetMinBlockNumber returns the lowest canonical block number.
func (r *BlockStateRepository) GetMinBlockNumber(ctx context.Context) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var minNum int64 = 0
	found := false
	for _, b := range r.blocks {
		if b.IsOrphaned {
			continue
		}
		if !found || b.Number < minNum {
			minNum = b.Number
			found = true
		}
	}
	return minNum, nil
}

// GetMaxBlockNumber returns the highest canonical block number.
func (r *BlockStateRepository) GetMaxBlockNumber(ctx context.Context) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var maxNum int64 = 0
	for _, b := range r.blocks {
		if b.IsOrphaned {
			continue
		}
		if b.Number > maxNum {
			maxNum = b.Number
		}
	}
	return maxNum, nil
}

// GetBackfillWatermark returns the highest block number verified as gap-free.
func (r *BlockStateRepository) GetBackfillWatermark(ctx context.Context) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.backfillWatermark, nil
}

// SetBackfillWatermark updates the watermark to the given block number.
func (r *BlockStateRepository) SetBackfillWatermark(ctx context.Context, watermark int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backfillWatermark = watermark
	return nil
}

// FindGaps finds missing block ranges between minBlock and maxBlock.
// Uses the backfill watermark to skip already-verified blocks.
func (r *BlockStateRepository) FindGaps(ctx context.Context, minBlock, maxBlock int64) ([]outbound.BlockRange, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if minBlock > maxBlock {
		return nil, nil
	}

	// Adjust minBlock based on watermark
	effectiveMin := minBlock
	if r.backfillWatermark >= minBlock {
		effectiveMin = r.backfillWatermark + 1
	}

	// If watermark covers the entire range, no gaps possible
	if effectiveMin > maxBlock {
		return nil, nil
	}

	// Build a set of existing canonical block numbers
	existing := make(map[int64]bool)
	for _, b := range r.blocks {
		if !b.IsOrphaned {
			existing[b.Number] = true
		}
	}

	// Find gaps
	gaps := make([]outbound.BlockRange, 0)
	var gapStart int64 = -1

	for num := effectiveMin; num <= maxBlock; num++ {
		if !existing[num] {
			// Missing block
			if gapStart < 0 {
				gapStart = num
			}
		} else {
			// Block exists - close any open gap
			if gapStart >= 0 {
				gaps = append(gaps, outbound.BlockRange{From: gapStart, To: num - 1})
				gapStart = -1
			}
		}
	}

	// Close final gap if it extends to maxBlock
	if gapStart >= 0 {
		gaps = append(gaps, outbound.BlockRange{From: gapStart, To: maxBlock})
	}

	return gaps, nil
}

// VerifyChainIntegrity verifies that the parent_hash chain is properly linked.
// Returns nil if the chain is valid, or an error describing the first broken link.
func (r *BlockStateRepository) VerifyChainIntegrity(ctx context.Context, fromBlock, toBlock int64) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if fromBlock >= toBlock {
		return nil // Nothing to verify
	}

	// Build a sorted list of canonical blocks in range
	type blockInfo struct {
		number     int64
		hash       string
		parentHash string
	}
	var blocksInRange []blockInfo
	for _, b := range r.blocks {
		if !b.IsOrphaned && b.Number >= fromBlock && b.Number <= toBlock {
			blocksInRange = append(blocksInRange, blockInfo{
				number:     b.Number,
				hash:       b.Hash,
				parentHash: b.ParentHash,
			})
		}
	}

	// Sort by block number
	sort.Slice(blocksInRange, func(i, j int) bool {
		return blocksInRange[i].number < blocksInRange[j].number
	})

	// Check each consecutive pair
	for i := 1; i < len(blocksInRange); i++ {
		curr := blocksInRange[i]
		prev := blocksInRange[i-1]

		// Only check if blocks are consecutive
		if curr.number == prev.number+1 {
			if curr.parentHash != prev.hash {
				return fmt.Errorf("chain integrity violation at block %d: parent_hash %s does not match hash %s of block %d",
					curr.number, curr.parentHash, prev.hash, prev.number)
			}
		}
	}

	return nil
}

// MarkPublishComplete marks a block as published.
func (r *BlockStateRepository) MarkPublishComplete(ctx context.Context, hash string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	block, ok := r.blocks[hash]
	if !ok {
		return fmt.Errorf("block with hash %s not found", hash)
	}

	block.BlockPublished = true
	r.blocks[hash] = block
	return nil
}

// GetReorgEventsByBlockRange retrieves reorg events within a block number range.
func (r *BlockStateRepository) GetReorgEventsByBlockRange(ctx context.Context, fromBlock, toBlock int64) ([]outbound.ReorgEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var filtered []outbound.ReorgEvent
	for _, e := range r.reorgEvents {
		if e.BlockNumber >= fromBlock && e.BlockNumber <= toBlock {
			filtered = append(filtered, e)
		}
	}

	// Sort by block number descending, then detection time descending
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].BlockNumber != filtered[j].BlockNumber {
			return filtered[i].BlockNumber > filtered[j].BlockNumber
		}
		return filtered[i].DetectedAt.After(filtered[j].DetectedAt)
	})

	return filtered, nil
}

// GetMinUnpublishedBlock returns the lowest canonical block number that has not been published.
// Returns (blockNum, true, nil) if found, (0, false, nil) if all blocks are published.
func (r *BlockStateRepository) GetMinUnpublishedBlock(ctx context.Context) (int64, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var minNum int64
	found := false
	for _, b := range r.blocks {
		if b.IsOrphaned || b.BlockPublished {
			continue
		}
		if !found || b.Number < minNum {
			minNum = b.Number
			found = true
		}
	}
	return minNum, found, nil
}

// GetBlocksWithIncompletePublish returns canonical blocks that have not been published.
// Used by backfill to recover from crashes.
func (r *BlockStateRepository) GetBlocksWithIncompletePublish(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var incomplete []outbound.BlockState
	for _, b := range r.blocks {
		if b.IsOrphaned {
			continue
		}

		// Only check BlockPublished - there's only 1 publish event that includes all data
		if !b.BlockPublished {
			bc := b
			incomplete = append(incomplete, bc)
		}

		if len(incomplete) >= limit {
			break
		}
	}

	// Sort by block number for consistent ordering
	sort.Slice(incomplete, func(i, j int) bool {
		return incomplete[i].Number < incomplete[j].Number
	})

	if len(incomplete) > limit {
		incomplete = incomplete[:limit]
	}

	return incomplete, nil
}
