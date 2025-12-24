// Package memory provides in-memory adapters for testing.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

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

// SaveBlock persists a block's state.
func (r *BlockStateRepository) SaveBlock(ctx context.Context, state outbound.BlockState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocks[state.Hash] = state
	return nil
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

// MarkBlocksOrphanedAfter marks all blocks after the given number as orphaned.
func (r *BlockStateRepository) MarkBlocksOrphanedAfter(ctx context.Context, number int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for hash, b := range r.blocks {
		if b.Number > number {
			b.IsOrphaned = true
			r.blocks[hash] = b
		}
	}
	return nil
}

// SaveReorgEvent records a chain reorganization event.
func (r *BlockStateRepository) SaveReorgEvent(ctx context.Context, event outbound.ReorgEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reorgEvents = append(r.reorgEvents, event)
	return nil
}

// GetReorgEvents retrieves reorg events, ordered by detection time descending.
func (r *BlockStateRepository) GetReorgEvents(ctx context.Context, limit int) ([]outbound.ReorgEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]outbound.ReorgEvent, len(r.reorgEvents))
	copy(result, r.reorgEvents)

	// Sort by detection time descending
	sort.Slice(result, func(i, j int) bool {
		return result[i].DetectedAt.After(result[j].DetectedAt)
	})

	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// GetReorgEventsByBlockRange retrieves reorg events within a block number range.
func (r *BlockStateRepository) GetReorgEventsByBlockRange(ctx context.Context, fromBlock, toBlock int64) ([]outbound.ReorgEvent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]outbound.ReorgEvent, 0)
	for _, e := range r.reorgEvents {
		if e.BlockNumber >= fromBlock && e.BlockNumber <= toBlock {
			result = append(result, e)
		}
	}
	return result, nil
}

// GetOrphanedBlocks retrieves orphaned blocks for analysis.
func (r *BlockStateRepository) GetOrphanedBlocks(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	orphaned := make([]outbound.BlockState, 0)
	for _, b := range r.blocks {
		if b.IsOrphaned {
			orphaned = append(orphaned, b)
		}
	}

	if len(orphaned) > limit {
		orphaned = orphaned[:limit]
	}
	return orphaned, nil
}

// PruneOldBlocks deletes blocks older than the given number.
func (r *BlockStateRepository) PruneOldBlocks(ctx context.Context, keepAfter int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for hash, b := range r.blocks {
		if b.Number < keepAfter && !b.IsOrphaned {
			delete(r.blocks, hash)
		}
	}
	return nil
}

// PruneOldReorgEvents deletes reorg events older than the given time.
func (r *BlockStateRepository) PruneOldReorgEvents(ctx context.Context, olderThan time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	newEvents := make([]outbound.ReorgEvent, 0)
	for _, e := range r.reorgEvents {
		if !e.DetectedAt.Before(olderThan) {
			newEvents = append(newEvents, e)
		}
	}
	r.reorgEvents = newEvents
	return nil
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
