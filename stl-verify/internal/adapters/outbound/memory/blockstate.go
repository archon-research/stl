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
	"slices"
	"sort"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that BlockStateRepository implements outbound.BlockStateRepository
var _ outbound.BlockStateRepository = (*BlockStateRepository)(nil)

// BlockStateRepository is an in-memory implementation for testing.
type BlockStateRepository struct {
	mu             sync.RWMutex
	blocks         map[string]outbound.BlockState // keyed by hash
	reorgEvents    []outbound.ReorgEvent
	backfillCursor outbound.BackfillCursor
}

// NewBlockStateRepository creates a new in-memory block state repository.
func NewBlockStateRepository() *BlockStateRepository {
	return &BlockStateRepository{
		blocks:      make(map[string]outbound.BlockState),
		reorgEvents: make([]outbound.ReorgEvent, 0),
	}
}

// SaveBlock persists a block's state with atomic version assignment.
// If a block with the same hash already exists, returns its existing version (idempotent).
// Returns the assigned version number.
// If a block with the same hash already exists, returns its existing version
// without modification (idempotent), matching the Postgres adapter behavior.
func (r *BlockStateRepository) SaveBlock(ctx context.Context, state outbound.BlockState) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if block with this hash already exists (matches postgres behavior).
	if existing, ok := r.blocks[state.Hash]; ok {
		return existing.Version, nil
	}

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

// GetLowestCanonicalAbove retrieves the lowest canonical block whose number is
// in (number, maxNumber], or nil when the range holds none.
func (r *BlockStateRepository) GetLowestCanonicalAbove(ctx context.Context, number, maxNumber int64) (*outbound.BlockState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var lowest *outbound.BlockState
	for _, b := range r.blocks {
		if b.IsOrphaned || b.Number <= number || b.Number > maxNumber {
			continue
		}
		if lowest == nil || b.Number < lowest.Number {
			bc := b
			lowest = &bc
		}
	}
	return lowest, nil
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

// ClearBlocksOrphaned clears the is_orphaned flag on every named block, or on
// none of them. Mirrors the postgres adapter contract: idempotent on
// already-canonical rows, and refuses the whole set when a hash is unknown or
// when a different canonical row already occupies one of the numbers.
func (r *BlockStateRepository) ClearBlocksOrphaned(ctx context.Context, hashes []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	healing := make(map[string]bool, len(hashes))
	for _, hash := range hashes {
		healing[hash] = true
	}
	for _, hash := range hashes {
		block, ok := r.blocks[hash]
		if !ok {
			return fmt.Errorf("clear orphan flag: block with hash %s not found", hash)
		}
		if !block.IsOrphaned {
			continue
		}
		// Leaving the orphan in place keeps the "highest version = canonical"
		// invariant; the live writer wins (PR #373 review).
		for otherHash, other := range r.blocks {
			if !healing[otherHash] && other.Number == block.Number && !other.IsOrphaned {
				return fmt.Errorf("clear orphan flag: refusing to un-orphan block %d: canonical row %s already holds this height", block.Number, otherHash)
			}
		}
	}

	for _, hash := range hashes {
		block := r.blocks[hash]
		block.IsOrphaned = false
		r.blocks[hash] = block
	}
	return nil
}

// HandleReorgAtomic atomically performs all reorg-related operations.
// In the memory implementation, this is naturally atomic since we hold the lock.
func (r *BlockStateRepository) HandleReorgAtomic(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if block already exists (idempotency).
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

	// 3. Rewind the backfill cursor to the common ancestor: FindGaps scans only
	// above the watermark, so a height left orphan-only here is never
	// re-fetched. The generation counts this commit even when the watermark
	// already sat low enough to need no move.
	r.rewindCursorLocked(commonAncestor)

	// 4. Calculate version for new block
	version := 0
	for _, b := range r.blocks {
		if b.Number == newBlock.Number {
			if b.Version >= version {
				version = b.Version + 1
			}
		}
	}

	// 5. Save new block
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
	return r.backfillCursor.Watermark, nil
}

// GetBackfillCursor returns the watermark together with its generation.
func (r *BlockStateRepository) GetBackfillCursor(ctx context.Context) (outbound.BackfillCursor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.backfillCursor, nil
}

// RewindBackfillWatermark lowers the watermark to the given block if it sits
// above it, and counts the rewind in the generation either way.
func (r *BlockStateRepository) RewindBackfillWatermark(ctx context.Context, to int64) (int64, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	previous := r.backfillCursor.Watermark
	r.rewindCursorLocked(to)
	return previous, previous > to, nil
}

// rewindCursorLocked is the cursor half of a rewind; callers hold the lock.
func (r *BlockStateRepository) rewindCursorLocked(to int64) {
	r.backfillCursor = outbound.BackfillCursor{
		Watermark:  min(r.backfillCursor.Watermark, to),
		Generation: r.backfillCursor.Generation + 1,
	}
}

// SeedBackfillCursor puts the cursor at a given position. Test-only: production
// moves it through AdvanceBackfillWatermark and HandleReorgAtomic, and an
// unconditional write is what let a reorg rewind be overwritten (ARCT-379).
func (r *BlockStateRepository) SeedBackfillCursor(watermark, generation int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backfillCursor = outbound.BackfillCursor{Watermark: watermark, Generation: generation}
}

// AdvanceBackfillWatermark moves the watermark as long as the stored cursor is
// still the expected one, and reports whether it changed.
func (r *BlockStateRepository) AdvanceBackfillWatermark(ctx context.Context, expected outbound.BackfillCursor, watermark int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.backfillCursor != expected {
		return false, nil
	}
	r.backfillCursor.Watermark = watermark
	return true, nil
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
	if r.backfillCursor.Watermark >= minBlock {
		effectiveMin = r.backfillCursor.Watermark + 1
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

// FindOrphanOnlyHeights returns block numbers in the range whose only blocks
// are orphaned, ascending.
func (r *BlockStateRepository) FindOrphanOnlyHeights(ctx context.Context, fromBlock, toBlock int64) ([]int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if fromBlock > toBlock {
		return nil, nil
	}

	orphaned := make(map[int64]bool)
	canonical := make(map[int64]bool)
	for _, b := range r.blocks {
		if b.Number < fromBlock || b.Number > toBlock {
			continue
		}
		if b.IsOrphaned {
			orphaned[b.Number] = true
		} else {
			canonical[b.Number] = true
		}
	}

	var heights []int64
	for number := range orphaned {
		if !canonical[number] {
			heights = append(heights, number)
		}
	}
	slices.Sort(heights)

	return heights, nil
}

// VerifyChainIntegrity verifies that the canonical chain over the range is
// unbroken: consecutive blocks are linked by parent_hash, no height between two
// canonical blocks is missing, no height above the last canonical block is
// missing, and no height holds two canonical rows. Returns nil if the chain is
// valid, or an error describing the first violation in ascending block order.
func (r *BlockStateRepository) VerifyChainIntegrity(ctx context.Context, fromBlock, toBlock int64) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if fromBlock >= toBlock {
		return nil // Nothing to verify
	}

	blocksInRange := r.canonicalBlocksOver(fromBlock, toBlock)
	if err := verifyOrderedPairs(blocksInRange, true); err != nil {
		return err
	}
	if len(blocksInRange) == 0 {
		return nil
	}
	if last := blocksInRange[len(blocksInRange)-1].Number; last < toBlock {
		return fmt.Errorf("chain integrity violation: canonical block(s) %d to %d missing after block %d",
			last+1, toBlock, last)
	}
	return nil
}

// VerifyParentLinks reports the violations that never repair themselves: a
// broken parent link and two canonical rows at one height. Missing heights are
// excluded, so the caller can run this above the backfill watermark, where a
// hole is the gap filler's live work rather than a defect.
func (r *BlockStateRepository) VerifyParentLinks(ctx context.Context, fromBlock, toBlock int64) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if fromBlock >= toBlock {
		return nil
	}
	return verifyOrderedPairs(r.canonicalBlocksOver(fromBlock, toBlock), false)
}

// canonicalBlocksOver returns the canonical blocks in [fromBlock, toBlock],
// ordered by number then version so two rows at one height keep a deterministic
// order, as the postgres adapter's window does.
func (r *BlockStateRepository) canonicalBlocksOver(fromBlock, toBlock int64) []outbound.BlockState {
	var blocks []outbound.BlockState
	for _, b := range r.blocks {
		if !b.IsOrphaned && b.Number >= fromBlock && b.Number <= toBlock {
			blocks = append(blocks, b)
		}
	}
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].Number != blocks[j].Number {
			return blocks[i].Number < blocks[j].Number
		}
		return blocks[i].Version < blocks[j].Version
	})
	return blocks
}

// verifyOrderedPairs reports the first violation between two adjacent canonical
// rows. The range's first block is never flagged: an unseeded chain's watermark
// starts at 0, far below its first block.
func verifyOrderedPairs(blocks []outbound.BlockState, reportMissing bool) error {
	for i := 1; i < len(blocks); i++ {
		curr, prev := blocks[i], blocks[i-1]

		if prev.Number == curr.Number {
			return fmt.Errorf("chain integrity violation: duplicate canonical rows at height %d: %s and %s",
				curr.Number, prev.Hash, curr.Hash)
		}
		if reportMissing && prev.Number < curr.Number-1 {
			return fmt.Errorf("chain integrity violation: canonical block(s) %d to %d missing between blocks %d and %d",
				prev.Number+1, curr.Number-1, prev.Number, curr.Number)
		}
		if curr.Number == prev.Number+1 && curr.ParentHash != prev.Hash {
			return fmt.Errorf("chain integrity violation at block %d: parent_hash %s does not match hash %s of block %d",
				curr.Number, curr.ParentHash, prev.Hash, prev.Number)
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
