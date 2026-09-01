package outbound

import (
	"context"
	"time"
)

// BlockState represents the persisted state of a block for tracking and reorg detection.
type BlockState struct {
	// Number is the block number (as int64 for easier comparison).
	Number int64

	// Hash is the block hash.
	Hash string

	// ParentHash is the parent block's hash (used for reorg detection).
	ParentHash string

	// ReceivedAt is when we received this block from the subscription.
	ReceivedAt int64

	// BlockTimestamp is the block's timestamp from the chain (Unix seconds).
	BlockTimestamp int64

	// IsOrphaned indicates this block was replaced during a chain reorganization.
	IsOrphaned bool

	// Version is the version of this block at its number (0 for first, 1 after first reorg, etc).
	Version int

	// BlockPublished tracks whether the single SQS publish event was successful.
	BlockPublished bool
}

// ReorgEvent represents a chain reorganization event.
type ReorgEvent struct {
	// ID is the unique identifier for this reorg event.
	ID int64

	// DetectedAt is when the reorg was detected.
	DetectedAt time.Time

	// BlockNumber is the block number where the reorg occurred.
	BlockNumber int64

	// OldHash is the hash of the block that was replaced (orphaned).
	OldHash string

	// NewHash is the hash of the new canonical block.
	NewHash string

	// Depth is how many blocks were reorganized (if known).
	Depth int
}

// BlockRange represents a range of block numbers (inclusive).
type BlockRange struct {
	// From is the starting block number (inclusive).
	From int64

	// To is the ending block number (inclusive).
	To int64
}

// BackfillCursor is what the gap filler compares against when it retires a
// range: the watermark plus the count of reorg commits behind it. A reorg whose
// common ancestor sits at or below the watermark changes only the generation,
// and that is the ordinary case — the watermark normally trails head by one, so
// without the generation a pass that straddled such a reorg would still match
// and advance over the height the reorg orphaned (ARCT-379).
type BackfillCursor struct {
	// Watermark is the highest block number verified as gap-free.
	Watermark int64

	// Generation counts the reorg commits that have touched this chain's cursor.
	Generation int64
}

// BlockStateRepository defines the interface for persisting block state.
// Used for tracking the last processed block, detecting reorgs, and deduplication.
type BlockStateRepository interface {
	// SaveBlock persists a block's state with atomic version assignment.
	// Returns the assigned version number. The version is calculated atomically
	// to prevent race conditions when multiple processes save blocks concurrently.
	// The state.Version field is ignored; the returned version should be used
	// for cache keys and event publishing.
	SaveBlock(ctx context.Context, state BlockState) (int, error)

	// GetLastBlock retrieves the most recently saved canonical (non-orphaned) block state.
	// Returns nil if no blocks have been saved yet.
	GetLastBlock(ctx context.Context) (*BlockState, error)

	// GetBlockByNumber retrieves a canonical block state by its number.
	// Returns nil if the block is not found.
	GetBlockByNumber(ctx context.Context, number int64) (*BlockState, error)

	// GetLowestCanonicalAbove retrieves the lowest canonical block whose number
	// is in (number, maxNumber]. Returns nil if the range holds none. The
	// un-orphan walk anchors on it, and the bound keeps an isolated orphan from
	// scanning the table.
	GetLowestCanonicalAbove(ctx context.Context, number, maxNumber int64) (*BlockState, error)

	// GetBlockByHash retrieves a block state by its hash.
	// Returns nil if the block is not found. Used for deduplication.
	GetBlockByHash(ctx context.Context, hash string) (*BlockState, error)

	// GetBlockVersionCount returns the number of times we've seen a block at this number.
	// This includes both canonical and orphaned blocks. Used to calculate version numbers
	// for BlockEvent when publishing - version = count of existing entries.
	GetBlockVersionCount(ctx context.Context, number int64) (int, error)

	// GetRecentBlocks retrieves the N most recent canonical blocks.
	// Used for reorg detection by checking parent hash chains.
	GetRecentBlocks(ctx context.Context, limit int) ([]BlockState, error)

	// MarkBlockOrphaned marks a block as orphaned during a reorg.
	// The block is kept for historical purposes but excluded from canonical queries.
	MarkBlockOrphaned(ctx context.Context, hash string) error

	// ClearBlocksOrphaned clears the is_orphaned flag on every named block, or
	// on none of them. Used by the backfill loop to self-heal a run that was
	// over-orphaned (e.g. a late-arriving block misclassified as a reorg);
	// healing it row by row would leave the chain half-restored when one row
	// fails, which neither the gap finder nor the un-orphan walk can repair.
	// Idempotent: clearing an already-canonical row is a no-op. Returns an
	// error, having changed nothing, if any hash is unknown or if another
	// canonical row already holds one of the heights.
	//
	// anchorHash is the canonical row the caller walked down from. The segment
	// is computed before this call, so a reorg landing in between can orphan
	// that anchor, and clearing would then promote a fork nothing descends
	// from. The anchor is re-read here under a row lock and the whole set
	// refused if it is no longer canonical (ARCT-379).
	ClearBlocksOrphaned(ctx context.Context, anchorHash string, hashes []string) error

	// HandleReorgAtomic atomically performs all reorg-related database operations:
	// 1. Saves the reorg event
	// 2. Marks all blocks after commonAncestor as orphaned
	// 3. Lowers the backfill watermark to commonAncestor if it sits above it,
	//    and bumps the cursor's generation either way
	// 4. Saves the new canonical block
	// This prevents inconsistent state if a crash occurs mid-reorg. Step 3 is
	// what puts an orphaned-but-not-replaced height back in FindGaps' range.
	// Returns the version assigned to the new block.
	HandleReorgAtomic(ctx context.Context, commonAncestor int64, event ReorgEvent, newBlock BlockState) (int, error)

	// GetMinBlockNumber returns the lowest canonical block number in the repository.
	// Returns 0 if no blocks exist.
	GetMinBlockNumber(ctx context.Context) (int64, error)

	// GetMaxBlockNumber returns the highest canonical block number in the repository.
	// Returns 0 if no blocks exist.
	GetMaxBlockNumber(ctx context.Context) (int64, error)

	// GetBackfillWatermark returns the highest block number that has been verified as gap-free.
	// Blocks at or below this number are guaranteed to have no gaps.
	// Returns 0 if no watermark has been set.
	GetBackfillWatermark(ctx context.Context) (int64, error)

	// GetBackfillCursor returns the watermark together with its generation.
	// Returns the zero cursor when no row has been written for this chain.
	GetBackfillCursor(ctx context.Context) (BackfillCursor, error)

	// RewindBackfillWatermark lowers the watermark to the given block if it
	// sits above it, and counts the rewind in the cursor's generation either
	// way. It reports the value it replaced and whether that value was above
	// the target. Every path that drops a canonical row without replacing it —
	// a reorg commit, the backfill's stale-chain recovery — must call it, or
	// the height it emptied stays below the watermark and out of FindGaps'
	// reach for good (ARCT-379).
	RewindBackfillWatermark(ctx context.Context, to int64) (previous int64, rewound bool, err error)

	// AdvanceBackfillWatermark moves the watermark to watermark, returning
	// false when the stored cursor is no longer expected. The gap filler
	// decides where to advance to from a cursor it read earlier, and
	// HandleReorgAtomic can move that cursor in between; an unconditional
	// write would put the rewound heights back out of FindGaps' reach and
	// leave the hole permanently unfilled (ARCT-379). The zero cursor also
	// seeds a chain whose row does not exist yet; any other expected cursor is
	// refused against a missing row.
	AdvanceBackfillWatermark(ctx context.Context, expected BackfillCursor, watermark int64) (bool, error)

	// FindGaps finds missing block ranges between minBlock and maxBlock.
	// Only considers canonical (non-orphaned) blocks.
	// Uses the backfill watermark to skip already-verified blocks.
	// Returns an empty slice if there are no gaps.
	FindGaps(ctx context.Context, minBlock, maxBlock int64) ([]BlockRange, error)

	// FindOrphanOnlyHeights returns block numbers in [fromBlock, toBlock] that
	// have an orphaned row and no canonical one. Such a height is a hole the
	// other checks under-report: FindGaps scans only above the backfill
	// watermark, and VerifyChainIntegrity reports only the first violation in
	// its watermark-bounded range. This enumerates every one of them, so an
	// empty result is the only "all clear". Ordered ascending and uncapped —
	// the result is bounded by the orphaned rows, which are bounded by reorgs.
	FindOrphanOnlyHeights(ctx context.Context, fromBlock, toBlock int64) ([]int64, error)

	// VerifyParentLinks reports the violations that never repair themselves: a
	// broken parent link between consecutive canonical blocks, and two
	// canonical rows at one height. Missing heights are excluded, so a caller
	// can run this above the backfill watermark, where a hole is the gap
	// filler's live work rather than a defect. Returns nil if there is none,
	// or an error describing the first in ascending block order.
	VerifyParentLinks(ctx context.Context, fromBlock, toBlock int64) error

	// VerifyChainIntegrity verifies that the canonical chain over
	// [fromBlock, toBlock] is unbroken: consecutive blocks are linked by
	// parent_hash, and no height between two canonical blocks is missing.
	// A height above the last canonical block in range, and two canonical rows
	// at one height, are violations too.
	// Returns nil if the chain is valid, or an error describing the first
	// violation in ascending block order. Heights below the range's first
	// canonical block are not violations — the backfill watermark starts at 0
	// on an unseeded chain, well below its first block.
	// This should be called after backfill completes to ensure eventual consistency.
	VerifyChainIntegrity(ctx context.Context, fromBlock, toBlock int64) error

	// MarkPublishComplete marks a block as published.
	// This is called after successful cache+publish operation.
	// Allows crash recovery: if a service crashes mid-publish, another service
	// (e.g., backfill) can check which publishes are incomplete and retry them.
	MarkPublishComplete(ctx context.Context, hash string) error

	// GetMinUnpublishedBlock returns the lowest canonical block number that has
	// not been published. Used by watermark advancement to avoid advancing past
	// blocks that still need publishing.
	// Returns (blockNum, true, nil) if found, (0, false, nil) if all published.
	GetMinUnpublishedBlock(ctx context.Context) (int64, bool, error)

	// GetBlocksWithIncompletePublish returns canonical blocks that have not been
	// published. Used by backfill to recover from crashes.
	GetBlocksWithIncompletePublish(ctx context.Context, limit int) ([]BlockState, error)

	// GetReorgEventsByBlockRange retrieves reorg events within a block number range.
	// Results are ordered by block number descending, then detection time descending.
	GetReorgEventsByBlockRange(ctx context.Context, fromBlock, toBlock int64) ([]ReorgEvent, error)
}
