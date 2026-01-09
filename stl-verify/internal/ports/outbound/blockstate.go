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

	// IsOrphaned indicates this block was replaced during a chain reorganization.
	IsOrphaned bool

	// Version is the version of this block at its number (0 for first, 1 after first reorg, etc).
	Version int
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

	// HandleReorgAtomic atomically performs all reorg-related database operations:
	// 1. Saves the reorg event
	// 2. Marks all blocks after commonAncestor as orphaned
	// 3. Saves the new canonical block
	// This prevents inconsistent state if a crash occurs mid-reorg.
	// Returns the version assigned to the new block.
	HandleReorgAtomic(ctx context.Context, event ReorgEvent, newBlock BlockState) (int, error)

	// GetReorgEvents retrieves reorg events, ordered by detection time descending.
	GetReorgEvents(ctx context.Context, limit int) ([]ReorgEvent, error)

	// GetReorgEventsByBlockRange retrieves reorg events within a block number range.
	GetReorgEventsByBlockRange(ctx context.Context, fromBlock, toBlock int64) ([]ReorgEvent, error)

	// GetOrphanedBlocks retrieves orphaned blocks for analysis.
	GetOrphanedBlocks(ctx context.Context, limit int) ([]BlockState, error)

	// PruneOldBlocks deletes blocks older than the given number.
	// Used to prevent unbounded growth of the block state table.
	// Only prunes non-orphaned blocks; orphaned blocks are kept for history.
	PruneOldBlocks(ctx context.Context, keepAfter int64) error

	// PruneOldReorgEvents deletes reorg events older than the given time.
	PruneOldReorgEvents(ctx context.Context, olderThan time.Time) error

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

	// SetBackfillWatermark updates the watermark to the given block number.
	// Should only be called after confirming all blocks up to this number exist.
	SetBackfillWatermark(ctx context.Context, watermark int64) error

	// FindGaps finds missing block ranges between minBlock and maxBlock.
	// Only considers canonical (non-orphaned) blocks.
	// Uses the backfill watermark to skip already-verified blocks.
	// Returns an empty slice if there are no gaps.
	FindGaps(ctx context.Context, minBlock, maxBlock int64) ([]BlockRange, error)
}
