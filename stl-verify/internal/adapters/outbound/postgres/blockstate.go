// blockstate.go provides a PostgreSQL implementation of BlockStateRepository.
//
// This adapter persists block states and reorg events to PostgreSQL for
// durable storage. It supports:
//   - Block state persistence with upsert semantics (ON CONFLICT UPDATE)
//   - Canonical and orphaned block tracking
//   - Reorg event recording for chain reorganization history
//   - Gap detection queries for backfill operations
//   - Automatic schema migration via embedded SQL
//
// The schema is defined in migrations/001_initial_schema.sql and is
// automatically applied via the Migrate() method.
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

//go:embed migrations/001_initial_schema.sql
var initialSchema string

// Compile-time check that BlockStateRepository implements outbound.BlockStateRepository
var _ outbound.BlockStateRepository = (*BlockStateRepository)(nil)

// BlockStateRepository is a PostgreSQL implementation of the outbound.BlockStateRepository port.
type BlockStateRepository struct {
	db *sql.DB
}

// NewBlockStateRepository creates a new PostgreSQL block state repository.
func NewBlockStateRepository(db *sql.DB) *BlockStateRepository {
	return &BlockStateRepository{db: db}
}

// Migrate creates the block_states and reorg_events tables if they don't exist.
func (r *BlockStateRepository) Migrate(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, initialSchema)
	if err != nil {
		return fmt.Errorf("failed to migrate tables: %w", err)
	}
	return nil
}

// SaveBlock persists a block's state.
func (r *BlockStateRepository) SaveBlock(ctx context.Context, state outbound.BlockState) error {
	query := `
		INSERT INTO block_states (number, hash, parent_hash, received_at, is_orphaned)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (number, hash) DO UPDATE SET
			parent_hash = EXCLUDED.parent_hash,
			received_at = EXCLUDED.received_at,
			is_orphaned = EXCLUDED.is_orphaned
	`
	_, err := r.db.ExecContext(ctx, query, state.Number, state.Hash, state.ParentHash, state.ReceivedAt, state.IsOrphaned)
	if err != nil {
		return fmt.Errorf("failed to save block state: %w", err)
	}
	return nil
}

// GetLastBlock retrieves the most recently saved canonical (non-orphaned) block state.
func (r *BlockStateRepository) GetLastBlock(ctx context.Context) (*outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned
		FROM block_states
		WHERE NOT is_orphaned
		ORDER BY number DESC
		LIMIT 1
	`
	var state outbound.BlockState
	err := r.db.QueryRowContext(ctx, query).Scan(&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get last block: %w", err)
	}
	return &state, nil
}

// GetBlockByNumber retrieves a canonical block state by its number.
func (r *BlockStateRepository) GetBlockByNumber(ctx context.Context, number int64) (*outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned
		FROM block_states
		WHERE number = $1 AND NOT is_orphaned
	`
	var state outbound.BlockState
	err := r.db.QueryRowContext(ctx, query, number).Scan(&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get block by number: %w", err)
	}
	return &state, nil
}

// GetBlockByHash retrieves a block state by its hash (includes orphaned blocks).
func (r *BlockStateRepository) GetBlockByHash(ctx context.Context, hash string) (*outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned
		FROM block_states
		WHERE hash = $1
	`
	var state outbound.BlockState
	err := r.db.QueryRowContext(ctx, query, hash).Scan(&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get block by hash: %w", err)
	}
	return &state, nil
}

// GetRecentBlocks retrieves the N most recent canonical blocks.
func (r *BlockStateRepository) GetRecentBlocks(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned
		FROM block_states
		WHERE NOT is_orphaned
		ORDER BY number DESC
		LIMIT $1
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent blocks: %w", err)
	}
	defer rows.Close()

	var states []outbound.BlockState
	for rows.Next() {
		var state outbound.BlockState
		if err := rows.Scan(&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned); err != nil {
			return nil, fmt.Errorf("failed to scan block state: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating block states: %w", err)
	}
	return states, nil
}

// MarkBlockOrphaned marks a block as orphaned during a reorg.
func (r *BlockStateRepository) MarkBlockOrphaned(ctx context.Context, hash string) error {
	query := `UPDATE block_states SET is_orphaned = TRUE WHERE hash = $1`
	_, err := r.db.ExecContext(ctx, query, hash)
	if err != nil {
		return fmt.Errorf("failed to mark block orphaned: %w", err)
	}
	return nil
}

// MarkBlocksOrphanedAfter marks all blocks after the given number as orphaned.
func (r *BlockStateRepository) MarkBlocksOrphanedAfter(ctx context.Context, number int64) error {
	query := `UPDATE block_states SET is_orphaned = TRUE WHERE number > $1 AND NOT is_orphaned`
	_, err := r.db.ExecContext(ctx, query, number)
	if err != nil {
		return fmt.Errorf("failed to mark blocks orphaned after %d: %w", number, err)
	}
	return nil
}

// SaveReorgEvent records a chain reorganization event.
func (r *BlockStateRepository) SaveReorgEvent(ctx context.Context, event outbound.ReorgEvent) error {
	query := `
		INSERT INTO reorg_events (detected_at, block_number, old_hash, new_hash, depth)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.db.ExecContext(ctx, query, event.DetectedAt, event.BlockNumber, event.OldHash, event.NewHash, event.Depth)
	if err != nil {
		return fmt.Errorf("failed to save reorg event: %w", err)
	}
	return nil
}

// GetReorgEvents retrieves reorg events, ordered by detection time descending.
func (r *BlockStateRepository) GetReorgEvents(ctx context.Context, limit int) ([]outbound.ReorgEvent, error) {
	query := `
		SELECT id, detected_at, block_number, old_hash, new_hash, depth
		FROM reorg_events
		ORDER BY detected_at DESC
		LIMIT $1
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get reorg events: %w", err)
	}
	defer rows.Close()

	var events []outbound.ReorgEvent
	for rows.Next() {
		var event outbound.ReorgEvent
		if err := rows.Scan(&event.ID, &event.DetectedAt, &event.BlockNumber, &event.OldHash, &event.NewHash, &event.Depth); err != nil {
			return nil, fmt.Errorf("failed to scan reorg event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating reorg events: %w", err)
	}
	return events, nil
}

// GetReorgEventsByBlockRange retrieves reorg events within a block number range.
func (r *BlockStateRepository) GetReorgEventsByBlockRange(ctx context.Context, fromBlock, toBlock int64) ([]outbound.ReorgEvent, error) {
	query := `
		SELECT id, detected_at, block_number, old_hash, new_hash, depth
		FROM reorg_events
		WHERE block_number >= $1 AND block_number <= $2
		ORDER BY block_number DESC, detected_at DESC
	`
	rows, err := r.db.QueryContext(ctx, query, fromBlock, toBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to get reorg events by block range: %w", err)
	}
	defer rows.Close()

	var events []outbound.ReorgEvent
	for rows.Next() {
		var event outbound.ReorgEvent
		if err := rows.Scan(&event.ID, &event.DetectedAt, &event.BlockNumber, &event.OldHash, &event.NewHash, &event.Depth); err != nil {
			return nil, fmt.Errorf("failed to scan reorg event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating reorg events: %w", err)
	}
	return events, nil
}

// GetOrphanedBlocks retrieves orphaned blocks for analysis.
func (r *BlockStateRepository) GetOrphanedBlocks(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned
		FROM block_states
		WHERE is_orphaned
		ORDER BY received_at DESC
		LIMIT $1
	`
	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get orphaned blocks: %w", err)
	}
	defer rows.Close()

	var states []outbound.BlockState
	for rows.Next() {
		var state outbound.BlockState
		if err := rows.Scan(&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned); err != nil {
			return nil, fmt.Errorf("failed to scan block state: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating block states: %w", err)
	}
	return states, nil
}

// PruneOldBlocks deletes canonical blocks older than the given number.
// Orphaned blocks are kept for historical analysis.
func (r *BlockStateRepository) PruneOldBlocks(ctx context.Context, keepAfter int64) error {
	query := `DELETE FROM block_states WHERE number < $1 AND NOT is_orphaned`
	_, err := r.db.ExecContext(ctx, query, keepAfter)
	if err != nil {
		return fmt.Errorf("failed to prune old blocks: %w", err)
	}
	return nil
}

// PruneOldReorgEvents deletes reorg events older than the given time.
func (r *BlockStateRepository) PruneOldReorgEvents(ctx context.Context, olderThan time.Time) error {
	query := `DELETE FROM reorg_events WHERE detected_at < $1`
	_, err := r.db.ExecContext(ctx, query, olderThan)
	if err != nil {
		return fmt.Errorf("failed to prune old reorg events: %w", err)
	}
	return nil
}

// GetMinBlockNumber returns the lowest canonical block number.
func (r *BlockStateRepository) GetMinBlockNumber(ctx context.Context) (int64, error) {
	query := `SELECT COALESCE(MIN(number), 0) FROM block_states WHERE NOT is_orphaned`
	var minNum int64
	err := r.db.QueryRowContext(ctx, query).Scan(&minNum)
	if err != nil {
		return 0, fmt.Errorf("failed to get min block number: %w", err)
	}
	return minNum, nil
}

// GetMaxBlockNumber returns the highest canonical block number.
func (r *BlockStateRepository) GetMaxBlockNumber(ctx context.Context) (int64, error) {
	query := `SELECT COALESCE(MAX(number), 0) FROM block_states WHERE NOT is_orphaned`
	var maxNum int64
	err := r.db.QueryRowContext(ctx, query).Scan(&maxNum)
	if err != nil {
		return 0, fmt.Errorf("failed to get max block number: %w", err)
	}
	return maxNum, nil
}

// GetBackfillWatermark returns the highest block number that has been verified as gap-free.
// Blocks at or below this number are guaranteed to have no gaps.
func (r *BlockStateRepository) GetBackfillWatermark(ctx context.Context) (int64, error) {
	var watermark int64
	err := r.db.QueryRowContext(ctx, `SELECT watermark FROM backfill_watermark WHERE id = 1`).Scan(&watermark)
	if err != nil {
		return 0, fmt.Errorf("failed to get backfill watermark: %w", err)
	}
	return watermark, nil
}

// SetBackfillWatermark updates the watermark to the given block number.
// Should only be called after confirming all blocks up to this number exist.
func (r *BlockStateRepository) SetBackfillWatermark(ctx context.Context, watermark int64) error {
	_, err := r.db.ExecContext(ctx, `UPDATE backfill_watermark SET watermark = $1 WHERE id = 1`, watermark)
	if err != nil {
		return fmt.Errorf("failed to set backfill watermark: %w", err)
	}
	return nil
}

// FindGaps finds missing block ranges between minBlock and maxBlock.
// Uses the backfill watermark to skip already-verified blocks, making this O(n) only
// for blocks above the watermark rather than the entire table.
func (r *BlockStateRepository) FindGaps(ctx context.Context, minBlock, maxBlock int64) ([]outbound.BlockRange, error) {
	if minBlock > maxBlock {
		return nil, nil
	}

	// Get the watermark - we only need to scan above this point
	watermark, err := r.GetBackfillWatermark(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get watermark: %w", err)
	}

	// Adjust minBlock to start from watermark+1 if watermark is higher
	effectiveMin := minBlock
	if watermark >= minBlock {
		effectiveMin = watermark + 1
	}

	// If the watermark already covers the entire range, no gaps possible
	if effectiveMin > maxBlock {
		return nil, nil
	}

	// This query finds gaps using window functions:
	// 1. Get all canonical block numbers in the range
	// 2. Use LAG to get the previous block number
	// 3. Where current - previous > 1, we have a gap
	query := `
		WITH blocks AS (
			SELECT number
			FROM block_states
			WHERE NOT is_orphaned AND number >= $1 AND number <= $2
			ORDER BY number
		),
		gaps AS (
			SELECT 
				LAG(number) OVER (ORDER BY number) + 1 AS gap_start,
				number - 1 AS gap_end
			FROM blocks
		)
		SELECT gap_start, gap_end
		FROM gaps
		WHERE gap_start IS NOT NULL AND gap_end >= gap_start
		ORDER BY gap_start
	`

	rows, err := r.db.QueryContext(ctx, query, effectiveMin, maxBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to find gaps: %w", err)
	}
	defer rows.Close()

	var gaps []outbound.BlockRange
	for rows.Next() {
		var gap outbound.BlockRange
		if err := rows.Scan(&gap.From, &gap.To); err != nil {
			return nil, fmt.Errorf("failed to scan gap: %w", err)
		}
		gaps = append(gaps, gap)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating gaps: %w", err)
	}

	// Also check for gap at the beginning (if effectiveMin is not in the DB)
	var firstBlock int64
	checkQuery := `SELECT COALESCE(MIN(number), $2 + 1) FROM block_states WHERE NOT is_orphaned AND number >= $1 AND number <= $2`
	if err := r.db.QueryRowContext(ctx, checkQuery, effectiveMin, maxBlock).Scan(&firstBlock); err != nil {
		return nil, fmt.Errorf("failed to check first block: %w", err)
	}
	if firstBlock > effectiveMin {
		gaps = append([]outbound.BlockRange{{From: effectiveMin, To: firstBlock - 1}}, gaps...)
	}

	return gaps, nil
}
