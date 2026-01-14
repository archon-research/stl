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
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

//go:embed migrations/001_initial_schema.sql
var initialSchema string

// Compile-time check that BlockStateRepository implements outbound.BlockStateRepository
var _ outbound.BlockStateRepository = (*BlockStateRepository)(nil)

// BlockStateRepository is a PostgreSQL implementation of the outbound.BlockStateRepository port.
type BlockStateRepository struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewBlockStateRepository creates a new PostgreSQL block state repository.
func NewBlockStateRepository(db *sql.DB, logger *slog.Logger) *BlockStateRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &BlockStateRepository{db: db, logger: logger}
}

// DB returns the underlying database connection for advanced queries.
func (r *BlockStateRepository) DB() *sql.DB {
	return r.db
}

// Migrate creates the block_states and reorg_events tables if they don't exist.
func (r *BlockStateRepository) Migrate(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, initialSchema)
	if err != nil {
		return fmt.Errorf("failed to migrate schema: %w", err)
	}
	return nil
}

// SaveBlock persists a block's state with atomic version assignment.
// The version is automatically assigned by a database trigger, ensuring uniqueness.
// The provided state.Version is ignored; the actual assigned version is returned.
// If a concurrent insert causes a unique constraint violation, the function retries
// with exponential backoff and jitter.
func (r *BlockStateRepository) SaveBlock(ctx context.Context, state outbound.BlockState) (int, error) {
	const (
		maxRetries     = 5
		baseDelay      = 10 * time.Millisecond
		maxDelay       = 50 * time.Millisecond
		jitterFraction = 0.5 // Add up to 50% jitter
	)

	for attempt := 0; attempt < maxRetries; attempt++ {
		version, err := r.trySaveBlock(ctx, state)
		if err == nil {
			return version, nil
		}

		// Check if this is a unique constraint violation (PostgreSQL error code 23505)
		// This can happen when concurrent inserts race for the same version number
		if isUniqueViolation(err) {
			r.logger.Debug("SaveBlock retry due to version conflict",
				"attempt", attempt+1,
				"block", state.Number,
				"hash", state.Hash)

			// Calculate exponential backoff with jitter
			delay := baseDelay * time.Duration(1<<attempt) // 2^attempt * baseDelay
			if delay > maxDelay {
				delay = maxDelay
			}
			// Add jitter: delay * (1 + random(0, jitterFraction))
			jitter := time.Duration(float64(delay) * jitterFraction * rand.Float64())
			delay += jitter

			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}

		return 0, err
	}

	return 0, fmt.Errorf("failed to save block after %d retries due to concurrent version conflicts", maxRetries)
}

// isUniqueViolation checks if the error is a PostgreSQL unique constraint violation.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	// PostgreSQL unique violation error contains "SQLSTATE 23505" or "unique constraint"
	errStr := err.Error()
	return strings.Contains(errStr, "23505") || strings.Contains(errStr, "unique constraint")
}

// trySaveBlock attempts to save a block once.
func (r *BlockStateRepository) trySaveBlock(ctx context.Context, state outbound.BlockState) (int, error) {
	// Check if a block with this hash already exists (duplicate detection).
	// If so, return the existing version without modifying the data.
	var existingVersion int
	err := r.db.QueryRowContext(ctx, `SELECT version FROM block_states WHERE hash = $1`, state.Hash).Scan(&existingVersion)
	if err == nil {
		// Block already exists - return its version without updating
		return existingVersion, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to check for existing block: %w", err)
	}

	// Insert the block - the trigger will assign the version automatically.
	// RETURNING gives us the version that was assigned by the trigger.
	query := `
		INSERT INTO block_states (number, hash, parent_hash, received_at, is_orphaned)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING version
	`
	var version int
	err = r.db.QueryRowContext(ctx, query, state.Number, state.Hash, state.ParentHash, state.ReceivedAt, state.IsOrphaned).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to save block state: %w", err)
	}

	return version, nil
}

// GetLastBlock retrieves the most recently saved canonical (non-orphaned) block state.
func (r *BlockStateRepository) GetLastBlock(ctx context.Context) (*outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned, version,
		       block_published, receipts_published, traces_published, blobs_published
		FROM block_states
		WHERE NOT is_orphaned
		ORDER BY number DESC
		LIMIT 1
	`
	var state outbound.BlockState
	err := r.db.QueryRowContext(ctx, query).Scan(
		&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
		&state.BlockPublished, &state.ReceiptsPublished, &state.TracesPublished, &state.BlobsPublished)
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
		SELECT number, hash, parent_hash, received_at, is_orphaned, version,
		       block_published, receipts_published, traces_published, blobs_published
		FROM block_states
		WHERE number = $1 AND NOT is_orphaned
	`
	var state outbound.BlockState
	err := r.db.QueryRowContext(ctx, query, number).Scan(
		&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
		&state.BlockPublished, &state.ReceiptsPublished, &state.TracesPublished, &state.BlobsPublished)
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
		SELECT number, hash, parent_hash, received_at, is_orphaned, version,
		       block_published, receipts_published, traces_published, blobs_published
		FROM block_states
		WHERE hash = $1
	`
	var state outbound.BlockState
	err := r.db.QueryRowContext(ctx, query, hash).Scan(
		&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
		&state.BlockPublished, &state.ReceiptsPublished, &state.TracesPublished, &state.BlobsPublished)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get block by hash: %w", err)
	}
	return &state, nil
}

// GetBlockVersionCount returns the next version number for blocks at a given number.
// If no blocks exist at that number, returns 0. Otherwise returns MAX(version) + 1.
func (r *BlockStateRepository) GetBlockVersionCount(ctx context.Context, number int64) (int, error) {
	query := `SELECT COALESCE(MAX(version), -1) + 1 FROM block_states WHERE number = $1`
	var count int
	err := r.db.QueryRowContext(ctx, query, number).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get block version count: %w", err)
	}
	return count, nil
}

// GetRecentBlocks retrieves the N most recent canonical blocks.
func (r *BlockStateRepository) GetRecentBlocks(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned, version,
		       block_published, receipts_published, traces_published, blobs_published
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
		if err := rows.Scan(
			&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
			&state.BlockPublished, &state.ReceiptsPublished, &state.TracesPublished, &state.BlobsPublished); err != nil {
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

// HandleReorgAtomic atomically performs all reorg-related database operations in a single transaction.
// This ensures consistency: either all operations succeed, or none do.
// The commonAncestor is derived from the ReorgEvent (BlockNumber - Depth).
func (r *BlockStateRepository) HandleReorgAtomic(ctx context.Context, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			r.logger.Error("failed to rollback transaction", "error", err)
		}
	}()

	// Calculate common ancestor from event
	commonAncestor := event.BlockNumber - int64(event.Depth)

	// 1. Acquire advisory lock for the new block number
	_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, newBlock.Number)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire advisory lock: %w", err)
	}

	// 2. Check if this block hash already exists (idempotency)
	var existingVersion int
	err = tx.QueryRowContext(ctx, `SELECT version FROM block_states WHERE hash = $1`, newBlock.Hash).Scan(&existingVersion)
	if err == nil {
		// Block already exists - commit and return existing version
		if commitErr := tx.Commit(); commitErr != nil {
			return 0, fmt.Errorf("failed to commit transaction: %w", commitErr)
		}
		return existingVersion, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to check for existing block: %w", err)
	}

	// 3. Save reorg event
	reorgQuery := `
		INSERT INTO reorg_events (detected_at, block_number, old_hash, new_hash, depth)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err = tx.ExecContext(ctx, reorgQuery, event.DetectedAt, event.BlockNumber, event.OldHash, event.NewHash, event.Depth)
	if err != nil {
		return 0, fmt.Errorf("failed to save reorg event: %w", err)
	}

	// 4. Mark old blocks as orphaned
	orphanQuery := `UPDATE block_states SET is_orphaned = TRUE WHERE number > $1 AND NOT is_orphaned`
	_, err = tx.ExecContext(ctx, orphanQuery, commonAncestor)
	if err != nil {
		return 0, fmt.Errorf("failed to mark blocks orphaned: %w", err)
	}

	// 5. Calculate version for new block
	var version int
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), -1) + 1 FROM block_states WHERE number = $1`, newBlock.Number).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to get next version: %w", err)
	}

	// 6. Insert new canonical block
	insertQuery := `
		INSERT INTO block_states (number, hash, parent_hash, received_at, is_orphaned, version)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = tx.ExecContext(ctx, insertQuery, newBlock.Number, newBlock.Hash, newBlock.ParentHash, newBlock.ReceivedAt, newBlock.IsOrphaned, version)
	if err != nil {
		return 0, fmt.Errorf("failed to save new block state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return version, nil
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

// VerifyChainIntegrity verifies that the parent_hash chain is properly linked.
// It checks that each block's parent_hash matches the hash of the previous block number.
// Returns nil if the chain is valid, or an error describing the first broken link.
func (r *BlockStateRepository) VerifyChainIntegrity(ctx context.Context, fromBlock, toBlock int64) error {
	if fromBlock >= toBlock {
		return nil // Nothing to verify
	}

	// Query that finds the first block where parent_hash doesn't match
	// the hash of the previous block number
	query := `
		WITH ordered_blocks AS (
			SELECT number, hash, parent_hash,
				LAG(hash) OVER (ORDER BY number) as prev_hash,
				LAG(number) OVER (ORDER BY number) as prev_number
			FROM block_states
			WHERE NOT is_orphaned AND number >= $1 AND number <= $2
		)
		SELECT number, hash, parent_hash, prev_hash, prev_number
		FROM ordered_blocks
		WHERE prev_hash IS NOT NULL
			AND prev_number = number - 1
			AND parent_hash != prev_hash
		LIMIT 1
	`

	var brokenBlock, prevBlockNum int64
	var blockHash, parentHash, prevHash string

	err := r.db.QueryRowContext(ctx, query, fromBlock, toBlock).Scan(
		&brokenBlock, &blockHash, &parentHash, &prevHash, &prevBlockNum,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // Chain is valid
		}
		return fmt.Errorf("failed to verify chain integrity: %w", err)
	}

	return fmt.Errorf("chain integrity violation at block %d: parent_hash %s does not match hash %s of block %d",
		brokenBlock, parentHash, prevHash, prevBlockNum)
}

// MarkPublishComplete marks a specific publish type as completed for a block.
func (r *BlockStateRepository) MarkPublishComplete(ctx context.Context, hash string, publishType outbound.PublishType) error {
	var column string
	switch publishType {
	case outbound.PublishTypeBlock:
		column = "block_published"
	case outbound.PublishTypeReceipts:
		column = "receipts_published"
	case outbound.PublishTypeTraces:
		column = "traces_published"
	case outbound.PublishTypeBlobs:
		column = "blobs_published"
	default:
		return fmt.Errorf("unknown publish type: %s", publishType)
	}

	query := fmt.Sprintf(`UPDATE block_states SET %s = TRUE WHERE hash = $1`, column)
	result, err := r.db.ExecContext(ctx, query, hash)
	if err != nil {
		return fmt.Errorf("failed to mark %s published: %w", publishType, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("block with hash %s not found", hash)
	}

	return nil
}

// GetBlocksWithIncompletePublish returns canonical blocks that have at least one
// publish type incomplete. Used by backfill to recover from crashes.
func (r *BlockStateRepository) GetBlocksWithIncompletePublish(ctx context.Context, limit int, disableBlobs bool) ([]outbound.BlockState, error) {
	var query string
	if disableBlobs {
		// Don't consider blobs_published when blobs are disabled
		query = `
			SELECT number, hash, parent_hash, received_at, is_orphaned, version,
			       block_published, receipts_published, traces_published, blobs_published
			FROM block_states
			WHERE NOT is_orphaned
			  AND (NOT block_published OR NOT receipts_published OR NOT traces_published)
			ORDER BY number ASC
			LIMIT $1
		`
	} else {
		query = `
			SELECT number, hash, parent_hash, received_at, is_orphaned, version,
			       block_published, receipts_published, traces_published, blobs_published
			FROM block_states
			WHERE NOT is_orphaned
			  AND (NOT block_published OR NOT receipts_published OR NOT traces_published OR NOT blobs_published)
			ORDER BY number ASC
			LIMIT $1
		`
	}

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get blocks with incomplete publish: %w", err)
	}
	defer rows.Close()

	var states []outbound.BlockState
	for rows.Next() {
		var state outbound.BlockState
		if err := rows.Scan(
			&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
			&state.BlockPublished, &state.ReceiptsPublished, &state.TracesPublished, &state.BlobsPublished); err != nil {
			return nil, fmt.Errorf("failed to scan block state: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating block states: %w", err)
	}
	return states, nil
}
