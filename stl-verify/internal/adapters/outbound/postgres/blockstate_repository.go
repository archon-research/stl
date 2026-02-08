// blockstate_repository.go provides a PostgreSQL implementation of BlockStateRepository.
//
// This adapter persists block states and reorg events to PostgreSQL for
// durable storage. It supports:
//   - Block state persistence with upsert semantics (ON CONFLICT UPDATE)
//   - Canonical and orphaned block tracking
//   - Reorg event recording for chain reorganization history
//   - Gap detection queries for backfill operations
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const tracerName = "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"

// Compile-time check that BlockStateRepository implements outbound.BlockStateRepository
var _ outbound.BlockStateRepository = (*BlockStateRepository)(nil)

// BlockStateRepository is a PostgreSQL implementation of the outbound.BlockStateRepository port.
type BlockStateRepository struct {
	pool    *pgxpool.Pool
	chainID int64
	logger  *slog.Logger
}

// NewBlockStateRepository creates a new PostgreSQL block state repository.
func NewBlockStateRepository(pool *pgxpool.Pool, chainID int64, logger *slog.Logger) *BlockStateRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &BlockStateRepository{pool: pool, chainID: chainID, logger: logger}
}

// Pool returns the underlying database pool for advanced queries.
func (r *BlockStateRepository) Pool() *pgxpool.Pool {
	return r.pool
}

// SaveBlock persists a block's state with atomic version assignment.
// Uses INSERT ... ON CONFLICT DO NOTHING to handle concurrent inserts safely.
// If the block already exists (by hash), returns its existing version.
// If it's a new block, the database trigger assigns the version atomically.
// The provided state.Version is ignored; the actual assigned version is returned.
func (r *BlockStateRepository) SaveBlock(ctx context.Context, state outbound.BlockState) (int, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "postgres.SaveBlock",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "INSERT"),
			attribute.String("db.table", "block_states"),
			attribute.Int64("block.number", state.Number),
			attribute.String("block.hash", state.Hash),
		),
	)
	defer span.End()

	cfg := retry.Config{
		MaxRetries:     10,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         true,
	}

	onRetry := func(attempt int, err error, backoff time.Duration) {
		r.logger.Debug("serialization failure, retrying",
			"attempt", attempt,
			"block", state.Number,
			"hash", state.Hash,
			"backoff", backoff)
		span.AddEvent("retry_attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", err.Error()),
		))
	}

	version, err := retry.Do(ctx, cfg, isSerializationFailure, onRetry, func() (int, error) {
		return r.saveBlockOnce(ctx, state)
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "SaveBlock failed")
	}
	return version, err
}

// saveBlockOnce attempts a single save operation with serializable isolation.
func (r *BlockStateRepository) saveBlockOnce(ctx context.Context, state outbound.BlockState) (int, error) {
	// Use a transaction with SERIALIZABLE isolation to prevent version race conditions.
	// This ensures that concurrent SaveBlock calls for the same block number will be serialized.
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			r.logger.Warn("failed to rollback transaction", "error", err)
		}
	}()

	// Check if a block with this hash already exists (duplicate detection).
	// Done inside the serializable transaction to prevent TOCTOU races.
	var existingVersion int
	err = tx.QueryRow(ctx, `SELECT version FROM block_states WHERE hash = $1`, state.Hash).Scan(&existingVersion)
	if err == nil {
		// Block already exists - return its version without updating
		return existingVersion, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("failed to check for existing block: %w", err)
	}

	// Insert the block - the trigger will assign the version automatically.
	// RETURNING gives us the version that was assigned by the trigger.
	query := `
		INSERT INTO block_states (chain_id, number, hash, parent_hash, received_at, is_orphaned)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING version
	`
	var version int
	err = tx.QueryRow(ctx, query, r.chainID, state.Number, state.Hash, state.ParentHash, state.ReceivedAt, state.IsOrphaned).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to save block state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return version, nil
}

// isSerializationFailure checks if the error is a PostgreSQL serialization failure (SQLSTATE 40001).
func isSerializationFailure(err error) bool {
	if err == nil {
		return false
	}
	// Use pgx's structured error type to check SQLSTATE code directly
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// SQLSTATE 40001 = serialization_failure
		return pgErr.Code == "40001"
	}
	return false
}

// isRetryableError checks if an error should trigger a retry.
// Retries on any error except context cancellation (shutdown signal).
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Don't retry on context cancellation (shutdown)
	if errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

// GetLastBlock retrieves the most recently saved canonical (non-orphaned) block state.
func (r *BlockStateRepository) GetLastBlock(ctx context.Context) (*outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned, version, block_published
		FROM block_states
		WHERE chain_id = $1 AND NOT is_orphaned
		ORDER BY number DESC
		LIMIT 1
	`
	var state outbound.BlockState
	err := r.pool.QueryRow(ctx, query, r.chainID).Scan(
		&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
		&state.BlockPublished)
	if errors.Is(err, pgx.ErrNoRows) {
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
		SELECT number, hash, parent_hash, received_at, is_orphaned, version, block_published
		FROM block_states
		WHERE chain_id = $1 AND number = $2 AND NOT is_orphaned
	`
	var state outbound.BlockState
	err := r.pool.QueryRow(ctx, query, r.chainID, number).Scan(
		&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
		&state.BlockPublished)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get block by number: %w", err)
	}
	return &state, nil
}

// GetBlockByHash retrieves a block state by its hash (includes orphaned blocks).
func (r *BlockStateRepository) GetBlockByHash(ctx context.Context, hash string) (*outbound.BlockState, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "postgres.GetBlockByHash",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "SELECT"),
			attribute.String("db.table", "block_states"),
			attribute.String("block.hash", hash),
		),
	)
	defer span.End()

	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned, version, block_published
		FROM block_states
		WHERE hash = $1
	`
	var state outbound.BlockState
	err := r.pool.QueryRow(ctx, query, hash).Scan(
		&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
		&state.BlockPublished)
	if errors.Is(err, pgx.ErrNoRows) {
		span.SetAttributes(attribute.Bool("db.row_found", false))
		return nil, nil
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get block by hash")
		return nil, fmt.Errorf("failed to get block by hash: %w", err)
	}
	span.SetAttributes(attribute.Bool("db.row_found", true))
	return &state, nil
}

// GetBlockVersionCount returns the next version number for blocks at a given number.
// If no blocks exist at that number, returns 0. Otherwise returns MAX(version) + 1.
func (r *BlockStateRepository) GetBlockVersionCount(ctx context.Context, number int64) (int, error) {
	query := `SELECT COALESCE(MAX(version), -1) + 1 FROM block_states WHERE chain_id = $1 AND number = $2`
	var count int
	err := r.pool.QueryRow(ctx, query, r.chainID, number).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get block version count: %w", err)
	}
	return count, nil
}

// GetRecentBlocks retrieves the N most recent canonical blocks.
func (r *BlockStateRepository) GetRecentBlocks(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned, version, block_published
		FROM block_states
		WHERE chain_id = $1 AND NOT is_orphaned
		ORDER BY number DESC
		LIMIT $2
	`
	rows, err := r.pool.Query(ctx, query, r.chainID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent blocks: %w", err)
	}
	defer rows.Close()

	var states []outbound.BlockState
	for rows.Next() {
		var state outbound.BlockState
		if err := rows.Scan(
			&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
			&state.BlockPublished); err != nil {
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
	_, err := r.pool.Exec(ctx, query, hash)
	if err != nil {
		return fmt.Errorf("failed to mark block orphaned: %w", err)
	}
	return nil
}

// HandleReorgAtomic atomically performs all reorg-related database operations in a single transaction.
// This ensures consistency: either all operations succeed, or none do.
// The commonAncestor is derived from the ReorgEvent (BlockNumber - Depth).
//
// Uses SERIALIZABLE isolation (consistent with SaveBlock) and includes retry logic
// for transient serialization failures (SQLSTATE 40001).
func (r *BlockStateRepository) HandleReorgAtomic(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "postgres.HandleReorgAtomic",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "TRANSACTION"),
			attribute.String("db.table", "block_states"),
			attribute.Int64("block.number", newBlock.Number),
			attribute.String("block.hash", newBlock.Hash),
			attribute.Int64("reorg.common_ancestor", commonAncestor),
			attribute.Int("reorg.depth", event.Depth),
		),
	)
	defer span.End()

	cfg := retry.Config{
		MaxRetries:     10,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         true,
	}

	onRetry := func(attempt int, err error, backoff time.Duration) {
		r.logger.Debug("serialization failure in HandleReorgAtomic, retrying",
			"attempt", attempt,
			"block", newBlock.Number,
			"hash", newBlock.Hash,
			"backoff", backoff)
		span.AddEvent("retry_attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", err.Error()),
		))
	}

	version, err := retry.Do(ctx, cfg, isSerializationFailure, onRetry, func() (int, error) {
		return r.handleReorgAtomicOnce(ctx, commonAncestor, event, newBlock)
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "HandleReorgAtomic failed")
	}
	return version, err
}

// handleReorgAtomicOnce attempts a single reorg operation with SERIALIZABLE isolation.
func (r *BlockStateRepository) handleReorgAtomicOnce(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	// Use SERIALIZABLE isolation for consistency with SaveBlock
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			r.logger.Warn("failed to rollback transaction", "error", err)
		}
	}()

	// 1. Acquire advisory lock namespaced by chain_id and block number
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, (r.chainID<<40)|newBlock.Number)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire advisory lock: %w", err)
	}

	// 2. Check if this block hash already exists (idempotency)
	var existingVersion int
	err = tx.QueryRow(ctx, `SELECT version FROM block_states WHERE hash = $1`, newBlock.Hash).Scan(&existingVersion)
	if err == nil {
		// Block already exists - commit and return existing version
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return 0, fmt.Errorf("failed to commit transaction: %w", commitErr)
		}
		return existingVersion, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("failed to check for existing block: %w", err)
	}

	// 3. Save reorg event
	reorgQuery := `
		INSERT INTO reorg_events (chain_id, detected_at, block_number, old_hash, new_hash, depth)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = tx.Exec(ctx, reorgQuery, r.chainID, event.DetectedAt, event.BlockNumber, event.OldHash, event.NewHash, event.Depth)
	if err != nil {
		return 0, fmt.Errorf("failed to save reorg event: %w", err)
	}

	// 4. Mark old blocks as orphaned
	orphanQuery := `UPDATE block_states SET is_orphaned = TRUE WHERE chain_id = $1 AND number > $2 AND NOT is_orphaned`
	_, err = tx.Exec(ctx, orphanQuery, r.chainID, commonAncestor)
	if err != nil {
		return 0, fmt.Errorf("failed to mark blocks orphaned: %w", err)
	}

	// 5. Insert new canonical block
	// We pass 0 as the version; the BEFORE INSERT trigger will automatically assign
	// the correct version (MAX(version) + 1) atomically.
	// We use RETURNING version to get the actually assigned version.
	insertQuery := `
		INSERT INTO block_states (chain_id, number, hash, parent_hash, received_at, is_orphaned, version)
		VALUES ($1, $2, $3, $4, $5, $6, 0)
		RETURNING version
	`
	var version int
	err = tx.QueryRow(ctx, insertQuery, r.chainID, newBlock.Number, newBlock.Hash, newBlock.ParentHash, newBlock.ReceivedAt, newBlock.IsOrphaned).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to save new block state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return version, nil
}

// GetReorgEvents retrieves reorg events, ordered by detection time descending.
func (r *BlockStateRepository) GetReorgEvents(ctx context.Context, limit int) ([]outbound.ReorgEvent, error) {
	query := `
		SELECT id, detected_at, block_number, old_hash, new_hash, depth
		FROM reorg_events
		WHERE chain_id = $1
		ORDER BY detected_at DESC
		LIMIT $2
	`
	rows, err := r.pool.Query(ctx, query, r.chainID, limit)
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
		WHERE chain_id = $1 AND block_number >= $2 AND block_number <= $3
		ORDER BY block_number DESC, detected_at DESC
	`
	rows, err := r.pool.Query(ctx, query, r.chainID, fromBlock, toBlock)
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
		SELECT number, hash, parent_hash, received_at, is_orphaned, version, block_published
		FROM block_states
		WHERE chain_id = $1 AND is_orphaned
		ORDER BY received_at DESC
		LIMIT $2
	`
	rows, err := r.pool.Query(ctx, query, r.chainID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get orphaned blocks: %w", err)
	}
	defer rows.Close()

	var states []outbound.BlockState
	for rows.Next() {
		var state outbound.BlockState
		if err := rows.Scan(
			&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
			&state.BlockPublished); err != nil {
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
	query := `DELETE FROM block_states WHERE chain_id = $1 AND number < $2 AND NOT is_orphaned`
	_, err := r.pool.Exec(ctx, query, r.chainID, keepAfter)
	if err != nil {
		return fmt.Errorf("failed to prune old blocks: %w", err)
	}
	return nil
}

// PruneOldReorgEvents deletes reorg events older than the given time.
func (r *BlockStateRepository) PruneOldReorgEvents(ctx context.Context, olderThan time.Time) error {
	query := `DELETE FROM reorg_events WHERE chain_id = $1 AND detected_at < $2`
	_, err := r.pool.Exec(ctx, query, r.chainID, olderThan)
	if err != nil {
		return fmt.Errorf("failed to prune old reorg events: %w", err)
	}
	return nil
}

// GetMinBlockNumber returns the lowest canonical block number.
func (r *BlockStateRepository) GetMinBlockNumber(ctx context.Context) (int64, error) {
	query := `SELECT COALESCE(MIN(number), 0) FROM block_states WHERE chain_id = $1 AND NOT is_orphaned`
	var minNum int64
	err := r.pool.QueryRow(ctx, query, r.chainID).Scan(&minNum)
	if err != nil {
		return 0, fmt.Errorf("failed to get min block number: %w", err)
	}
	return minNum, nil
}

// GetMaxBlockNumber returns the highest canonical block number.
func (r *BlockStateRepository) GetMaxBlockNumber(ctx context.Context) (int64, error) {
	query := `SELECT COALESCE(MAX(number), 0) FROM block_states WHERE chain_id = $1 AND NOT is_orphaned`
	var maxNum int64
	err := r.pool.QueryRow(ctx, query, r.chainID).Scan(&maxNum)
	if err != nil {
		return 0, fmt.Errorf("failed to get max block number: %w", err)
	}
	return maxNum, nil
}

// GetBackfillWatermark returns the highest block number that has been verified as gap-free.
// Blocks at or below this number are guaranteed to have no gaps.
func (r *BlockStateRepository) GetBackfillWatermark(ctx context.Context) (int64, error) {
	var watermark int64
	err := r.pool.QueryRow(ctx, `SELECT watermark FROM backfill_watermark WHERE chain_id = $1`, r.chainID).Scan(&watermark)
	if err != nil {
		return 0, fmt.Errorf("failed to get backfill watermark: %w", err)
	}
	return watermark, nil
}

// SetBackfillWatermark updates the watermark to the given block number.
// Should only be called after confirming all blocks up to this number exist.
func (r *BlockStateRepository) SetBackfillWatermark(ctx context.Context, watermark int64) error {
	_, err := r.pool.Exec(ctx, `UPDATE backfill_watermark SET watermark = $1 WHERE chain_id = $2`, watermark, r.chainID)
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
			WHERE chain_id = $1 AND NOT is_orphaned AND number >= $2 AND number <= $3
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

	rows, err := r.pool.Query(ctx, query, r.chainID, effectiveMin, maxBlock)
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
	checkQuery := `SELECT COALESCE(MIN(number), $3 + 1) FROM block_states WHERE chain_id = $1 AND NOT is_orphaned AND number >= $2 AND number <= $3`
	if err := r.pool.QueryRow(ctx, checkQuery, r.chainID, effectiveMin, maxBlock).Scan(&firstBlock); err != nil {
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
			WHERE chain_id = $1 AND NOT is_orphaned AND number >= $2 AND number <= $3
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

	err := r.pool.QueryRow(ctx, query, r.chainID, fromBlock, toBlock).Scan(
		&brokenBlock, &blockHash, &parentHash, &prevHash, &prevBlockNum,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // Chain is valid
		}
		return fmt.Errorf("failed to verify chain integrity: %w", err)
	}

	return fmt.Errorf("chain integrity violation at block %d: parent_hash %s does not match hash %s of block %d",
		brokenBlock, parentHash, prevHash, prevBlockNum)
}

// MarkPublishComplete marks a block as published.
// Includes retry logic for transient database errors.
func (r *BlockStateRepository) MarkPublishComplete(ctx context.Context, hash string) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "postgres.MarkPublishComplete",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "UPDATE"),
			attribute.String("db.table", "block_states"),
			attribute.String("block.hash", hash),
		),
	)
	defer span.End()

	query := `UPDATE block_states SET block_published = TRUE WHERE hash = $1`

	cfg := retry.Config{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         true,
	}

	onRetry := func(attempt int, err error, backoff time.Duration) {
		r.logger.Debug("retrying MarkPublishComplete",
			"attempt", attempt,
			"hash", hash,
			"backoff", backoff,
			"error", err)
		span.AddEvent("retry_attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", err.Error()),
		))
	}

	err := retry.DoVoid(ctx, cfg, isRetryableError, onRetry, func() error {
		result, err := r.pool.Exec(ctx, query, hash)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			return fmt.Errorf("block with hash %s not found", hash)
		}
		return nil
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to mark published")
		return fmt.Errorf("failed to mark block published: %w", err)
	}

	return nil
}

// GetBlocksWithIncompletePublish returns canonical blocks that have not been published.
// Used by backfill to recover from crashes.
func (r *BlockStateRepository) GetBlocksWithIncompletePublish(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	query := `
		SELECT number, hash, parent_hash, received_at, is_orphaned, version, block_published
		FROM block_states
		WHERE chain_id = $1
		  AND NOT is_orphaned
		  AND NOT block_published
		ORDER BY number ASC
		LIMIT $2
	`

	rows, err := r.pool.Query(ctx, query, r.chainID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get blocks with incomplete publish: %w", err)
	}
	defer rows.Close()

	var states []outbound.BlockState
	for rows.Next() {
		var state outbound.BlockState
		if err := rows.Scan(
			&state.Number, &state.Hash, &state.ParentHash, &state.ReceivedAt, &state.IsOrphaned, &state.Version,
			&state.BlockPublished); err != nil {
			return nil, fmt.Errorf("failed to scan block state: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating block states: %w", err)
	}
	return states, nil
}
