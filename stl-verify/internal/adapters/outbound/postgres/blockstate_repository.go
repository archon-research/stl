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
	if state.BlockTimestamp == 0 {
		return 0, fmt.Errorf("BlockTimestamp is required (used as created_at for hypertable partitioning)")
	}

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
		r.logger.Debug("retryable tx error, retrying",
			"attempt", attempt,
			"block", state.Number,
			"hash", state.Hash,
			"backoff", backoff)
		span.AddEvent("retry_attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", err.Error()),
		))
	}

	version, err := retry.Do(ctx, cfg, isRetryableTxError, onRetry, func() (int, error) {
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
	// Use READ COMMITTED isolation with an advisory lock to serialize version assignment.
	// TimescaleDB hypertables use chunk-level constraints, so SERIALIZABLE isolation
	// alone cannot detect version races in the assign_block_version() trigger — it
	// produces unique constraint violations (23505) instead of serialization failures
	// (40001). The advisory lock serializes inserts for the same (chain_id, number),
	// and READ COMMITTED allows the trigger to see committed changes from the
	// lock-holder (SERIALIZABLE snapshots would not).
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			r.logger.Warn("failed to rollback transaction", "error", err)
		}
	}()

	// Acquire advisory lock to serialize version assignment for this block number.
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1::int, $2::int)`, r.chainID, state.Number)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire advisory lock: %w", err)
	}

	// Check if a block with this hash already exists (duplicate detection).
	// Done inside the serializable transaction to prevent TOCTOU races.
	var existingVersion int
	err = tx.QueryRow(ctx, `SELECT version FROM block_states WHERE chain_id = $1 AND hash = $2`, r.chainID, state.Hash).Scan(&existingVersion)
	if err == nil {
		// Block already exists - return its version without updating
		return existingVersion, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("failed to check for existing block: %w", err)
	}

	// Insert the block - the trigger will assign the version automatically.
	// RETURNING gives us the version that was assigned by the trigger.
	query := `
		INSERT INTO block_states (chain_id, number, hash, parent_hash, received_at, is_orphaned, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING version
	`
	var version int
	err = tx.QueryRow(ctx, query, r.chainID, state.Number, state.Hash, state.ParentHash, state.ReceivedAt, state.IsOrphaned, time.Unix(state.BlockTimestamp, 0).UTC()).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to save block state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return version, nil
}

// isRetryableTxError checks if the error is a PostgreSQL serialization failure (SQLSTATE 40001)
// or deadlock (SQLSTATE 40P01). Both are transient and safe to retry.
func isRetryableTxError(err error) bool {
	if err == nil {
		return false
	}
	// Use pgx's structured error type to check SQLSTATE code directly
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// SQLSTATE 40001 = serialization_failure
		// SQLSTATE 40P01 = deadlock_detected
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
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
		WHERE chain_id = $1 AND hash = $2
	`
	var state outbound.BlockState
	err := r.pool.QueryRow(ctx, query, r.chainID, hash).Scan(
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
	query := `UPDATE block_states SET is_orphaned = TRUE WHERE chain_id = $1 AND hash = $2`
	_, err := r.pool.Exec(ctx, query, r.chainID, hash)
	if err != nil {
		return fmt.Errorf("failed to mark block orphaned: %w", err)
	}
	return nil
}

// ClearBlockOrphaned clears the is_orphaned flag on a block identified by hash.
// Used by the backfill loop to self-heal rows that were over-orphaned by a
// previous reorg whose new canonical chain actually contained the same hash.
// Idempotent: clearing an already-canonical row is a no-op.
// Returns an error if the block does not exist.
//
// Runs inside a transaction that takes the same per-(chain_id, number)
// advisory lock used by saveBlockOnce and handleReorgAtomicOnce. Without the
// lock a concurrent live reorg can insert a new canonical row at the same
// number between this method's lookup and update, leaving two non-orphaned
// rows at one height and breaking the "highest version = canonical"
// invariant. The lookup is split from the UPDATE (two statements) to avoid
// the TimescaleDB XX000 quirk on self-referencing UPDATE-with-SELECT-from-
// same-hypertable that the external review flagged. See VEC-277 / PR #373
// review Finding 6.
func (r *BlockStateRepository) ClearBlockOrphaned(ctx context.Context, hash string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && rbErr != pgx.ErrTxClosed {
			r.logger.Warn("failed to rollback ClearBlockOrphaned transaction", "error", rbErr)
		}
	}()

	// Look up the row first to find its number (used for the advisory lock
	// namespace). If the row does not exist at all, surface the same
	// not-found error callers have always seen.
	var number int64
	var isOrphaned bool
	err = tx.QueryRow(ctx,
		`SELECT number, is_orphaned FROM block_states WHERE chain_id = $1 AND hash = $2`,
		r.chainID, hash).Scan(&number, &isOrphaned)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("clear orphan flag: block with hash %s not found", hash)
		}
		return fmt.Errorf("failed to look up block by hash: %w", err)
	}

	// Acquire the same per-block advisory lock saveBlockOnce uses, so a
	// concurrent live insert/reorg at this number is serialised against us.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1::int, $2::int)`, r.chainID, number); err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}

	// Idempotent: if the row is already canonical there is nothing to do.
	if !isOrphaned {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}

	// Guard: never clear the orphan flag if a different canonical row already
	// exists at this number. That would leave two non-orphaned rows at one
	// height and break "highest version = canonical". The live writer must
	// win in that case — the orphan stays.
	var conflicting int
	if err := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM block_states
		 WHERE chain_id = $1 AND number = $2 AND hash <> $3 AND NOT is_orphaned`,
		r.chainID, number, hash).Scan(&conflicting); err != nil {
		return fmt.Errorf("failed to check for conflicting canonical row: %w", err)
	}
	if conflicting > 0 {
		// Surface as an error so the caller (backfill loop) knows the heal
		// did not happen and a real reorg is in flight. The backfill retry
		// path treats this as transient — the new canonical row will be
		// picked up by the regular gap finder on the next cycle.
		return fmt.Errorf("clear orphan flag: refusing to un-orphan block %d hash %s: another canonical row already exists at this number", number, hash)
	}

	tag, err := tx.Exec(ctx,
		`UPDATE block_states SET is_orphaned = FALSE WHERE chain_id = $1 AND hash = $2`,
		r.chainID, hash)
	if err != nil {
		return fmt.Errorf("failed to clear block orphan flag: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Should be unreachable — we already saw the row above under the same
		// transaction — but kept for defence-in-depth.
		return fmt.Errorf("clear orphan flag: block with hash %s not found", hash)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// HandleReorgAtomic atomically performs all reorg-related database operations in a single transaction.
// This ensures consistency: either all operations succeed, or none do.
// The commonAncestor is derived from the ReorgEvent (BlockNumber - Depth).
//
// Uses SERIALIZABLE isolation (consistent with SaveBlock) and includes retry logic
// for transient tx errors (SQLSTATE 40001 serialization failure, 40P01 deadlock).
func (r *BlockStateRepository) HandleReorgAtomic(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	if newBlock.BlockTimestamp == 0 {
		return 0, fmt.Errorf("BlockTimestamp is required (used as created_at for hypertable partitioning)")
	}

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
		r.logger.Debug("retryable tx error in HandleReorgAtomic, retrying",
			"attempt", attempt,
			"block", newBlock.Number,
			"hash", newBlock.Hash,
			"backoff", backoff)
		span.AddEvent("retry_attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.String("error", err.Error()),
		))
	}

	version, err := retry.Do(ctx, cfg, isRetryableTxError, onRetry, func() (int, error) {
		return r.handleReorgAtomicOnce(ctx, commonAncestor, event, newBlock)
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "HandleReorgAtomic failed")
	}
	return version, err
}

// handleReorgAtomicOnce attempts a single reorg operation.
func (r *BlockStateRepository) handleReorgAtomicOnce(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	// READ COMMITTED plus an advisory lock, consistent with saveBlockOnce.
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
			r.logger.Warn("failed to rollback transaction", "error", err)
		}
	}()

	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1::int, $2::int)`, r.chainID, newBlock.Number)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire advisory lock: %w", err)
	}

	// Idempotency: an earlier attempt may already have stored this hash.
	var existingVersion int
	err = tx.QueryRow(ctx, `SELECT version FROM block_states WHERE chain_id = $1 AND hash = $2`, r.chainID, newBlock.Hash).Scan(&existingVersion)
	if err == nil {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return 0, fmt.Errorf("failed to commit transaction: %w", commitErr)
		}
		return existingVersion, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("failed to check for existing block: %w", err)
	}

	if err := r.saveReorgEvent(ctx, tx, event); err != nil {
		return 0, err
	}
	if err := r.orphanBlocksAbove(ctx, tx, commonAncestor); err != nil {
		return 0, err
	}
	rewoundFrom, rewound, err := r.rewindWatermarkTo(ctx, tx, commonAncestor)
	if err != nil {
		return 0, err
	}
	version, err := r.insertCanonicalBlock(ctx, tx, newBlock)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if rewound {
		r.reportWatermarkRewind(ctx, rewoundFrom, commonAncestor, newBlock.Number)
	}
	return version, nil
}

// saveReorgEvent records the reorg for the validator and the runbooks.
func (r *BlockStateRepository) saveReorgEvent(ctx context.Context, tx pgx.Tx, event outbound.ReorgEvent) error {
	query := `
		INSERT INTO reorg_events (chain_id, detected_at, block_number, old_hash, new_hash, depth)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	if _, err := tx.Exec(ctx, query, r.chainID, event.DetectedAt, event.BlockNumber, event.OldHash, event.NewHash, event.Depth); err != nil {
		return fmt.Errorf("failed to save reorg event: %w", err)
	}
	return nil
}

// orphanBlocksAbove drops the losing fork out of the canonical view.
func (r *BlockStateRepository) orphanBlocksAbove(ctx context.Context, tx pgx.Tx, commonAncestor int64) error {
	query := `UPDATE block_states SET is_orphaned = TRUE WHERE chain_id = $1 AND number > $2 AND NOT is_orphaned`
	if _, err := tx.Exec(ctx, query, r.chainID, commonAncestor); err != nil {
		return fmt.Errorf("failed to mark blocks orphaned: %w", err)
	}
	return nil
}

// rewindWatermarkTo lowers the backfill watermark to commonAncestor and reports
// the value it replaced. FindGaps scans only above the watermark, so a height
// orphanBlocksAbove leaves without a canonical row is never re-fetched unless
// the watermark drops back below it (ARCT-379).
func (r *BlockStateRepository) rewindWatermarkTo(ctx context.Context, tx pgx.Tx, commonAncestor int64) (int64, bool, error) {
	query := `
		WITH previous AS (
			SELECT watermark FROM backfill_watermark WHERE chain_id = $1
		)
		UPDATE backfill_watermark SET watermark = $2
		WHERE chain_id = $1 AND watermark > $2
		RETURNING (SELECT watermark FROM previous)
	`
	var previous int64
	err := tx.QueryRow(ctx, query, r.chainID, commonAncestor).Scan(&previous)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to rewind backfill watermark: %w", err)
	}
	return previous, true, nil
}

// insertCanonicalBlock stores the reorg's winning block. Version 0 is a
// placeholder: the BEFORE INSERT trigger assigns MAX(version)+1 atomically.
func (r *BlockStateRepository) insertCanonicalBlock(ctx context.Context, tx pgx.Tx, newBlock outbound.BlockState) (int, error) {
	query := `
		INSERT INTO block_states (chain_id, number, hash, parent_hash, received_at, is_orphaned, version, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, 0, $7)
		RETURNING version
	`
	var version int
	err := tx.QueryRow(ctx, query, r.chainID, newBlock.Number, newBlock.Hash, newBlock.ParentHash,
		newBlock.ReceivedAt, newBlock.IsOrphaned, time.Unix(newBlock.BlockTimestamp, 0).UTC()).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("failed to save new block state: %w", err)
	}
	return version, nil
}

// reportWatermarkRewind makes the rewind visible: it silently re-opens a range
// the gap filler had already retired, and only the winning reorg attempt logs.
func (r *BlockStateRepository) reportWatermarkRewind(ctx context.Context, from, to, block int64) {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.Int64("backfill.watermark_rewound_from", from),
		attribute.Int64("backfill.watermark_rewound_to", to),
	)
	r.logger.Info("rewound backfill watermark", "from", from, "to", to, "block", block)
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
// Returns 0 if no watermark exists yet (e.g., first run for a new chain).
func (r *BlockStateRepository) GetBackfillWatermark(ctx context.Context) (int64, error) {
	var watermark int64
	err := r.pool.QueryRow(ctx, `SELECT watermark FROM backfill_watermark WHERE chain_id = $1`, r.chainID).Scan(&watermark)
	if errors.Is(err, pgx.ErrNoRows) {
		// No watermark exists yet for this chain - return 0 to start from beginning
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get backfill watermark: %w", err)
	}
	return watermark, nil
}

// SetBackfillWatermark updates the watermark to the given block number.
// Should only be called after confirming all blocks up to this number exist.
// Uses UPSERT so the row is auto-created for new chains that have no watermark yet.
func (r *BlockStateRepository) SetBackfillWatermark(ctx context.Context, watermark int64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO backfill_watermark (chain_id, watermark) VALUES ($1, $2)
		 ON CONFLICT (chain_id) DO UPDATE SET watermark = EXCLUDED.watermark`,
		r.chainID, watermark)
	if err != nil {
		return fmt.Errorf("failed to set backfill watermark: %w", err)
	}
	return nil
}

// AdvanceBackfillWatermark moves the watermark from expected to watermark and
// reports whether the row changed.
func (r *BlockStateRepository) AdvanceBackfillWatermark(ctx context.Context, expected, watermark int64) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`INSERT INTO backfill_watermark (chain_id, watermark) VALUES ($1, $3)
		 ON CONFLICT (chain_id) DO UPDATE SET watermark = EXCLUDED.watermark
		 WHERE backfill_watermark.watermark = $2`,
		r.chainID, expected, watermark)
	if err != nil {
		return false, fmt.Errorf("failed to advance backfill watermark: %w", err)
	}
	return tag.RowsAffected() > 0, nil
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

// FindOrphanOnlyHeights returns block numbers in the range whose only rows are
// orphaned. Ascending and uncapped: the result is bounded by the orphaned rows,
// which are bounded by reorgs, and a capped count would under-report a storm.
func (r *BlockStateRepository) FindOrphanOnlyHeights(ctx context.Context, fromBlock, toBlock int64) ([]int64, error) {
	if fromBlock > toBlock {
		return nil, nil
	}

	query := `
		SELECT DISTINCT orphaned.number
		FROM block_states orphaned
		WHERE orphaned.chain_id = $1
			AND orphaned.is_orphaned
			AND orphaned.number >= $2
			AND orphaned.number <= $3
			AND NOT EXISTS (
				SELECT 1 FROM block_states canonical
				WHERE canonical.chain_id = orphaned.chain_id
					AND canonical.number = orphaned.number
					AND NOT canonical.is_orphaned
			)
		ORDER BY orphaned.number
	`

	rows, err := r.pool.Query(ctx, query, r.chainID, fromBlock, toBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to find orphan-only heights: %w", err)
	}
	defer rows.Close()

	var heights []int64
	for rows.Next() {
		var number int64
		if err := rows.Scan(&number); err != nil {
			return nil, fmt.Errorf("failed to scan orphan-only height: %w", err)
		}
		heights = append(heights, number)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating orphan-only heights: %w", err)
	}

	return heights, nil
}

// VerifyChainIntegrity verifies that the canonical chain over the range is
// unbroken: consecutive blocks are linked by parent_hash, and no height between
// two canonical blocks is missing. Returns nil if the chain is valid, or an
// error describing the first violation in ascending block order.
func (r *BlockStateRepository) VerifyChainIntegrity(ctx context.Context, fromBlock, toBlock int64) error {
	if fromBlock >= toBlock {
		return nil // Nothing to verify
	}

	// LAG leaves the range's first block unpaired, so it is never flagged: an
	// unseeded chain's watermark starts at 0, far below its first block.
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
		WHERE prev_number IS NOT NULL
			AND (
				prev_number < number - 1
				OR (prev_number = number - 1 AND parent_hash != prev_hash)
			)
		ORDER BY number
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

	if prevBlockNum < brokenBlock-1 {
		return fmt.Errorf("chain integrity violation: canonical block(s) %d to %d missing between blocks %d and %d",
			prevBlockNum+1, brokenBlock-1, prevBlockNum, brokenBlock)
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

	query := `UPDATE block_states SET block_published = TRUE WHERE chain_id = $1 AND hash = $2`

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
		result, err := r.pool.Exec(ctx, query, r.chainID, hash)
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

// GetMinUnpublishedBlock returns the lowest canonical block number that has not been published.
// Returns (blockNum, true, nil) if found, (0, false, nil) if all blocks are published.
//
// Uses the existing partial index idx_block_states_chain_incomplete_publish
// (chain_id, number) WHERE NOT is_orphaned AND NOT block_published, so Postgres
// resolves MIN(number) with a single index lookup — O(1) regardless of table size.
func (r *BlockStateRepository) GetMinUnpublishedBlock(ctx context.Context) (int64, bool, error) {
	var blockNum *int64
	query := `SELECT MIN(number) FROM block_states WHERE chain_id = $1 AND NOT is_orphaned AND NOT block_published`
	err := r.pool.QueryRow(ctx, query, r.chainID).Scan(&blockNum)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get min unpublished block: %w", err)
	}
	if blockNum == nil {
		return 0, false, nil
	}
	return *blockNum, true, nil
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
