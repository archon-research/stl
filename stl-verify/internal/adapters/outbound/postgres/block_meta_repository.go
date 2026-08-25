package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that BlockMetaRepository implements outbound.BlockMetaRepository.
var _ outbound.BlockMetaRepository = (*BlockMetaRepository)(nil)

// BlockMetaRepository is a PostgreSQL implementation of the outbound.BlockMetaRepository port.
type BlockMetaRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewBlockMetaRepository creates a new PostgreSQL block_meta repository.
func NewBlockMetaRepository(pool *pgxpool.Pool, logger *slog.Logger) (*BlockMetaRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BlockMetaRepository{pool: pool, logger: logger}, nil
}

// pendingBlocksQuery resolves the blocks referenced by the observation tables but not yet in
// block_meta, one chain at a time. Per-table chain resolution (verified against the schemas):
//   - borrower, borrower_collateral, sparklend_reserve_data carry protocol_id -> protocol.chain_id.
//   - allocation_position, protocol_event carry chain_id natively.
//   - prime_debt (Sky) has no chain column and is Ethereum mainnet, so its chain is the constant 1.
//
// The (block_number, block_version) > (afterNumber, afterVersion) predicate is a keyset cursor: it
// pages the OUTPUT without re-returning rows already handled this run. The NOT EXISTS anti-join
// keeps the loader resumable across runs (a fresh run restarts the cursor at -1 and the anti-join
// skips blocks a prior run already loaded). A block newly referenced BELOW the cursor mid-run is
// picked up on the next run, which is acceptable for a historical backfill.
//
// Cost note: the cursor pages the output, but the INPUT — the 6-table referenced UNION — is still
// recomputed on every batch, and only protocol_event has an index leading with
// (block_number, block_version); the other arms lead with user_id/protocol_id/chain_id, so their
// contribution is a scan. For a millions-of-blocks full-history backfill this is roughly
// O(N^2/batch). It is correct and fine at moderate scale, but before running against prod-sized
// history this should be reworked to materialize the referenced set once per run into an indexed
// temp table and page from that. Tracked as a follow-up (see PR).
const pendingBlocksQuery = `
WITH referenced AS (
    SELECT p.chain_id, b.block_number, b.block_version
      FROM borrower b JOIN protocol p ON p.id = b.protocol_id
    UNION
    SELECT p.chain_id, bc.block_number, bc.block_version
      FROM borrower_collateral bc JOIN protocol p ON p.id = bc.protocol_id
    UNION
    SELECT ap.chain_id, ap.block_number, ap.block_version FROM allocation_position ap
    UNION
    SELECT pe.chain_id, pe.block_number, pe.block_version FROM protocol_event pe
    UNION
    SELECT p.chain_id, sr.block_number, sr.block_version
      FROM sparklend_reserve_data sr JOIN protocol p ON p.id = sr.protocol_id
    UNION
    SELECT 1::int AS chain_id, pd.block_number, pd.block_version FROM prime_debt pd
)
SELECT r.block_number, r.block_version
  FROM referenced r
 WHERE r.chain_id = $1
   AND (r.block_number > $3 OR (r.block_number = $3 AND r.block_version > $4))
   AND NOT EXISTS (
       SELECT 1 FROM block_meta m
        WHERE m.chain_id = r.chain_id
          AND m.block_number = r.block_number
          AND m.block_version = r.block_version)
 ORDER BY r.block_number, r.block_version
 LIMIT $2`

// PendingBlocks returns the next batch of blocks missing from block_meta for chainID.
func (r *BlockMetaRepository) PendingBlocks(ctx context.Context, chainID int64, limit int, afterNumber int64, afterVersion int) ([]outbound.BlockRef, error) {
	rows, err := r.pool.Query(ctx, pendingBlocksQuery, chainID, limit, afterNumber, afterVersion)
	if err != nil {
		return nil, fmt.Errorf("querying pending blocks: %w", err)
	}
	defer rows.Close()

	var out []outbound.BlockRef
	for rows.Next() {
		var b outbound.BlockRef
		if err := rows.Scan(&b.Number, &b.Version); err != nil {
			return nil, fmt.Errorf("scanning pending block: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// blockMetaStageColumns are the block_meta columns the loader fills, in COPY/INSERT order.
var blockMetaStageColumns = []string{"chain_id", "block_number", "block_version", "block_timestamp"}

// Upsert COPYs the batch into a session-scoped TEMP table (dropped at commit) and then does a single
// INSERT ... SELECT ... ON CONFLICT DO NOTHING. COPY is an order of magnitude faster than per-row
// INSERTs at the millions-of-blocks scale of a full-history backfill, and folding the whole batch
// into one INSERT keeps the conflict check server-side.
//
// The arbiter must name block_meta's FULL primary key, processing_version included. It is not a column
// this loader supplies -- assign_processing_version_block_meta sets it in a BEFORE INSERT trigger, which
// runs before conflict detection, so the arbiter sees the assigned value. Naming only the three natural-key
// columns matches no unique constraint and fails at runtime with 42P10, "there is no unique or exclusion
// constraint matching the ON CONFLICT specification". That is what this did until the block_meta review:
// the integration test below built its own block_meta with a three-column PK, so CI never exercised the
// real shape and the two PRs were individually green and jointly broken.
//
// Known gap, not fixed here: the loader never sets build_id, so every row carries the pre-tracking
// sentinel 0. The trigger's retry branch keys reuse on build_id, so a re-run of the same block reuses
// processing_version 0 and ON CONFLICT drops it -- correct for an idempotent retry, but it also means a
// genuine correction (same block, different timestamp) is discarded with the loader reporting success.
// The MAX+1 reprocess arm is therefore unreachable from this writer. Reaching it needs the loader to
// register a build and pass its id.
func (r *BlockMetaRepository) Upsert(ctx context.Context, rows []outbound.BlockMetaRow) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// The three integer stage columns are bigint so pgx's CopyFrom binary encoding (which uses the
	// destination column OIDs) matches the int64 row values exactly; block_timestamp is timestamptz.
	// The INSERT below assignment-casts the integers down to block_meta's integer columns (chain_id,
	// block_version).
	copyRows := make([][]any, len(rows))
	for i, row := range rows {
		copyRows[i] = []any{row.ChainID, row.BlockNumber, int64(row.BlockVersion), row.BlockTimestamp}
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE block_meta_stage (
		chain_id        bigint      NOT NULL,
		block_number    bigint      NOT NULL,
		block_version   bigint      NOT NULL,
		block_timestamp timestamptz NOT NULL
	) ON COMMIT DROP`); err != nil {
		return 0, fmt.Errorf("create stage table: %w", err)
	}

	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"block_meta_stage"}, blockMetaStageColumns, pgx.CopyFromRows(copyRows)); err != nil {
		return 0, fmt.Errorf("copy into stage: %w", err)
	}

	ct, err := tx.Exec(ctx, `
INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
SELECT chain_id, block_number, block_version, block_timestamp FROM block_meta_stage
ON CONFLICT (chain_id, block_number, block_version, processing_version) DO NOTHING`)
	if err != nil {
		return 0, fmt.Errorf("insert from stage: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return ct.RowsAffected(), nil
}
