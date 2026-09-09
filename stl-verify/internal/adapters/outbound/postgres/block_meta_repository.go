package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

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
// referencedBlocksQuery is the set of blocks a chain's observation tables reference. It is evaluated
// ONCE per run into a temp table: the six arms scan (only protocol_event has an index leading with
// block_number), so re-running it per batch made enumeration cost O(batches x full scan) -- measured
// at ~7s per batch against staging's 1.4M referenced blocks, independent of how far the cursor had
// advanced, which is ~4h for chain 1 alone before a single header is read.
// workListStatements build the run's work list. Separate statements because the INSERT takes a
// parameter, and a multi-statement string cannot be sent as one prepared statement.
var workListStatements = []string{
	`CREATE TEMP TABLE block_meta_worklist (block_number bigint NOT NULL, block_version integer NOT NULL) ON COMMIT DROP`,
	`INSERT INTO block_meta_worklist (block_number, block_version)
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
	    AND NOT EXISTS (
	        SELECT 1 FROM block_meta m
	         WHERE m.chain_id = r.chain_id
	           AND m.block_number = r.block_number
	           AND m.block_version = r.block_version)`,
	`CREATE INDEX ON block_meta_worklist (block_number, block_version)`,
	`ANALYZE block_meta_worklist`,
}

// blockWorkList pages the run's temp work-list. It holds one pooled connection for the run, because a
// temp table belongs to the session that created it; the transaction is open for the same reason
// (ON COMMIT DROP), so nothing survives a crash and a fresh run recomputes the set.
type blockWorkList struct {
	conn   *pgxpool.Conn
	tx     pgx.Tx
	logger *slog.Logger
	after  outbound.BlockRef
}

// OpenWorkList evaluates the referenced set for chainID once and returns a cursor over it.
func (r *BlockMetaRepository) OpenWorkList(ctx context.Context, chainID int64) (outbound.BlockWorkList, error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire a connection for the work list: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("begin the work-list transaction: %w", err)
	}
	for _, stmt := range workListStatements {
		var args []any
		if strings.Contains(stmt, "$1") {
			args = []any{chainID}
		}
		if _, err := tx.Exec(ctx, stmt, args...); err != nil {
			rollback(ctx, tx, r.logger)
			conn.Release()
			return nil, fmt.Errorf("materialize the work list: %w", err)
		}
	}
	return &blockWorkList{conn: conn, tx: tx, logger: r.logger,
		after: outbound.BlockRef{Number: -1, Version: -1}}, nil
}

// Next pages the work-list with a keyset cursor, so the ordered read is not restarted per batch.
func (w *blockWorkList) Next(ctx context.Context, limit int) ([]outbound.BlockRef, error) {
	if w.tx == nil {
		return nil, fmt.Errorf("work list is closed")
	}
	rows, err := w.tx.Query(ctx, `
		SELECT block_number, block_version FROM block_meta_worklist
		 WHERE (block_number, block_version) > ($1, $2)
		 ORDER BY block_number, block_version
		 LIMIT $3`, w.after.Number, w.after.Version, limit)
	if err != nil {
		return nil, fmt.Errorf("reading the work list: %w", err)
	}
	defer rows.Close()
	var out []outbound.BlockRef
	for rows.Next() {
		var b outbound.BlockRef
		if err := rows.Scan(&b.Number, &b.Version); err != nil {
			return nil, fmt.Errorf("scanning a work-list row: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating the work list: %w", err)
	}
	if len(out) > 0 {
		w.after = out[len(out)-1]
	}
	return out, nil
}

func (w *blockWorkList) Close(ctx context.Context) {
	if w.tx == nil {
		return
	}
	if err := w.tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		w.logger.Error("closing the block_meta work list", "error", err)
	}
	w.conn.Release()
	w.tx, w.conn = nil, nil
}

// blockMetaStageColumns are the block_meta columns the loader fills, in COPY/INSERT order.
var blockMetaStageColumns = []string{"chain_id", "block_number", "block_version", "block_timestamp"}

// Upsert COPYs the batch into a session-scoped TEMP table (dropped at commit) and then does a single
// INSERT ... SELECT ... ON CONFLICT DO NOTHING. COPY is an order of magnitude faster than per-row
// INSERTs at the millions-of-blocks scale of a full-history backfill, and folding the whole batch
// into one INSERT keeps the conflict check server-side.
//
// The arbiter is block_meta's primary key, the natural key (chain_id, block_number, block_version):
// a header time is immutable, so a re-run is a no-op and a mis-parse is corrected by an operator
// deleting and reloading the affected coordinates, not by a second row.
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
ON CONFLICT (chain_id, block_number, block_version) DO NOTHING`)
	if err != nil {
		return 0, fmt.Errorf("insert from stage: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return ct.RowsAffected(), nil
}
