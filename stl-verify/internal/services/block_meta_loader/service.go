// Package block_meta_loader fills the block_meta dimension for one chain by reading the
// authoritative on-chain block-header timestamp from that chain's S3 raw-block archive.
//
// block_meta ((chain_id, block_number, block_version) -> block_timestamp) is the source of
// block_timestamp for the observation tables that carry no event-time column (borrower,
// borrower_collateral, allocation_position, prime_debt, protocol_event, sparklend_reserve_data).
// The raw_data_backup worker archives each block as {partition}/{block}_{version}_block.json.gz in a
// per-chain bucket; the block header carries the exact on-chain timestamp (hex). This loader reads it
// straight from that archive — authoritative, and reaching the full history the archive holds — rather
// than the block_states rolling window or the onchain_token_price proxy.
//
// Run PER CHAIN (like raw-data-backup): one invocation, one CHAIN_ID, one S3 bucket. Idempotent
// (ON CONFLICT DO NOTHING) and resumable (the work-list is only blocks not yet in block_meta).
package block_meta_loader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config for a single-chain run.
type Config struct {
	ChainID   int64  // the chain whose block_meta rows this run fills
	Bucket    string // that chain's raw-block S3 bucket (validate with chainutil.ValidateS3BucketForChain in main)
	BatchSize int    // blocks fetched+upserted per iteration; defaults to 500 if 0
}

// blockRef is one (block_number, block_version) observed on this chain but not yet in block_meta.
type blockRef struct {
	Number  int64
	Version int
}

// Service reads block headers from S3 and upserts block_meta for one chain.
type Service struct {
	cfg    Config
	db     *pgxpool.Pool
	reader outbound.S3Reader
	logger *slog.Logger
}

func New(cfg Config, db *pgxpool.Pool, reader outbound.S3Reader, logger *slog.Logger) (*Service, error) {
	if cfg.ChainID == 0 {
		return nil, fmt.Errorf("chain id is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	return &Service{cfg: cfg, db: db, reader: reader, logger: logger}, nil
}

// Run fills block_meta for cfg.ChainID until no referenced block is missing. Returns rows upserted.
func (s *Service) Run(ctx context.Context) (int64, error) {
	var total int64
	for {
		refs, err := s.pendingBlocks(ctx)
		if err != nil {
			return total, fmt.Errorf("loading pending blocks: %w", err)
		}
		if len(refs) == 0 {
			return total, nil
		}
		rows := make([][]any, 0, len(refs))
		for _, r := range refs {
			ts, err := s.blockTimestamp(ctx, r)
			if err != nil {
				// Fail hard: a referenced block missing from the archive is the deep-tail gap and
				// must be surfaced (bulk-download it), not silently skipped.
				return total, fmt.Errorf("chain %d block %d/%d: %w", s.cfg.ChainID, r.Number, r.Version, err)
			}
			rows = append(rows, []any{s.cfg.ChainID, r.Number, int64(r.Version), ts})
		}
		n, err := s.upsert(ctx, rows)
		if err != nil {
			return total, fmt.Errorf("upserting block_meta: %w", err)
		}
		total += n
		s.logger.Info("block_meta batch", "chain", s.cfg.ChainID, "upserted", n, "total", total)
	}
}

// blockTimestamp reads {partition}/{block}_{version}_block.json.gz from S3 and parses the on-chain
// header timestamp (hex, e.g. "0x67c00000"). The S3Reader adapter auto-decompresses .gz keys, so the
// stream yields plain JSON — the loader must not gunzip again.
func (s *Service) blockTimestamp(ctx context.Context, r blockRef) (time.Time, error) {
	key := s3key.Build(r.Number, r.Version, s3key.Block)
	rc, err := s.reader.StreamFile(ctx, s.cfg.Bucket, key)
	if err != nil {
		return time.Time{}, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return time.Time{}, fmt.Errorf("read %s: %w", key, err)
	}
	var hdr struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &hdr); err != nil {
		return time.Time{}, fmt.Errorf("decode %s: %w", key, err)
	}
	if hdr.Timestamp == "" {
		return time.Time{}, fmt.Errorf("block %s has no timestamp field", key)
	}
	sec, err := hexutil.ParseInt64(hdr.Timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse timestamp %q for %s: %w", hdr.Timestamp, key, err)
	}
	return time.Unix(sec, 0).UTC(), nil
}

// pendingBlocks returns a batch of (block_number, block_version) referenced by the observation tables
// on this chain but not yet present in block_meta.
//
// Per-table chain resolution (verified against the schemas):
//   - borrower, borrower_collateral, sparklend_reserve_data carry protocol_id (BIGINT -> protocol.id);
//     the chain is protocol.chain_id, reached by joining protocol.
//   - allocation_position, protocol_event carry chain_id natively.
//   - prime_debt (Sky) has no chain column and is Ethereum mainnet, so its chain is the constant 1.
func (s *Service) pendingBlocks(ctx context.Context) ([]blockRef, error) {
	const q = `
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
          AND m.block_version = r.block_version)
 ORDER BY r.block_number, r.block_version
 LIMIT $2`
	rows, err := s.db.Query(ctx, q, s.cfg.ChainID, s.cfg.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []blockRef
	for rows.Next() {
		var r blockRef
		if err := rows.Scan(&r.Number, &r.Version); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// stageColumns are the block_meta columns the loader fills, in COPY/INSERT order.
var stageColumns = []string{"chain_id", "block_number", "block_version", "block_timestamp"}

// upsert writes a batch into block_meta, ON CONFLICT DO NOTHING (a block's time is immutable once known).
//
// It COPYs the batch into a session-scoped TEMP table (dropped at commit) and then does a single
// INSERT ... SELECT ... ON CONFLICT DO NOTHING. COPY is an order of magnitude faster than per-row
// INSERTs at the millions-of-blocks scale of a full-history backfill, and folding the whole batch into
// one INSERT keeps the conflict check server-side.
func (s *Service) upsert(ctx context.Context, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer s.rollback(ctx, tx)

	// The stage columns are all bigint so pgx's CopyFrom binary encoding (which uses the
	// destination column OIDs) matches the int64/int row values exactly; the INSERT below
	// assignment-casts them down to block_meta's integer columns.
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE block_meta_stage (
		chain_id        bigint      NOT NULL,
		block_number    bigint      NOT NULL,
		block_version   bigint      NOT NULL,
		block_timestamp timestamptz NOT NULL
	) ON COMMIT DROP`); err != nil {
		return 0, fmt.Errorf("create stage table: %w", err)
	}

	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"block_meta_stage"}, stageColumns, pgx.CopyFromRows(rows)); err != nil {
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

// rollback rolls back tx and logs a genuine failure; a rollback after a successful commit returns
// pgx.ErrTxClosed and is expected.
func (s *Service) rollback(ctx context.Context, tx pgx.Tx) {
	if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		s.logger.Error("rollback block_meta upsert tx", "chain", s.cfg.ChainID, "error", err)
	}
}
