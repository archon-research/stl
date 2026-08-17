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
//
// STATUS: first-draft scaffold. Compiles/tests via CI (Go is not in the dev env). TODOs inline.
package block_meta_loader

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
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
			rows = append(rows, []any{s.cfg.ChainID, r.Number, r.Version, ts})
		}
		n, err := s.upsert(ctx, rows)
		if err != nil {
			return total, fmt.Errorf("upserting block_meta: %w", err)
		}
		total += n
		s.logger.Info("block_meta batch", "chain", s.cfg.ChainID, "upserted", n, "total", total)
	}
}

// blockTimestamp reads {partition}/{block}_{version}_block.json.gz from S3, gunzips it, and parses the
// on-chain header timestamp (hex, e.g. "0x67c00000").
func (s *Service) blockTimestamp(ctx context.Context, r blockRef) (time.Time, error) {
	key := s3key.Build(r.Number, r.Version, s3key.Block)
	rc, err := s.reader.StreamFile(ctx, s.cfg.Bucket, key)
	if err != nil {
		return time.Time{}, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer rc.Close()
	gz, err := gzip.NewReader(rc)
	if err != nil {
		return time.Time{}, fmt.Errorf("gunzip %s: %w", key, err)
	}
	defer gz.Close()
	var hdr struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.NewDecoder(gz).Decode(&hdr); err != nil {
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
// TODO: finalise the per-table chain resolution — borrower/borrower_collateral/sparklend_reserve_data
// resolve chain via protocol.chain_id; allocation_position/protocol_event carry chain_id natively;
// prime_debt is the Sky constant (chain 1). The union below is the shape; verify each arm on live data.
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

// upsert writes a batch into block_meta, ON CONFLICT DO NOTHING (a block's time is immutable once known).
func (s *Service) upsert(ctx context.Context, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	// TODO: switch to COPY into a temp table + INSERT ... SELECT ON CONFLICT for throughput at scale.
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(
			`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp)
			 VALUES ($1, $2, $3, $4) ON CONFLICT (chain_id, block_number, block_version) DO NOTHING`,
			r...)
	}
	br := s.db.SendBatch(ctx, batch)
	defer br.Close()
	var n int64
	for range rows {
		ct, err := br.Exec()
		if err != nil {
			return n, err
		}
		n += ct.RowsAffected()
	}
	return n, nil
}
