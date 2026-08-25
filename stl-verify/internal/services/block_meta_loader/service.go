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
// The block_meta reads/writes live behind the outbound.BlockMetaRepository port; this service owns
// only the S3-header decode and the batch loop.
package block_meta_loader

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockheader"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// maxBatchSize bounds rows per Upsert transaction. See the clamp in New for the measurement behind it.
const maxBatchSize = 5000

// Config for a single-chain run.
type Config struct {
	ChainID   int64  // the chain whose block_meta rows this run fills
	Bucket    string // that chain's raw-block S3 bucket (validate with chainutil.ValidateS3BucketForChain in main)
	BatchSize int    // blocks fetched+upserted per iteration; defaults to 500 if 0, clamped to maxBatchSize
}

// Service reads block headers from S3 and upserts block_meta for one chain.
type Service struct {
	cfg    Config
	repo   outbound.BlockMetaRepository
	reader outbound.S3Reader
	logger *slog.Logger
}

// New validates the configuration and dependencies for a single-chain run.
func New(cfg Config, repo outbound.BlockMetaRepository, reader outbound.S3Reader, logger *slog.Logger) (*Service, error) {
	if cfg.ChainID <= 0 {
		return nil, fmt.Errorf("chain id must be positive, got %d", cfg.ChainID)
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("repository is required")
	}
	if reader == nil {
		return nil, fmt.Errorf("s3 reader is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	// Upper clamp, not just a default. Each row's BEFORE INSERT trigger takes a transaction-scoped
	// pg_advisory_xact_lock, and Upsert commits a whole batch in one transaction, so every lock is held
	// to commit and occupies the shared lock table. Measured on stock settings
	// (max_locks_per_transaction=64, max_connections=100): 12,000 rows in one transaction succeeds,
	// 15,000 fails with "out of shared memory". The lock table is SHARED, so concurrent per-chain
	// loaders lower the real ceiling non-deterministically and exhaustion can fail unrelated
	// transactions -- hence a bound well under the measured single-writer limit rather than near it.
	// BATCH_SIZE is operator-set, and the natural instinct on a tens-of-millions-row backfill is a big
	// number.
	if cfg.BatchSize > maxBatchSize {
		cfg.BatchSize = maxBatchSize
	}
	return &Service{cfg: cfg, repo: repo, reader: reader, logger: logger}, nil
}

// Run fills block_meta for cfg.ChainID until no referenced block is missing. Returns rows upserted.
// It walks the pending blocks with a keyset cursor so the ordered scan is not restarted each batch,
// and checks for cancellation between batches so a SIGTERM stops it promptly.
func (s *Service) Run(ctx context.Context) (int64, error) {
	var total int64
	afterNumber, afterVersion := int64(-1), -1
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}

		refs, err := s.repo.PendingBlocks(ctx, s.cfg.ChainID, s.cfg.BatchSize, afterNumber, afterVersion)
		if err != nil {
			return total, fmt.Errorf("loading pending blocks: %w", err)
		}
		if len(refs) == 0 {
			return total, nil
		}

		rows := make([]outbound.BlockMetaRow, 0, len(refs))
		for _, r := range refs {
			ts, err := blockheader.ReadTimestampFromS3(ctx, s.reader, s.cfg.Bucket, r.Number, r.Version)
			if err != nil {
				// Fail hard: a referenced block missing from the archive is the deep-tail gap and
				// must be surfaced (bulk-download it), not silently skipped.
				return total, fmt.Errorf("chain %d block %d/%d: %w", s.cfg.ChainID, r.Number, r.Version, err)
			}
			rows = append(rows, outbound.BlockMetaRow{
				ChainID:        s.cfg.ChainID,
				BlockNumber:    r.Number,
				BlockVersion:   r.Version,
				BlockTimestamp: ts,
			})
		}

		n, err := s.repo.Upsert(ctx, rows)
		if err != nil {
			return total, fmt.Errorf("upserting block_meta: %w", err)
		}
		total += n

		// Advance the cursor past this batch; refs are ordered by (number, version).
		last := refs[len(refs)-1]
		afterNumber, afterVersion = last.Number, last.Version

		s.logger.Info("block_meta batch", "chain", s.cfg.ChainID, "upserted", n, "total", total)
	}
}
