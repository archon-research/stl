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
// The block_meta reads/writes live behind the outbound.BlockMetaRepository port, and archive-version
// resolution behind outbound.ArchiveReader; this service owns the S3-header decode and the batch loop.
package block_meta_loader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockheader"
	"golang.org/x/sync/errgroup"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// maxBatchSize bounds rows per Upsert statement. See the clamp in New for why it is bounded.
const maxBatchSize = 5000

// defaultConcurrency is how many header reads are in flight per batch when the
// caller sets none. Ten takes chain 1's first pass from hours to under one.
const defaultConcurrency = 10

// Config for a single-chain run.
type Config struct {
	ChainID   int64  // the chain whose block_meta rows this run fills
	Bucket    string // that chain's raw-block S3 bucket (validate with chainutil.ValidateS3BucketForChain in main)
	BatchSize int    // blocks fetched+upserted per iteration, one statement each; defaults to 500 if 0, clamped to maxBatchSize

	// OnProgress, when set, is called after every committed batch with the running total, so a
	// Temporal activity can heartbeat a real number rather than a bare liveness ping. Optional.
	OnProgress func(total int64)

	// Concurrency bounds how many block headers are read from S3 at once within a batch. The reads
	// dominate the run: at 982k pending blocks and 25 ms an object, one at a time is 6.8 hours.
	// Defaults to defaultConcurrency.
	Concurrency int

	// HeadMargin excludes the newest blocks, counted back from the chain's head. The archive trails
	// the indexers there, so without it every repeated run reports normal lag as missing objects.
	// Zero means no margin.
	HeadMargin int64
}

// Service reads block headers from S3 and upserts block_meta for one chain.
type Service struct {
	cfg     Config
	repo    outbound.BlockMetaRepository
	reader  outbound.S3Reader
	archive outbound.ArchiveReader
	logger  *slog.Logger
	metrics *loaderMetrics
}

// New validates the configuration and dependencies for a single-chain run.
func New(cfg Config, repo outbound.BlockMetaRepository, reader outbound.S3Reader, archive outbound.ArchiveReader, logger *slog.Logger) (*Service, error) {
	if cfg.ChainID <= 0 {
		return nil, fmt.Errorf("chain id must be positive, got %d", cfg.ChainID)
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
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
	if archive == nil {
		return nil, fmt.Errorf("archive reader is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	// Upper clamp, not just a default: every header in a batch is read from S3 and held in memory
	// before its single INSERT, so an operator-set BATCH_SIZE in the tens of thousands means a large
	// in-memory batch and one oversized statement.
	if cfg.BatchSize > maxBatchSize {
		cfg.BatchSize = maxBatchSize
	}
	// A metrics failure is not a reason to refuse a run: the counter is a tripwire on the work
	// list's size, and recordPaged is nil-safe, so the load proceeds without it.
	metrics, err := newLoaderMetrics(cfg.ChainID)
	if err != nil {
		logger.Warn("block_meta loader metrics unavailable", "error", err)
	}
	return &Service{cfg: cfg, repo: repo, reader: reader, archive: archive, logger: logger, metrics: metrics}, nil
}

// Run fills block_meta for cfg.ChainID until no referenced block is missing. Returns rows upserted.
// The pending set is enumerated once into a work list and paged with a keyset cursor, so the six-table
// union is not re-run per batch; cancellation is checked between batches so a SIGTERM stops it promptly.
// A block newly referenced mid-run is picked up by the next run, which is what a backfill needs.
func (s *Service) Run(ctx context.Context) (int64, error) {
	var total int64
	var misses []string
	work, err := s.repo.OpenWorkList(ctx, s.cfg.ChainID, s.cfg.HeadMargin)
	if err != nil {
		return total, fmt.Errorf("opening the work list: %w", err)
	}
	defer work.Close(ctx)
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		refs, err := work.Next(ctx, s.cfg.BatchSize)
		if err != nil {
			return total, fmt.Errorf("loading pending blocks: %w", err)
		}
		if len(refs) == 0 {
			// Misses are carried to the end rather than returned at the first one: the list
			// is paged in ascending order, so stopping mid-page would leave every later block
			// unloaded. What could be read is committed, and the run still fails naming them.
			if len(misses) > 0 {
				return total, fmt.Errorf("chain %d: %d referenced block(s) absent from the archive: %s",
					s.cfg.ChainID, len(misses), strings.Join(cappedMisses(misses), ", "))
			}
			return total, nil
		}
		s.metrics.recordPaged(ctx, s.cfg.ChainID, len(refs))

		rows, batchMisses, err := s.readBatch(ctx, refs)
		if err != nil {
			return total, err
		}
		misses = append(misses, batchMisses...)

		n, err := s.repo.Upsert(ctx, rows)
		if err != nil {
			return total, fmt.Errorf("upserting block_meta: %w", err)
		}
		total += n

		if s.cfg.OnProgress != nil {
			s.cfg.OnProgress(total)
		}
		s.logger.Info("block_meta batch", "chain", s.cfg.ChainID, "upserted", n, "total", total)
	}
}

// maxNamedMisses bounds how many absent blocks the final error names. The list is
// a lead for a bulk-download, not a manifest; the count carries the scale.
const maxNamedMisses = 20

func cappedMisses(misses []string) []string {
	if len(misses) <= maxNamedMisses {
		return misses
	}
	return append(misses[:maxNamedMisses:maxNamedMisses], "...")
}

// readBatch reads one batch's headers from S3 with bounded concurrency, returning
// the rows it could build and the blocks the archive did not hold.
//
// An absent object is a miss, not a failure: the caller carries them to the end so
// one deep-tail hole cannot stop every later block on the chain. Anything else —
// a cancelled context, a transport error, a corrupt payload — is returned as is,
// because it says nothing about whether the block exists.
func (s *Service) readBatch(ctx context.Context, refs []outbound.BlockRef) ([]outbound.BlockMetaRow, []string, error) {
	rows := make([]outbound.BlockMetaRow, len(refs))
	found := make([]bool, len(refs))
	misses := make([]string, len(refs))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.cfg.Concurrency)
	for i, r := range refs {
		if gctx.Err() != nil {
			break
		}
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil
			}
			row, miss, err := s.readOne(gctx, r)
			if err != nil {
				return err
			}
			if miss != "" {
				misses[i] = miss
				return nil
			}
			rows[i] = row
			found[i] = true
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	// Compacted in index order, so a run reads the same regardless of completion order.
	out := rows[:0]
	var absent []string
	for i := range refs {
		if found[i] {
			out = append(out, rows[i])
		} else if misses[i] != "" {
			absent = append(absent, misses[i])
		}
	}
	return out, absent, nil
}

// readOne reads one block's header at its requested version, falling back to the archive's
// own highest version at that height when the requested one is absent.
//
// block_meta.block_version matches the archive's own version, not a referencing table's — the
// two usually agree, but the raw-block-bulk-downloader's December backfill wrote a swath of
// deep history under key version 1 regardless of the referencing table's own bookkeeping, so a
// read pinned to the requested version 404s even though the archive holds the block elsewhere.
// Resolving the real version is the correct read, not a workaround: it is the same rule
// listHighestVersionReceipts and internal/pkg/blockversion already apply to raw-bucket reads.
func (s *Service) readOne(ctx context.Context, r outbound.BlockRef) (outbound.BlockMetaRow, string, error) {
	row, err := s.readAt(ctx, r.Number, r.Version)
	if err == nil {
		return row, "", nil
	}
	if !errors.Is(err, outbound.ErrObjectNotFound) {
		return outbound.BlockMetaRow{}, "", fmt.Errorf("chain %d block %d/%d: %w", s.cfg.ChainID, r.Number, r.Version, err)
	}

	highest, found, err := s.archive.HighestVersion(ctx, r.Number)
	if err != nil {
		return outbound.BlockMetaRow{}, "", fmt.Errorf(
			"chain %d block %d/%d: resolving the archived version: %w", s.cfg.ChainID, r.Number, r.Version, err)
	}
	if !found || highest == r.Version {
		// Nothing archived at this height at all, or the listing agrees with what already
		// 404ed: a genuine hole, not a version mismatch.
		return outbound.BlockMetaRow{}, fmt.Sprintf("%d/%d", r.Number, r.Version), nil
	}

	row, err = s.readAt(ctx, r.Number, highest)
	if err != nil {
		if errors.Is(err, outbound.ErrObjectNotFound) {
			// The occupied version can hold receipts/traces with no block object (a
			// raw-block-bulk-downloader partial upload) — still a miss, nothing else to try.
			return outbound.BlockMetaRow{}, fmt.Sprintf("%d/%d", r.Number, r.Version), nil
		}
		return outbound.BlockMetaRow{}, "", fmt.Errorf(
			"chain %d block %d/%d at resolved version %d: %w", s.cfg.ChainID, r.Number, r.Version, highest, err)
	}
	return row, "", nil
}

func (s *Service) readAt(ctx context.Context, blockNumber int64, version int) (outbound.BlockMetaRow, error) {
	ts, err := blockheader.ReadTimestampFromS3(ctx, s.reader, s.cfg.Bucket, blockNumber, version)
	if err != nil {
		return outbound.BlockMetaRow{}, err
	}
	return outbound.BlockMetaRow{
		ChainID:        s.cfg.ChainID,
		BlockNumber:    blockNumber,
		BlockVersion:   version,
		BlockTimestamp: ts,
	}, nil
}
