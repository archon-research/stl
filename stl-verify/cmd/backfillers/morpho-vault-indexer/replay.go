package main

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// replayV2StructuredEvents re-walks the S3 receipts over [from,to] and feeds
// every VaultV2 structured event (adapter / cap / fee) from a persisted V2 vault
// through the same handler path the live worker uses (via ReplayMetaMorphoLog),
// bounded by an optional JSONL partition checkpoint.
//
// It runs after discovery/probe/persist so the vault registry, loaded here from
// the DB, already contains this run's newly-persisted vaults alongside any from
// earlier runs.
func replayV2StructuredEvents(
	ctx context.Context,
	logger *slog.Logger,
	s3Reader outbound.S3Reader,
	ethClient *ethclient.Client,
	multicaller outbound.Multicaller,
	pool *pgxpool.Pool,
	buildID buildregistry.BuildID,
	cfg config,
) error {
	svc, err := buildReplayService(logger, multicaller, pool, buildID, cfg.chainID)
	if err != nil {
		return fmt.Errorf("building replay service: %w", err)
	}
	if err := svc.LoadVaultRegistry(ctx); err != nil {
		return err
	}

	v2Vaults := svc.V2VaultAddresses()
	if len(v2Vaults) == 0 {
		logger.Info("no VaultV2 vaults known — skipping structured-event replay")
		return nil
	}

	topics, err := morpho_indexer.VaultV2StructuredEventTopics()
	if err != nil {
		return fmt.Errorf("deriving VaultV2 structured topics: %w", err)
	}

	checkpoint, err := openReplayCheckpoint(cfg.replayProgressFile)
	if err != nil {
		return fmt.Errorf("opening replay checkpoint: %w", err)
	}
	defer checkpoint.Close()

	tsCache := newBlockTimestampCache(ethClient)

	logger.Info("starting VaultV2 structured-event replay",
		"v2Vaults", len(v2Vaults),
		"from", cfg.from,
		"to", cfg.to,
		"checkpoint", cfg.replayProgressFile)

	// A partition is recorded done only after every event in it replays
	// successfully; the run hard-stops on the first failure. Every write below
	// goes through the same idempotent repo methods as live indexing, so a
	// resumed run that reprocesses an in-flight partition is safe.
	for _, part := range replayPartitionPrefixes(cfg.from, cfg.to) {
		if checkpoint.isDone(part) {
			logger.Info("skipping already-replayed partition", "partition", part)
			continue
		}
		if err := replayPartition(ctx, logger, s3Reader, svc, tsCache, cfg, part, v2Vaults, topics); err != nil {
			return fmt.Errorf("replaying partition %s: %w", part, err)
		}
		// Only a partition whose full range lies within [from,to] may be
		// checkpointed. A boundary partition is completeness-checked against the
		// [from,to] intersection alone, so recording it done would let a later run
		// reusing this progress file with WIDER bounds skip the now-in-scope
		// blocks — a silent cross-run hole. It replayed above (cheap, idempotent);
		// it just stays out of the checkpoint and replays again next run.
		if !partitionFullyCovered(part, cfg.from, cfg.to) {
			continue
		}
		if err := checkpoint.markDone(part); err != nil {
			return fmt.Errorf("recording partition %s done: %w", part, err)
		}
	}

	logger.Info("VaultV2 structured-event replay complete")
	return nil
}

// buildReplayService constructs the morpho-indexer Service wired for replay
// (no SQS consumer, no block cache) plus the repositories it needs.
func buildReplayService(logger *slog.Logger, multicaller outbound.Multicaller, pool *pgxpool.Pool, buildID buildregistry.BuildID, chainID int64) (*morpho_indexer.Service, error) {
	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}
	morphoRepo, err := postgres.NewMorphoRepository(pool, logger, buildID)
	if err != nil {
		return nil, fmt.Errorf("creating morpho repository: %w", err)
	}
	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, buildID, 0)
	if err != nil {
		return nil, fmt.Errorf("creating protocol repository: %w", err)
	}
	eventRepo := postgres.NewEventRepository(logger, buildID)

	svcConfig := morpho_indexer.ConfigDefaults()
	svcConfig.ChainID = chainID
	svcConfig.Logger = logger

	return morpho_indexer.NewReplayService(svcConfig, multicaller, txManager, protocolRepo, morphoRepo, eventRepo)
}

// openReplayCheckpoint returns a nil (no-op, nil-safe) checkpoint when no
// progress file is configured, else an append-only JSONL checkpoint at path.
func openReplayCheckpoint(path string) (*checkpoint, error) {
	if path == "" {
		return nil, nil
	}
	return loadCheckpoint(path)
}

// replayPartition collects, orders, and replays every structured V2 log in one
// S3 partition. Ordering is correctness-critical: handlers hard-error on an
// Allocate for an adapter they never saw AddAdapter for, so logs replay in
// strict (blockNumber, logIndex) order.
func replayPartition(
	ctx context.Context,
	logger *slog.Logger,
	s3Reader outbound.S3Reader,
	svc *morpho_indexer.Service,
	tsCache *blockTimestampCache,
	cfg config,
	part string,
	v2Vaults map[common.Address]struct{},
	topics map[common.Hash]struct{},
) error {
	entries, err := collectPartitionV2Logs(ctx, s3Reader, cfg.bucket, part, cfg.from, cfg.to, v2Vaults, topics)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	sortV2LogEntries(entries)

	for _, e := range entries {
		blockTimestamp, err := tsCache.timestampAt(ctx, e.blockHash)
		if err != nil {
			return err
		}
		if err := svc.ReplayMetaMorphoLog(ctx, e.log, e.blockNumber, e.blockHash, e.blockVersion, blockTimestamp); err != nil {
			return fmt.Errorf("replaying log tx=%s index=%d block=%d: %w", e.log.TransactionHash, e.logIndex, e.blockNumber, err)
		}
	}

	logger.Info("replayed partition", "partition", part, "events", len(entries))
	return nil
}

// collectPartitionV2Logs downloads the highest-version receipt file per block in
// the partition (ascending) and returns the structured V2 log entries in them,
// each stamped with the block's S3 version.
func collectPartitionV2Logs(
	ctx context.Context,
	s3Reader outbound.S3Reader,
	bucket, part string,
	from, to int64,
	v2Vaults map[common.Address]struct{},
	topics map[common.Hash]struct{},
) ([]v2LogEntry, error) {
	receiptKeys, err := listHighestVersionReceipts(ctx, s3Reader, bucket, part)
	if err != nil {
		return nil, fmt.Errorf("listing receipts for partition %s: %w", part, err)
	}

	type inRangeReceipt struct {
		key         string
		blockNumber int64
		version     int
	}
	presentBlocks := make([]int64, 0, len(receiptKeys))
	inRange := make([]inRangeReceipt, 0, len(receiptKeys))
	for _, key := range receiptKeys {
		parsed, ok := s3key.Parse(key)
		if !ok || parsed.BlockNumber < from || parsed.BlockNumber > to {
			continue
		}
		presentBlocks = append(presentBlocks, parsed.BlockNumber)
		inRange = append(inRange, inRangeReceipt{key: key, blockNumber: parsed.BlockNumber, version: parsed.Version})
	}

	// A block in the partition's [from,to] intersection with no receipt key would
	// contribute nothing yet still let the partition be marked done — a permanent
	// silent hole (re-runs skip the checkpointed partition). The discovery scan
	// only WARNs on such gaps; replay must hard-stop (no silent holes) so the
	// checkpoint stays unwritten and a repaired-S3 re-run retries.
	if err := requireCompletePartition(part, presentBlocks, from, to); err != nil {
		return nil, err
	}

	var entries []v2LogEntry
	for _, r := range inRange {
		receipts, err := downloadReceipts(ctx, s3Reader, bucket, r.key)
		if err != nil {
			return nil, fmt.Errorf("downloading %s: %w", r.key, err)
		}
		blockEntries, err := filterV2Logs(receipts, r.blockNumber, v2Vaults, topics)
		if err != nil {
			return nil, fmt.Errorf("filtering %s: %w", r.key, err)
		}
		for i := range blockEntries {
			blockEntries[i].blockVersion = r.version
		}
		entries = append(entries, blockEntries...)
	}
	return entries, nil
}

// requireCompletePartition errors if any block in the partition's [from,to]
// intersection is absent from presentBlocks. The expected range comes from the
// partition prefix (not from the present blocks), so an entirely empty partition
// — the maximal hole — is caught too.
func requireCompletePartition(part string, presentBlocks []int64, from, to int64) error {
	partStart, partEnd, ok := partitionBlockRange(part)
	if !ok {
		return fmt.Errorf("cannot parse partition range from prefix %q", part)
	}
	if partStart < from {
		partStart = from
	}
	if partEnd > to {
		partEnd = to
	}
	if partStart > partEnd {
		return nil // partition does not intersect [from,to]
	}

	present := make(map[int64]bool, len(presentBlocks))
	for _, bn := range presentBlocks {
		present[bn] = true
	}
	var missing []int64
	for bn := partStart; bn <= partEnd; bn++ {
		if !present[bn] {
			missing = append(missing, bn)
			if len(missing) >= 8 {
				break // bound the reported list; one missing block already fails the run
			}
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("partition %s missing receipt block(s) in [%d,%d] (S3 gap): %v", part, partStart, partEnd, missing)
	}
	return nil
}

// partitionBlockRange parses the "start-end" partition prefix into its inclusive
// block bounds.
func partitionBlockRange(part string) (start, end int64, ok bool) {
	before, after, found := strings.Cut(part, "-")
	if !found {
		return 0, 0, false
	}
	start, err1 := strconv.ParseInt(before, 10, 64)
	end, err2 := strconv.ParseInt(after, 10, 64)
	if err1 != nil || err2 != nil || start > end {
		return 0, 0, false
	}
	return start, end, true
}

// partitionFullyCovered reports whether the partition's entire block range lies
// within [from,to]. Only fully-covered partitions may be checkpointed (see the
// replay loop): a boundary partition the run's bounds only partially overlap is
// completeness-checked against the intersection alone, so recording it done
// would hide the blocks a later wider-bounds run brings into scope.
func partitionFullyCovered(part string, from, to int64) bool {
	partStart, partEnd, ok := partitionBlockRange(part)
	if !ok {
		return false
	}
	return partStart >= from && partEnd <= to
}

// replayPartitionPrefixes returns the S3 partition prefixes covering [from,to]
// in ascending start-block order. Replay must process partitions in block order
// so an AddAdapter in an earlier partition lands before a later partition's
// Allocate. partitionsForRange sorts lexicographically ("10000-10999" before
// "2000-2999"), which is fine for the order-agnostic discovery scan but wrong
// here; building the list from aligned block starts keeps it numeric-ascending.
func replayPartitionPrefixes(from, to int64) []string {
	var parts []string
	seen := make(map[string]bool)
	add := func(block int64) {
		p := partition.GetPartition(block)
		if !seen[p] {
			seen[p] = true
			parts = append(parts, p)
		}
	}
	for block := from - (from % partition.BlockRangeSize); block <= to; block += partition.BlockRangeSize {
		add(block)
	}
	add(to)
	return parts
}

// v2LogEntry is a single VaultV2 structured-event log queued for replay, carrying
// the block coordinates processMetaMorphoLog needs.
type v2LogEntry struct {
	log          shared.Log
	blockNumber  int64
	logIndex     int64
	blockVersion int
	blockHash    common.Hash
}

// filterV2Logs collects the logs in receipts emitted by a known VaultV2 vault
// (address in v2Vaults) whose topic0 is a structured V2 event (in topics). A
// malformed log index or a matching receipt with no block hash is a structural
// data defect that stops the run rather than being silently skipped.
func filterV2Logs(receipts []shared.TransactionReceipt, blockNumber int64, v2Vaults map[common.Address]struct{}, topics map[common.Hash]struct{}) ([]v2LogEntry, error) {
	var entries []v2LogEntry
	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			if len(log.Topics) == 0 {
				continue
			}
			if _, ok := v2Vaults[common.HexToAddress(log.Address)]; !ok {
				continue
			}
			if _, ok := topics[common.HexToHash(log.Topics[0])]; !ok {
				continue
			}
			if receipt.BlockHash == "" {
				return nil, fmt.Errorf("receipt %s at block %d carries a V2 structured log with no block hash", receipt.TransactionHash, blockNumber)
			}
			logIndex, err := strconv.ParseInt(log.LogIndex, 0, 64)
			if err != nil {
				return nil, fmt.Errorf("parsing log index %q (tx %s): %w", log.LogIndex, receipt.TransactionHash, err)
			}
			entries = append(entries, v2LogEntry{
				log:         log,
				blockNumber: blockNumber,
				logIndex:    logIndex,
				blockHash:   common.HexToHash(receipt.BlockHash),
			})
		}
	}
	return entries, nil
}

// sortV2LogEntries sorts entries in strict (blockNumber, logIndex) ascending
// order so AddAdapter always lands before that adapter's first Allocate.
func sortV2LogEntries(entries []v2LogEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].blockNumber != entries[j].blockNumber {
			return entries[i].blockNumber < entries[j].blockNumber
		}
		return entries[i].logIndex < entries[j].logIndex
	})
}

// headerTimeFetcher is the subset of *ethclient.Client the timestamp cache
// needs, narrowed so the cache can be tested with a fake.
type headerTimeFetcher interface {
	HeaderByHash(ctx context.Context, hash common.Hash) (*ethtypes.Header, error)
}

// blockTimestampCache memoizes block header timestamps for the replay run so a
// block bearing several events is fetched from the node only once. Timestamps
// are absent from S3 receipts, so they come from the header.
//
// Keyed and fetched by block HASH, not number: the replay pins every state read
// to the log's block hash (the receipt's canonical block), so its timestamp must
// come from that exact block. A number-pinned HeaderByNumber could return a
// different block across a reorg, stamping snapshots with the wrong time.
type blockTimestampCache struct {
	fetcher headerTimeFetcher
	cache   map[common.Hash]time.Time
}

func newBlockTimestampCache(fetcher headerTimeFetcher) *blockTimestampCache {
	return &blockTimestampCache{fetcher: fetcher, cache: make(map[common.Hash]time.Time)}
}

func (c *blockTimestampCache) timestampAt(ctx context.Context, blockHash common.Hash) (time.Time, error) {
	if ts, ok := c.cache[blockHash]; ok {
		return ts, nil
	}
	header, err := c.fetcher.HeaderByHash(ctx, blockHash)
	if err != nil {
		return time.Time{}, fmt.Errorf("fetching header for block %s: %w", blockHash.Hex(), err)
	}
	ts := time.Unix(int64(header.Time), 0).UTC()
	c.cache[blockHash] = ts
	return ts, nil
}
