// Package block_republish re-publishes an already-mined block under a new
// version, so every consumer of the chain's block feed appends it as a
// correction. What it repairs, what it deliberately does not write, and how an
// operator starts a run are in the block-republisher binary's package doc.
package block_republish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/archiveblock"
	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ErrStructuralData marks a failure that reproduces identically on every
// attempt: a block the node will not serve, a payload it answers null for, a
// height that is not a height. A transient fault (RPC/archive/cache/SNS network
// error, throttling, timeout) must never wrap it — surviving those is what a
// retry envelope is for.
var ErrStructuralData = errors.New("structural data defect")

// ErrCanonicalHashMoved marks a height whose block is not the one the run
// derived: it reorged since, or the node answered wrong. Deliberately NOT
// structural — the chain settles and a later attempt succeeds, whereas
// publishing what was read would enshrine a second losing fork.
var ErrCanonicalHashMoved = errors.New("the height's block is not the one the run derived")

// finalityDepth matches the watcher's own FinalityBlockCount: the depth past
// which it stops looking for reorgs.
const finalityDepth = 64

// Config is the deployment's static configuration. EnableTraces and EnableBlobs
// must match the flags this chain's watcher runs with, so a republished block
// carries the same data types the live one did; a consumer that expects traces
// dead-letters a block published without them.
type Config struct {
	ChainID      int64
	EnableTraces bool
	EnableBlobs  bool
	Logger       *slog.Logger
}

// Phase names the step a republish has entered. A caller reports it as activity
// liveness, so a worker that dies mid-block is noticed long before the attempt's
// own timeout.
type Phase string

const (
	PhaseFetching   Phase = "fetching"
	PhaseCaching    Phase = "caching"
	PhasePublishing Phase = "publishing"
)

// PhaseReporter is told each phase as it starts. A nil reporter is a caller that
// needs no liveness signal.
type PhaseReporter func(ctx context.Context, phase Phase)

func (r PhaseReporter) enter(ctx context.Context, phase Phase) {
	if r != nil {
		r(ctx, phase)
	}
}

// Result is what one republished block landed as.
type Result struct {
	BlockNumber    int64    `json:"blockNumber"`
	BlockHash      string   `json:"blockHash"`
	ParentHash     string   `json:"parentHash"`
	BlockTimestamp int64    `json:"blockTimestamp"`
	Version        int      `json:"version"`
	DataTypes      []string `json:"dataTypes"`
}

// Service republishes single blocks. It holds no per-run state.
type Service struct {
	config  Config
	client  outbound.BlockchainClient
	archive outbound.ArchiveReader
	cache   outbound.BlockCacheWriter
	sink    outbound.EventSink
	logger  *slog.Logger
}

func NewService(config Config, client outbound.BlockchainClient, archive outbound.ArchiveReader, cache outbound.BlockCacheWriter, sink outbound.EventSink) (*Service, error) {
	if config.ChainID <= 0 {
		return nil, fmt.Errorf("ChainID must be positive, got %d", config.ChainID)
	}
	if client == nil {
		return nil, fmt.Errorf("blockchain client is required")
	}
	if archive == nil {
		return nil, fmt.Errorf("archive version reader is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("block cache is required")
	}
	if sink == nil {
		return nil, fmt.Errorf("event sink is required")
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		config:  config,
		client:  client,
		archive: archive,
		cache:   cache,
		sink:    sink,
		logger:  logger.With("component", "block-republish"),
	}, nil
}

// NextFreeVersion reports the version a repair of this height must land in —
// one past what the raw archive already holds, and the first correction slot
// where it holds nothing — with the canonical block that repair must carry.
// Callers settle both once and hand them to Republish, so a retried republish
// reuses the slot instead of stepping past the objects its own first attempt
// caused, and verifies against the block this read saw. It refuses a height the
// archive already holds the canonical block for — the whole check happens here,
// before anything is cached or published.
func (s *Service) NextFreeVersion(ctx context.Context, blockNumber int64) (int, string, error) {
	head, err := s.settledHeight(ctx, blockNumber)
	if err != nil {
		return 0, "", err
	}
	block, err := s.canonicalHeader(ctx, blockNumber, head)
	if err != nil {
		return 0, "", err
	}

	highest, archived, err := s.archivedTopVersion(ctx, blockNumber)
	if err != nil {
		return 0, "", err
	}
	if archived {
		if err := s.refuseIfAlreadyCanonical(ctx, blockNumber, block.Hash, highest); err != nil {
			return 0, "", err
		}
	} else {
		s.logger.Warn("repairing a height the archive holds nothing for; no archived object to compare with the canonical chain",
			"chainID", s.config.ChainID,
			"block", blockNumber,
		)
	}

	version := s3key.NextVersion(highest, archived)
	s.logger.Info("derived the republish version from the archive",
		"chainID", s.config.ChainID,
		"block", blockNumber,
		"archived", archived,
		"highestArchivedVersion", highest,
		"version", version,
		"hash", block.Hash,
	)
	return version, block.Hash, nil
}

// ArchivedVersion reports the version to republish a height at when the raw
// archive has already been repaired ahead of the indexers: the bulk downloader
// writes the canonical objects and tells no indexer, so the event must go out AT
// the version those objects occupy rather than one past it. It refuses anything
// but that state, and unlike NextFreeVersion it may answer 0 — a repair of a
// height the archive never held writes there.
func (s *Service) ArchivedVersion(ctx context.Context, blockNumber int64) (int, string, error) {
	head, err := s.settledHeight(ctx, blockNumber)
	if err != nil {
		return 0, "", err
	}
	block, err := s.canonicalHeader(ctx, blockNumber, head)
	if err != nil {
		return 0, "", err
	}

	version, archived, err := s.archivedTopVersion(ctx, blockNumber)
	if err != nil {
		return 0, "", err
	}
	if !archived {
		return 0, "", fmt.Errorf("archiveRepaired set but the archive holds nothing at block %d: %w", blockNumber, ErrStructuralData)
	}
	if err := s.confirmArchiveIsCanonical(ctx, blockNumber, block.Hash, version); err != nil {
		return 0, "", err
	}

	s.logger.Info("derived the republish version from the repaired archive",
		"chainID", s.config.ChainID,
		"block", blockNumber,
		"archiveRepaired", true,
		"version", version,
		"hash", block.Hash,
	)
	return version, block.Hash, nil
}

// confirmArchiveIsCanonical holds an archiveRepaired run to the one state it
// exists for: publishing at a version whose archived block is not the canonical
// one would enshrine a fork in the slot meant to correct it.
func (s *Service) confirmArchiveIsCanonical(ctx context.Context, blockNumber int64, hash string, version int) error {
	archivedHash, found, err := s.archivedHash(ctx, blockNumber, version)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("the archive's top version %d at block %d names no block; drop archiveRepaired: %w",
			version, blockNumber, ErrStructuralData)
	}
	if !strings.EqualFold(archivedHash, hash) {
		return fmt.Errorf("the archive's top version %d at block %d is not the canonical block (%s, want %s); drop archiveRepaired: %w",
			version, blockNumber, archivedHash, hash, ErrStructuralData)
	}
	return nil
}

// settledHeight refuses a height that is not one, and one the chain is still
// free to reorg, returning the head both derivations then compare against.
func (s *Service) settledHeight(ctx context.Context, blockNumber int64) (int64, error) {
	if err := validateHeight(blockNumber); err != nil {
		return 0, err
	}
	head, err := s.client.GetCurrentBlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("reading the chain head: %w", err)
	}
	if err := refuseNearHead(blockNumber, head); err != nil {
		return 0, err
	}
	return head, nil
}

func (s *Service) archivedTopVersion(ctx context.Context, blockNumber int64) (int, bool, error) {
	highest, archived, err := s.archive.HighestVersion(ctx, blockNumber)
	if err != nil {
		// An object under the height's own prefix that carries no version is a
		// slot nothing can read, and no attempt will read it differently.
		if errors.Is(err, s3key.ErrUnrecognisedKey) {
			return 0, false, fmt.Errorf("reading what the archive holds at block %d: %v: %w", blockNumber, err, ErrStructuralData)
		}
		return 0, false, fmt.Errorf("reading what the archive holds at block %d: %w", blockNumber, err)
	}
	return highest, archived, nil
}

// refuseIfAlreadyCanonical stops a height whose top archived version already
// holds the canonical block. Republishing it would append an identical
// correction that every reader then prefers — permanently, in S3 and in every
// indexer — so the operator's list, not a retry, is what has to change. A version
// naming no block at all is a height to repair, which is why only a hash that
// matches refuses.
func (s *Service) refuseIfAlreadyCanonical(ctx context.Context, blockNumber int64, hash string, version int) error {
	archivedHash, found, err := s.archivedHash(ctx, blockNumber, version)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if strings.EqualFold(archivedHash, hash) {
		return fmt.Errorf("block %d is already canonical in the archive at version %d (hash %s); nothing to republish: %w",
			blockNumber, version, archivedHash, ErrStructuralData)
	}
	return nil
}

// archivedHash reads the block the archive names at a version. An object no
// attempt can read — not a gzip stream, empty, or a hash beyond the prefix —
// stops the height rather than burning the retry envelope on a fixed verdict.
func (s *Service) archivedHash(ctx context.Context, blockNumber int64, version int) (string, bool, error) {
	hash, found, err := s.archive.BlockHashAt(ctx, blockNumber, version)
	if errors.Is(err, archiveblock.ErrUnreadable) {
		return "", false, fmt.Errorf("reading the block the archive holds at block %d version %d: %v: %w",
			blockNumber, version, err, ErrStructuralData)
	}
	if err != nil {
		return "", false, fmt.Errorf("reading the block the archive holds at block %d version %d: %w", blockNumber, version, err)
	}
	return hash, found, nil
}

// Republish caches the block the run derived under version, reporting each phase
// as it starts, and announces it on the chain's block feed. It takes the
// canonical hash rather than re-reading it: the derivation read it at a height
// already at least finalityDepth blocks below the head, where a second read can
// only confirm what the first one saw. It never reads the archive either — a
// repeat of the same (height, version) re-caches the same keys and re-publishes
// the same event, which every consumer already deduplicates.
func (s *Service) Republish(ctx context.Context, blockNumber int64, version int, canonicalHash string, report PhaseReporter) (Result, error) {
	if err := validateTarget(blockNumber, version, canonicalHash); err != nil {
		return Result{}, err
	}
	report.enter(ctx, PhaseFetching)

	fetched, err := s.fetchAtHeight(ctx, blockNumber, canonicalHash)
	if err != nil {
		return Result{}, err
	}

	report.enter(ctx, PhaseCaching)
	if err := s.cache.SetBlockData(ctx, s.config.ChainID, blockNumber, version, fetched.data); err != nil {
		return Result{}, fmt.Errorf("caching block %d at version %d: %w", blockNumber, version, err)
	}

	report.enter(ctx, PhasePublishing)
	if err := s.publish(ctx, blockNumber, version, fetched.header); err != nil {
		return Result{}, err
	}

	s.logger.Info("republished block",
		"chainID", s.config.ChainID,
		"block", blockNumber,
		"hash", fetched.header.Hash,
		"parentHash", fetched.header.ParentHash,
		"version", version,
		"dataTypes", fetched.dataTypes,
	)
	return Result{
		BlockNumber:    blockNumber,
		BlockHash:      fetched.header.Hash,
		ParentHash:     fetched.header.ParentHash,
		BlockTimestamp: fetched.header.timestamp,
		Version:        version,
		DataTypes:      fetched.dataTypes,
	}, nil
}

func validateTarget(blockNumber int64, version int, canonicalHash string) error {
	if err := validateHeight(blockNumber); err != nil {
		return err
	}
	// 0 is a legal target only for the archiveRepaired derivation, which is the
	// caller that answers it; the default one never yields a slot below 1.
	if version < 0 {
		return fmt.Errorf("version must not be negative, got %d: %w", version, ErrStructuralData)
	}
	// The derivation read the canonical block to settle the version; without its
	// hash there is nothing to hold the fetched payloads to.
	if canonicalHash == "" {
		return fmt.Errorf("block %d was handed no canonical hash to verify against: %w", blockNumber, ErrStructuralData)
	}
	return nil
}

func validateHeight(blockNumber int64) error {
	if blockNumber <= 0 {
		return fmt.Errorf("block number must be positive, got %d: %w", blockNumber, ErrStructuralData)
	}
	return nil
}

// refuseNearHead keeps a repair off the part of the chain that is still moving.
// The canonical hash is read once, when the version is derived, and the payload
// fetched against it moments later; a height inside the reorg window can be
// orphaned between the two — writing a second losing fork into the slot meant to
// correct the first.
func refuseNearHead(blockNumber, head int64) error {
	if blockNumber > head {
		return fmt.Errorf("block %d is above the chain head %d: %w", blockNumber, head, ErrStructuralData)
	}
	if head-blockNumber < finalityDepth {
		return fmt.Errorf("block %d is %d blocks below the chain head %d, inside the %d-block reorg window: %w",
			blockNumber, head-blockNumber, head, finalityDepth, ErrStructuralData)
	}
	return nil
}

// blockHeader is the part of an eth_getBlockByNumber answer the event carries.
type blockHeader struct {
	Number     string `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Timestamp  string `json:"timestamp"`

	timestamp int64
}

func (s *Service) canonicalHeader(ctx context.Context, blockNumber, head int64) (blockHeader, error) {
	raw, err := s.client.GetBlockByNumber(ctx, blockNumber, false)
	// Not structural: the head read above already proved a synced node knows this
	// height, so a null here is a replica behind that head, and the next attempt
	// asks a different one.
	if isUpstreamNull(raw, err) {
		return blockHeader{}, fmt.Errorf("the node has no block %d, %d blocks below the chain head %d",
			blockNumber, head-blockNumber, head)
	}
	if err != nil {
		return blockHeader{}, fmt.Errorf("reading block %d by number: %w", blockNumber, err)
	}
	return decodeHeader(blockNumber, raw)
}

// fetchedBlock is what one height's reads produced: the payload set to cache,
// the data types it covers, and the header the event carries.
type fetchedBlock struct {
	data      outbound.BlockDataInput
	dataTypes []string
	header    blockHeader
}

// fetchAtHeight reads the payload the event will point at, by number: an archive
// node serves trace_block by hash only within a few thousand blocks of the head,
// and every height this repairs is far older than that. Each answer is then held
// to the hash the run derived, which is what asking by hash used to guarantee.
func (s *Service) fetchAtHeight(ctx context.Context, blockNumber int64, hash string) (fetchedBlock, error) {
	var fetched fetchedBlock
	var err error

	if fetched.data.Block, fetched.header, err = s.readBlock(ctx, blockNumber, hash); err != nil {
		return fetchedBlock{}, err
	}
	fetched.dataTypes = []string{"block"}

	if fetched.data.Receipts, err = s.readList(ctx, "receipts", blockNumber, hash, fetched.data.Block, s.client.GetBlockReceipts); err != nil {
		return fetchedBlock{}, err
	}
	fetched.dataTypes = append(fetched.dataTypes, "receipts")

	// Traces and blobs follow the same switches the watcher runs with, so the
	// republished cache entry carries the data types the live one did.
	if s.config.EnableTraces {
		if fetched.data.Traces, err = s.readList(ctx, "traces", blockNumber, hash, fetched.data.Block, s.client.GetBlockTraces); err != nil {
			return fetchedBlock{}, err
		}
		fetched.dataTypes = append(fetched.dataTypes, "traces")
	}
	if s.config.EnableBlobs {
		// An empty sidecar list is the truth for every block without blob
		// transactions, so presence is all an answer can be held to.
		if fetched.data.Blobs, err = s.readPayload(ctx, "blobs", blockNumber, s.client.GetBlobSidecars); err != nil {
			return fetchedBlock{}, err
		}
		fetched.dataTypes = append(fetched.dataTypes, "blobs")
	}
	return fetched, nil
}

// numberRead is one data type's by-number read on the blockchain port.
type numberRead func(ctx context.Context, blockNumber int64) (json.RawMessage, error)

// readPayload issues one read and refuses an incomplete answer rather than
// caching a hole: a consumer that finds one data type missing dead-letters the
// block, and the republish would have to be redone anyway.
func (s *Service) readPayload(ctx context.Context, name string, blockNumber int64, read numberRead) (json.RawMessage, error) {
	raw, err := read(ctx, blockNumber)
	// Not structural, for the same reason canonicalHeader's null is not.
	if isUpstreamNull(raw, err) {
		return nil, fmt.Errorf("the node has no %s for block %d; a node this far below the head that cannot serve it is behind",
			name, blockNumber)
	}
	if err != nil {
		return nil, fmt.Errorf("fetching %s for block %d: %w", name, blockNumber, err)
	}
	return raw, nil
}

// readBlock answers with the payload to cache and the header the event carries:
// the block fetched by number is the one document that names both.
func (s *Service) readBlock(ctx context.Context, blockNumber int64, hash string) (json.RawMessage, blockHeader, error) {
	raw, err := s.readPayload(ctx, "block", blockNumber, func(ctx context.Context, number int64) (json.RawMessage, error) {
		return s.client.GetBlockByNumber(ctx, number, true)
	})
	if err != nil {
		return nil, blockHeader{}, err
	}
	if err := confirmPayloadHash(blockNumber, hash, raw); err != nil {
		return nil, blockHeader{}, err
	}
	header, err := decodeHeader(blockNumber, raw)
	if err != nil {
		return nil, blockHeader{}, err
	}
	return raw, header, nil
}

func (s *Service) readList(ctx context.Context, name string, blockNumber int64, hash string, block json.RawMessage, read numberRead) (json.RawMessage, error) {
	raw, err := s.readPayload(ctx, name, blockNumber, read)
	if err != nil {
		return nil, err
	}
	if err := confirmListDescribesBlock(name, blockNumber, hash, raw, block); err != nil {
		return nil, err
	}
	return raw, nil
}

// confirmListDescribesBlock holds a receipts or traces answer to the block the
// header named. It is not structural: a replica behind the head answers the same
// by-number call with another height's list, or with none at all, and the next
// attempt asks again — the verdict an upstream null gets.
func confirmListDescribesBlock(name string, blockNumber int64, hash string, list, block json.RawMessage) error {
	got, err := archiveblock.ListBlockHash(list)
	if errors.Is(err, archiveblock.ErrEmptyList) {
		return confirmBlockHasNoTransactions(name, blockNumber, block)
	}
	if err != nil {
		return fmt.Errorf("the %s fetched for block %d name no block: %w", name, blockNumber, err)
	}
	if !strings.EqualFold(got, hash) {
		return fmt.Errorf("the %s fetched for block %d name block %s, not the canonical %s", name, blockNumber, got, hash)
	}
	return nil
}

// confirmBlockHasNoTransactions is what makes an empty list an answer rather
// than a hole: a block with transactions has both receipts and traces.
func confirmBlockHasNoTransactions(name string, blockNumber int64, block json.RawMessage) error {
	populated, err := archiveblock.HasTransactions(block)
	if err != nil {
		return fmt.Errorf("reading the transactions of block %d: %w", blockNumber, err)
	}
	if populated {
		return fmt.Errorf("the node has no %s for block %d, which has transactions", name, blockNumber)
	}
	return nil
}

// confirmPayloadHash holds the block payload to the hash the run derived: a node
// serves an orphan, or a neighbouring height, by number just as happily. Not
// structural — the height reorged after the derivation read it, or the node
// answered wrong, and both clear on a later attempt or a re-derived run.
func confirmPayloadHash(blockNumber int64, hash string, block json.RawMessage) error {
	got, found := archiveblock.HashFromPayload(block)
	if !found {
		return fmt.Errorf("the payload fetched for block %d carries no hash, want the derived %s", blockNumber, hash)
	}
	if !strings.EqualFold(got, hash) {
		return fmt.Errorf("block %d came back as %s, not the %s this run derived: %w",
			blockNumber, got, hash, ErrCanonicalHashMoved)
	}
	return nil
}

func (s *Service) publish(ctx context.Context, blockNumber int64, version int, block blockHeader) error {
	// Both are honest, and no consumer branches on either: the height already
	// published a different block, and this one was fetched by hand rather than
	// observed live.
	event := outbound.BlockEvent{
		ChainID:        s.config.ChainID,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      block.Hash,
		ParentHash:     block.ParentHash,
		BlockTimestamp: block.timestamp,
		ReceivedAt:     time.Now().UTC(),
		IsReorg:        true,
		IsBackfill:     true,
	}
	if err := s.sink.Publish(ctx, event); err != nil {
		return fmt.Errorf("publishing block %d at version %d: %w", blockNumber, version, err)
	}
	return nil
}

func decodeHeader(blockNumber int64, raw json.RawMessage) (blockHeader, error) {
	var block blockHeader
	if err := json.Unmarshal(raw, &block); err != nil {
		return blockHeader{}, fmt.Errorf("decoding block %d: %v: %w", blockNumber, err, ErrStructuralData)
	}
	if block.Hash == "" {
		return blockHeader{}, fmt.Errorf("block %d came back without a hash: %w", blockNumber, ErrStructuralData)
	}
	if err := confirmHeight(blockNumber, block.Number); err != nil {
		return blockHeader{}, err
	}
	timestamp, err := hexutil.ParseInt64(block.Timestamp)
	if err != nil {
		return blockHeader{}, fmt.Errorf("decoding block %d timestamp %q: %v: %w",
			blockNumber, block.Timestamp, err, ErrStructuralData)
	}
	block.timestamp = timestamp
	return block, nil
}

// confirmHeight holds the answer to the height that was asked for. A replica
// serving a neighbouring block by number would otherwise have it cached and
// published under the requested one, as that height's correction.
func confirmHeight(blockNumber int64, number string) error {
	if number == "" {
		return fmt.Errorf("block %d came back without a number: %w", blockNumber, ErrStructuralData)
	}
	got, err := hexutil.ParseInt64(number)
	if err != nil {
		return fmt.Errorf("decoding block %d number %q: %v: %w", blockNumber, number, err, ErrStructuralData)
	}
	if got != blockNumber {
		return fmt.Errorf("asked for block %d and got block %d: %w", blockNumber, got, ErrStructuralData)
	}
	return nil
}

// isUpstreamNull reports the one RPC answer that means "no such block or
// payload": the adapter turns a literal JSON null into ErrUpstreamNullResult,
// while a raw batch element carries the four bytes "null" instead.
func isUpstreamNull(raw json.RawMessage, err error) bool {
	if errors.Is(err, rpcutil.ErrUpstreamNullResult) {
		return true
	}
	return err == nil && rpcutil.IsNullOrEmpty(raw)
}
