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

// ErrCanonicalHashMoved marks a reorg observed mid-republish: the height's
// canonical hash is no longer the one whose payload was fetched. Deliberately
// NOT structural — the chain settles and a later attempt succeeds, whereas
// publishing what was read would enshrine a second losing fork.
var ErrCanonicalHashMoved = errors.New("the height's canonical hash moved mid-republish")

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

// NextFreeVersion reports the version a repair of this height must land in: one
// past what the raw archive already holds, and the first correction slot where it
// holds nothing. Callers settle this once and hand it to Republish, so a retried
// republish reuses the slot instead of stepping past the objects its own first
// attempt caused. It refuses a height the archive already holds the canonical
// block for — the whole check happens here, before anything is cached or
// published.
func (s *Service) NextFreeVersion(ctx context.Context, blockNumber int64) (int, error) {
	head, err := s.settledHeight(ctx, blockNumber)
	if err != nil {
		return 0, err
	}

	highest, archived, err := s.archivedTopVersion(ctx, blockNumber)
	if err != nil {
		return 0, err
	}
	if archived {
		if err := s.refuseIfAlreadyCanonical(ctx, blockNumber, head, highest); err != nil {
			return 0, err
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
	)
	return version, nil
}

// ArchivedVersion reports the version to republish a height at when the raw
// archive has already been repaired ahead of the indexers: the bulk downloader
// writes the canonical objects and tells no indexer, so the event must go out AT
// the version those objects occupy rather than one past it. It refuses anything
// but that state, and unlike NextFreeVersion it may answer 0 — a repair of a
// height the archive never held writes there.
func (s *Service) ArchivedVersion(ctx context.Context, blockNumber int64) (int, error) {
	head, err := s.settledHeight(ctx, blockNumber)
	if err != nil {
		return 0, err
	}

	version, archived, err := s.archivedTopVersion(ctx, blockNumber)
	if err != nil {
		return 0, err
	}
	if !archived {
		return 0, fmt.Errorf("archiveRepaired set but the archive holds nothing at block %d: %w", blockNumber, ErrStructuralData)
	}

	hash, err := s.confirmArchiveIsCanonical(ctx, blockNumber, head, version)
	if err != nil {
		return 0, err
	}

	s.logger.Info("derived the republish version from the repaired archive",
		"chainID", s.config.ChainID,
		"block", blockNumber,
		"archiveRepaired", true,
		"version", version,
		"hash", hash,
	)
	return version, nil
}

// confirmArchiveIsCanonical holds an archiveRepaired run to the one state it
// exists for: publishing at a version whose archived block is not the canonical
// one would enshrine a fork in the slot meant to correct it.
func (s *Service) confirmArchiveIsCanonical(ctx context.Context, blockNumber, head int64, version int) (string, error) {
	archivedHash, found, err := s.archivedHash(ctx, blockNumber, version)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("the archive's top version %d at block %d names no block; drop archiveRepaired: %w",
			version, blockNumber, ErrStructuralData)
	}

	block, err := s.canonicalHeader(ctx, blockNumber, head)
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(archivedHash, block.Hash) {
		return "", fmt.Errorf("the archive's top version %d at block %d is not the canonical block (%s, want %s); drop archiveRepaired: %w",
			version, blockNumber, archivedHash, block.Hash, ErrStructuralData)
	}
	return archivedHash, nil
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
func (s *Service) refuseIfAlreadyCanonical(ctx context.Context, blockNumber, head int64, version int) error {
	archivedHash, found, err := s.archivedHash(ctx, blockNumber, version)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	block, err := s.canonicalHeader(ctx, blockNumber, head)
	if err != nil {
		return err
	}
	if strings.EqualFold(archivedHash, block.Hash) {
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

// Republish caches the canonical block at blockNumber under version, reporting
// each phase as it starts, and announces it on the chain's block feed. It never
// reads the archive: a repeat of the same (height, version) re-caches the same
// keys and re-publishes the same event, which every consumer already
// deduplicates.
func (s *Service) Republish(ctx context.Context, blockNumber int64, version int, report PhaseReporter) (Result, error) {
	if err := validateTarget(blockNumber, version); err != nil {
		return Result{}, err
	}
	report.enter(ctx, PhaseFetching)

	head, err := s.client.GetCurrentBlockNumber(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("reading the chain head: %w", err)
	}
	if err := refuseNearHead(blockNumber, head); err != nil {
		return Result{}, err
	}

	block, err := s.canonicalHeader(ctx, blockNumber, head)
	if err != nil {
		return Result{}, err
	}

	data, dataTypes, err := s.fetchPinnedToHash(ctx, blockNumber, block.Hash)
	if err != nil {
		return Result{}, err
	}

	if err := s.confirmStillCanonical(ctx, blockNumber, block.Hash); err != nil {
		return Result{}, err
	}

	report.enter(ctx, PhaseCaching)
	if err := s.cache.SetBlockData(ctx, s.config.ChainID, blockNumber, version, data); err != nil {
		return Result{}, fmt.Errorf("caching block %d at version %d: %w", blockNumber, version, err)
	}

	report.enter(ctx, PhasePublishing)
	if err := s.publish(ctx, blockNumber, version, block); err != nil {
		return Result{}, err
	}

	s.logger.Info("republished block",
		"chainID", s.config.ChainID,
		"block", blockNumber,
		"hash", block.Hash,
		"parentHash", block.ParentHash,
		"version", version,
		"dataTypes", dataTypes,
	)
	return Result{
		BlockNumber:    blockNumber,
		BlockHash:      block.Hash,
		ParentHash:     block.ParentHash,
		BlockTimestamp: block.timestamp,
		Version:        version,
		DataTypes:      dataTypes,
	}, nil
}

func validateTarget(blockNumber int64, version int) error {
	if err := validateHeight(blockNumber); err != nil {
		return err
	}
	// 0 is a legal target only for the archiveRepaired derivation, which is the
	// caller that answers it; the default one never yields a slot below 1.
	if version < 0 {
		return fmt.Errorf("version must not be negative, got %d: %w", version, ErrStructuralData)
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
// The two by-number reads are seconds apart, so a height inside the reorg window
// can pass the canonical check and be orphaned moments later — writing a second
// losing fork into the slot meant to correct the first.
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

// fetchPinnedToHash reads the payload the event will point at, pinned to the
// hash rather than the number so every data type describes one block even if the
// height reorgs while the batch is in flight.
func (s *Service) fetchPinnedToHash(ctx context.Context, blockNumber int64, hash string) (outbound.BlockDataInput, []string, error) {
	fetched, err := s.client.GetBlockDataByHash(ctx, blockNumber, hash, true)
	if err != nil {
		return outbound.BlockDataInput{}, nil, fmt.Errorf("fetching block %d at hash %s: %w", blockNumber, hash, err)
	}

	dataTypes, err := validatePayloads(blockNumber, hash, s.publishedPayloads(fetched))
	if err != nil {
		return outbound.BlockDataInput{}, nil, err
	}
	if err := confirmPayloadHash(blockNumber, hash, fetched.Block); err != nil {
		return outbound.BlockDataInput{}, nil, err
	}

	data := outbound.BlockDataInput{Block: fetched.Block, Receipts: fetched.Receipts}
	if s.config.EnableTraces {
		data.Traces = fetched.Traces
	}
	if s.config.EnableBlobs {
		data.Blobs = fetched.Blobs
	}
	return data, dataTypes, nil
}

type payload struct {
	name     string
	raw      json.RawMessage
	fetchErr error
}

// publishedPayloads lists what this chain's watcher caches for a block. Block
// and receipts are unconditional; traces and blobs follow the same switches the
// watcher runs with, so the republished cache entry matches the live one.
func (s *Service) publishedPayloads(fetched outbound.BlockData) []payload {
	payloads := []payload{
		{name: "block", raw: fetched.Block, fetchErr: fetched.BlockErr},
		{name: "receipts", raw: fetched.Receipts, fetchErr: fetched.ReceiptsErr},
	}
	if s.config.EnableTraces {
		payloads = append(payloads, payload{name: "traces", raw: fetched.Traces, fetchErr: fetched.TracesErr})
	}
	if s.config.EnableBlobs {
		payloads = append(payloads, payload{name: "blobs", raw: fetched.Blobs, fetchErr: fetched.BlobsErr})
	}
	return payloads
}

// validatePayloads refuses an incomplete answer rather than caching a hole: a
// consumer that finds one data type missing dead-letters the block, and the
// republish would have to be redone anyway.
func validatePayloads(blockNumber int64, hash string, payloads []payload) ([]string, error) {
	names := make([]string, 0, len(payloads))
	for _, p := range payloads {
		// Not structural, for the same reason canonicalHeader's null is not.
		if isUpstreamNull(p.raw, p.fetchErr) {
			return nil, fmt.Errorf("the node has no %s for block %d at hash %s; a node this far below the head that cannot serve it is behind",
				p.name, blockNumber, hash)
		}
		if p.fetchErr != nil {
			return nil, fmt.Errorf("fetching %s for block %d at hash %s: %w", p.name, blockNumber, hash, p.fetchErr)
		}
		names = append(names, p.name)
	}
	return names, nil
}

// confirmPayloadHash holds the by-hash answer to the hash it was pinned to:
// pinning keeps the data types consistent with each other, not with the header
// the event carries beside them.
func confirmPayloadHash(blockNumber int64, hash string, block json.RawMessage) error {
	got, found := archiveblock.HashFromPayload(block)
	if !found {
		return fmt.Errorf("the payload fetched for block %d at hash %s carries no hash: %w",
			blockNumber, hash, ErrStructuralData)
	}
	if !strings.EqualFold(got, hash) {
		return fmt.Errorf("the payload fetched for block %d at hash %s names block %s instead: %w",
			blockNumber, hash, got, ErrStructuralData)
	}
	return nil
}

// confirmStillCanonical re-reads the height once the payload is in hand. Pinning
// to the hash keeps the payload self-consistent but says nothing about whether
// that hash is still canonical — a node serves an orphan by hash just as
// happily. Without this second read, a republish started moments before a reorg
// would enshrine a fresh losing fork as the correction.
func (s *Service) confirmStillCanonical(ctx context.Context, blockNumber int64, want string) error {
	raw, err := s.client.GetBlockByNumber(ctx, blockNumber, false)
	if isUpstreamNull(raw, err) {
		return fmt.Errorf("block %d left the canonical chain mid-republish: %w", blockNumber, ErrCanonicalHashMoved)
	}
	if err != nil {
		return fmt.Errorf("re-reading block %d by number: %w", blockNumber, err)
	}
	current, err := decodeHeader(blockNumber, raw)
	if err != nil {
		return err
	}
	if !strings.EqualFold(current.Hash, want) {
		return fmt.Errorf("block %d moved from %s to %s between the two reads: %w",
			blockNumber, want, current.Hash, ErrCanonicalHashMoved)
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
