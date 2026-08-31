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

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ErrStructuralData marks a failure that reproduces identically on every
// attempt: a block the node will not serve, a payload it answers null for, a
// version below 1. A transient fault (RPC/cache/SNS network error, throttling,
// timeout) must never wrap it — surviving those is what a retry envelope is for.
var ErrStructuralData = errors.New("structural data defect")

// ErrCanonicalHashMoved marks a reorg observed mid-republish: the height's
// canonical hash is no longer the one whose payload was fetched. Deliberately
// NOT structural — the chain settles and a later attempt succeeds, whereas
// publishing what was read would enshrine a second losing fork.
var ErrCanonicalHashMoved = errors.New("the height's canonical hash moved mid-republish")

// minVersion is the lowest version a republish may target; version 0 holds the
// data being corrected.
const minVersion = 1

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
	config Config
	client outbound.BlockchainClient
	cache  outbound.BlockCacheWriter
	sink   outbound.EventSink
	logger *slog.Logger
}

func NewService(config Config, client outbound.BlockchainClient, cache outbound.BlockCacheWriter, sink outbound.EventSink) (*Service, error) {
	if config.ChainID <= 0 {
		return nil, fmt.Errorf("ChainID must be positive, got %d", config.ChainID)
	}
	if client == nil {
		return nil, fmt.Errorf("blockchain client is required")
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
		config: config,
		client: client,
		cache:  cache,
		sink:   sink,
		logger: logger.With("component", "block-republish"),
	}, nil
}

// Republish caches the canonical block at blockNumber under version and
// announces it on the chain's block feed.
func (s *Service) Republish(ctx context.Context, blockNumber int64, version int) (Result, error) {
	if err := validateTarget(blockNumber, version); err != nil {
		return Result{}, err
	}

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

	if err := s.cache.SetBlockData(ctx, s.config.ChainID, blockNumber, version, data); err != nil {
		return Result{}, fmt.Errorf("caching block %d at version %d: %w", blockNumber, version, err)
	}

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
	if blockNumber <= 0 {
		return fmt.Errorf("block number must be positive, got %d: %w", blockNumber, ErrStructuralData)
	}
	if version < minVersion {
		return fmt.Errorf("version must be at least %d, got %d — version 0 is the slot being corrected: %w",
			minVersion, version, ErrStructuralData)
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
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Timestamp  string `json:"timestamp"`

	timestamp int64
}

func (s *Service) canonicalHeader(ctx context.Context, blockNumber, head int64) (blockHeader, error) {
	raw, err := s.client.GetBlockByNumber(ctx, blockNumber, false)
	if isUpstreamNull(raw, err) {
		return blockHeader{}, fmt.Errorf("the node has no block %d, %d blocks below the chain head %d: %w",
			blockNumber, head-blockNumber, head, ErrStructuralData)
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
		if isUpstreamNull(p.raw, p.fetchErr) {
			return nil, fmt.Errorf("the node has no %s for block %d at hash %s: %w",
				p.name, blockNumber, hash, ErrStructuralData)
		}
		if p.fetchErr != nil {
			return nil, fmt.Errorf("fetching %s for block %d at hash %s: %w", p.name, blockNumber, hash, p.fetchErr)
		}
		names = append(names, p.name)
	}
	return names, nil
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
	timestamp, err := hexutil.ParseInt64(block.Timestamp)
	if err != nil {
		return blockHeader{}, fmt.Errorf("decoding block %d timestamp %q: %v: %w",
			blockNumber, block.Timestamp, err, ErrStructuralData)
	}
	block.timestamp = timestamp
	return block, nil
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
