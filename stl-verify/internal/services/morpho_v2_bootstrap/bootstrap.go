// Package morpho_v2_bootstrap heals Morpho VaultV2 vaults that were discovered
// before VaultV2 discovery became atomic (VEC-218).
//
// Those vaults have a morpho_vault row but no morpho_adapter / adapter_state /
// cap / fee rows, and live indexing never revisits them: the SQS handler
// short-circuits on IsKnownVault before it would enumerate anything, and their
// AddAdapter / cap / fee events are historical, so they never arrive on the live
// stream again. One run of this service repairs every one of them.
//
// It runs in two passes over the V2 vaults of the configured chain:
//
//  1. Seed — enumerate each vault's current adapter set on-chain and write one
//     registry row plus one realAssets() snapshot per adapter, all pinned to a
//     single finalized block.
//  2. Replay — sweep eth_getLogs from the VaultV2 factory deploy block to that
//     same block for the 10 VaultV2 governance events, and feed each one through
//     the live handler path (Service.ReplayMetaMorphoLog) in (block, logIndex)
//     order, rebuilding the adapter/cap/fee history.
//
// Every write goes through the same idempotent repository methods live indexing
// uses, so re-running is safe. Any failure stops the run: a partial pass leaves
// no silent holes because a re-run simply redoes the work.
package morpho_v2_bootstrap

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/blocktime"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// canonicalBlockVersion is the block_version stamped on every row this job
// writes. The run pins to a FINALIZED block and reads history from the canonical
// chain, so there is no reorg incarnation to distinguish: 0 is the first (and
// here only) version of each block, matching what the watcher publishes for a
// block it has never had to re-emit.
const canonicalBlockVersion = 0

// progressLogEvery bounds the run's log volume: with ~313 vaults and thousands
// of chunks, one line per unit would bury the failures that matter.
const progressLogEvery = 25

// ChainReader is the node surface the bootstrap needs: the historical log sweep
// plus the two header reads that pin the run and date each replayed event.
// *ethclient.Client satisfies it.
type ChainReader interface {
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]ethtypes.Log, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*ethtypes.Header, error)
	HeaderByHash(ctx context.Context, hash common.Hash) (*ethtypes.Header, error)
}

// V2Replayer is the morpho-indexer surface the bootstrap drives. It is
// satisfied by the *morpho_indexer.Service built with NewReplayService, which is
// the point: the bootstrap owns no handler logic of its own, it only decides
// which vaults and which logs to feed through the live path.
type V2Replayer interface {
	LoadVaultRegistry(ctx context.Context) error
	V2VaultAddresses() map[common.Address]struct{}
	SeedV2VaultAdapters(ctx context.Context, vaultAddress common.Address, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error
	ReplayMetaMorphoLog(ctx context.Context, log shared.Log, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error
}

type Config struct {
	ChainID int64
	// BlockChunkSize bounds one eth_getLogs request's block range. The sweep
	// halves it automatically when a provider reports a result cap, so this is a
	// throughput knob, not a correctness one.
	BlockChunkSize int64
	// AddressBatchSize bounds how many vault addresses ride in one eth_getLogs
	// address filter.
	AddressBatchSize int
	Logger           *slog.Logger
}

func ConfigDefaults() Config {
	return Config{
		BlockChunkSize:   10_000,
		AddressBatchSize: 100,
	}
}

func (c Config) validate() error {
	if c.ChainID <= 0 {
		return fmt.Errorf("ChainID must be positive, got %d", c.ChainID)
	}
	if c.BlockChunkSize <= 0 {
		return fmt.Errorf("BlockChunkSize must be positive, got %d", c.BlockChunkSize)
	}
	if c.AddressBatchSize <= 0 {
		return fmt.Errorf("AddressBatchSize must be positive, got %d", c.AddressBatchSize)
	}
	if c.Logger == nil {
		return fmt.Errorf("logger is required")
	}
	return nil
}

type Service struct {
	config       Config
	chain        ChainReader
	replay       V2Replayer
	deployBlock  int64
	configTopics []common.Hash
	logger       *slog.Logger
}

func NewService(config Config, chain ChainReader, replay V2Replayer) (*Service, error) {
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	if chain == nil {
		return nil, fmt.Errorf("chain reader is required")
	}
	if replay == nil {
		return nil, fmt.Errorf("replayer is required")
	}
	deployBlock, err := morpho_indexer.VaultV2FactoryDeployBlock(config.ChainID)
	if err != nil {
		return nil, err
	}
	topics, err := configEventTopics()
	if err != nil {
		return nil, err
	}
	return &Service{
		config:       config,
		chain:        chain,
		replay:       replay,
		deployBlock:  deployBlock,
		configTopics: topics,
		logger:       config.Logger.With("component", "morpho-v2-bootstrap"),
	}, nil
}

// Run performs one complete bootstrap pass. It is the body of the Temporal
// activity, and is safe to invoke repeatedly.
func (s *Service) Run(ctx context.Context) error {
	head, err := s.pinFinalizedHead(ctx)
	if err != nil {
		return err
	}
	vaults, err := s.loadV2Vaults(ctx)
	if err != nil {
		return err
	}
	if len(vaults) == 0 {
		s.logger.Info("no VaultV2 vaults on this chain — nothing to bootstrap", "chainID", s.config.ChainID)
		return nil
	}

	s.logger.Info("starting VaultV2 bootstrap",
		"chainID", s.config.ChainID,
		"vaults", len(vaults),
		"fromBlock", s.deployBlock,
		"headBlock", head.number,
		"headHash", head.hash.Hex())

	if err := s.seedAdapterState(ctx, vaults, head); err != nil {
		return err
	}
	if err := s.replayConfigHistory(ctx, vaults, head); err != nil {
		return err
	}

	s.logger.Info("VaultV2 bootstrap complete", "vaults", len(vaults), "headBlock", head.number)
	return nil
}

// pinnedBlock is the single block every state read in a run is pinned to, so the
// enumerated adapter sets and their realAssets seeds describe one consistent
// chain state rather than a smear across the run's wall-clock duration.
type pinnedBlock struct {
	number    int64
	hash      common.Hash
	timestamp time.Time
}

// pinFinalizedHead resolves the run's anchor block. Finalized rather than
// latest: the seed writes permanent snapshots, and a latest-pinned run could
// record state from a block that is subsequently reorged out, leaving rows no
// canonical block ever produced.
func (s *Service) pinFinalizedHead(ctx context.Context) (pinnedBlock, error) {
	header, err := s.chain.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return pinnedBlock{}, fmt.Errorf("fetching finalized head: %w", err)
	}
	if header == nil {
		return pinnedBlock{}, fmt.Errorf("node returned no finalized head")
	}
	return pinnedBlock{
		number:    header.Number.Int64(),
		hash:      header.Hash(),
		timestamp: time.Unix(int64(header.Time), 0).UTC(),
	}, nil
}

// loadV2Vaults reads every persisted VaultV2 for the configured chain, sorted by
// address so a run's order — and its advisory-lock acquisition order — is
// deterministic.
func (s *Service) loadV2Vaults(ctx context.Context) ([]common.Address, error) {
	if err := s.replay.LoadVaultRegistry(ctx); err != nil {
		return nil, fmt.Errorf("loading vault registry: %w", err)
	}
	known := s.replay.V2VaultAddresses()
	vaults := make([]common.Address, 0, len(known))
	for address := range known {
		vaults = append(vaults, address)
	}
	slices.SortFunc(vaults, func(a, b common.Address) int {
		return bytes.Compare(a.Bytes(), b.Bytes())
	})
	return vaults, nil
}

// seedAdapterState gives every vault its current adapter registry rows and one
// realAssets snapshot each, all at the run's pinned block. This is the half that
// clears VEC-219's adapter_data_missing gate, so it runs before the history
// replay: if the (much longer) replay fails, the current-state repair has
// already landed.
func (s *Service) seedAdapterState(ctx context.Context, vaults []common.Address, head pinnedBlock) error {
	for i, vault := range vaults {
		if err := s.replay.SeedV2VaultAdapters(ctx, vault, head.number, head.hash, canonicalBlockVersion, head.timestamp); err != nil {
			return fmt.Errorf("seeding adapters for vault %s at block %d: %w", vault.Hex(), head.number, err)
		}
		if (i+1)%progressLogEvery == 0 {
			s.logger.Info("seeding adapters", "done", i+1, "total", len(vaults))
		}
	}
	s.logger.Info("adapter seed complete", "vaults", len(vaults), "block", head.number)
	return nil
}

// replayConfigHistory walks [factory deploy block, head] in chunks, feeding every
// VaultV2 governance event emitted by a known V2 vault through the live handler
// path in strict chain order.
func (s *Service) replayConfigHistory(ctx context.Context, vaults []common.Address, head pinnedBlock) error {
	batches := batchAddresses(vaults, s.config.AddressBatchSize)
	chunks := chunkBlockRange(s.deployBlock, head.number, s.config.BlockChunkSize)
	timestamps := blocktime.New(s.chain)

	s.logger.Info("starting config-event replay",
		"chunks", len(chunks),
		"addressBatches", len(batches),
		"topics", len(s.configTopics))

	replayed := 0
	for i, chunk := range chunks {
		logs, err := s.fetchChunkLogs(ctx, batches, chunk)
		if err != nil {
			return err
		}
		sortLogs(logs)
		if err := s.replayLogs(ctx, logs, timestamps); err != nil {
			return fmt.Errorf("replaying config events in [%d,%d]: %w", chunk.From, chunk.To, err)
		}
		replayed += len(logs)
		if (i+1)%progressLogEvery == 0 {
			s.logger.Info("replaying config events",
				"chunksDone", i+1, "chunks", len(chunks), "eventsReplayed", replayed)
		}
	}

	s.logger.Info("config-event replay complete", "eventsReplayed", replayed)
	return nil
}

// fetchChunkLogs collects one chunk's logs across every address batch. The
// batches' results interleave, so the caller sorts before replaying.
func (s *Service) fetchChunkLogs(ctx context.Context, batches [][]common.Address, chunk blockRange) ([]ethtypes.Log, error) {
	var logs []ethtypes.Log
	for _, batch := range batches {
		batchLogs, err := s.fetchLogs(ctx, batch, s.configTopics, chunk)
		if err != nil {
			return nil, err
		}
		logs = append(logs, batchLogs...)
	}
	return logs, nil
}

// replayLogs drives already-ordered logs through the live handler path, dating
// each from its own block header.
func (s *Service) replayLogs(ctx context.Context, logs []ethtypes.Log, timestamps *blocktime.Cache) error {
	for _, l := range logs {
		// The sweep's upper bound is a finalized block, so a reorged-out log
		// cannot legitimately appear. Replaying one would write state from a
		// block that is not on the canonical chain, so treat it as the data
		// anomaly it is rather than filtering it away silently.
		if l.Removed {
			return fmt.Errorf("node returned a removed log at block %d index %d (tx %s) within the finalized range",
				l.BlockNumber, l.Index, l.TxHash.Hex())
		}
		blockTimestamp, err := timestamps.TimestampAt(ctx, l.BlockHash)
		if err != nil {
			return err
		}
		if err := s.replay.ReplayMetaMorphoLog(ctx, toSharedLog(l), int64(l.BlockNumber), l.BlockHash, canonicalBlockVersion, blockTimestamp); err != nil {
			return fmt.Errorf("replaying log tx=%s index=%d block=%d: %w", l.TxHash.Hex(), l.Index, l.BlockNumber, err)
		}
	}
	return nil
}

// configEventTopics returns the VaultV2 governance topic0 set as a stable,
// sorted slice — the eth_getLogs topic filter and the order requests are issued
// in must not vary run to run.
func configEventTopics() ([]common.Hash, error) {
	set, err := morpho_indexer.VaultV2ConfigEventTopics()
	if err != nil {
		return nil, fmt.Errorf("deriving VaultV2 config event topics: %w", err)
	}
	topics := make([]common.Hash, 0, len(set))
	for topic := range set {
		topics = append(topics, topic)
	}
	slices.SortFunc(topics, func(a, b common.Hash) int {
		return bytes.Compare(a.Bytes(), b.Bytes())
	})
	return topics, nil
}
