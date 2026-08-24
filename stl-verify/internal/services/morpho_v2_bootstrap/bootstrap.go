// Package morpho_v2_bootstrap heals Morpho VaultV2 vaults that were discovered
// before VaultV2 discovery became atomic (VEC-218).
//
// Those vaults have a morpho_vault row but no morpho_adapter / adapter_state /
// cap / fee rows, and live indexing never revisits them: the SQS handler
// short-circuits on IsKnownVault before it would enumerate anything, and their
// AddAdapter / cap / fee events are historical, so they never arrive on the live
// stream again. One run of this service repairs every one of them.
//
// It runs two passes over the V2 vaults of the configured chain, in this order:
//
//  1. Replay — sweep eth_getLogs from the VaultV2 factory deploy block to a
//     pinned finalized block for the 10 VaultV2 governance events, and feed each
//     one through the live handler path (Service.ReplayMetaMorphoLog) in
//     (block, logIndex) order, rebuilding the adapter/cap/fee history.
//  2. Seed — enumerate each vault's current adapter set at that same block and
//     write one realAssets() snapshot per adapter, so no active adapter is left
//     without state.
//
// The order is load-bearing, not a preference — see seedAdapterState.
//
// Both passes cover only the vaults the pinned head can speak for; a vault first
// seen above it is deferred to the next run — see loadV2Vaults.
//
// Every write goes through the same idempotent repository methods live indexing
// uses, so re-running is safe. Any failure fails the run — the seed pass first
// finishes the vaults it can (see seedAdapterState), but a vault it could not
// heal is always reported, never left as a silent hole.
//
// The replay pass records its position after each completed chunk through a
// ProgressStore, so an attempt that dies part-way (a pod kill, a deploy) resumes
// at the next chunk instead of re-sweeping hours of history. Correctness never
// depends on it: a run that resumes from nothing simply does the work again.
package morpho_v2_bootstrap

import (
	"bytes"
	"context"
	"errors"
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
// writes. The sweep reads the canonical chain below a finalized head, so the
// blocks it replays are final and 0 is the version the watcher assigns a block
// it never had to re-emit.
//
// Known gap (VEC-218 follow-up): a block the watcher DID re-emit before it
// finalized carries version >= 1 in block_states, and block_version is part of
// the primary key of morpho_vault_cap / morpho_vault_fee. For such a block this
// job would write a version-0 row beside live indexing's version-1 row rather
// than deduping against it. Resolving the version from block_states by
// (chain_id, number, hash) — as the morpho-vault-backfill does from
// the S3 key — needs a repository the service does not currently hold, so it is
// deferred rather than half-solved here.
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
	// V2VaultsFirstSeen maps every persisted VaultV2 to the block it was first
	// SEEN at, which is an upper bound on its deployment block — the run's head
	// filter needs that bound, see loadV2Vaults.
	V2VaultsFirstSeen() map[common.Address]int64
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
	progress     ProgressStore
	deployBlock  int64
	configTopics []common.Hash
	logger       *slog.Logger
}

func NewService(config Config, chain ChainReader, replay V2Replayer, progress ProgressStore) (*Service, error) {
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	if chain == nil {
		return nil, fmt.Errorf("chain reader is required")
	}
	if replay == nil {
		return nil, fmt.Errorf("replayer is required")
	}
	if progress == nil {
		return nil, fmt.Errorf("progress store is required")
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
		progress:     progress,
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
	scope, err := s.loadV2Vaults(ctx, head)
	if err != nil {
		return err
	}
	if len(scope.vaults) == 0 {
		return s.emptyScopeError(scope, head)
	}

	s.logger.Info("starting VaultV2 bootstrap",
		"chainID", s.config.ChainID,
		"vaults", len(scope.vaults),
		"deferredVaults", scope.deferred,
		"fromBlock", s.deployBlock,
		"headBlock", head.number,
		"headHash", head.hash.Hex())

	if err := s.replayConfigHistory(ctx, scope.vaults, head); err != nil {
		return err
	}
	if err := s.seedAdapterState(ctx, scope.vaults, head); err != nil {
		return err
	}

	s.logger.Info("VaultV2 bootstrap complete",
		"vaults", len(scope.vaults), "deferredVaults", scope.deferred, "headBlock", head.number)
	return nil
}

// emptyScopeError explains a run with nothing in scope. A repair job that heals
// nothing must not report success — it is only ever triggered because V2 vaults
// are known to be missing rows, so it is the one outcome nobody would notice if
// it returned nil.
//
// The two causes need opposite responses, which is why the message branches:
// deferral is a finalized head that has not caught up yet and clears itself,
// while an empty set with nothing deferred means the run is pointed somewhere
// that has no V2 vaults at all.
func (s *Service) emptyScopeError(scope v2VaultScope, head pinnedBlock) error {
	if scope.deferred > 0 {
		return fmt.Errorf("all %d known VaultV2 vaults of chain %d were first seen above the pinned head %d — re-run once finality passes them",
			scope.deferred, s.config.ChainID, head.number)
	}
	return fmt.Errorf("no VaultV2 vaults of chain %d were first seen at or below the pinned head %d: the bootstrap has nothing to repair, which means it is pointed at the wrong chain or database",
		s.config.ChainID, head.number)
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
	// A finalized head below the VaultV2 factory deploy block would make the
	// sweep range inverted, which yields zero chunks and therefore a run that
	// replays nothing and still reports success. It means the node is not on the
	// configured chain, so fail instead.
	if number := header.Number.Int64(); number < s.deployBlock {
		return pinnedBlock{}, fmt.Errorf("finalized head %d is below the chain-%d VaultV2 factory deploy block %d: the RPC endpoint is not on the configured chain",
			number, s.config.ChainID, s.deployBlock)
	}
	return pinnedBlock{
		number:    header.Number.Int64(),
		hash:      header.Hash(),
		timestamp: time.Unix(int64(header.Time), 0).UTC(),
	}, nil
}

// v2VaultScope is what a run will work on: the vaults it heals, sorted by
// address so a run's order — and its advisory-lock acquisition order — is
// deterministic, plus how many it left to a later head.
type v2VaultScope struct {
	vaults   []common.Address
	deferred int
}

// loadV2Vaults reads every persisted VaultV2 for the configured chain that the
// run's pinned head can speak for.
//
// morpho_vault.created_at_block is where DISCOVERY first saw the vault (V2
// discovery triggers on the first observed AccrueInterest, and the legacy upsert
// only converges the column downward as earlier sightings arrive), so it is an
// UPPER BOUND: deployment <= first seen. The inequality is used in the direction
// that bound supports — first seen <= head means the vault certainly had code at
// the head, so no vault is ever probed at a block it did not exist at.
//
// A vault first seen ABOVE the head is DEFERRED, not excluded: it may well have
// been enumerable there, but nothing proves it, and live indexing has owned that
// vault since its first event. The next run pins a later finalized head that
// includes it, which is why each skip is logged loudly rather than quietly.
func (s *Service) loadV2Vaults(ctx context.Context, head pinnedBlock) (v2VaultScope, error) {
	if err := s.replay.LoadVaultRegistry(ctx); err != nil {
		return v2VaultScope{}, fmt.Errorf("loading vault registry: %w", err)
	}
	known := s.replay.V2VaultsFirstSeen()

	scope := v2VaultScope{vaults: make([]common.Address, 0, len(known))}
	for address, firstSeenBlock := range known {
		if firstSeenBlock > head.number {
			scope.deferred++
			s.logger.Warn("vault first seen above the pinned head, deferring it: live indexing covers it, and the next run's later head includes it",
				"vault", address.Hex(), "firstSeenBlock", firstSeenBlock, "headBlock", head.number)
			continue
		}
		scope.vaults = append(scope.vaults, address)
	}
	slices.SortFunc(scope.vaults, func(a, b common.Address) int {
		return bytes.Compare(a.Bytes(), b.Bytes())
	})
	return scope, nil
}

// seedAdapterState gives every vault its current adapter registry rows and one
// realAssets snapshot each, at the run's pinned block. It clears VEC-219's
// adapter_data_missing gate: an active adapter with no state row.
//
// It runs after replayConfigHistory, but no longer has to. Membership is an
// append-only log of observations at their own block positions, so the seed's
// head-block ASSERTION and the replay's historical transitions cannot corrupt
// each other in either order: whichever arrives second is simply another row (or,
// for an assertion whose answer the log already gives, no row at all), and every
// snapshot hangs off an identity id that no lifecycle observation can move.
// Replaying first is kept because it is cheaper — with the history already on
// record the head assertion usually writes nothing.
//
// One vault's failure does not stop the pass. A vault-shaped contract this job
// cannot probe fails identically on every future run, so aborting at the first
// one would leave every vault after it unhealed forever — a permanent poison
// pill in a repair job. The run still fails, naming every vault it could not
// seed: healing as much as possible is the point, hiding a hole is not.
func (s *Service) seedAdapterState(ctx context.Context, vaults []common.Address, head pinnedBlock) error {
	var failures []error
	for i, vault := range vaults {
		if err := s.replay.SeedV2VaultAdapters(ctx, vault, head.number, head.hash, canonicalBlockVersion, head.timestamp); err != nil {
			wrapped := fmt.Errorf("seeding adapters for vault %s at block %d: %w", vault.Hex(), head.number, err)
			// A cancelled run fails every remaining vault identically, so
			// collecting those would bury the cause under one error per vault. The
			// ones that failed on their own before it still ride out: the returned
			// error is what Temporal shows the operator, so dropping them would
			// hide a hole behind the cancellation.
			if ctx.Err() != nil {
				return errors.Join(append(failures, wrapped)...)
			}
			failures = append(failures, wrapped)
			s.logger.Error("vault seed failed; continuing so healable vaults still heal",
				"vault", vault.Hex(), "block", head.number, "error", err)
			continue
		}
		if (i+1)%progressLogEvery == 0 {
			s.logger.Info("seeding adapters", "done", i+1, "total", len(vaults))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d of %d vaults could not be seeded: %w", len(failures), len(vaults), errors.Join(failures...))
	}

	s.logger.Info("adapter seed complete", "vaults", len(vaults), "block", head.number)
	return nil
}

// replayConfigHistory walks [resume point, head] in chunks, feeding every
// VaultV2 governance event emitted by a known V2 vault through the live handler
// path in strict chain order, and records each chunk once it is fully replayed.
func (s *Service) replayConfigHistory(ctx context.Context, vaults []common.Address, head pinnedBlock) error {
	vaultsDigest := vaultSetDigest(vaults)
	from, err := s.resumeBlock(ctx, vaultsDigest)
	if err != nil {
		return err
	}

	batches := batchAddresses(vaults, s.config.AddressBatchSize)
	chunks := chunkBlockRange(from, head.number, s.config.BlockChunkSize)
	timestamps := blocktime.New(s.chain)

	s.logger.Info("starting config-event replay",
		"fromBlock", from,
		"chunks", len(chunks),
		"addressBatches", len(batches),
		"topics", len(s.configTopics))

	replayed := 0
	for i, chunk := range chunks {
		count, err := s.replayChunk(ctx, batches, chunk, timestamps)
		if err != nil {
			return err
		}
		if err := s.recordSweepProgress(ctx, vaultsDigest, chunk.To); err != nil {
			return err
		}
		replayed += count
		if (i+1)%progressLogEvery == 0 {
			s.logger.Info("replaying config events",
				"chunksDone", i+1, "chunks", len(chunks), "eventsReplayed", replayed)
		}
	}

	s.logger.Info("config-event replay complete", "eventsReplayed", replayed)
	return nil
}

// replayChunk fetches one chunk's logs across every address batch and drives
// them through the live handler path in chain order, returning how many it
// replayed.
func (s *Service) replayChunk(ctx context.Context, batches [][]common.Address, chunk blockRange, timestamps *blocktime.Cache) (int, error) {
	logs, err := s.fetchChunkLogs(ctx, batches, chunk)
	if err != nil {
		return 0, err
	}
	sortLogs(logs)
	if err := s.replayLogs(ctx, logs, timestamps); err != nil {
		return 0, fmt.Errorf("replaying config events in [%d,%d]: %w", chunk.From, chunk.To, err)
	}
	return len(logs), nil
}

// resumeBlock is the first block this run must sweep: the block after the last
// chunk an earlier attempt completed, or the factory deploy block when no usable
// record exists.
//
// A record is usable only when it was written for this chain, this sweep start,
// and this exact V2 vault set. The head is deliberately NOT part of that match:
// it is the finalized head, so it advances between attempts, and resuming under
// a higher one simply covers the blocks that have finalized since. A CHANGED
// VAULT SET is the case that must not resume — the recorded chunks were fetched
// through an address filter that never mentioned the vault that joined, so
// skipping them would silently lose its whole governance history.
//
// The digest is computed over the HEAD-FILTERED set (loadV2Vaults), so a later
// head that admits a previously deferred vault changes it and re-sweeps the whole
// range. That is the same rule, deliberately: the vault it admits is exactly one
// the recorded chunks were never fetched for.
func (s *Service) resumeBlock(ctx context.Context, vaultsDigest string) (int64, error) {
	recorded, found, err := s.progress.LoadProgress(ctx)
	if err != nil {
		return 0, fmt.Errorf("loading sweep progress: %w", err)
	}
	if !found {
		s.logger.Info("no sweep progress recorded, sweeping the full range", "fromBlock", s.deployBlock)
		return s.deployBlock, nil
	}
	current := s.sweepProgressAt(vaultsDigest, 0)
	if !recorded.sameScope(current) {
		s.logger.Warn("recorded sweep progress belongs to another run, sweeping the full range",
			"fromBlock", s.deployBlock,
			"chainID", current.ChainID,
			"recordedChainID", recorded.ChainID,
			"recordedFromBlock", recorded.FromBlock,
			"vaultSetChanged", recorded.VaultsDigest != current.VaultsDigest)
		return s.deployBlock, nil
	}
	if recorded.LastCompletedTo < s.deployBlock {
		s.logger.Warn("recorded sweep progress sits below the factory deploy block, sweeping the full range",
			"lastCompletedTo", recorded.LastCompletedTo, "fromBlock", s.deployBlock)
		return s.deployBlock, nil
	}

	s.logger.Info("resuming the sweep from recorded progress",
		"lastCompletedTo", recorded.LastCompletedTo, "fromBlock", recorded.LastCompletedTo+1)
	return recorded.LastCompletedTo + 1, nil
}

// recordSweepProgress marks every block up to lastCompletedTo replayed. Called
// only once a whole chunk has landed, so a resumed run restarts on a chunk
// boundary.
func (s *Service) recordSweepProgress(ctx context.Context, vaultsDigest string, lastCompletedTo int64) error {
	if err := s.progress.SaveProgress(ctx, s.sweepProgressAt(vaultsDigest, lastCompletedTo)); err != nil {
		return fmt.Errorf("recording sweep progress at block %d: %w", lastCompletedTo, err)
	}
	return nil
}

// sweepProgressAt stamps a sweep position with the scope it is true for.
func (s *Service) sweepProgressAt(vaultsDigest string, lastCompletedTo int64) SweepProgress {
	return SweepProgress{
		ChainID:         s.config.ChainID,
		FromBlock:       s.deployBlock,
		VaultsDigest:    vaultsDigest,
		LastCompletedTo: lastCompletedTo,
	}
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
