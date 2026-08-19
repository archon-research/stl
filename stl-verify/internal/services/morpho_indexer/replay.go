package morpho_indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// LoadVaultRegistry loads all known vaults for the configured chain into the
// in-memory registry. Called by Start before the SQS loop and by the backfiller's
// replay phase before feeding logs through ReplayMetaMorphoLog.
func (s *Service) LoadVaultRegistry(ctx context.Context) error {
	if err := s.vaultRegistry.LoadFromDB(ctx, s.morphoRepo, s.config.ChainID); err != nil {
		return fmt.Errorf("loading vault registry: %w", err)
	}
	return nil
}

// V2VaultAddresses returns the addresses of every registered Morpho VaultV2
// vault. LoadVaultRegistry must have run first.
func (s *Service) V2VaultAddresses() map[common.Address]struct{} {
	return s.vaultRegistry.V2VaultAddresses()
}

// ReplayMetaMorphoLog routes a single already-persisted VaultV2 vault's
// structured-event log through the exact handler path the live SQS consumer uses
// (processMetaMorphoLog): the audit-log write plus the typed adapter / cap / fee
// dispatch. The log's emitter is taken as the vault address; it must already be
// in the registry (LoadVaultRegistry), and callers filter to V2-vault logs whose
// topic0 is one of VaultV2StructuredEventTopics.
//
// Every write goes through the same idempotent repository methods as live
// indexing (GetOrCreate / ON CONFLICT DO NOTHING / trigger-versioned snapshots /
// append-only membership observations keyed on the log's own position), so replaying
// a log more than once — the resume case after a partially-completed run — is safe.
func (s *Service) ReplayMetaMorphoLog(ctx context.Context, log shared.Log, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	// Hash-pinned reads carry no block argument, so unstamped the archiving
	// decorator keys every replayed batch at block 0 and they overwrite each other.
	ctx = archiving.WithBlockVersion(ctx, blockVersion)
	ctx = archiving.WithBlockNumber(ctx, blockNumber)
	if len(log.Topics) == 0 {
		return fmt.Errorf("replay log has no topics")
	}
	// The replay constructor nils the user/token/cache/consumer/receipt-token
	// ports, so only the VaultV2 structured events (whose handlers never touch
	// them) are safe here. A share-accounting log (V1/V2 Deposit/Withdraw/
	// Transfer/AccrueInterest) would nil-deref saveVaultPositionInTx, so reject
	// any non-structured topic up front rather than panicking mid-handler.
	topic0 := common.HexToHash(log.Topics[0])
	if _, ok := s.v2StructuredTopics[topic0]; !ok {
		return fmt.Errorf("ReplayMetaMorphoLog: topic %s is not a VaultV2 structured event; replay handles only the adapter/allocation/cap/fee surface", topic0.Hex())
	}
	vaultAddress := common.HexToAddress(log.Address)
	return s.processMetaMorphoLog(ctx, log, vaultAddress, s.config.ChainID, blockNumber, blockHash, blockVersion, blockTimestamp)
}

// SeedV2VaultAdapters enumerates an already-persisted VaultV2's current adapter
// set at (blockNumber, blockHash) and writes one adapter registry row plus one
// realAssets() state snapshot per adapter — the same enumerate → classify →
// upsert → seed path discovery runs, for a vault that already exists and so
// never goes through discovery again.
//
// It exists for the morpho-v2-bootstrap job: the ~313 V2 vaults discovered
// before discovery became atomic have a vault row but no adapters, and the live
// stream never revisits them (IsKnownVault short-circuits before enumeration).
//
// Reads-then-persist, like discoverAndRegisterVault: every on-chain read
// completes before the transaction opens, so a transient RPC failure commits
// nothing and a re-triggered run starts clean.
//
// Re-running is safe, and it is now also QUIET. The enumeration is an ASSERTION
// recorded as observed_via = bootstrap_seed, so a re-run whose head-block answer
// already matches the membership log appends nothing at all — where the old
// registry recorded a fresh "added at the finalized head" every time and hung the
// seed snapshot off it.
func (s *Service) SeedV2VaultAdapters(ctx context.Context, vaultAddress common.Address, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}
	adapters, err := s.readV2Adapters(ctx, vaultAddress, blockNumber, blockHash)
	if err != nil {
		return fmt.Errorf("reading adapters for vault %s: %w", vaultAddress.Hex(), err)
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.seedDiscoveredAdapters(ctx, tx, vault, vaultAddress, blockNumber, blockVersion, blockTimestamp, adapters, entity.MembershipFromBootstrapSeed)
	})
}

// vaultV2StructuredEventNames are the VaultV2 events processMetaMorphoLog routes
// to a typed, structured handler (adapter registry, allocation snapshots, caps,
// fee config). It is the ExtractMetaMorphoEvent typed set minus the shared
// ERC4626/ERC20 surface (Deposit/Withdraw/Transfer/AccrueInterest), which the
// backfiller's V2 replay deliberately excludes — those are share-accounting
// events, not V2-specific governance/allocation state. Keep in sync with the
// dispatch switch in processMetaMorphoLog.
var vaultV2StructuredEventNames = []string{
	"AddAdapter", "RemoveAdapter",
	"Allocate", "Deallocate", "ForceDeallocate",
	"IncreaseAbsoluteCap", "DecreaseAbsoluteCap",
	"IncreaseRelativeCap", "DecreaseRelativeCap",
	"SetPerformanceFee", "SetManagementFee",
	"SetPerformanceFeeRecipient", "SetManagementFeeRecipient",
}

// vaultV2ConfigEventNames are the VaultV2 governance events — the structured set
// minus the three allocation events (Allocate / Deallocate / ForceDeallocate).
//
// It is the topic set the morpho-v2-bootstrap job sweeps with eth_getLogs. The
// allocation events are excluded on purpose: replaying them would issue one
// hash-pinned realAssets() read per historical allocation across every V2 vault
// (the dominant cost of a full history walk) to reconstruct a time series
// nothing consumes today. The bootstrap's adapter seed already establishes each
// adapter's CURRENT realAssets, and live indexing appends every allocation from
// the run onwards.
var vaultV2ConfigEventNames = []string{
	"AddAdapter", "RemoveAdapter",
	"IncreaseAbsoluteCap", "DecreaseAbsoluteCap",
	"IncreaseRelativeCap", "DecreaseRelativeCap",
	"SetPerformanceFee", "SetManagementFee",
	"SetPerformanceFeeRecipient", "SetManagementFeeRecipient",
}

// VaultV2StructuredEventTopics returns the topic0 hashes of the 13 VaultV2
// structured events, derived from the registered ABI so they never drift from
// the canonical signatures.
func VaultV2StructuredEventTopics() (map[common.Hash]struct{}, error) {
	return vaultV2EventTopics(vaultV2StructuredEventNames)
}

// VaultV2ConfigEventTopics returns the topic0 hashes of the 10 VaultV2
// governance (adapter / cap / fee) events. Every one is also a structured topic,
// so logs matched on this set pass ReplayMetaMorphoLog's guard.
func VaultV2ConfigEventTopics() (map[common.Hash]struct{}, error) {
	return vaultV2EventTopics(vaultV2ConfigEventNames)
}

func vaultV2EventTopics(names []string) (map[common.Hash]struct{}, error) {
	abiV2, err := abis.GetVaultV2EventsABI()
	if err != nil {
		return nil, fmt.Errorf("loading VaultV2 events ABI: %w", err)
	}
	topics := make(map[common.Hash]struct{}, len(names))
	for _, name := range names {
		ev, ok := abiV2.Events[name]
		if !ok {
			return nil, fmt.Errorf("VaultV2 event %q not found in ABI", name)
		}
		topics[ev.ID] = struct{}{}
	}
	return topics, nil
}
