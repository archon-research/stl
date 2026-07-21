package morpho_indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
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
// replay-tolerant MarkAdapterRemoved), so replaying a log more than once — the
// resume case after a partially-completed run — is safe.
func (s *Service) ReplayMetaMorphoLog(ctx context.Context, log shared.Log, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	vaultAddress := common.HexToAddress(log.Address)
	return s.processMetaMorphoLog(ctx, log, vaultAddress, s.config.ChainID, blockNumber, blockHash, blockVersion, blockTimestamp)
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

// VaultV2StructuredEventTopics returns the topic0 hashes of the 13 VaultV2
// structured events, derived from the registered ABI so they never drift from
// the canonical signatures.
func VaultV2StructuredEventTopics() (map[common.Hash]struct{}, error) {
	abiV2, err := abis.GetVaultV2EventsABI()
	if err != nil {
		return nil, fmt.Errorf("loading VaultV2 events ABI: %w", err)
	}
	topics := make(map[common.Hash]struct{}, len(vaultV2StructuredEventNames))
	for _, name := range vaultV2StructuredEventNames {
		ev, ok := abiV2.Events[name]
		if !ok {
			return nil, fmt.Errorf("VaultV2 structured event %q not found in ABI", name)
		}
		topics[ev.ID] = struct{}{}
	}
	return topics, nil
}
