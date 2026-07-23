package morpho_indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// This file holds the Morpho VaultV2 structured-event handlers: the adapter
// registry, allocation snapshots, cap snapshots, and fee-config updates dispatched
// from processMetaMorphoLog. They share resolveV2Vault's version guard.

// resolveV2Vault looks up the vault and asserts it is a VaultV2. The adapter /
// cap / fee events these handlers serve are emitted only by VaultV2 vaults, so
// a missing vault, or one recorded as V1/V1.1, is unexpected data drift we fail
// on rather than silently skip.
func (s *Service) resolveV2Vault(vaultAddress common.Address) (*entity.MorphoVault, error) {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return nil, fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}
	if vault.VaultVersion != entity.MorphoVaultV2 {
		return nil, fmt.Errorf("VaultV2-only event on non-V2 vault %s (version %d)", vaultAddress.Hex(), vault.VaultVersion)
	}
	return vault, nil
}

// handleAddAdapter classifies the new adapter on-chain and records it in the
// adapter registry. An unclassifiable adapter is persisted as Unknown behind a
// WARN, mirroring the VaultShaped discovery sentinel so a future adapter kind
// surfaces instead of being dropped.
func (s *Service) handleAddAdapter(ctx context.Context, e *AddAdapterEvent, vaultAddress common.Address, blockNumber int64) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		_, err := s.probeAndUpsertAdapter(ctx, tx, vault, vaultAddress, e.Account, blockNumber)
		return err
	})
}

// handleRemoveAdapter marks the adapter inactive from this block onward. If we
// never witnessed the adapter's AddAdapter (it predates the vault's discovery on
// the live stream — AddAdapter events for mid-life-discovered V2 vaults are
// always historical and never replayed on SNS/SQS), the adapter is lazily
// registered first so the removal closes an audit-consistent row, instead of
// MarkAdapterRemoved failing with a 0-rows error and poisoning the FIFO queue.
func (s *Service) handleRemoveAdapter(ctx context.Context, e *RemoveAdapterEvent, vaultAddress common.Address, blockNumber int64) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if _, err := s.ensureAdapterRegistered(ctx, tx, vault, vaultAddress, e.Account, blockNumber); err != nil {
			return err
		}
		return s.morphoRepo.MarkAdapterRemoved(ctx, tx, vault.ID, e.Account.Bytes(), blockNumber)
	})
}

// handleAllocation snapshots an adapter's realAssets() after an Allocate or
// Deallocate. The event's `change` is a signed per-id delta, not a running
// total, so the authoritative per-adapter value is read from realAssets()
// (hash-pinned, state read). An allocation for an adapter we never saw AddAdapter
// for is not a poison pill: the adapter address comes from the vault's own event
// and is identity-verified by the on-chain type probe, so it is lazily registered
// (self-heal) rather than hard-failing and stalling the whole morpho queue.
func (s *Service) handleAllocation(ctx context.Context, adapter, vaultAddress common.Address, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}

	realAssets, err := s.blockchainSvc.getAdapterRealAssets(ctx, adapter, blockHash)
	if err != nil {
		return fmt.Errorf("fetching realAssets for adapter %s: %w", adapter.Hex(), err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		adapterID, err := s.ensureAdapterRegistered(ctx, tx, vault, vaultAddress, adapter, blockNumber)
		if err != nil {
			return err
		}
		state, err := entity.NewMorphoAdapterState(adapterID, blockNumber, blockVersion, blockTimestamp, realAssets)
		if err != nil {
			return fmt.Errorf("creating adapter state entity: %w", err)
		}
		return s.morphoRepo.SaveAdapterState(ctx, tx, state)
	})
}

// ensureAdapterRegistered returns the registry id of the active adapter for
// (vault, adapter), lazily registering it at firstSeenBlock if we never witnessed
// its AddAdapter. Used by the Allocate/Deallocate and RemoveAdapter paths, which
// can legitimately reach an adapter that predates the vault's mid-life discovery.
func (s *Service) ensureAdapterRegistered(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, vaultAddress, adapter common.Address, firstSeenBlock int64) (int64, error) {
	active, err := s.morphoRepo.GetActiveAdapter(ctx, tx, vault.ID, adapter.Bytes())
	if err != nil {
		return 0, fmt.Errorf("looking up active adapter %s: %w", adapter.Hex(), err)
	}
	if active != nil {
		return active.ID, nil
	}
	s.logger.Warn("adapter registered lazily; AddAdapter predates vault discovery",
		"vault", vaultAddress.Hex(), "adapter", adapter.Hex(), "block", firstSeenBlock)
	return s.probeAndUpsertAdapter(ctx, tx, vault, vaultAddress, adapter, firstSeenBlock)
}

// probeAndUpsertAdapter classifies an adapter on-chain and upserts its registry
// row at firstSeenBlock. Shared by the live AddAdapter handler and the lazy
// self-heal on Allocate/RemoveAdapter, which classify inside their transaction.
// (Discovery-time enumeration classifies up-front, before its atomic persist tx,
// and calls upsertAdapterRow directly with the already-read type.)
//
// A probe TRANSPORT error propagates (transient ⇒ SQS retries); only a clean
// both-revert probe records type Unknown.
func (s *Service) probeAndUpsertAdapter(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, vaultAddress, adapter common.Address, firstSeenBlock int64) (int64, error) {
	adapterType, err := s.blockchainSvc.getAdapterType(ctx, adapter, firstSeenBlock)
	if err != nil {
		return 0, fmt.Errorf("classifying adapter %s: %w", adapter.Hex(), err)
	}
	return s.upsertAdapterRow(ctx, tx, vault, vaultAddress, adapter, adapterType, firstSeenBlock)
}

// upsertAdapterRow records an already-classified adapter in the registry within
// tx, WARNing on an Unknown type (mirroring the VaultShaped discovery sentinel so
// a future adapter kind surfaces instead of being dropped). The upsert is
// incarnation-aware (GetOrCreateAdapter), so a later replay of the true AddAdapter
// converges the row rather than duplicating it. Shared by probeAndUpsertAdapter
// (type probed inside the tx) and the discovery seed (type probed up-front).
func (s *Service) upsertAdapterRow(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, vaultAddress, adapter common.Address, adapterType entity.MorphoAdapterType, firstSeenBlock int64) (int64, error) {
	if adapterType == entity.MorphoAdapterTypeUnknown {
		s.logger.Warn("VaultV2 adapter of unknown type — recorded as Unknown for later curation",
			"vault", vaultAddress.Hex(), "adapter", adapter.Hex(), "block", firstSeenBlock)
	}
	adapterEntity, err := entity.NewMorphoAdapter(vault.ID, adapter.Bytes(), vault.AssetTokenID, adapterType, firstSeenBlock, nil)
	if err != nil {
		return 0, fmt.Errorf("creating adapter entity: %w", err)
	}
	id, err := s.morphoRepo.GetOrCreateAdapter(ctx, tx, adapterEntity)
	if err != nil {
		return 0, fmt.Errorf("persisting adapter: %w", err)
	}
	return id, nil
}

// handleForceDeallocate emits an ops WARN and writes NO state.
//
// The contract's forceDeallocate() calls the shared internal deallocate path
// (deallocateInternal) which emits the Deallocate event, so every
// ForceDeallocate log is accompanied by a Deallocate log in the same
// transaction that already triggers the adapter-state snapshot via
// handleAllocation. Writing a second snapshot here would duplicate it. The WARN
// is the value this handler adds: a sentinel used the emergency exit path.
func (s *Service) handleForceDeallocate(ctx context.Context, e *ForceDeallocateEvent, vaultAddress common.Address, blockNumber int64) error {
	if _, err := s.resolveV2Vault(vaultAddress); err != nil {
		return err
	}
	s.logger.Warn("VaultV2 forceDeallocate — sentinel emergency exit",
		"vault", vaultAddress.Hex(),
		"adapter", e.Adapter.Hex(),
		"assets", e.Assets.String(),
		"onBehalf", e.OnBehalf.Hex(),
		"penaltyAssets", e.PenaltyAssets.String(),
		"block", blockNumber)
	return nil
}

// handleCapChange snapshots one cap id's full on-chain state after any of the 4
// cap events. Like handleAllocation snapshots realAssets(), it reads the pair
// (absoluteCap, relativeCap) directly from the vault at the log's block hash
// rather than carrying a value forward from a prior row: the event carries only
// the single field it changed, so the authoritative full state is the on-chain
// read. The read is hash-pinned (state read), so the row is an end-of-block
// snapshot for that block.
//
// Sibling cap events in the same block (a cap id typically sets its absolute and
// relative limits in one block) each read the same block hash and therefore
// build byte-identical rows; the mvc trigger's same-build lookup plus
// SaveVaultCap's ON CONFLICT DO NOTHING correctly dedupe them to one row (same
// rationale as adapter_state's same-block snapshots).
func (s *Service) handleCapChange(ctx context.Context, vaultAddress common.Address, capID common.Hash, idData []byte, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}

	absolute, relative, err := s.blockchainSvc.getVaultCaps(ctx, vaultAddress, capID, blockHash)
	if err != nil {
		return fmt.Errorf("reading caps for %s: %w", capID.Hex(), err)
	}

	vaultCap, err := entity.NewMorphoVaultCap(vault.ID, capID.Bytes(), idData, absolute, relative, blockNumber, blockVersion, blockTimestamp)
	if err != nil {
		return fmt.Errorf("creating vault cap entity: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.morphoRepo.SaveVaultCap(ctx, tx, vaultCap)
	})
}

// updateVaultFee applies a single-field fee-config change to the vault. Only the
// field carried on the triggering Set* event is set on the update; the others
// stay nil so UpdateVaultFeeConfig leaves their columns untouched.
func (s *Service) updateVaultFee(ctx context.Context, vaultAddress common.Address, update entity.MorphoVaultFeeUpdate) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.morphoRepo.UpdateVaultFeeConfig(ctx, tx, vault.ID, update)
	})
}
