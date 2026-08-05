package morpho_indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
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

// handleAddAdapter classifies the new adapter on-chain, records it in the adapter
// registry, and seeds its first realAssets() snapshot — mirroring what discovery
// does for the adapters a mid-life-discovered vault already holds. The seed is what
// keeps a freshly registered adapter from looking like adapter_data_missing to
// VEC-219's composition probe until the vault's first allocation, which can be many
// hours later. An unclassifiable adapter is persisted as Unknown behind a WARN
// (warnIfUnknownAdapterType). Both chain reads run before the transaction opens so a
// pooled DB connection never sits idle across a chain round-trip.
func (s *Service) handleAddAdapter(ctx context.Context, e *AddAdapterEvent, vaultAddress common.Address, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}
	adapterType, err := s.resolveAdapterType(ctx, e.Account, blockNumber)
	if err != nil {
		return err
	}
	realAssets, err := s.readSeedRealAssets(ctx, e.Account, adapterType, blockHash)
	if err != nil {
		return err
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		adapterID, err := s.upsertAdapterRow(ctx, tx, vault, vaultAddress, e.Account, adapterType, blockNumber)
		if err != nil {
			return err
		}
		return s.saveAdapterSeedState(ctx, tx, adapterID, realAssets, blockNumber, blockVersion, blockTimestamp)
	})
}

// readSeedRealAssets reads the realAssets() seed for an adapter being registered,
// returning nil when an adapter the type probe could not classify does not serve the
// getter at all.
//
// The tolerance is gated structurally, not "best effort". For a MODELLED adapter kind
// realAssets() must answer: the vault itself calls it while allocating, so a revert is
// contract drift. Registration is different — setIsAdapter never touches
// realAssets() — so an adapter that classified as Unknown (both type probes reverted)
// may legitimately not serve it. Hard-failing there defeated the Unknown sentinel,
// whose entire purpose is to record an unmodelled adapter kind behind a WARN instead
// of poison-pilling the block forever. Registered with no seed, VEC-219's composition
// probe reports it as adapter_data_missing, which is the honest answer: an adapter we
// cannot classify is one we cannot price. A multicall TRANSPORT error still propagates
// for every type — that is transient and must retry.
func (s *Service) readSeedRealAssets(ctx context.Context, adapter common.Address, adapterType entity.MorphoAdapterType, blockHash common.Hash) (*big.Int, error) {
	realAssets, err := s.blockchainSvc.getAdapterRealAssets(ctx, adapter, blockHash)
	if err == nil {
		return realAssets, nil
	}
	if adapterType == entity.MorphoAdapterTypeUnknown && errors.Is(err, errAdapterRealAssetsReverted) {
		s.logger.Warn("unclassified VaultV2 adapter does not serve realAssets() — registering it with no state seed",
			"adapter", adapter.Hex(), "block_hash", blockHash.Hex())
		return nil, nil
	}
	return nil, fmt.Errorf("seeding realAssets for adapter %s: %w", adapter.Hex(), err)
}

// saveAdapterSeedState writes a freshly registered adapter's first realAssets
// snapshot, or nothing when the adapter served no reading (see readSeedRealAssets).
func (s *Service) saveAdapterSeedState(ctx context.Context, tx pgx.Tx, adapterID int64, realAssets *big.Int, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	if realAssets == nil {
		return nil
	}
	state, err := entity.NewMorphoAdapterState(adapterID, blockNumber, blockVersion, blockTimestamp, realAssets)
	if err != nil {
		return fmt.Errorf("creating adapter state entity: %w", err)
	}
	return s.morphoRepo.SaveAdapterState(ctx, tx, state)
}

// handleRemoveAdapter marks the adapter inactive from this block onward, registering the
// incarnation to close first when none is on record (see ensureIncarnationToClose for
// when that happens and why the registration is not the Allocate path's).
func (s *Service) handleRemoveAdapter(ctx context.Context, e *RemoveAdapterEvent, vaultAddress common.Address, blockNumber int64) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}
	probedType, err := s.resolveAdapterTypeIfUnregistered(ctx, vault, e.Account, blockNumber)
	if err != nil {
		return err
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.ensureIncarnationToClose(ctx, tx, vault, vaultAddress, e.Account, blockNumber, probedType); err != nil {
			return err
		}
		return s.morphoRepo.MarkAdapterRemoved(ctx, tx, vault.ID, e.Account.Bytes(), blockNumber)
	})
}

// ensureIncarnationToClose guarantees MarkAdapterRemoved has a row to close, and
// registers NOTHING when one already exists. Which of those happens is
// EnsureIncarnationToClose's decision, made and acted on under one advisory lock so no
// concurrent writer can split it; the distinction from the Allocate path's
// ensureAdapterRegistered (a removal asks about a COVERING incarnation, not an active
// row) is stated on that port method. This adds the ops narration.
//
// A nil probedType means the pre-transaction check found the adapter active. If the
// decisive read then finds no incarnation covering the block, we have no type to
// record and no live single-consumer path that could explain it, so we fail hard
// rather than record a defaulted classification; SQS redelivers and the pre-tx check
// re-probes.
func (s *Service) ensureIncarnationToClose(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, vaultAddress, adapter common.Address, removedAtBlock int64, probedType *entity.MorphoAdapterType) error {
	candidate, err := s.unobservedIncarnation(vault, adapter, removedAtBlock, probedType)
	if err != nil {
		return err
	}

	registered, err := s.morphoRepo.EnsureIncarnationToClose(ctx, tx, vault.ID, adapter.Bytes(), removedAtBlock, candidate)
	if errors.Is(err, outbound.ErrAdapterUnclassified) {
		return fmt.Errorf("adapter %s has no incarnation covering block %d at transaction time but no type was probed before the transaction: %w", adapter.Hex(), removedAtBlock, err)
	}
	if err != nil {
		return fmt.Errorf("ensuring adapter %s has an incarnation to close at block %d: %w", adapter.Hex(), removedAtBlock, err)
	}
	if !registered {
		return nil
	}

	s.logger.Warn("adapter registered lazily; AddAdapter predates vault discovery",
		"vault", vaultAddress.Hex(), "adapter", adapter.Hex(), "block", removedAtBlock)
	s.warnIfUnknownAdapterType(vaultAddress, adapter, candidate.AdapterType, removedAtBlock)
	return nil
}

// unobservedIncarnation builds the incarnation a removal registers when the registry has
// none to close: a zero-length lifetime at the removal block, whose added_at_block is a
// lower bound rather than an observation. It is nil when no type was probed, which is how
// the registry is told to refuse a registration rather than default the classification.
func (s *Service) unobservedIncarnation(vault *entity.MorphoVault, adapter common.Address, removedAtBlock int64, probedType *entity.MorphoAdapterType) (*entity.MorphoAdapter, error) {
	if probedType == nil {
		return nil, nil
	}
	adapterEntity, err := entity.NewMorphoAdapter(vault.ID, adapter.Bytes(), vault.AssetTokenID, *probedType, removedAtBlock, &removedAtBlock)
	if err != nil {
		return nil, fmt.Errorf("creating adapter entity: %w", err)
	}
	return adapterEntity, nil
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

	probedType, err := s.resolveAdapterTypeIfUnregistered(ctx, vault, adapter, blockNumber)
	if err != nil {
		return err
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		adapterID, err := s.ensureAdapterRegistered(ctx, tx, vault, vaultAddress, adapter, blockNumber, probedType)
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
// (vault, adapter). The GetActiveAdapter read here is the decisive read-your-writes
// lookup — it runs inside the caller's transaction, so it sees rows this event
// wrote earlier; it is NOT itself serialized (only the GetOrCreateAdapter call on
// the miss path takes the per-(vault, address) advisory lock). On a miss it lazily
// registers the adapter at firstSeenBlock using probedType — the classification
// already resolved before the transaction opened (see
// resolveAdapterTypeIfUnregistered). Used by the Allocate/Deallocate path, which can
// legitimately reach an adapter that predates the vault's mid-life discovery. The
// RemoveAdapter path needs a different decisive question and uses
// ensureIncarnationToClose instead.
//
// A nil probedType means the pre-transaction check found the adapter already
// registered. If the decisive read then disagrees (the adapter is absent), we have
// no type to record and no live single-consumer path that could have removed it, so
// we fail hard rather than record a defaulted type; SQS redelivers and the pre-tx
// check re-probes.
func (s *Service) ensureAdapterRegistered(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, vaultAddress, adapter common.Address, firstSeenBlock int64, probedType *entity.MorphoAdapterType) (int64, error) {
	active, err := s.morphoRepo.GetActiveAdapter(ctx, tx, vault.ID, adapter.Bytes())
	if err != nil {
		return 0, fmt.Errorf("looking up active adapter %s: %w", adapter.Hex(), err)
	}
	if active != nil {
		return active.ID, nil
	}
	if probedType == nil {
		return 0, fmt.Errorf("adapter %s absent at transaction time but no type was probed before the transaction", adapter.Hex())
	}
	s.logger.Warn("adapter registered lazily; AddAdapter predates vault discovery",
		"vault", vaultAddress.Hex(), "adapter", adapter.Hex(), "block", firstSeenBlock)
	return s.upsertAdapterRow(ctx, tx, vault, vaultAddress, adapter, *probedType, firstSeenBlock)
}

// resolveAdapterType probes an adapter's on-chain type. A probe TRANSPORT error
// propagates (transient ⇒ SQS retries); a clean both-revert probe yields
// MorphoAdapterTypeUnknown, which is recorded behind a WARN (warnIfUnknownAdapterType). The probe is a
// chain round-trip, so every caller runs it BEFORE opening its write transaction —
// a pooled DB connection must never sit idle across it.
func (s *Service) resolveAdapterType(ctx context.Context, adapter common.Address, atBlock int64) (entity.MorphoAdapterType, error) {
	adapterType, err := s.blockchainSvc.getAdapterType(ctx, adapter, atBlock)
	if err != nil {
		return entity.MorphoAdapterTypeUnknown, fmt.Errorf("classifying adapter %s: %w", adapter.Hex(), err)
	}
	return adapterType, nil
}

// resolveAdapterTypeIfUnregistered resolves the type the lazy self-heal would need,
// probing on-chain only when the adapter is not already registered — so the probe
// (and its idle-connection hazard) is skipped entirely on the hot path where the
// adapter is known. It returns nil when the adapter is already active: the in-tx
// GetActiveAdapter will find it and no type is needed. Membership is read from
// committed state via the pool (GetActiveAdaptersByVault); the decisive
// read-then-write stays inside the transaction under the advisory lock.
func (s *Service) resolveAdapterTypeIfUnregistered(ctx context.Context, vault *entity.MorphoVault, adapter common.Address, firstSeenBlock int64) (*entity.MorphoAdapterType, error) {
	active, err := s.morphoRepo.GetActiveAdaptersByVault(ctx, vault.ID)
	if err != nil {
		return nil, fmt.Errorf("looking up active adapters for vault %d: %w", vault.ID, err)
	}
	for _, a := range active {
		if bytes.Equal(a.Address, adapter.Bytes()) {
			return nil, nil
		}
	}
	adapterType, err := s.resolveAdapterType(ctx, adapter, firstSeenBlock)
	if err != nil {
		return nil, err
	}
	return &adapterType, nil
}

// upsertAdapterRow records an already-classified adapter as an OPEN incarnation within
// tx. The upsert is incarnation-aware (GetOrCreateAdapter), so a later replay of the
// true AddAdapter converges the row rather than duplicating it. Its three callers — the
// AddAdapter handler, the Allocate lazy self-heal, and the discovery seed — each resolve
// the type before opening the transaction and pass it in here. The RemoveAdapter heal
// uses EnsureIncarnationToClose instead: it must not converge anything.
func (s *Service) upsertAdapterRow(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, vaultAddress, adapter common.Address, adapterType entity.MorphoAdapterType, firstSeenBlock int64) (int64, error) {
	s.warnIfUnknownAdapterType(vaultAddress, adapter, adapterType, firstSeenBlock)
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

// warnIfUnknownAdapterType surfaces an adapter the on-chain type probe could not
// classify. Mirrors the VaultShaped discovery sentinel: an unmodelled adapter kind is
// recorded behind a WARN rather than dropped, so it can be curated later. Canonical
// statement of that convention for the adapter registry.
func (s *Service) warnIfUnknownAdapterType(vaultAddress, adapter common.Address, adapterType entity.MorphoAdapterType, atBlock int64) {
	if adapterType != entity.MorphoAdapterTypeUnknown {
		return
	}
	s.logger.Warn("VaultV2 adapter of unknown type — recorded as Unknown for later curation",
		"vault", vaultAddress.Hex(), "adapter", adapter.Hex(), "block", atBlock)
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

// handleFeeChange snapshots the vault's FULL on-chain fee config after any of the
// 4 Set* fee events. Like handleCapChange reads the (absoluteCap, relativeCap)
// pair, it reads the full config — performanceFee, managementFee and both
// recipients — directly from the vault at the log's block hash rather than
// carrying a value forward: the event carries only the single field it changed,
// so the authoritative full state is the on-chain read. The read is hash-pinned
// (state read), so the row is an end-of-block snapshot for that block.
//
// Sibling fee events in the same block (a Set* fee event and its recipient often
// land together) each read the same block hash and therefore build byte-identical
// rows; the mvf trigger's same-build lookup plus SaveVaultFee's ON CONFLICT DO
// NOTHING correctly dedupe them to one row (same rationale as caps).
//
// Unlike the discovery seed, this path does NOT tolerate errNoVaultFeeSurface: the
// event itself proves the vault has a fee surface, so getters that all revert is
// drift that must stop the block.
func (s *Service) handleFeeChange(ctx context.Context, vaultAddress common.Address, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}

	fees, err := s.blockchainSvc.getVaultFees(ctx, vaultAddress, blockHash)
	if err != nil {
		return fmt.Errorf("reading fees for vault %s: %w", vaultAddress.Hex(), err)
	}

	vaultFee, err := entity.NewMorphoVaultFee(vault.ID, fees.performanceFee, fees.managementFee,
		fees.performanceFeeRecipient.Bytes(), fees.managementFeeRecipient.Bytes(),
		blockNumber, blockVersion, blockTimestamp)
	if err != nil {
		return fmt.Errorf("creating vault fee entity: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.morphoRepo.SaveVaultFee(ctx, tx, vaultFee)
	})
}
