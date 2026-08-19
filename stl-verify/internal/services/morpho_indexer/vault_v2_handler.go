package morpho_indexer

import (
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

// handleAddAdapter classifies the new adapter on-chain, records the membership
// transition it witnessed, and seeds its first realAssets() snapshot — mirroring what
// discovery does for the adapters a mid-life-discovered vault already holds. The seed is
// what keeps a freshly registered adapter from looking like adapter_data_missing to
// VEC-219's composition probe until the vault's first allocation, which can be many
// hours later. An unclassifiable adapter is persisted as Unknown behind a WARN
// (warnIfUnknownAdapterType). Both chain reads run before the transaction opens so a
// pooled DB connection never sits idle across a chain round-trip.
//
// The observation is always recorded, even when the adapter is already known to be a
// member (discovery may have seeded it first): an AddAdapter log is the evidence of WHEN
// the set changed, and keeping it is what makes the true add block a MIN over the log in
// any arrival order.
func (s *Service) handleAddAdapter(ctx context.Context, e *AddAdapterEvent, vaultAddress common.Address, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time, logIndex int32) error {
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
	s.warnIfUnknownAdapterType(vaultAddress, e.Account, adapterType, blockNumber)
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		adapterID, _, err := s.observeAdapterMembership(ctx, tx, vault, e.Account, entity.MorphoAdapterMembership{
			BlockNumber:  blockNumber,
			BlockVersion: blockVersion,
			LogIndex:     logIndex,
			Timestamp:    blockTimestamp,
			IsMember:     true,
			AdapterType:  &adapterType,
			ObservedVia:  entity.MembershipFromAddAdapter,
		})
		if err != nil {
			return err
		}
		return s.saveAdapterSeedState(ctx, tx, adapterID, realAssets, blockNumber, blockVersion, blockTimestamp)
	})
}

// observeAdapterMembership records one observation about (vault, adapter), creating the
// adapter's identity row on first sight. Every VaultV2 write path funnels through here, so
// the identity an observation is attached to is built in exactly one place.
func (s *Service) observeAdapterMembership(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, adapter common.Address, membership entity.MorphoAdapterMembership) (int64, bool, error) {
	adapterID, appended, err := s.morphoRepo.ObserveAdapterMembership(ctx, tx, &entity.MorphoAdapterObservation{
		Identity: entity.MorphoAdapterIdentity{
			MorphoVaultID: vault.ID,
			Address:       adapter.Bytes(),
			AssetTokenID:  vault.AssetTokenID,
		},
		Membership: membership,
	})
	if err != nil {
		return 0, false, fmt.Errorf("recording adapter %s membership at block %d: %w", adapter.Hex(), membership.BlockNumber, err)
	}
	return adapterID, appended, nil
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

// handleRemoveAdapter records that the adapter was NOT in the vault's set from this log
// onward. It is an unconditional append with no lookup and no decision: a removal for an
// adapter we have never seen creates the identity row and records one untyped
// observation, which is the truthful record rather than an error.
//
// Deliberately no type probe and no pre-transaction membership read. Both existed only to
// satisfy "the incarnation a removal closes must be registered with a type"; there is no
// incarnation and no heal row any more, and an observation of NON-membership needs no
// classification (the adapter_type CHECK exempts exactly this case). That removes one
// chain round-trip and one DB read from the removal path.
func (s *Service) handleRemoveAdapter(ctx context.Context, e *RemoveAdapterEvent, vaultAddress common.Address, blockNumber int64, blockVersion int, blockTimestamp time.Time, logIndex int32) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}
	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		_, _, err := s.observeAdapterMembership(ctx, tx, vault, e.Account, entity.MorphoAdapterMembership{
			BlockNumber:  blockNumber,
			BlockVersion: blockVersion,
			LogIndex:     logIndex,
			Timestamp:    blockTimestamp,
			IsMember:     false,
			AdapterType:  nil,
			ObservedVia:  entity.MembershipFromRemoveAdapter,
		})
		return err
	})
}

// handleAllocation snapshots an adapter's realAssets() after an Allocate or
// Deallocate. The event's `change` is a signed per-id delta, not a running
// total, so the authoritative per-adapter value is read from realAssets()
// (hash-pinned, state read).
//
// The membership side of it is an ASSERTION, not a transition: the VaultV2 contract
// refuses to allocate to an unregistered adapter, so the log proves the adapter was a
// member at that position — but it witnesses no change. So it is recorded only when the
// log does not already say so there, which is what keeps a table sized for governance
// events from taking one row per allocation. An allocation for an adapter we never saw
// AddAdapter for is therefore not a poison pill either: the adapter address comes from the
// vault's own event and is identity-verified by the on-chain type probe, so the membership
// it implies is recorded (behind a WARN) rather than stalling the whole morpho queue.
func (s *Service) handleAllocation(ctx context.Context, adapter, vaultAddress common.Address, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time, logIndex int32) error {
	vault, err := s.resolveV2Vault(vaultAddress)
	if err != nil {
		return err
	}

	realAssets, err := s.blockchainSvc.getAdapterRealAssets(ctx, adapter, blockHash)
	if err != nil {
		return fmt.Errorf("fetching realAssets for adapter %s: %w", adapter.Hex(), err)
	}

	position := entity.BlockPosition{BlockNumber: blockNumber, BlockVersion: blockVersion, LogIndex: logIndex}
	probedType, err := s.resolveAdapterTypeIfUnregistered(ctx, vault, adapter, position)
	if err != nil {
		return err
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		adapterID, err := s.assertAllocatedAdapterIsMember(ctx, tx, vault, vaultAddress, adapter, position, blockTimestamp, probedType)
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

// assertAllocatedAdapterIsMember records the membership an Allocate/Deallocate log
// implies and returns the adapter's registry id. The decision "does the log already say
// this" is the repository's, made under its advisory lock inside the caller's transaction,
// so it also sees rows this event wrote earlier (read-your-writes).
//
// The WARN fires only when the assertion actually added something — i.e. when we are
// learning about an adapter whose AddAdapter we never observed. On the hot path, where the
// log already answers, nothing is written and nothing is logged.
//
// A nil probedType means the pre-transaction read found the adapter already a member at
// this position. If the in-transaction decision disagrees, we would have to record
// membership with no classification, which the registry refuses with
// ErrAdapterUnclassified — the event fails hard rather than defaulting a type; SQS
// redelivers and the pre-transaction read re-probes.
func (s *Service) assertAllocatedAdapterIsMember(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, vaultAddress, adapter common.Address, at entity.BlockPosition, blockTimestamp time.Time, probedType *entity.MorphoAdapterType) (int64, error) {
	adapterID, appended, err := s.observeAdapterMembership(ctx, tx, vault, adapter, entity.MorphoAdapterMembership{
		BlockNumber:  at.BlockNumber,
		BlockVersion: at.BlockVersion,
		LogIndex:     at.LogIndex,
		Timestamp:    blockTimestamp,
		IsMember:     true,
		AdapterType:  probedType,
		ObservedVia:  entity.MembershipFromAllocation,
	})
	if errors.Is(err, outbound.ErrAdapterUnclassified) {
		return 0, fmt.Errorf("adapter %s was a member before the transaction but is not at block %d inside it, so no type was probed: %w",
			adapter.Hex(), at.BlockNumber, err)
	}
	if err != nil {
		return 0, err
	}
	if appended {
		s.logger.Warn("adapter membership inferred from an Allocate; no AddAdapter observed",
			"vault", vaultAddress.Hex(), "adapter", adapter.Hex(), "block", at.BlockNumber)
		if probedType != nil {
			s.warnIfUnknownAdapterType(vaultAddress, adapter, *probedType, at.BlockNumber)
		}
	}
	return adapterID, nil
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

// resolveAdapterTypeIfUnregistered resolves the classification an assertion would need,
// probing on-chain only when the log does not already place the adapter in the vault's set
// AT THIS POSITION — so the probe (and its idle-connection hazard) is skipped entirely on
// the hot path where the adapter is known. It returns nil when the adapter is already a
// member there: the repository's own decision will find the same answer and append
// nothing, so no type is needed.
//
// The read is position-scoped rather than "is it a member NOW", which also fixes a wart:
// the backfiller replays historical blocks, and asking about the present would probe (or
// decline to probe) on the strength of an answer from thousands of blocks later. It is
// read from committed state via the pool; the decisive read-then-write stays inside the
// transaction under the advisory lock.
func (s *Service) resolveAdapterTypeIfUnregistered(ctx context.Context, vault *entity.MorphoVault, adapter common.Address, at entity.BlockPosition) (*entity.MorphoAdapterType, error) {
	member, err := s.morphoRepo.GetActiveAdapterAt(ctx, vault.ID, adapter.Bytes(), at)
	if err != nil {
		return nil, fmt.Errorf("looking up adapter %s membership at block %d: %w", adapter.Hex(), at.BlockNumber, err)
	}
	if member != nil {
		return nil, nil
	}
	adapterType, err := s.resolveAdapterType(ctx, adapter, at.BlockNumber)
	if err != nil {
		return nil, err
	}
	return &adapterType, nil
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
