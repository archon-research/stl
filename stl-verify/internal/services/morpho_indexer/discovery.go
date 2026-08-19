package morpho_indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// This file holds Morpho vault discovery: the on-chain probe that turns an
// unknown address into a registered V1 / V1.1 / V2 vault, together with the
// adapter and fee-config seeds a VaultV2's discovery transaction writes.

// MorphoBlueVaultCandidates returns the addresses from a Morpho Blue event
// that could be MetaMorpho V1/V1.1 vaults — caller and onBehalf for the
// position-changing events, caller and borrower for Liquidate. Used by both
// the live indexer (this package) and the morpho-vault-indexer backfiller
// for V1/V1.1 vault discovery, since those vaults are characterised by
// their interaction with Morpho Blue rather than by emitting a uniquely
// shaped event of their own.
//
// V2 vaults can also appear here when they use a Morpho Blue market adapter,
// but the V2 4-field AccrueInterest topic catches them earlier via
// IsVaultActivityEvent — most candidates surfaced through this function are
// V1/V1.1.
//
// Borrower is included for Liquidate symmetry with the backfiller. In
// practice MetaMorpho V1/V1.1 vaults don't borrow on Morpho Blue, so the
// borrower slot almost always resolves to known-not-vault on first probe;
// it's retained so the same selector contract holds for both code paths
// even if a future vault flavour borrows.
//
// Caller-side filtering (zero address, MorphoBlueAddress, already-known)
// happens at the call site.
func MorphoBlueVaultCandidates(event MorphoBlueEvent) []common.Address {
	switch e := event.(type) {
	case *SupplyEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *WithdrawEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *BorrowEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *RepayEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *SupplyCollateralEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *WithdrawCollateralEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *LiquidateEvent:
		return []common.Address{e.Caller, e.Borrower}
	default:
		return nil
	}
}

// discoveredAdapter is one enumerated VaultV2 adapter with everything read
// on-chain for it BEFORE the discovery persist transaction: its classified type
// (number-pinned — adapter identity is immutable) and its hash-pinned realAssets()
// seed. Reading up-front is what lets the vault and all its adapters persist in a
// single atomic transaction.
type discoveredAdapter struct {
	address     common.Address
	adapterType entity.MorphoAdapterType
	realAssets  *big.Int
}

// vaultDiscoveryReads holds every on-chain value discovery needs, read before the
// persist transaction so the vault row and its adapters commit atomically.
// adapters and fees are populated only for a VaultV2 (nil/empty for V1/V1.1);
// adapters is sorted by address for deterministic advisory-lock acquisition in the
// persist step.
type vaultDiscoveryReads struct {
	metadata      *VaultMetadata
	assetMetadata TokenMetadata
	adapters      []discoveredAdapter
	fees          *vaultFeeConfig
}

// discoverAndRegisterVault probes vaultAddress on-chain, persists the vault, its
// asset token, and (for a VaultV2) all its enumerated adapters + seeded states in
// a single transaction, then registers the vault in the in-memory registry.
// Returns *ErrNotVault if the address is definitively not a Morpho-family vault;
// transient errors (RPC, DB) propagate as plain errors so the caller can retry.
//
// Used by both the IsVaultActivityEvent path (V2) via tryDiscoverVault and the
// Morpho Blue caller/onBehalf path (V1/V1.1) via discoverV1V11VaultsInReceipt.
// blockHash / blockVersion / blockTimestamp are the triggering event's; they feed
// only the V2 adapter enumeration/seed (hash-pinned reads) and are inert for the
// V1/V1.1 path.
//
// Reads-then-persist ordering is the atomicity guarantee (VEC-218 review): ALL
// on-chain reads happen first, then everything persists in one WithTransaction.
// A transient read failure commits nothing, so a worker restart never reloads a
// discovered-but-adapterless vault (whose historical AddAdapter events never
// replay on the live stream) and skip its enumeration forever — SQS redelivery
// re-discovers cleanly.
func (s *Service) discoverAndRegisterVault(ctx context.Context, vaultAddress common.Address, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	reads, err := s.readVaultForDiscovery(ctx, vaultAddress, blockNumber, blockHash)
	if err != nil {
		return err
	}

	vault, err := s.persistDiscoveredVault(ctx, vaultAddress, chainID, blockNumber, blockVersion, blockTimestamp, reads)
	if err != nil {
		return err
	}

	// Register in-memory only after the atomic persist commits.
	s.vaultRegistry.RegisterVault(vaultAddress, vault)
	return nil
}

// readVaultForDiscovery performs every on-chain read discovery needs, before any
// DB write: vault metadata, its asset token metadata, and — for a VaultV2 — the
// full enumerated adapter set with each adapter's type and hash-pinned realAssets
// seed, plus the hash-pinned fee config. A transport error on any read bubbles so
// nothing is persisted.
func (s *Service) readVaultForDiscovery(ctx context.Context, vaultAddress common.Address, blockNumber int64, blockHash common.Hash) (*vaultDiscoveryReads, error) {
	metadata, err := s.blockchainSvc.getVaultMetadata(ctx, vaultAddress, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("fetching vault metadata: %w", err)
	}

	assetMetadata, err := s.blockchainSvc.getTokenMetadata(ctx, metadata.Asset, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("fetching asset token metadata: %w", err)
	}

	reads := &vaultDiscoveryReads{metadata: metadata, assetMetadata: assetMetadata}
	if metadata.Version == entity.MorphoVaultV2 {
		adapters, err := s.readV2Adapters(ctx, vaultAddress, blockNumber, blockHash)
		if err != nil {
			return nil, err
		}
		reads.adapters = adapters

		fees, err := s.readV2FeeConfigForSeed(ctx, vaultAddress, blockNumber, blockHash)
		if err != nil {
			return nil, err
		}
		reads.fees = fees
	}
	return reads, nil
}

// readV2FeeConfigForSeed reads the fee config discovery seeds from, returning nil
// when the contract serves no VaultV2 fee surface at all.
//
// The vault probe accepts an address on curator() + liquidityAdapter() alone, so it
// never proves the fee getters exist. Hard-requiring them here poisoned such an
// address's discovery forever — its triggering AccrueInterest retried and the vault
// was never registered. Skipping the seed instead leaves the vault honestly
// fee-row-less; a partially served surface is drift and still propagates (see
// getVaultFees).
func (s *Service) readV2FeeConfigForSeed(ctx context.Context, vaultAddress common.Address, blockNumber int64, blockHash common.Hash) (*vaultFeeConfig, error) {
	fees, err := s.blockchainSvc.getVaultFees(ctx, vaultAddress, blockHash)
	if errors.Is(err, errNoVaultFeeSurface) {
		s.logger.Warn("vault-shaped contract has no VaultV2 fee surface — registering it with no fee config seeded",
			"vault", vaultAddress.Hex(), "block", blockNumber)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("seeding fee config for vault %s: %w", vaultAddress.Hex(), err)
	}
	return fees, nil
}

// readV2Adapters enumerates a freshly-discovered VaultV2's adapter set (hash-pinned
// — the set is versioned state) and, for each adapter, classifies its type
// (number-pinned identity) and reads its hash-pinned realAssets() seed. The result
// is sorted by address so the persist transaction acquires the per-adapter membership
// advisory locks (and the mam/mas trigger locks after them) in a deterministic order
// (ADR-0002 §3 batch rule).
func (s *Service) readV2Adapters(ctx context.Context, vaultAddress common.Address, blockNumber int64, blockHash common.Hash) ([]discoveredAdapter, error) {
	addresses, err := s.blockchainSvc.enumerateVaultAdapters(ctx, vaultAddress, blockHash)
	if err != nil {
		return nil, fmt.Errorf("enumerating adapters: %w", err)
	}

	adapters := make([]discoveredAdapter, 0, len(addresses))
	for _, adapter := range addresses {
		adapterType, err := s.blockchainSvc.getAdapterType(ctx, adapter, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("classifying adapter %s: %w", adapter.Hex(), err)
		}
		realAssets, err := s.readSeedRealAssets(ctx, adapter, adapterType, blockHash)
		if err != nil {
			return nil, err
		}
		adapters = append(adapters, discoveredAdapter{address: adapter, adapterType: adapterType, realAssets: realAssets})
	}
	slices.SortFunc(adapters, func(a, b discoveredAdapter) int {
		return bytes.Compare(a.address.Bytes(), b.address.Bytes())
	})
	return adapters, nil
}

// persistDiscoveredVault writes the vault, its asset token, protocol, receipt
// token, and every enumerated adapter plus its seeded adapter_state row in a
// SINGLE transaction. Either the whole vault (including adapters) commits or
// nothing does, so the vault row can never exist without the adapters its
// historical AddAdapter events would have created.
func (s *Service) persistDiscoveredVault(ctx context.Context, vaultAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, reads *vaultDiscoveryReads) (*entity.MorphoVault, error) {
	var vault *entity.MorphoVault
	if err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, reads.metadata.Asset, reads.assetMetadata.Symbol, reads.assetMetadata.Decimals, &blockNumber)
		if err != nil {
			return fmt.Errorf("getting asset token: %w", err)
		}

		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", s.deployBlock)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}

		vault, err = entity.NewMorphoVault(chainID, protocolID, vaultAddress.Bytes(), reads.metadata.Name, reads.metadata.Symbol, tokenID, reads.metadata.Version, blockNumber)
		if err != nil {
			return fmt.Errorf("creating vault entity: %w", err)
		}
		vaultID, err := s.morphoRepo.GetOrCreateVault(ctx, tx, vault)
		if err != nil {
			return fmt.Errorf("persisting vault: %w", err)
		}
		vault.ID = vaultID

		receiptToken, err := entity.NewReceiptToken(chainID, protocolID, tokenID, blockNumber, vaultAddress, reads.metadata.Symbol)
		if err != nil {
			return fmt.Errorf("creating receipt token entity: %w", err)
		}
		if _, err := s.receiptTokenRepo.GetOrCreateReceiptToken(ctx, tx, *receiptToken); err != nil {
			return fmt.Errorf("registering receipt token: %w", err)
		}

		if err := s.seedDiscoveredAdapters(ctx, tx, vault, vaultAddress, blockNumber, blockVersion, blockTimestamp, reads.adapters, entity.MembershipFromDiscovery); err != nil {
			return err
		}
		return s.seedDiscoveredFees(ctx, tx, vault, blockNumber, blockVersion, blockTimestamp, reads.fees)
	}); err != nil {
		return nil, fmt.Errorf("persisting vault %s: %w", vaultAddress.Hex(), err)
	}
	return vault, nil
}

// seedDiscoveredAdapters records the membership each enumerated adapter's presence in
// adapters(i) asserts, and seeds one adapter_state row from its already-read hash-pinned
// realAssets, within the discovery transaction. Seeding at discovery keeps the adapter
// registry and its state consistent from the first moment (VEC-219's
// composition-completeness probe treats an active adapter with no state row as
// adapter_data_missing); subsequent Allocate/Deallocate events append as normal. adapters
// arrive address-sorted (see readV2Adapters); V1/V1.1 vaults pass an empty slice.
//
// What it records is an ASSERTION, not an add: an adapters(i) enumeration witnesses no
// transition, it reads the set's contents at the end of the discovery block — which is why
// it carries entity.EndOfBlockLogIndex, ordering above every log in that block, and why a
// re-run whose answer already matches the log writes nothing. A later replay of the true
// AddAdapter appends its own transition at its own, lower block, and the add block is a MIN
// over those; nothing here has to be converged or walked back.
//
// observedVia distinguishes a mid-life discovery from the Temporal bootstrap's head seed,
// which drives the same loop.
func (s *Service) seedDiscoveredAdapters(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, vaultAddress common.Address, blockNumber int64, blockVersion int, blockTimestamp time.Time, adapters []discoveredAdapter, observedVia entity.MembershipSource) error {
	for _, a := range adapters {
		s.warnIfUnknownAdapterType(vaultAddress, a.address, a.adapterType, blockNumber)
		adapterType := a.adapterType
		adapterID, _, err := s.observeAdapterMembership(ctx, tx, vault, a.address, entity.MorphoAdapterMembership{
			BlockNumber:  blockNumber,
			BlockVersion: blockVersion,
			LogIndex:     entity.EndOfBlockLogIndex,
			Timestamp:    blockTimestamp,
			IsMember:     true,
			AdapterType:  &adapterType,
			ObservedVia:  observedVia,
		})
		if err != nil {
			return err
		}
		if err := s.saveAdapterSeedState(ctx, tx, adapterID, a.realAssets, blockNumber, blockVersion, blockTimestamp); err != nil {
			return fmt.Errorf("seeding adapter state for %s: %w", a.address.Hex(), err)
		}
	}
	return nil
}

// seedDiscoveredFees snapshots a freshly discovered VaultV2's full fee config from
// the already-read hash-pinned values, within the discovery transaction. Without
// it a mid-life-discovered vault carries NO fee row until a Set* fee event fires
// again — and governance typically sets fees once, at deployment (sparkUSDTbc:
// blocks 24765788 / 24765805), so "again" may be never. fees is nil for V1/V1.1,
// which have no VaultV2 fee surface.
//
// Caps have no equivalent seed and deliberately so: a cap id is an opaque keccak
// hash of the id-data the curator chose, and the contract exposes no enumeration
// getter, so there is no set to read. Cap history therefore starts at the first cap
// event we witness or the backfiller replays.
func (s *Service) seedDiscoveredFees(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault, blockNumber int64, blockVersion int, blockTimestamp time.Time, fees *vaultFeeConfig) error {
	if fees == nil {
		return nil
	}
	vaultFee, err := entity.NewMorphoVaultFee(vault.ID, fees.performanceFee, fees.managementFee,
		fees.performanceFeeRecipient.Bytes(), fees.managementFeeRecipient.Bytes(),
		blockNumber, blockVersion, blockTimestamp)
	if err != nil {
		return fmt.Errorf("creating vault fee entity: %w", err)
	}
	if err := s.morphoRepo.SaveVaultFee(ctx, tx, vaultFee); err != nil {
		return fmt.Errorf("seeding vault fee config: %w", err)
	}
	return nil
}

// tryDiscoverVault attempts to discover a new MetaMorpho/VaultV2 vault from
// a vault-emitted log (currently only the V2 4-field AccrueInterest topic
// per IsVaultActivityEvent). Validates the log decodes, then probes and
// registers via discoverAndRegisterVault, then processes the triggering log
// against the now-known vault.
func (s *Service) tryDiscoverVault(ctx context.Context, log shared.Log, vaultAddress common.Address, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	ctx, span := s.telemetry.StartSpan(ctx, "morpho.discoverVault",
		attribute.String("vault.address", vaultAddress.Hex()),
		attribute.String("discovery.path", "vaultActivity"))
	defer span.End()

	// Validate this is a decodable MetaMorpho event before making on-chain calls.
	if _, err := s.eventExtractor.ExtractMetaMorphoEvent(log); err != nil {
		return &ErrNotVault{Err: fmt.Errorf("event decode failed: %w", err)}
	}

	if err := s.discoverAndRegisterVault(ctx, vaultAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp); err != nil {
		return err
	}

	return s.processMetaMorphoLog(ctx, log, vaultAddress, chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
}

// discoverV1V11VaultsInReceipt is the pre-walk for V1/V1.1 vault discovery
// via the Morpho Blue caller/onBehalf path. It iterates the receipt's
// Morpho Blue logs once, extracts candidate addresses (caller, onBehalf —
// or caller, borrower for Liquidate), and probes each unknown address.
// Successful probes register the vault in the in-memory registry so the
// caller's main loop sees it as a known vault when it reaches the vault's
// own Deposit / Transfer / V1 AccrueInterest logs in the same receipt.
//
// Mirrors the morpho-vault-indexer backfiller's emitMorphoBlueCandidates,
// keeping the live and offline V1/V1.1 discovery contracts uniform — the
// backfiller is recovery-only, so the live indexer must cover V1/V1.1
// discovery itself.
//
// ErrNotVault outcomes mark the address as known-not-vault (cached) so
// repeat appearances of the same EOA / non-vault contract short-circuit.
// Transient errors propagate so the receipt fails and SQS redelivers —
// they must NOT mark the address as not-vault, or a real V1/V1.1 vault
// that was momentarily unreachable would be permanently black-holed.
//
// The seen map dedupes within the receipt: if the same address appears
// as caller AND onBehalf in one Supply, OR as caller in two Morpho Blue
// logs in the same receipt, only one probe fires per receipt. After the
// first probe the registry cache (IsKnownVault / IsKnownNotVault) handles
// further short-circuiting.
func (s *Service) discoverV1V11VaultsInReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	morphoBlueAddr := MorphoBlueAddress
	var errs []error
	seen := make(map[common.Address]bool)

	for _, log := range receipt.Logs {
		if common.HexToAddress(log.Address) != morphoBlueAddr {
			continue
		}
		if !s.eventExtractor.IsMorphoBlueEvent(log) {
			continue
		}

		event, parseErr := s.eventExtractor.ExtractMorphoBlueEvent(log)
		if parseErr != nil {
			// Re-parse failure here is structurally impossible today —
			// the same log will be extracted again in processMorphoBlueLog
			// from the same extractor. If a future change introduces
			// non-determinism (e.g. ABI fallback, caching), this branch
			// becomes a silent discovery hole, so log a Warn rather than
			// swallow.
			s.logger.Warn("re-parse of Morpho Blue event failed in discovery pre-walk (should not happen — investigate)",
				"tx", log.TransactionHash,
				"topic", log.Topics[0],
				"error", parseErr)
			continue
		}

		for _, addr := range MorphoBlueVaultCandidates(event) {
			if seen[addr] {
				continue
			}
			seen[addr] = true
			if addr == (common.Address{}) || addr == morphoBlueAddr {
				continue
			}
			if s.vaultRegistry.IsKnownVault(addr) || s.vaultRegistry.IsKnownNotVault(addr) {
				continue
			}

			probeCtx, probeSpan := s.telemetry.StartSpan(ctx, "morpho.discoverVault",
				attribute.String("vault.address", addr.Hex()),
				attribute.String("discovery.path", "morphoBlue"))
			probeErr := s.discoverAndRegisterVault(probeCtx, addr, chainID, blockNumber, blockHash, blockVersion, blockTimestamp)
			probeSpan.End()
			if probeErr == nil {
				continue
			}
			var nv *ErrNotVault
			if errors.As(probeErr, &nv) {
				s.vaultRegistry.MarkNotVault(addr)
				if nv.VaultShaped {
					s.logger.Warn("vault-shaped address rejected by probe (Morpho Blue path) — possible new vault flavour",
						"address", addr.Hex(),
						"reason", probeErr)
				} else {
					s.logger.Debug("not a Morpho-family vault (Morpho Blue path)",
						"address", addr.Hex(),
						"reason", probeErr)
				}
				continue
			}
			s.logger.Warn("V1/V1.1 vault discovery via Morpho Blue path failed (will retry)",
				"address", addr.Hex(),
				"error", probeErr)
			errs = append(errs, fmt.Errorf("V1/V1.1 discovery for %s in tx %s: %w", addr.Hex(), receipt.TransactionHash, probeErr))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
