package entity

import "fmt"

// MorphoAdapterType classifies a VaultV2 liquidity adapter by the kind of
// venue it routes assets into.
//
// A VaultV2 never touches Morpho Blue directly: it holds a set of adapter
// contracts, each wrapping one downstream venue. MarketV1 wraps a Morpho Blue
// market; VaultV1 wraps a nested MetaMorpho V1 vault. Unknown is a
// forward-compatible sentinel (mirroring the VaultShaped discovery pattern) so
// an adapter of a not-yet-modelled type is recorded rather than dropped.
type MorphoAdapterType int16

const (
	MorphoAdapterTypeMarketV1 MorphoAdapterType = 1  // MorphoMarketV1AdapterV2 (Morpho Blue market)
	MorphoAdapterTypeVaultV1  MorphoAdapterType = 2  // MorphoVaultV1Adapter (nested MetaMorpho V1 vault)
	MorphoAdapterTypeUnknown  MorphoAdapterType = 99 // unrecognised adapter type, recorded for later curation
)

// MorphoAdapter is a liquidity adapter registered on a Morpho VaultV2 (registry
// row, one per adapter per vault).
//
// AddedAtBlock and RemovedAtBlock bound the adapter's active lifetime: an
// adapter is active while RemovedAtBlock is nil, and a removed-then-re-added
// adapter is a new row (the UNIQUE key includes AddedAtBlock). AddedAtBlock is
// the block at which we first observed the adapter on-chain — the AddAdapter
// block when witnessed live or replayed, or the vault-discovery / first-
// allocation block for an adapter that predates our discovery; it converges to
// the true AddAdapter block once history is replayed (see GetOrCreateAdapter).
// AssetTokenID is the vault's underlying asset, the unit of realAssets().
type MorphoAdapter struct {
	ID             int64
	MorphoVaultID  int64
	Address        []byte // 20 bytes, the adapter contract address
	AssetTokenID   int64
	AdapterType    MorphoAdapterType
	AddedAtBlock   int64
	RemovedAtBlock *int64 // nil while the adapter is active
}

// NewMorphoAdapter creates a new MorphoAdapter entity with validation.
func NewMorphoAdapter(morphoVaultID int64, address []byte, assetTokenID int64, adapterType MorphoAdapterType, addedAtBlock int64, removedAtBlock *int64) (*MorphoAdapter, error) {
	a := &MorphoAdapter{
		MorphoVaultID:  morphoVaultID,
		Address:        address,
		AssetTokenID:   assetTokenID,
		AdapterType:    adapterType,
		AddedAtBlock:   addedAtBlock,
		RemovedAtBlock: removedAtBlock,
	}
	if err := a.Validate(); err != nil {
		return nil, fmt.Errorf("NewMorphoAdapter: %w", err)
	}
	return a, nil
}

func (a *MorphoAdapter) Validate() error {
	if a.MorphoVaultID <= 0 {
		return fmt.Errorf("morphoVaultID must be positive, got %d", a.MorphoVaultID)
	}
	if len(a.Address) != 20 {
		return fmt.Errorf("address must be 20 bytes, got %d", len(a.Address))
	}
	if a.AssetTokenID <= 0 {
		return fmt.Errorf("assetTokenID must be positive, got %d", a.AssetTokenID)
	}
	if a.AdapterType != MorphoAdapterTypeMarketV1 && a.AdapterType != MorphoAdapterTypeVaultV1 && a.AdapterType != MorphoAdapterTypeUnknown {
		return fmt.Errorf("adapterType must be 1, 2, or 99, got %d", a.AdapterType)
	}
	if a.AddedAtBlock <= 0 {
		return fmt.Errorf("addedAtBlock must be positive, got %d", a.AddedAtBlock)
	}
	if a.RemovedAtBlock != nil && *a.RemovedAtBlock < a.AddedAtBlock {
		return fmt.Errorf("removedAtBlock must be >= addedAtBlock (%d), got %d", a.AddedAtBlock, *a.RemovedAtBlock)
	}
	return nil
}
