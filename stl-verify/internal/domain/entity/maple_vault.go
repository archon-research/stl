package entity

import "fmt"

// maxTokenDecimals is the upper bound we accept for an ERC-20 / ERC-4626
// decimals() value. decimals() is a uint8 on-chain, but real tokens cluster
// at 6/8/18; anything past 36 is almost certainly a misconfigured seed or a
// decode error rather than a legitimate vault, so we reject it.
const maxTokenDecimals = 36

// MapleVault represents a Syrup ERC-4626 vault registered for indexing.
type MapleVault struct {
	ID             int64
	ChainID        int64
	ProtocolID     int64
	AssetTokenID   int64
	Address        []byte
	Name           string
	Symbol         string
	PoolAddress    []byte
	VaultVersion   int16
	CreatedAtBlock int64
	// Decimals is the vault's share decimals, which an ERC-4626 vault
	// inherits from its underlying asset at deploy time. Sourced from the
	// `token` row referenced by AssetTokenID. Drives the share-unit
	// (10^Decimals) used for share-price display.
	Decimals uint8
}

// NewMapleVault creates a new MapleVault entity with validation.
func NewMapleVault(
	chainID, protocolID, assetTokenID int64,
	address []byte,
	name, symbol string,
	poolAddress []byte,
	vaultVersion int16,
	createdAtBlock int64,
	decimals uint8,
) (*MapleVault, error) {
	v := &MapleVault{
		ChainID:        chainID,
		ProtocolID:     protocolID,
		AssetTokenID:   assetTokenID,
		Address:        address,
		Name:           name,
		Symbol:         symbol,
		PoolAddress:    poolAddress,
		VaultVersion:   vaultVersion,
		CreatedAtBlock: createdAtBlock,
		Decimals:       decimals,
	}
	if err := v.Validate(); err != nil {
		return nil, fmt.Errorf("NewMapleVault: %w", err)
	}
	return v, nil
}

// Validate ensures the entity satisfies all invariants.
func (v *MapleVault) Validate() error {
	if v.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", v.ChainID)
	}
	if v.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", v.ProtocolID)
	}
	if v.AssetTokenID <= 0 {
		return fmt.Errorf("assetTokenID must be positive, got %d", v.AssetTokenID)
	}
	if len(v.Address) != 20 {
		return fmt.Errorf("address must be 20 bytes, got %d", len(v.Address))
	}
	if len(v.PoolAddress) != 20 {
		return fmt.Errorf("poolAddress must be 20 bytes, got %d", len(v.PoolAddress))
	}
	if v.VaultVersion <= 0 {
		return fmt.Errorf("vaultVersion must be positive, got %d", v.VaultVersion)
	}
	if v.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", v.CreatedAtBlock)
	}
	if v.Decimals == 0 || v.Decimals > maxTokenDecimals {
		return fmt.Errorf("decimals must be in [1, %d], got %d", maxTokenDecimals, v.Decimals)
	}
	return nil
}
