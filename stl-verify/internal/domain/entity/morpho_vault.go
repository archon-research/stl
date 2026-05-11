package entity

import "fmt"

// MorphoVaultVersion represents the version of a Morpho-family vault.
//
// Versions tracked here include the original MetaMorpho V1, the MetaMorpho V1.1
// variant (adds skimRecipient()), and the new VaultV2 standard from
// morpho-org/vault-v2 (different ABI: no MORPHO(), exposes curator() and
// liquidityAdapter()).
type MorphoVaultVersion int16

const (
	MorphoVaultV1   MorphoVaultVersion = 1 // MetaMorpho V1 (no skimRecipient)
	MorphoVaultV1_1 MorphoVaultVersion = 2 // MetaMorpho V1.1 (adds skimRecipient)
	MorphoVaultV2   MorphoVaultVersion = 3 // Morpho VaultV2 (new standard, MORPHO() reverts)
)

// MorphoVault represents a Morpho-family vault (MetaMorpho V1/V1.1 or VaultV2).
type MorphoVault struct {
	ID             int64
	ChainID        int64
	ProtocolID     int64
	Address        []byte // 20 bytes
	Name           string
	Symbol         string
	AssetTokenID   int64
	VaultVersion   MorphoVaultVersion
	CreatedAtBlock int64
}

// NewMorphoVault creates a new MorphoVault entity with validation.
func NewMorphoVault(chainID, protocolID int64, address []byte, name, symbol string, assetTokenID int64, vaultVersion MorphoVaultVersion, createdAtBlock int64) (*MorphoVault, error) {
	v := &MorphoVault{
		ChainID:        chainID,
		ProtocolID:     protocolID,
		Address:        address,
		Name:           name,
		Symbol:         symbol,
		AssetTokenID:   assetTokenID,
		VaultVersion:   vaultVersion,
		CreatedAtBlock: createdAtBlock,
	}
	if err := v.Validate(); err != nil {
		return nil, fmt.Errorf("NewMorphoVault: %w", err)
	}
	return v, nil
}

func (v *MorphoVault) Validate() error {
	if v.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", v.ChainID)
	}
	if v.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", v.ProtocolID)
	}
	if len(v.Address) != 20 {
		return fmt.Errorf("address must be 20 bytes, got %d", len(v.Address))
	}
	if v.AssetTokenID <= 0 {
		return fmt.Errorf("assetTokenID must be positive, got %d", v.AssetTokenID)
	}
	if v.VaultVersion != MorphoVaultV1 && v.VaultVersion != MorphoVaultV1_1 && v.VaultVersion != MorphoVaultV2 {
		return fmt.Errorf("vaultVersion must be 1, 2, or 3, got %d", v.VaultVersion)
	}
	if v.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", v.CreatedAtBlock)
	}
	return nil
}
