package entity

import "fmt"

// MorphoVaultVersion represents the version of a MetaMorpho vault.
type MorphoVaultVersion int16

const (
	MorphoVaultV1 MorphoVaultVersion = 1 // MetaMorpho V1.1
	MorphoVaultV2 MorphoVaultVersion = 2 // MetaMorpho V2
)

// MorphoVault represents a MetaMorpho vault.
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
	if err := v.validate(); err != nil {
		return nil, err
	}
	return v, nil
}

func (v *MorphoVault) validate() error {
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
	if v.VaultVersion != MorphoVaultV1 && v.VaultVersion != MorphoVaultV2 {
		return fmt.Errorf("vaultVersion must be 1 or 2, got %d", v.VaultVersion)
	}
	if v.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", v.CreatedAtBlock)
	}
	return nil
}
