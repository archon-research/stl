package entity

import "fmt"

// FluidVault is a discovered Fluid lending vault (registry row, one per vault).
//
// Granularity is per-vault aggregate: a single FluidVaultState row per vault
// per block captures the vault's total collateral and total debt, which is all
// the fsUSDS backed-breakdown needs (VEC-436). CollateralTokenID and
// DebtTokenID reference the token registry by its surrogate id, resolved by
// the indexer from each token's natural key (chain_id, address) before
// construction. Both are mainnet ERC-20s for the tracked vaults, so neither is
// ever a raw symbol.
type FluidVault struct {
	ID                int64
	ChainID           int64
	ProtocolID        int64
	Address           []byte // 20 bytes, the vault contract address
	VaultType         string // Fluid vault classification, e.g. "T1" (smart-collateral/-debt typing)
	CollateralTokenID int64
	DebtTokenID       int64
	CreatedAtBlock    int64
}

func NewFluidVault(chainID, protocolID int64, address []byte, vaultType string, collateralTokenID, debtTokenID, createdAtBlock int64) (*FluidVault, error) {
	v := &FluidVault{
		ChainID:           chainID,
		ProtocolID:        protocolID,
		Address:           address,
		VaultType:         vaultType,
		CollateralTokenID: collateralTokenID,
		DebtTokenID:       debtTokenID,
		CreatedAtBlock:    createdAtBlock,
	}
	if err := v.Validate(); err != nil {
		return nil, fmt.Errorf("NewFluidVault: %w", err)
	}
	return v, nil
}

func (v *FluidVault) Validate() error {
	if v.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", v.ChainID)
	}
	if v.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", v.ProtocolID)
	}
	if len(v.Address) != 20 {
		return fmt.Errorf("address must be 20 bytes, got %d", len(v.Address))
	}
	if v.VaultType == "" {
		return fmt.Errorf("vaultType must not be empty")
	}
	if v.CollateralTokenID <= 0 {
		return fmt.Errorf("collateralTokenID must be positive, got %d", v.CollateralTokenID)
	}
	if v.DebtTokenID <= 0 {
		return fmt.Errorf("debtTokenID must be positive, got %d", v.DebtTokenID)
	}
	if v.CreatedAtBlock <= 0 {
		return fmt.Errorf("createdAtBlock must be positive, got %d", v.CreatedAtBlock)
	}
	return nil
}
