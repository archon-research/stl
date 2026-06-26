package fluid_vault_indexer

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// VaultEntireData is the subset of Fluid's VaultResolver.getVaultEntireData
// tuple that the indexer consumes. The resolver returns a much larger struct
// (configs, limits, branch state, per-user liquidity data); only the fields
// driving the per-vault aggregate snapshot are decoded.
//
// Verified against the deployed mainnet resolver
// 0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC. The resolver returns actual
// (not raw) token amounts for totalSupplyVault / totalBorrowVault.
type VaultEntireData struct {
	Vault       common.Address
	IsSmartCol  bool
	IsSmartDebt bool

	// CollateralToken is supplyToken.token0; DebtToken is borrowToken.token0.
	// For a smart vault the second leg lives in token1 and IsSmartCol/IsSmartDebt
	// is set — such vaults are out of scope (skipped) for this indexer.
	CollateralToken common.Address
	DebtToken       common.Address
	VaultType       *big.Int

	TotalSupplyVault *big.Int // aggregate collateral, actual amount (collateral decimals)
	TotalBorrowVault *big.Int // aggregate debt, actual amount (debt decimals)

	SupplyExchangePrice *big.Int // exchangePricesAndRates.vaultSupplyExchangePrice
	BorrowExchangePrice *big.Int // exchangePricesAndRates.vaultBorrowExchangePrice
	SupplyRate          *big.Int // exchangePricesAndRates.supplyRateVault (int256, may be negative)
	BorrowRate          *big.Int // exchangePricesAndRates.borrowRateVault (int256, may be negative)
}

// IsPlainSingle reports whether the vault is a plain single-collateral /
// single-debt vault (neither smart-collateral nor smart-debt). Smart / DEX
// vaults are out of scope and skipped by the indexer.
func (v *VaultEntireData) IsPlainSingle() bool {
	return !v.IsSmartCol && !v.IsSmartDebt
}
