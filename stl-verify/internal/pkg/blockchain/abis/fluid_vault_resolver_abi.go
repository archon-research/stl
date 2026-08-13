package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetFluidVaultResolverABI returns the read surface of Fluid's VaultResolver
// periphery contract that the fluid-vault-indexer consumes: enumerate vaults
// (getAllVaultsAddresses) and read a single vault's full state
// (getVaultEntireData).
//
// The VaultEntireData tuple is reproduced verbatim from the deployed mainnet
// resolver 0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC (verified against the
// Instadapp/fluid-contracts-public deployments/mainnet/VaultResolver.json ABI).
// Field order and types are load-bearing: go-ethereum decodes the tuple
// positionally, so a reordered or retyped field silently corrupts every value
// after it. Do not edit without re-checking against the deployed ABI.
//
// Only the fields the indexer reads are named in the consuming decoder; the
// full tuple is kept here so the ABI round-trips against real return data.
func GetFluidVaultResolverABI() (*abi.ABI, error) {
	return ParseABI(`[
       {
          "inputs": [],
          "name": "getAllVaultsAddresses",
          "outputs": [{"name": "vaults_", "type": "address[]"}],
          "stateMutability": "view",
          "type": "function"
       },
       {
          "inputs": [{"name": "vault_", "type": "address"}],
          "name": "getVaultEntireData",
          "outputs": [{
             "name": "vaultData_",
             "type": "tuple",
             "components": [
                {"name": "vault", "type": "address"},
                {"name": "isSmartCol", "type": "bool"},
                {"name": "isSmartDebt", "type": "bool"},
                {"name": "constantVariables", "type": "tuple", "components": [
                   {"name": "liquidity", "type": "address"},
                   {"name": "factory", "type": "address"},
                   {"name": "operateImplementation", "type": "address"},
                   {"name": "adminImplementation", "type": "address"},
                   {"name": "secondaryImplementation", "type": "address"},
                   {"name": "deployer", "type": "address"},
                   {"name": "supply", "type": "address"},
                   {"name": "borrow", "type": "address"},
                   {"name": "supplyToken", "type": "tuple", "components": [
                      {"name": "token0", "type": "address"},
                      {"name": "token1", "type": "address"}
                   ]},
                   {"name": "borrowToken", "type": "tuple", "components": [
                      {"name": "token0", "type": "address"},
                      {"name": "token1", "type": "address"}
                   ]},
                   {"name": "vaultId", "type": "uint256"},
                   {"name": "vaultType", "type": "uint256"},
                   {"name": "supplyExchangePriceSlot", "type": "bytes32"},
                   {"name": "borrowExchangePriceSlot", "type": "bytes32"},
                   {"name": "userSupplySlot", "type": "bytes32"},
                   {"name": "userBorrowSlot", "type": "bytes32"}
                ]},
                {"name": "configs", "type": "tuple", "components": [
                   {"name": "supplyRateMagnifier", "type": "uint16"},
                   {"name": "borrowRateMagnifier", "type": "uint16"},
                   {"name": "collateralFactor", "type": "uint16"},
                   {"name": "liquidationThreshold", "type": "uint16"},
                   {"name": "liquidationMaxLimit", "type": "uint16"},
                   {"name": "withdrawalGap", "type": "uint16"},
                   {"name": "liquidationPenalty", "type": "uint16"},
                   {"name": "borrowFee", "type": "uint16"},
                   {"name": "oracle", "type": "address"},
                   {"name": "oraclePriceOperate", "type": "uint256"},
                   {"name": "oraclePriceLiquidate", "type": "uint256"},
                   {"name": "rebalancer", "type": "address"},
                   {"name": "lastUpdateTimestamp", "type": "uint256"}
                ]},
                {"name": "exchangePricesAndRates", "type": "tuple", "components": [
                   {"name": "lastStoredLiquiditySupplyExchangePrice", "type": "uint256"},
                   {"name": "lastStoredLiquidityBorrowExchangePrice", "type": "uint256"},
                   {"name": "lastStoredVaultSupplyExchangePrice", "type": "uint256"},
                   {"name": "lastStoredVaultBorrowExchangePrice", "type": "uint256"},
                   {"name": "liquiditySupplyExchangePrice", "type": "uint256"},
                   {"name": "liquidityBorrowExchangePrice", "type": "uint256"},
                   {"name": "vaultSupplyExchangePrice", "type": "uint256"},
                   {"name": "vaultBorrowExchangePrice", "type": "uint256"},
                   {"name": "supplyRateLiquidity", "type": "uint256"},
                   {"name": "borrowRateLiquidity", "type": "uint256"},
                   {"name": "supplyRateVault", "type": "int256"},
                   {"name": "borrowRateVault", "type": "int256"},
                   {"name": "rewardsOrFeeRateSupply", "type": "int256"},
                   {"name": "rewardsOrFeeRateBorrow", "type": "int256"}
                ]},
                {"name": "totalSupplyAndBorrow", "type": "tuple", "components": [
                   {"name": "totalSupplyVault", "type": "uint256"},
                   {"name": "totalBorrowVault", "type": "uint256"},
                   {"name": "totalSupplyLiquidityOrDex", "type": "uint256"},
                   {"name": "totalBorrowLiquidityOrDex", "type": "uint256"},
                   {"name": "absorbedSupply", "type": "uint256"},
                   {"name": "absorbedBorrow", "type": "uint256"}
                ]},
                {"name": "limitsAndAvailability", "type": "tuple", "components": [
                   {"name": "withdrawLimit", "type": "uint256"},
                   {"name": "withdrawableUntilLimit", "type": "uint256"},
                   {"name": "withdrawable", "type": "uint256"},
                   {"name": "borrowLimit", "type": "uint256"},
                   {"name": "borrowableUntilLimit", "type": "uint256"},
                   {"name": "borrowable", "type": "uint256"},
                   {"name": "borrowLimitUtilization", "type": "uint256"},
                   {"name": "minimumBorrowing", "type": "uint256"}
                ]},
                {"name": "vaultState", "type": "tuple", "components": [
                   {"name": "totalPositions", "type": "uint256"},
                   {"name": "topTick", "type": "int256"},
                   {"name": "currentBranch", "type": "uint256"},
                   {"name": "totalBranch", "type": "uint256"},
                   {"name": "totalBorrow", "type": "uint256"},
                   {"name": "totalSupply", "type": "uint256"},
                   {"name": "currentBranchState", "type": "tuple", "components": [
                      {"name": "status", "type": "uint256"},
                      {"name": "minimaTick", "type": "int256"},
                      {"name": "debtFactor", "type": "uint256"},
                      {"name": "partials", "type": "uint256"},
                      {"name": "debtLiquidity", "type": "uint256"},
                      {"name": "baseBranchId", "type": "uint256"},
                      {"name": "baseBranchMinima", "type": "int256"}
                   ]}
                ]},
                {"name": "liquidityUserSupplyData", "type": "tuple", "components": [
                   {"name": "modeWithInterest", "type": "bool"},
                   {"name": "supply", "type": "uint256"},
                   {"name": "withdrawalLimit", "type": "uint256"},
                   {"name": "lastUpdateTimestamp", "type": "uint256"},
                   {"name": "expandPercent", "type": "uint256"},
                   {"name": "expandDuration", "type": "uint256"},
                   {"name": "baseWithdrawalLimit", "type": "uint256"},
                   {"name": "withdrawableUntilLimit", "type": "uint256"},
                   {"name": "withdrawable", "type": "uint256"},
                   {"name": "decayEndTimestamp", "type": "uint256"},
                   {"name": "decayAmount", "type": "uint256"}
                ]},
                {"name": "liquidityUserBorrowData", "type": "tuple", "components": [
                   {"name": "modeWithInterest", "type": "bool"},
                   {"name": "borrow", "type": "uint256"},
                   {"name": "borrowLimit", "type": "uint256"},
                   {"name": "lastUpdateTimestamp", "type": "uint256"},
                   {"name": "expandPercent", "type": "uint256"},
                   {"name": "expandDuration", "type": "uint256"},
                   {"name": "baseBorrowLimit", "type": "uint256"},
                   {"name": "maxBorrowLimit", "type": "uint256"},
                   {"name": "borrowableUntilLimit", "type": "uint256"},
                   {"name": "borrowable", "type": "uint256"},
                   {"name": "borrowLimitUtilization", "type": "uint256"}
                ]}
             ]
          }],
          "stateMutability": "view",
          "type": "function"
       }
    ]`)
}
