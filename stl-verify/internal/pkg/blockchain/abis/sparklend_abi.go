package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

func GetSparklendUserReservesDataABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [
				{"name": "provider", "type": "address"},
				{"name": "user", "type": "address"}
			],
			"name": "getUserReservesData",
			"outputs": [
				{
					"components": [
						{"name": "underlyingAsset", "type": "address"},
						{"name": "scaledATokenBalance", "type": "uint256"},
						{"name": "usageAsCollateralEnabledOnUser", "type": "bool"},
						{"name": "scaledVariableDebt", "type": "uint256"}
					],
					"name": "",
					"type": "tuple[]"
				},
				{"name": "", "type": "uint8"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}

func GetSparklendReserveDataABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [
				{"name": "asset", "type": "address"}
			],
			"name": "getReserveData",
			"outputs": [
				{
					"components": [
						{"name": "configuration", "type": "uint256"},
						{"name": "liquidityIndex", "type": "uint128"},
						{"name": "currentLiquidityRate", "type": "uint128"},
						{"name": "variableBorrowIndex", "type": "uint128"},
						{"name": "currentVariableBorrowRate", "type": "uint128"},
						{"name": "currentStableBorrowRate", "type": "uint128"},
						{"name": "lastUpdateTimestamp", "type": "uint40"},
						{"name": "id", "type": "uint16"},
						{"name": "aTokenAddress", "type": "address"},
						{"name": "stableDebtTokenAddress", "type": "address"},
						{"name": "variableDebtTokenAddress", "type": "address"},
						{"name": "interestRateStrategyAddress", "type": "address"},
						{"name": "accruedToTreasury", "type": "uint128"},
						{"name": "unbacked", "type": "uint128"},
						{"name": "isolationModeTotalDebt", "type": "uint128"}
					],
					"name": "",
					"type": "tuple"
				}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
