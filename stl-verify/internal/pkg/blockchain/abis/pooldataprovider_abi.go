package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

func GetPoolDataProviderUserReserveDataABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [
				{"name": "asset", "type": "address"},
				{"name": "user", "type": "address"}
			],
			"name": "getUserReserveData",
			"outputs": [
				{"name": "currentATokenBalance", "type": "uint256"},
				{"name": "currentStableDebt", "type": "uint256"},
				{"name": "currentVariableDebt", "type": "uint256"},
				{"name": "principalStableDebt", "type": "uint256"},
				{"name": "scaledVariableDebt", "type": "uint256"},
				{"name": "stableBorrowRate", "type": "uint256"},
				{"name": "liquidityRate", "type": "uint256"},
				{"name": "stableRateLastUpdated", "type": "uint40"},
				{"name": "usageAsCollateralEnabled", "type": "bool"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
