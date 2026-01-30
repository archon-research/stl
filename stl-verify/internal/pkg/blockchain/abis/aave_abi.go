package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

func GetAaveUserReservesDataABI() (*abi.ABI, error) {
	return ParseABI(`[{
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
					{"name": "scaledVariableDebt", "type": "uint256"},
					{"name": "stableBorrowRate", "type": "uint256"},
					{"name": "principalStableDebt", "type": "uint256"},
					{"name": "stableBorrowLastUpdateTimestamp", "type": "uint256"}
				],
				"type": "tuple[]"
			},
			{"name": "", "type": "uint8"}
		],
		"stateMutability": "view",
		"type": "function"
	}]`)
}
