package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

func GetPoolDataProviderUserReserveDataABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [
				{
					"name": "asset",
					"type": "address"
				},
				{
					"name": "user",
					"type": "address"
				}
			],
			"name": "getUserReserveData",
			"outputs": [
				{
					"name": "currentATokenBalance",
					"type": "uint256"
				},
				{
					"name": "currentStableDebt",
					"type": "uint256"
				},
				{
					"name": "currentVariableDebt",
					"type": "uint256"
				},
				{
					"name": "principalStableDebt",
					"type": "uint256"
				},
				{
					"name": "scaledVariableDebt",
					"type": "uint256"
				},
				{
					"name": "stableBorrowRate",
					"type": "uint256"
				},
				{
					"name": "liquidityRate",
					"type": "uint256"
				},
				{
					"name": "stableRateLastUpdated",
					"type": "uint40"
				},
				{
					"name": "usageAsCollateralEnabled",
					"type": "bool"
				}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}

func getPoolDataProviderReserveConfigurationABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [
				{
					"name": "asset",
					"type": "address"
				}
			],
			"name": "getReserveConfigurationData",
			"outputs": [
				{
					"name": "decimals",
					"type": "uint256"
				},
				{
					"name": "ltv",
					"type": "uint256"
				},
				{
					"name": "liquidationThreshold",
					"type": "uint256"
				},
				{
					"name": "liquidationBonus",
					"type": "uint256"
				},
				{
					"name": "reserveFactor",
					"type": "uint256"
				},
				{
					"name": "usageAsCollateralEnabled",
					"type": "bool"
				},
				{
					"name": "borrowingEnabled",
					"type": "bool"
				},
				{
					"name": "stableBorrowRateEnabled",
					"type": "bool"
				},
				{
					"name": "isActive",
					"type": "bool"
				},
				{
					"name": "isFrozen",
					"type": "bool"
				}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}

func getPoolDataProviderReserveData() (*abi.ABI, error) {
	return ParseABI(`[
	{
        "inputs": [
            {
                "internalType": "address",
                "name": "asset",
                "type": "address"
            }
        ],
        "name": "getReserveData",
        "outputs": [
            {
                "internalType": "uint256",
                "name": "unbacked",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "accruedToTreasuryScaled",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "totalAToken",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "totalStableDebt",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "totalVariableDebt",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "liquidityRate",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "variableBorrowRate",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "stableBorrowRate",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "averageStableBorrowRate",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "liquidityIndex",
                "type": "uint256"
            },
            {
                "internalType": "uint256",
                "name": "variableBorrowIndex",
                "type": "uint256"
            },
            {
                "internalType": "uint40",
                "name": "lastUpdateTimestamp",
                "type": "uint40"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }
		]`)
}