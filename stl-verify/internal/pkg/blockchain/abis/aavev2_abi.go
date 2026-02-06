package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

func GetAaveV2PoolDataProviderReserveDataABI() (*abi.ABI, error) {
	return ParseABI(`[
       {
          "inputs": [
             {"name": "asset", "type": "address"}
          ],
          "name": "getReserveData",
          "outputs": [
             {"name": "availableLiquidity", "type": "uint256"},
             {"name": "totalStableDebt", "type": "uint256"},
             {"name": "totalVariableDebt", "type": "uint256"},
             {"name": "liquidityRate", "type": "uint256"},
             {"name": "variableBorrowRate", "type": "uint256"},
             {"name": "stableBorrowRate", "type": "uint256"},
             {"name": "averageStableBorrowRate", "type": "uint256"},
             {"name": "liquidityIndex", "type": "uint256"},
             {"name": "variableBorrowIndex", "type": "uint256"},
             {"name": "lastUpdateTimestamp", "type": "uint40"}
          ],
          "stateMutability": "view",
          "type": "function"
       }
    ]`)
}
