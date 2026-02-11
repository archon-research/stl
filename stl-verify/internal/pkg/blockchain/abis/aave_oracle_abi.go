package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetAaveOracleABI returns the ABI for the Aave V3 Oracle contract.
// Includes both batch getAssetsPrices(address[]) and individual getAssetPrice(address).
func GetAaveOracleABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [
				{"name": "assets", "type": "address[]"}
			],
			"name": "getAssetsPrices",
			"outputs": [
				{"name": "", "type": "uint256[]"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [
				{"name": "asset", "type": "address"}
			],
			"name": "getAssetPrice",
			"outputs": [
				{"name": "", "type": "uint256"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
