package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetAaveOracleABI returns the ABI for the Aave V3 Oracle contract.
// Includes both batch getAssetsPrices(address[]) and individual getAssetPrice(address).
func GetAaveOracleABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"name": "getAssetsPrices",
			"inputs": [
				{"name": "assets", "type": "address[]"}
			],
			"outputs": [
				{"name": "", "type": "uint256[]"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"name": "getAssetPrice",
			"inputs": [
				{"name": "asset", "type": "address"}
			],
			"outputs": [
				{"name": "", "type": "uint256"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
