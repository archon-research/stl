package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetAaveOracleABI returns the ABI for the Aave V3 Oracle contract.
// Used to fetch asset prices via getAssetsPrices(address[]).
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
		}
	]`)
}
