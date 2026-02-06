package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetSparkLendOracleABI returns the ABI for the Aave/SparkLend Oracle contract.
// Used to fetch asset prices via getAssetsPrices(address[]).
func GetSparkLendOracleABI() (*abi.ABI, error) {
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
