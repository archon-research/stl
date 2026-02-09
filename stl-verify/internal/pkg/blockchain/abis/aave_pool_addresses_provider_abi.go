package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetPoolAddressesProviderABI returns the ABI for the Aave/SparkLend PoolAddressesProvider contract.
// Used to resolve the oracle address via getPriceOracle().
func GetPoolAddressesProviderABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "getPriceOracle",
			"outputs": [
				{"name": "", "type": "address"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
