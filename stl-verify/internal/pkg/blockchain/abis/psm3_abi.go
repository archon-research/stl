package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetPSM3ABI returns the minimal Spark PSM3 ABI used by the psm3-indexer.
func GetPSM3ABI() (*abi.ABI, error) {
	return ParseABI(`[
       {
          "inputs": [],
          "name": "pocket",
          "outputs": [{"name": "", "type": "address"}],
          "stateMutability": "view",
          "type": "function"
       },
       {
          "inputs": [],
          "name": "rateProvider",
          "outputs": [{"name": "", "type": "address"}],
          "stateMutability": "view",
          "type": "function"
       },
       {
          "inputs": [],
          "name": "totalAssets",
          "outputs": [{"name": "", "type": "uint256"}],
          "stateMutability": "view",
          "type": "function"
       },
       {
          "inputs": [],
          "name": "usds",
          "outputs": [{"name": "", "type": "address"}],
          "stateMutability": "view",
          "type": "function"
       },
       {
          "inputs": [],
          "name": "susds",
          "outputs": [{"name": "", "type": "address"}],
          "stateMutability": "view",
          "type": "function"
       },
       {
          "inputs": [],
          "name": "usdc",
          "outputs": [{"name": "", "type": "address"}],
          "stateMutability": "view",
          "type": "function"
       }
    ]`)
}

// GetRateProviderABI returns the minimal SSR rate provider ABI.
func GetRateProviderABI() (*abi.ABI, error) {
	return ParseABI(`[
       {
          "inputs": [],
          "name": "getConversionRate",
          "outputs": [{"name": "", "type": "uint256"}],
          "stateMutability": "view",
          "type": "function"
       }
    ]`)
}
