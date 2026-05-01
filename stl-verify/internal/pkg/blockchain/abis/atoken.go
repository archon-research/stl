package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetATokenReadABI returns the ABI fragment for the extra AToken read methods
// used by allocation tracking in addition to the ERC20 ABI.
func GetATokenReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "totalSupply",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "account", "type": "address"}],
			"name": "scaledBalanceOf",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "scaledTotalSupply",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
