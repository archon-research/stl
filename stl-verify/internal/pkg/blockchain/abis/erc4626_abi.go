package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetERC4626ABI returns the ABI for EIP-4626 view functions used in oracle pricing:
// convertToAssets(uint256) for share-to-underlying conversion, and asset() to resolve
// the underlying token address for on-chain decimals verification.
func GetERC4626ABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "shares", "type": "uint256"}],
			"name": "convertToAssets",
			"outputs": [{"name": "assets", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "asset",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
