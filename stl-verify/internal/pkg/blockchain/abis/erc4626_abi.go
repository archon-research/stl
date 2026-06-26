package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetERC4626ABI returns the ABI for the EIP-4626 convertToAssets(uint256) view
// function, used to price vault shares in underlying-token units.
func GetERC4626ABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "shares", "type": "uint256"}],
			"name": "convertToAssets",
			"outputs": [{"name": "assets", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
