package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetCurveNGPoolABI returns the ABI for the single Curve StableSwap-NG view
// function used in oracle pricing: get_virtual_price(), the 1e18-scaled
// fee-accruing invariant per LP token.
func GetCurveNGPoolABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "get_virtual_price",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
