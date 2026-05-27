package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetCurveMetaRegistryReadABI returns the ABI for the Curve MetaRegistry
// (mainnet `0xF98B45FA17DE75FB1aD0e7aFD971b0ca00e379fC`).
//
// `get_gauge(pool)` resolves the gauge address for a known pool during the
// one-time worker bootstrap before event-driven gauge discovery takes over.
func GetCurveMetaRegistryReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "_pool", "type": "address"}],
			"name": "get_gauge",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
