package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetCurveLPTokenEventsABI returns the ABI for the ERC-20 Transfer event on a
// Curve LP token contract.
//
// Pre-factory pools (3pool, stETH-classic) use a dedicated LP-token contract
// (e.g. steCRV at 0x06325440...); factory-NG pools (e.g. stETH-ng) emit Transfer
// directly from the pool address. Either way the event shape is identical.
func GetCurveLPTokenEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "from",  "type": "address"},
				{"indexed": true,  "name": "to",    "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`)
}

// GetCurveLPTokenReadABI returns the ABI for the ERC-20 view methods read by
// the worker during snapshots (`totalSupply`) and per-user reconciliation
// (`balanceOf`).
func GetCurveLPTokenReadABI() (*abi.ABI, error) {
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
			"name": "balanceOf",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
