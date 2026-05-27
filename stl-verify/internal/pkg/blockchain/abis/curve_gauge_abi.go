package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetCurveGaugeEventsABI returns the ABI for Curve liquidity gauge events.
//
// Covers the deposit / withdraw / boost-update event surface common to the
// canonical LiquidityGauge implementations. `UpdateLiquidityLimit` exposes
// pre/post `working_balance` and `working_supply` so consumers can derive
// boost factor without an extra eth_call.
func GetCurveGaugeEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "provider", "type": "address"},
				{"indexed": false, "name": "value",    "type": "uint256"}
			],
			"name": "Deposit",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "provider", "type": "address"},
				{"indexed": false, "name": "value",    "type": "uint256"}
			],
			"name": "Withdraw",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "user",             "type": "address"},
				{"indexed": false, "name": "original_balance", "type": "uint256"},
				{"indexed": false, "name": "original_supply",  "type": "uint256"},
				{"indexed": false, "name": "working_balance",  "type": "uint256"},
				{"indexed": false, "name": "working_supply",   "type": "uint256"}
			],
			"name": "UpdateLiquidityLimit",
			"type": "event"
		}
	]`)
}

// GetCurveGaugeReadABI returns the ABI for Curve gauge view methods read by
// the event-triggered multicall. `reward_data(address)` returns the canonical
// six-field tuple emitted by current LiquidityGauge implementations.
func GetCurveGaugeReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "inflation_rate",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "working_supply",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "totalSupply",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "is_killed",
			"outputs": [{"name": "", "type": "bool"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "reward_count",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "", "type": "uint256"}],
			"name": "reward_tokens",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "", "type": "address"}],
			"name": "reward_data",
			"outputs": [
				{"name": "token",         "type": "address"},
				{"name": "distributor",   "type": "address"},
				{"name": "period_finish", "type": "uint256"},
				{"name": "rate",          "type": "uint256"},
				{"name": "last_update",   "type": "uint256"},
				{"name": "integral",      "type": "uint256"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "lp_token",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
