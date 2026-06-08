package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetBalancerV2ComposableStableEventsABI returns the ABI for the events
// emitted by the Balancer V2 ComposableStablePool contract itself (as
// opposed to the Vault — see balancer_v2_vault_abi.go for those).
//
// Composable stable pools embed amplification ramp scheduling, per-token
// rate-provider plumbing, swap-fee + pause governance, and an ERC-20 BPT
// implementation directly on the pool contract; each surface has its own
// event. The plan maps AmpUpdate* / TokenRate* / SwapFeePercentageChanged /
// PausedStateChanged onto balancer_pool_parameter_event rows and Transfer
// onto balancer_user_bpt_position rows.
func GetBalancerV2ComposableStableEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "startValue", "type": "uint256"},
				{"indexed": false, "name": "endValue",   "type": "uint256"},
				{"indexed": false, "name": "startTime",  "type": "uint256"},
				{"indexed": false, "name": "endTime",    "type": "uint256"}
			],
			"name": "AmpUpdateStarted",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "currentValue", "type": "uint256"}
			],
			"name": "AmpUpdateStopped",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "tokenIndex",    "type": "uint256"},
				{"indexed": true,  "name": "provider",      "type": "address"},
				{"indexed": false, "name": "cacheDuration", "type": "uint256"}
			],
			"name": "TokenRateProviderSet",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "tokenIndex", "type": "uint256"},
				{"indexed": false, "name": "rate",       "type": "uint256"}
			],
			"name": "TokenRateCacheUpdated",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "swapFeePercentage", "type": "uint256"}
			],
			"name": "SwapFeePercentageChanged",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "paused", "type": "bool"}
			],
			"name": "PausedStateChanged",
			"type": "event"
		},
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

// GetBalancerV2ComposableStableReadABI returns the ABI for the pool-contract
// view methods called by the event-triggered multicall: amp + BPT rate +
// supply + per-token rates + scaling factors + swap-fee + pause state. The
// worker writes the result into balancer_pool_state on every event.
func GetBalancerV2ComposableStableReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "getAmplificationParameter",
			"outputs": [
				{"name": "value",      "type": "uint256"},
				{"name": "isUpdating", "type": "bool"},
				{"name": "precision",  "type": "uint256"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "getRate",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "getActualSupply",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "token", "type": "address"}],
			"name": "getTokenRate",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "getScalingFactors",
			"outputs": [{"name": "", "type": "uint256[]"}],
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
			"name": "getSwapFeePercentage",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "getPausedState",
			"outputs": [
				{"name": "paused",               "type": "bool"},
				{"name": "pauseWindowEndTime",   "type": "uint256"},
				{"name": "bufferPeriodEndTime",  "type": "uint256"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
