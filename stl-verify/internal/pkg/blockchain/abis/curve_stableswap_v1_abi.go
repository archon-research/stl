package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetCurveStableswapV1EventsABI returns the ABI for pre-NG Curve Stableswap V1 pool events.
//
// Covers both stETH-classic (2 coins) and 3pool (3 coins). To make a single ABI
// work across pool sizes, `token_amounts` and `fees` are encoded as dynamic
// `uint256[]` arrays instead of pool-specific fixed-size `uint256[N_COINS]`.
// The go-ethereum decoder accepts both encodings against the dynamic shape.
//
// Note: V1 `RemoveLiquidityOne` has only three fields (provider, token_amount,
// coin_amount) — no `coin_index` and no `token_supply`. The NG variant of the
// same event is defined separately in curve_stableswap_ng_abi.go.
//
// V1 emits `NewFee(fee, admin_fee)` for fee changes. NG emits `ApplyNewFee`
// with a different shape; the worker should map both into the same DB event
// kind, but decoder ABIs are distinct.
func GetCurveStableswapV1EventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "buyer",         "type": "address"},
				{"indexed": false, "name": "sold_id",       "type": "int128"},
				{"indexed": false, "name": "tokens_sold",   "type": "uint256"},
				{"indexed": false, "name": "bought_id",     "type": "int128"},
				{"indexed": false, "name": "tokens_bought", "type": "uint256"}
			],
			"name": "TokenExchange",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "provider",      "type": "address"},
				{"indexed": false, "name": "token_amounts", "type": "uint256[]"},
				{"indexed": false, "name": "fees",          "type": "uint256[]"},
				{"indexed": false, "name": "invariant",     "type": "uint256"},
				{"indexed": false, "name": "token_supply",  "type": "uint256"}
			],
			"name": "AddLiquidity",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "provider",      "type": "address"},
				{"indexed": false, "name": "token_amounts", "type": "uint256[]"},
				{"indexed": false, "name": "fees",          "type": "uint256[]"},
				{"indexed": false, "name": "token_supply",  "type": "uint256"}
			],
			"name": "RemoveLiquidity",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "provider",     "type": "address"},
				{"indexed": false, "name": "token_amount", "type": "uint256"},
				{"indexed": false, "name": "coin_amount",  "type": "uint256"}
			],
			"name": "RemoveLiquidityOne",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "provider",      "type": "address"},
				{"indexed": false, "name": "token_amounts", "type": "uint256[]"},
				{"indexed": false, "name": "fees",          "type": "uint256[]"},
				{"indexed": false, "name": "invariant",     "type": "uint256"},
				{"indexed": false, "name": "token_supply",  "type": "uint256"}
			],
			"name": "RemoveLiquidityImbalance",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "fee",       "type": "uint256"},
				{"indexed": false, "name": "admin_fee", "type": "uint256"}
			],
			"name": "NewFee",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "old_A",        "type": "uint256"},
				{"indexed": false, "name": "new_A",        "type": "uint256"},
				{"indexed": false, "name": "initial_time", "type": "uint256"},
				{"indexed": false, "name": "future_time",  "type": "uint256"}
			],
			"name": "RampA",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "A", "type": "uint256"},
				{"indexed": false, "name": "t", "type": "uint256"}
			],
			"name": "StopRampA",
			"type": "event"
		}
	]`)
}

// GetCurveStableswapV1ReadABI returns the ABI for pre-NG Curve Stableswap V1 view methods
// used by the event-triggered multicall.
func GetCurveStableswapV1ReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "i", "type": "uint256"}],
			"name": "balances",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "get_virtual_price",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "A",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "fee",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [
				{"name": "i",  "type": "int128"},
				{"name": "j",  "type": "int128"},
				{"name": "dx", "type": "uint256"}
			],
			"name": "get_dy",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
