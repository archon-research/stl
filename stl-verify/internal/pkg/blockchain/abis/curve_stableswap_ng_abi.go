package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetCurveStableswapNGEventsABI returns the ABI for Stableswap-NG factory pool events
// (e.g. stETH-ng at 0x21E27a5E5513D6e65C4f830167390997aA84843a).
//
// Notable differences vs V1 (see curve_stableswap_v1_abi.go):
//   - `RemoveLiquidityOne` has five fields including `coin_index` (uint256, NOT
//     int128) and `token_supply`.
//   - There is a `TokenExchangeUnderlying` variant emitted by NG meta-pools.
//   - The fee-change event is `ApplyNewFee(fee, offpeg_fee_multiplier)` instead
//     of `NewFee(fee, admin_fee)`. The worker maps both onto the same DB
//     `event_kind = 'NewFee'`, but the on-chain decoding is split per ABI.
func GetCurveStableswapNGEventsABI() (*abi.ABI, error) {
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
				{"indexed": true,  "name": "buyer",         "type": "address"},
				{"indexed": false, "name": "sold_id",       "type": "int128"},
				{"indexed": false, "name": "tokens_sold",   "type": "uint256"},
				{"indexed": false, "name": "bought_id",     "type": "int128"},
				{"indexed": false, "name": "tokens_bought", "type": "uint256"}
			],
			"name": "TokenExchangeUnderlying",
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
				{"indexed": false, "name": "coin_amount",  "type": "uint256"},
				{"indexed": false, "name": "coin_index",   "type": "uint256"},
				{"indexed": false, "name": "token_supply", "type": "uint256"}
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
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "fee",                   "type": "uint256"},
				{"indexed": false, "name": "offpeg_fee_multiplier", "type": "uint256"}
			],
			"name": "ApplyNewFee",
			"type": "event"
		}
	]`)
}

// GetCurveStableswapNGReadABI returns the ABI for Stableswap-NG view methods used by
// the event-triggered multicall. Adds `last_price` and `price_oracle` (NG-only
// built-in EMA oracle) on top of the V1 read surface.
func GetCurveStableswapNGReadABI() (*abi.ABI, error) {
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
			"inputs": [{"name": "i", "type": "uint256"}],
			"name": "last_price",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "i", "type": "uint256"}],
			"name": "price_oracle",
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
