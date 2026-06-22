package abis

// VEC-260 §13: swap event signatures + stableswap view methods cast-verified on mainnet (this session).
// Liquidity/param event field shapes follow the standard Curve interface and MUST be confirmed against
// live logs per pool class before merge.

import "github.com/ethereum/go-ethereum/accounts/abi"

// CurveStableswapABI returns the ABI fragment covering both pre-NG and NG stableswap pools.
// The handler selects which view methods to call based on pool_kind at runtime.
func CurveStableswapABI() (*abi.ABI, error) {
	return ParseABI(curveStableswapJSON)
}

const curveStableswapJSON = `[
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
			{"indexed": true,  "name": "provider",     "type": "address"},
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
			{"indexed": true,  "name": "provider",     "type": "address"},
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
			{"indexed": false, "name": "token_amount",  "type": "uint256"},
			{"indexed": false, "name": "coin_amount",   "type": "uint256"},
			{"indexed": false, "name": "token_supply",  "type": "uint256"}
		],
		"name": "RemoveLiquidityOne",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "provider",     "type": "address"},
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
		"inputs": [{"name": "i", "type": "uint256"}],
		"name": "coins",
		"outputs": [{"name": "", "type": "address"}],
		"stateMutability": "view",
		"type": "function"
	},
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
		"name": "price_oracle",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "last_price",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "stored_rates",
		"outputs": [{"name": "", "type": "uint256[2]"}],
		"stateMutability": "view",
		"type": "function"
	}
]`
