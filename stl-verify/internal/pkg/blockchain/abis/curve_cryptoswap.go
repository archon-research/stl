package abis

// VEC-260 §13: swap event signatures + cryptoswap view methods cast-verified on mainnet (this session).
// Liquidity/param event field shapes follow the standard Curve interface and MUST be confirmed against
// live logs per pool class before merge.

import "github.com/ethereum/go-ethereum/accounts/abi"

// CurveCryptoswapABI returns the ABI fragment for Curve cryptoswap (NG tricrypto) pools.
func CurveCryptoswapABI() (*abi.ABI, error) {
	return ParseABI(curveCryptoswapJSON)
}

const curveCryptoswapJSON = `[
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "buyer",             "type": "address"},
			{"indexed": false, "name": "sold_id",           "type": "uint256"},
			{"indexed": false, "name": "tokens_sold",       "type": "uint256"},
			{"indexed": false, "name": "bought_id",         "type": "uint256"},
			{"indexed": false, "name": "tokens_bought",     "type": "uint256"},
			{"indexed": false, "name": "fee",               "type": "uint256"},
			{"indexed": false, "name": "packed_price_scale","type": "uint256"}
		],
		"name": "TokenExchange",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "provider",      "type": "address"},
			{"indexed": false, "name": "token_amounts", "type": "uint256[]"},
			{"indexed": false, "name": "fee",           "type": "uint256"},
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
			{"indexed": false, "name": "token_supply",  "type": "uint256"}
		],
		"name": "RemoveLiquidity",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "provider",    "type": "address"},
			{"indexed": false, "name": "token_amount", "type": "uint256"},
			{"indexed": false, "name": "coin_index",   "type": "uint256"},
			{"indexed": false, "name": "coin_amount",  "type": "uint256"},
			{"indexed": false, "name": "approx_fee",   "type": "uint256"}
		],
		"name": "RemoveLiquidityOne",
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
		"name": "gamma",
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
			{"name": "i",  "type": "uint256"},
			{"name": "j",  "type": "uint256"},
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
		"inputs": [{"name": "i", "type": "uint256"}],
		"name": "price_scale",
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
		"inputs": [{"name": "i", "type": "uint256"}],
		"name": "last_prices",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "D",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "xcp_profit",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`
