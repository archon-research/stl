package abis

// VEC-260 §13: swap/liquidity/parameter event signatures and cryptoswap view methods cast-verified
// against the canonical Tricrypto-NG source and the TriCryptoUSDC pool on mainnet. NewParameters carries
// no admin_fee (that field exists only on the ADMIN_FEE constant); CommitNewParameters indexes deadline.
// admin_fee() is absent on Tricrypto-NG, so config admin_fee reads the ADMIN_FEE constant.

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
		"anonymous": false,
		"inputs": [
			{"indexed": false, "name": "initial_A",     "type": "uint256"},
			{"indexed": false, "name": "future_A",      "type": "uint256"},
			{"indexed": false, "name": "initial_gamma", "type": "uint256"},
			{"indexed": false, "name": "future_gamma",  "type": "uint256"},
			{"indexed": false, "name": "initial_time",  "type": "uint256"},
			{"indexed": false, "name": "future_time",   "type": "uint256"}
		],
		"name": "RampAgamma",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": false, "name": "mid_fee",              "type": "uint256"},
			{"indexed": false, "name": "out_fee",              "type": "uint256"},
			{"indexed": false, "name": "fee_gamma",            "type": "uint256"},
			{"indexed": false, "name": "allowed_extra_profit", "type": "uint256"},
			{"indexed": false, "name": "adjustment_step",      "type": "uint256"},
			{"indexed": false, "name": "ma_time",              "type": "uint256"}
		],
		"name": "NewParameters",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "deadline",             "type": "uint256"},
			{"indexed": false, "name": "mid_fee",              "type": "uint256"},
			{"indexed": false, "name": "out_fee",              "type": "uint256"},
			{"indexed": false, "name": "fee_gamma",            "type": "uint256"},
			{"indexed": false, "name": "allowed_extra_profit", "type": "uint256"},
			{"indexed": false, "name": "adjustment_step",      "type": "uint256"},
			{"indexed": false, "name": "ma_time",              "type": "uint256"}
		],
		"name": "CommitNewParameters",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "admin",  "type": "address"},
			{"indexed": false, "name": "tokens", "type": "uint256"}
		],
		"name": "ClaimAdminFee",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "sender",   "type": "address"},
			{"indexed": true,  "name": "receiver", "type": "address"},
			{"indexed": false, "name": "value",    "type": "uint256"}
		],
		"name": "Transfer",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "owner",   "type": "address"},
			{"indexed": true,  "name": "spender", "type": "address"},
			{"indexed": false, "name": "value",   "type": "uint256"}
		],
		"name": "Approval",
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
	},
	{
		"inputs": [{"name": "i", "type": "uint256"}],
		"name": "admin_balances",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "lp_price",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "xcp_profit_a",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "last_prices_timestamp",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [
			{"name": "i",  "type": "uint256"},
			{"name": "j",  "type": "uint256"},
			{"name": "dy", "type": "uint256"}
		],
		"name": "get_dx",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [
			{"name": "token_amount", "type": "uint256"},
			{"name": "i",            "type": "uint256"}
		],
		"name": "calc_withdraw_one_coin",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "initial_A_gamma",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "future_A_gamma",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "initial_A_gamma_time",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "future_A_gamma_time",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "mid_fee",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "out_fee",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "fee_gamma",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "allowed_extra_profit",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "adjustment_step",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "ma_time",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [],
		"name": "ADMIN_FEE",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`
