package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetUniswapV3PoolEventsABI returns the ABI for the Uniswap V3 pool contract
// events the worker subscribes to. Sourced from the canonical
// UniswapV3Pool.sol shipped with v3-core, with field names matching the
// reference implementation so log decoding yields familiar argument keys.
//
// `Flash` is included for completeness so callers can recognise (and
// intentionally drop) it without falling back to a "missing topic" error path;
// the plan does not require a typed projection for flash loans.
func GetUniswapV3PoolEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "sender",       "type": "address"},
				{"indexed": true,  "name": "recipient",    "type": "address"},
				{"indexed": false, "name": "amount0",      "type": "int256"},
				{"indexed": false, "name": "amount1",      "type": "int256"},
				{"indexed": false, "name": "sqrtPriceX96", "type": "uint160"},
				{"indexed": false, "name": "liquidity",    "type": "uint128"},
				{"indexed": false, "name": "tick",         "type": "int24"}
			],
			"name": "Swap",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "sender",    "type": "address"},
				{"indexed": true,  "name": "owner",     "type": "address"},
				{"indexed": true,  "name": "tickLower", "type": "int24"},
				{"indexed": true,  "name": "tickUpper", "type": "int24"},
				{"indexed": false, "name": "amount",    "type": "uint128"},
				{"indexed": false, "name": "amount0",   "type": "uint256"},
				{"indexed": false, "name": "amount1",   "type": "uint256"}
			],
			"name": "Mint",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "owner",     "type": "address"},
				{"indexed": true,  "name": "tickLower", "type": "int24"},
				{"indexed": true,  "name": "tickUpper", "type": "int24"},
				{"indexed": false, "name": "amount",    "type": "uint128"},
				{"indexed": false, "name": "amount0",   "type": "uint256"},
				{"indexed": false, "name": "amount1",   "type": "uint256"}
			],
			"name": "Burn",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "owner",     "type": "address"},
				{"indexed": false, "name": "recipient", "type": "address"},
				{"indexed": true,  "name": "tickLower", "type": "int24"},
				{"indexed": true,  "name": "tickUpper", "type": "int24"},
				{"indexed": false, "name": "amount0",   "type": "uint128"},
				{"indexed": false, "name": "amount1",   "type": "uint128"}
			],
			"name": "Collect",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "sqrtPriceX96", "type": "uint160"},
				{"indexed": false, "name": "tick",         "type": "int24"}
			],
			"name": "Initialize",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "observationCardinalityNextOld", "type": "uint16"},
				{"indexed": false, "name": "observationCardinalityNextNew", "type": "uint16"}
			],
			"name": "IncreaseObservationCardinalityNext",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "feeProtocol0Old", "type": "uint8"},
				{"indexed": false, "name": "feeProtocol1Old", "type": "uint8"},
				{"indexed": false, "name": "feeProtocol0New", "type": "uint8"},
				{"indexed": false, "name": "feeProtocol1New", "type": "uint8"}
			],
			"name": "SetFeeProtocol",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "sender",    "type": "address"},
				{"indexed": true,  "name": "recipient", "type": "address"},
				{"indexed": false, "name": "amount0",   "type": "uint128"},
				{"indexed": false, "name": "amount1",   "type": "uint128"}
			],
			"name": "CollectProtocol",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "sender",    "type": "address"},
				{"indexed": true,  "name": "recipient", "type": "address"},
				{"indexed": false, "name": "amount0",   "type": "uint256"},
				{"indexed": false, "name": "amount1",   "type": "uint256"},
				{"indexed": false, "name": "paid0",     "type": "uint256"},
				{"indexed": false, "name": "paid1",     "type": "uint256"}
			],
			"name": "Flash",
			"type": "event"
		}
	]`)
}

// GetUniswapV3PoolReadABI returns the ABI for the view methods called by the
// event-triggered multicall: slot0 / liquidity / observe + the static
// token0 / token1 / fee / tickSpacing reads used at registry bootstrap and
// balanceOf for sanity-reading the pool's own token reserves.
func GetUniswapV3PoolReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "slot0",
			"outputs": [
				{"name": "sqrtPriceX96",               "type": "uint160"},
				{"name": "tick",                       "type": "int24"},
				{"name": "observationIndex",           "type": "uint16"},
				{"name": "observationCardinality",     "type": "uint16"},
				{"name": "observationCardinalityNext", "type": "uint16"},
				{"name": "feeProtocol",                "type": "uint8"},
				{"name": "unlocked",                   "type": "bool"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "liquidity",
			"outputs": [{"name": "", "type": "uint128"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "secondsAgos", "type": "uint32[]"}],
			"name": "observe",
			"outputs": [
				{"name": "tickCumulatives",                    "type": "int56[]"},
				{"name": "secondsPerLiquidityCumulativeX128s", "type": "uint160[]"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "observationCardinalityNext", "type": "uint16"}],
			"name": "increaseObservationCardinalityNext",
			"outputs": [],
			"stateMutability": "nonpayable",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "token0",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "token1",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "fee",
			"outputs": [{"name": "", "type": "uint24"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "tickSpacing",
			"outputs": [{"name": "", "type": "int24"}],
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
