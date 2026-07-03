package uniswapv3indexer

import (
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

// poolABIOnce parses poolEventsJSON exactly once: PoolABI is on the per-receipt
// hot path (DecodeEvents), so re-parsing the JSON on every matched receipt is
// pure waste. Callers still get the historical (*abi.ABI, error) signature.
var poolABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	return abis.ParseABI(poolEventsJSON)
})

// PoolABI returns the ABI fragment covering all 9 Uniswap V3 pool events.
// Signatures match v3-core's UniswapV3Pool.sol exactly, including which
// arguments are indexed.
func PoolABI() (*abi.ABI, error) {
	return poolABIOnce()
}

const poolEventsJSON = `[
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
			{"indexed": false, "name": "sender",     "type": "address"},
			{"indexed": true,  "name": "owner",      "type": "address"},
			{"indexed": true,  "name": "tickLower",  "type": "int24"},
			{"indexed": true,  "name": "tickUpper",  "type": "int24"},
			{"indexed": false, "name": "amount",     "type": "uint128"},
			{"indexed": false, "name": "amount0",    "type": "uint256"},
			{"indexed": false, "name": "amount1",    "type": "uint256"}
		],
		"name": "Mint",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "owner",      "type": "address"},
			{"indexed": false, "name": "recipient",  "type": "address"},
			{"indexed": true,  "name": "tickLower",  "type": "int24"},
			{"indexed": true,  "name": "tickUpper",  "type": "int24"},
			{"indexed": false, "name": "amount0",    "type": "uint128"},
			{"indexed": false, "name": "amount1",    "type": "uint128"}
		],
		"name": "Collect",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "owner",      "type": "address"},
			{"indexed": true,  "name": "tickLower",  "type": "int24"},
			{"indexed": true,  "name": "tickUpper",  "type": "int24"},
			{"indexed": false, "name": "amount",     "type": "uint128"},
			{"indexed": false, "name": "amount0",    "type": "uint256"},
			{"indexed": false, "name": "amount1",    "type": "uint256"}
		],
		"name": "Burn",
		"type": "event"
	},
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
			{"indexed": true,  "name": "sender",    "type": "address"},
			{"indexed": true,  "name": "recipient", "type": "address"},
			{"indexed": false, "name": "amount0",   "type": "uint256"},
			{"indexed": false, "name": "amount1",   "type": "uint256"},
			{"indexed": false, "name": "paid0",     "type": "uint256"},
			{"indexed": false, "name": "paid1",     "type": "uint256"}
		],
		"name": "Flash",
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
	}
]`
