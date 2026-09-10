// Package uniswapv4indexer decodes and snapshots Uniswap V4 pools. One singleton
// PoolManager holds every pool, so logs route by the PoolId in topics[1] rather
// than by emitting address, and all state is read from the StateView periphery.
package uniswapv4indexer

import (
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

var poolManagerABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	return abis.ParseABI(poolManagerEventsJSON)
})

// PoolManagerABI covers every event the v4-core PoolManager emits, including the
// ERC6909 set it inherits; signatures and indexed flags match PoolManager.sol.
func PoolManagerABI() (*abi.ABI, error) {
	return poolManagerABIOnce()
}

const poolManagerEventsJSON = `[
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "id",           "type": "bytes32"},
			{"indexed": true,  "name": "currency0",    "type": "address"},
			{"indexed": true,  "name": "currency1",    "type": "address"},
			{"indexed": false, "name": "fee",          "type": "uint24"},
			{"indexed": false, "name": "tickSpacing",  "type": "int24"},
			{"indexed": false, "name": "hooks",        "type": "address"},
			{"indexed": false, "name": "sqrtPriceX96", "type": "uint160"},
			{"indexed": false, "name": "tick",         "type": "int24"}
		],
		"name": "Initialize",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "id",             "type": "bytes32"},
			{"indexed": true,  "name": "sender",         "type": "address"},
			{"indexed": false, "name": "tickLower",      "type": "int24"},
			{"indexed": false, "name": "tickUpper",      "type": "int24"},
			{"indexed": false, "name": "liquidityDelta", "type": "int256"},
			{"indexed": false, "name": "salt",           "type": "bytes32"}
		],
		"name": "ModifyLiquidity",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "id",           "type": "bytes32"},
			{"indexed": true,  "name": "sender",       "type": "address"},
			{"indexed": false, "name": "amount0",      "type": "int128"},
			{"indexed": false, "name": "amount1",      "type": "int128"},
			{"indexed": false, "name": "sqrtPriceX96", "type": "uint160"},
			{"indexed": false, "name": "liquidity",    "type": "uint128"},
			{"indexed": false, "name": "tick",         "type": "int24"},
			{"indexed": false, "name": "fee",          "type": "uint24"}
		],
		"name": "Swap",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "id",      "type": "bytes32"},
			{"indexed": true,  "name": "sender",  "type": "address"},
			{"indexed": false, "name": "amount0", "type": "uint256"},
			{"indexed": false, "name": "amount1", "type": "uint256"}
		],
		"name": "Donate",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "id",          "type": "bytes32"},
			{"indexed": false, "name": "protocolFee", "type": "uint24"}
		],
		"name": "ProtocolFeeUpdated",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true, "name": "protocolFeeController", "type": "address"}
		],
		"name": "ProtocolFeeControllerUpdated",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": false, "name": "caller", "type": "address"},
			{"indexed": true,  "name": "from",   "type": "address"},
			{"indexed": true,  "name": "to",     "type": "address"},
			{"indexed": true,  "name": "id",     "type": "uint256"},
			{"indexed": false, "name": "amount", "type": "uint256"}
		],
		"name": "Transfer",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "owner",   "type": "address"},
			{"indexed": true,  "name": "spender", "type": "address"},
			{"indexed": true,  "name": "id",      "type": "uint256"},
			{"indexed": false, "name": "amount",  "type": "uint256"}
		],
		"name": "Approval",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true,  "name": "owner",    "type": "address"},
			{"indexed": true,  "name": "operator", "type": "address"},
			{"indexed": false, "name": "approved", "type": "bool"}
		],
		"name": "OperatorSet",
		"type": "event"
	},
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true, "name": "user",     "type": "address"},
			{"indexed": true, "name": "newOwner", "type": "address"}
		],
		"name": "OwnershipTransferred",
		"type": "event"
	}
]`

var positionManagerABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	return abis.ParseABI(positionManagerEventsJSON)
})

// PositionManagerABI returns the ERC-721 Transfer fragment. Kept apart from
// PoolManagerABI: its inherited ERC-6909 Transfer is a different event, and one
// shared topic0 map would decode either against the other.
func PositionManagerABI() (*abi.ABI, error) {
	return positionManagerABIOnce()
}

const positionManagerEventsJSON = `[
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true, "name": "from",    "type": "address"},
			{"indexed": true, "name": "to",      "type": "address"},
			{"indexed": true, "name": "tokenId", "type": "uint256"}
		],
		"name": "Transfer",
		"type": "event"
	}
]`
