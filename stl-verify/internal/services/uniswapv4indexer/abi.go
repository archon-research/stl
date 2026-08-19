// Package uniswapv4indexer decodes and snapshots Uniswap V4 pools. Unlike V3,
// every pool lives inside one singleton PoolManager and is addressed by its
// PoolId (keccak256 of the abi-encoded PoolKey), so logs are routed by
// topics[1] rather than by emitting address, and all state is read through the
// separate StateView periphery contract.
package uniswapv4indexer

import (
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

// poolManagerABIOnce parses poolManagerEventsJSON exactly once: the ABI is on
// the per-receipt hot path (DecodeEvents), so re-parsing the JSON on every
// receipt is pure waste.
var poolManagerABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	return abis.ParseABI(poolManagerEventsJSON)
})

// PoolManagerABI returns the ABI fragment covering all 10 events the v4-core
// PoolManager can emit, including the ERC6909 claim-token set it inherits.
// Signatures match PoolManager.sol exactly, including which arguments are
// indexed. Two tests split that guarantee: abi_test.go pins each topic0, which
// fixes the signature but says nothing about the indexed flags, and
// event_decode_test.go decodes verbatim mainnet logs, which pins the layout.
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
