package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetBalancerV2VaultEventsABI returns the ABI for the Balancer V2 Vault
// (0xBA12222222228d8Ba445958a75a0704d566BF2C8) events the worker subscribes
// to. Field names match the canonical Vault interface so log decoding yields
// the familiar argument keys.
//
// Balancer V2 routes all pool flows through a single Vault contract: Swap and
// liquidity events live here, not on the pool contract itself. The poolId is
// the indexed key that joins these events back to balancer_pool.pool_id.
//
// PoolRegistered is included for backfilling deployment_block during worker
// bootstrap (the plan's typed projection tables do not store it, but the ABI
// needs to recognise the topic so the decoder can drop / reuse it cleanly).
func GetBalancerV2VaultEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "poolId",    "type": "bytes32"},
				{"indexed": true,  "name": "tokenIn",   "type": "address"},
				{"indexed": true,  "name": "tokenOut",  "type": "address"},
				{"indexed": false, "name": "amountIn",  "type": "uint256"},
				{"indexed": false, "name": "amountOut", "type": "uint256"}
			],
			"name": "Swap",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "poolId",             "type": "bytes32"},
				{"indexed": true,  "name": "liquidityProvider",  "type": "address"},
				{"indexed": false, "name": "tokens",             "type": "address[]"},
				{"indexed": false, "name": "deltas",             "type": "int256[]"},
				{"indexed": false, "name": "protocolFeeAmounts", "type": "uint256[]"}
			],
			"name": "PoolBalanceChanged",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "poolId",       "type": "bytes32"},
				{"indexed": true,  "name": "assetManager", "type": "address"},
				{"indexed": true,  "name": "token",        "type": "address"},
				{"indexed": false, "name": "cashDelta",    "type": "int256"},
				{"indexed": false, "name": "managedDelta", "type": "int256"}
			],
			"name": "PoolBalanceManaged",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "poolId",         "type": "bytes32"},
				{"indexed": true,  "name": "poolAddress",    "type": "address"},
				{"indexed": false, "name": "specialization", "type": "uint8"}
			],
			"name": "PoolRegistered",
			"type": "event"
		}
	]`)
}

// GetBalancerV2VaultReadABI returns the ABI for the view methods called by
// the event-triggered multicall against the Vault. The worker hydrates a
// pool's current token balances + lastChangeBlock via a single getPoolTokens
// read; per-pool parameter reads live on the pool contract itself
// (balancer_v2_composable_stable_abi.go).
func GetBalancerV2VaultReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "poolId", "type": "bytes32"}],
			"name": "getPoolTokens",
			"outputs": [
				{"name": "tokens",          "type": "address[]"},
				{"name": "balances",        "type": "uint256[]"},
				{"name": "lastChangeBlock", "type": "uint256"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
