package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetUniswapV3NFPMEventsABI returns the ABI for the NonfungiblePositionManager
// (NFPM) contract events the worker subscribes to.
//
// `Transfer` is the standard ERC-721 transfer event: it fires on mint
// (from = 0x0), burn (to = 0x0), and ownership transfer. All three indices
// (from, to, tokenId) are indexed in the canonical ERC-721 ABI; some open-
// source decoders model tokenId as non-indexed which would break topic
// resolution against on-chain logs, so we keep all three indexed here.
func GetUniswapV3NFPMEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "tokenId",   "type": "uint256"},
				{"indexed": false, "name": "liquidity", "type": "uint128"},
				{"indexed": false, "name": "amount0",   "type": "uint256"},
				{"indexed": false, "name": "amount1",   "type": "uint256"}
			],
			"name": "IncreaseLiquidity",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "tokenId",   "type": "uint256"},
				{"indexed": false, "name": "liquidity", "type": "uint128"},
				{"indexed": false, "name": "amount0",   "type": "uint256"},
				{"indexed": false, "name": "amount1",   "type": "uint256"}
			],
			"name": "DecreaseLiquidity",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true,  "name": "tokenId",   "type": "uint256"},
				{"indexed": false, "name": "recipient", "type": "address"},
				{"indexed": false, "name": "amount0",   "type": "uint256"},
				{"indexed": false, "name": "amount1",   "type": "uint256"}
			],
			"name": "Collect",
			"type": "event"
		},
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
	]`)
}

// GetUniswapV3NFPMReadABI returns the ABI for the NFPM view methods called by
// the event-triggered multicall. Only `positions(tokenId)` is currently
// required; it returns the per-position state needed for
// uniswap_v3_position_state writes.
func GetUniswapV3NFPMReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "tokenId", "type": "uint256"}],
			"name": "positions",
			"outputs": [
				{"name": "nonce",                    "type": "uint96"},
				{"name": "operator",                 "type": "address"},
				{"name": "token0",                   "type": "address"},
				{"name": "token1",                   "type": "address"},
				{"name": "fee",                      "type": "uint24"},
				{"name": "tickLower",                "type": "int24"},
				{"name": "tickUpper",                "type": "int24"},
				{"name": "liquidity",                "type": "uint128"},
				{"name": "feeGrowthInside0LastX128", "type": "uint256"},
				{"name": "feeGrowthInside1LastX128", "type": "uint256"},
				{"name": "tokensOwed0",              "type": "uint128"},
				{"name": "tokensOwed1",              "type": "uint128"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
