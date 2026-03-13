// Package uniswapv3 provides a reusable manager for reading Uniswap V3
// NFT-based positions and computing underlying token amounts.
package uniswapv3

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// PositionManagers maps chain names to well-known Uniswap V3
// NonfungiblePositionManager contract addresses.
var PositionManagers = map[string]common.Address{
	"mainnet":     common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
	"arbitrum":    common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
	"optimism":    common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"),
	"base":        common.HexToAddress("0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1"),
	"avalanche-c": common.HexToAddress("0x655C406EBFa14EE2006250925e54ec43AD184f8B"),
	"monad":       common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88"), // placeholder — update when deployed
}

// Position holds the on-chain data for a single Uniswap V3 NFT position.
type Position struct {
	TokenID   *big.Int
	Token0    common.Address
	Token1    common.Address
	Fee       *big.Int
	TickLower int
	TickUpper int
	Liquidity *big.Int
}

// PoolState holds the current state of a Uniswap V3 pool.
type PoolState struct {
	SqrtPriceX96 *big.Int
	Tick         int
	Token0       common.Address
	Token1       common.Address
}

// PositionAmounts holds the computed underlying token amounts for a position.
type PositionAmounts struct {
	Amount0 *big.Int
	Amount1 *big.Int
}
