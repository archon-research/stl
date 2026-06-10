package uniswap_tracker

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// PoolConfig defines a Uniswap V3 pool to track.
type PoolConfig struct {
	Address common.Address
	ChainID int64
	Name    string // Human-readable label (e.g. "AUSD/USDC 0.01%")
}

// PoolsForChainID filters the pool list by chain ID.
func PoolsForChainID(pools []PoolConfig, chainID int64) []PoolConfig {
	var result []PoolConfig
	for _, p := range pools {
		if p.ChainID == chainID {
			result = append(result, p)
		}
	}
	return result
}

// TokenBalance holds a single token's balance within a pool.
type TokenBalance struct {
	Address  common.Address `json:"address"`
	TokenID  int64          `json:"token_id"`
	Balance  string         `json:"balance"`
	Decimals int            `json:"decimals"`
	Symbol   string         `json:"symbol"`
	PriceUSD *big.Float     `json:"-"` // from onchain_token_price, not persisted
}

// PoolSnapshot holds a complete snapshot of a Uniswap V3 pool's state.
type PoolSnapshot struct {
	PoolAddress          common.Address
	ChainID              int64
	BlockNumber          int64
	Token0               TokenBalance
	Token1               TokenBalance
	Fee                  int    // fee tier in hundredths of a bip (e.g. 100 = 0.01%)
	SqrtPriceX96         string // raw Q64.96 value as string
	CurrentTick          int
	Price                string // token1/token0 price (human-readable)
	ActiveLiquidity      *big.Int
	TvlUSD               *big.Float
	TwapTick             *int    // TWAP tick (nil if observe fails)
	TwapPrice            *string // TWAP price (nil if observe fails)
	FeeGrowthGlobal0X128 string  // cumulative fees in token0 per liquidity unit
	FeeGrowthGlobal1X128 string  // cumulative fees in token1 per liquidity unit
}
