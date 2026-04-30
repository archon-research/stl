package entity

import (
	"encoding/json"
	"fmt"
	"time"
)

// UniswapPoolSnapshot represents a point-in-time state of a Uniswap V3 pool.
type UniswapPoolSnapshot struct {
	PoolAddress          []byte
	ChainID              int64
	BlockNumber          int64
	BlockVersion         int             // always 0 for finalized blocks
	Token0               json.RawMessage // JSONB
	Token1               json.RawMessage // JSONB
	Fee                  int
	SqrtPriceX96         string
	CurrentTick          int
	Price                string // token1/token0
	ActiveLiquidity      string // NUMERIC as string
	TvlUSD               *string
	TwapTick             *int
	TwapPrice            *string
	FeeGrowthGlobal0X128 string // cumulative token0 fees per liquidity (raw uint256)
	FeeGrowthGlobal1X128 string // cumulative token1 fees per liquidity (raw uint256)
	SnapshotTime         time.Time
}

func (s *UniswapPoolSnapshot) Validate() error {
	if len(s.PoolAddress) != 20 {
		return fmt.Errorf("pool address must be 20 bytes, got %d", len(s.PoolAddress))
	}
	if s.ChainID <= 0 {
		return fmt.Errorf("chain ID must be positive, got %d", s.ChainID)
	}
	if s.BlockNumber <= 0 {
		return fmt.Errorf("block number must be positive, got %d", s.BlockNumber)
	}
	if s.Fee <= 0 {
		return fmt.Errorf("fee must be positive, got %d", s.Fee)
	}
	if s.SnapshotTime.IsZero() {
		return fmt.Errorf("snapshot_time is required")
	}
	return nil
}
