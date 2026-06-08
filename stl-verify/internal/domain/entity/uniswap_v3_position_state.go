package entity

import (
	"math/big"
	"time"
)

// UniswapV3PositionStateSource enumerates the allowed values for
// uniswap_v3_position_state.source. Mirrors the CHECK constraint in the
// migration.
const (
	UniswapV3PositionStateSourceEvent    = "event"
	UniswapV3PositionStateSourceSnapshot = "snapshot"
)

// UniswapV3PositionState is a per-block snapshot of the per-position
// liquidity + unclaimed fee state of an NFPM-managed Uniswap V3 NFT.
// Written on NFPM IncreaseLiquidity / DecreaseLiquidity / Collect, and via
// a positions(tokenId) multicall on each so that all five fields below
// reflect the post-event NFPM accounting.
type UniswapV3PositionState struct {
	UniswapV3PositionID      int64
	BlockNumber              int64
	BlockVersion             int32
	Timestamp                time.Time
	Source                   string
	Liquidity                *big.Int
	TokensOwed0              *big.Int
	TokensOwed1              *big.Int
	FeeGrowthInside0LastX128 *big.Int
	FeeGrowthInside1LastX128 *big.Int
}
