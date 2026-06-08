package entity

import (
	"math/big"
	"time"
)

// UniswapV3PoolStateSource enumerates the allowed values for
// uniswap_v3_pool_state.source. Mirrors the CHECK constraint in the
// migration.
const (
	UniswapV3PoolStateSourceEvent    = "event"
	UniswapV3PoolStateSourceSnapshot = "snapshot"
)

// UniswapV3PoolState is a per-block snapshot of a Uniswap V3 pool's price /
// liquidity / oracle cumulatives. Written by the event-triggered multicall
// on every pool event, plus a one-time bootstrap row at worker startup
// (Source = "snapshot").
//
// TickCumulative and SecsPerLiquidityCumulativeX128 are the oracle's running
// totals at the block, captured from a single `observe([0])` reading. Storing
// them on every state row makes TWAP queries a self-join differencing two
// rows at the chosen timestamps — no separate observations table is needed.
//
// Balance0 / Balance1 come from a balanceOf read against the pool's own ERC-20
// reserves and are stored as a sanity-check / fee-growth input; they are
// nullable because callers may skip them during high-throughput backfill.
type UniswapV3PoolState struct {
	UniswapV3PoolID                int64
	BlockNumber                    int64
	BlockVersion                   int32
	Timestamp                      time.Time
	Source                         string
	SqrtPriceX96                   *big.Int
	Tick                           int32
	Liquidity                      *big.Int
	ObservationIndex               *int32
	ObservationCardinality         *int32
	ObservationCardinalityNext     *int32
	FeeProtocol                    *int32
	Unlocked                       *bool
	TickCumulative                 *big.Int
	SecsPerLiquidityCumulativeX128 *big.Int
	Balance0                       *big.Int
	Balance1                       *big.Int
}
