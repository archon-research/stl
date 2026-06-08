package entity

import (
	"math/big"
	"time"
)

// CurvePoolStateSource enumerates the allowed values for curve_pool_state.source.
// Mirrors the CHECK constraint in the migration.
const (
	CurvePoolStateSourceEvent    = "event"
	CurvePoolStateSourceSnapshot = "snapshot"
)

// CurvePoolState is a per-block snapshot of a Curve pool's reserve and
// parameter state. Written by the event-triggered multicall on every pool
// event, plus a one-time bootstrap row at worker startup
// (Source = "snapshot"). PriceOracle / LastPrice are NG-only and may be nil
// on V1 pools.
type CurvePoolState struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int32
	Timestamp    time.Time
	Source       string
	Balances     []*big.Int // One entry per coin in coin-index order; raw token amounts.
	TotalSupply  *big.Int   // LP-token totalSupply.
	VirtualPrice *big.Int
	AFactor      *big.Int
	Fee          *big.Int   // Base fee in Curve's 1e10 scale.
	PriceOracle  []*big.Int // NG only.
	LastPrice    []*big.Int // NG only.
}
