package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CurveGaugeStateSource enumerates the allowed values for curve_gauge_state.source.
// Mirrors the CHECK constraint in the migration.
const (
	CurveGaugeStateSourceEvent    = "event"
	CurveGaugeStateSourceSnapshot = "snapshot"
)

// CurveGaugeState is a per-block snapshot of a Curve liquidity gauge's
// emissions and reward configuration. Written by the event-triggered
// multicall on any pool or gauge event for a gauged pool.
//
// The three Reward* slices are parallel and must have length equal to
// *RewardCount (or all be nil if RewardCount is nil) — enforced by a CHECK
// constraint on the table.
type CurveGaugeState struct {
	CurveGaugeID       int64
	BlockNumber        int64
	BlockVersion       int32
	Timestamp          time.Time
	Source             string
	InflationRate      *big.Int
	WorkingSupply      *big.Int
	TotalSupply        *big.Int
	IsKilled           *bool
	RewardCount        *int32
	RewardTokens       []common.Address
	RewardRates        []*big.Int
	RewardPeriodFinish []time.Time
}
