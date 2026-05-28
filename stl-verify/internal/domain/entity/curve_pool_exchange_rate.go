package entity

import (
	"math/big"
	"time"
)

// CurvePoolExchangeRate is a single (i, j) directional snapshot of
// `get_dy(i, j, 10^decimals[i])` produced by the event-triggered multicall.
// One row per directional pair per snapshot; an N-coin pool produces
// N*(N-1) rows per snapshot.
//
// Dy is nullable: get_dy can revert on degenerate pool states (paused
// pools, empty reserves, exotic Stableswap-NG variants). On revert the
// worker sets Dy=nil so the DB column lands as NULL, letting consumers
// distinguish revert from a genuine zero with IS NOT NULL. Dx stays
// non-nil because it's an input we control.
type CurvePoolExchangeRate struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int32
	Timestamp    time.Time
	I            int16
	J            int16
	Dx           *big.Int
	Dy           *big.Int // nil = get_dy reverted; persisted as NULL
}
