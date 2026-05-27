package entity

import (
	"math/big"
	"time"
)

// CurvePoolExchangeRate is a single (i, j) directional snapshot of
// `get_dy(i, j, 10^decimals[i])` produced by the event-triggered multicall.
// One row per directional pair per snapshot; an N-coin pool produces
// N*(N-1) rows per snapshot.
type CurvePoolExchangeRate struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int32
	Timestamp    time.Time
	I            int16
	J            int16
	Dx           *big.Int
	Dy           *big.Int
}
