package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CurvePoolSwap is a per-event fact row for a Curve `TokenExchange`
// (or `TokenExchangeUnderlying` on NG meta-pools) emission.
//
// IsUnderlying distinguishes the two: false → (sold_id, bought_id) index
// into pool.coins (wrapped); true → they index into pool.underlying_coins
// (the meta-pool's expanded set). Without this flag the (sold_id, bought_id)
// columns are ambiguous between the wrapped and underlying coin sets and
// volume aggregates double-count or mis-route.
type CurvePoolSwap struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int32
	Timestamp    time.Time
	TxHash       common.Hash
	LogIndex     int32
	Buyer        common.Address
	SoldID       int16
	TokensSold   *big.Int
	BoughtID     int16
	TokensBought *big.Int
	IsUnderlying bool
}
