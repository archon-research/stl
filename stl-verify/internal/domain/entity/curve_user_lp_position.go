package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CurveUserLpPosition is a per-event fact row tracking a user's LP-token
// balance over time. Sourced from ERC-20 `Transfer` events on the pool's LP
// token contract (or on the pool address itself for NG factory pools where
// the pool is its own LP token).
//
// One INSERT per Transfer per side: a single Transfer event produces two rows,
// one for the `from` user (Delta negative) and one for the `to` user (Delta
// positive). Mints and burns appear as Transfers from / to the zero address.
type CurveUserLpPosition struct {
	CurvePoolID  int64
	UserAddress  common.Address
	BlockNumber  int64
	BlockVersion int32
	Timestamp    time.Time
	TxHash       common.Hash
	LogIndex     int32
	LpBalance    *big.Int
	Delta        *big.Int
}
