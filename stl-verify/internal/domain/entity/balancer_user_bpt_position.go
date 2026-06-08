package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// BalancerUserBptPosition is a per-event fact row tracking a user's BPT
// (Balancer Pool Token) balance over time. Sourced from ERC-20 `Transfer`
// events on the pool contract itself (composable_stable, where the pool is
// its own BPT) or on the dedicated LP token contract for legacy stable pools.
//
// One INSERT per Transfer per side: a single Transfer event produces two
// rows, one for the `from` user (Delta negative) and one for the `to` user
// (Delta positive). Mints and burns appear as Transfers from / to the zero
// address.
type BalancerUserBptPosition struct {
	BalancerPoolID int64
	UserAddress    common.Address
	BlockNumber    int64
	BlockVersion   int32
	Timestamp      time.Time
	TxHash         common.Hash
	LogIndex       int32
	BptBalance     *big.Int
	Delta          *big.Int
}
