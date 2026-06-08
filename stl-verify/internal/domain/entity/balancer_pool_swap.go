package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// BalancerPoolSwap is a per-event fact row for a Vault `Swap(poolId, tokenIn,
// tokenOut, amountIn, amountOut)` emission. TokenInIdx / TokenOutIdx
// reference balancer_pool_token.token_index — the worker resolves the
// indexed tokenIn / tokenOut addresses against the join table before writing.
type BalancerPoolSwap struct {
	BalancerPoolID int64
	BlockNumber    int64
	BlockVersion   int32
	Timestamp      time.Time
	TxHash         common.Hash
	LogIndex       int32
	TokenInIdx     int16
	TokenOutIdx    int16
	AmountIn       *big.Int
	AmountOut      *big.Int
}
