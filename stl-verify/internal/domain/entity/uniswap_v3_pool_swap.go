package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// UniswapV3PoolSwap is a per-event fact row for a Uniswap V3 pool `Swap`
// emission. Amount0 and Amount1 are signed deltas from the pool's
// perspective (positive ⇒ token flowed in, negative ⇒ token flowed out);
// stored as *big.Int because int256 cannot fit in any native Go integer.
//
// SqrtPriceX96After / LiquidityAfter / TickAfter are the post-swap pool
// state values carried in the event payload (Uniswap V3 emits the new state
// alongside the trade so consumers don't need a follow-up slot0 read).
type UniswapV3PoolSwap struct {
	UniswapV3PoolID   int64
	BlockNumber       int64
	BlockVersion      int32
	Timestamp         time.Time
	TxHash            common.Hash
	LogIndex          int32
	Sender            common.Address
	Recipient         common.Address
	Amount0           *big.Int
	Amount1           *big.Int
	SqrtPriceX96After *big.Int
	LiquidityAfter    *big.Int
	TickAfter         int32
}
