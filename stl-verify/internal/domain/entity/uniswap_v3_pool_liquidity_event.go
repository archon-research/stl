package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// UniswapV3PoolLiquidityEventKind enumerates the values allowed in
// uniswap_v3_pool_liquidity_event.event_kind. Matches the CHECK constraint
// on the table.
//
// These are the *pool-side* Mint / Burn / Collect emissions, distinct from
// the NFPM-side IncreaseLiquidity / DecreaseLiquidity / Collect events that
// drive UniswapV3PositionState — the pool's own ticks change here, while
// the NFPM events change the per-position fee accounting.
const (
	UniswapV3PoolLiquidityEventMint    = "Mint"
	UniswapV3PoolLiquidityEventBurn    = "Burn"
	UniswapV3PoolLiquidityEventCollect = "Collect"
)

// UniswapV3PoolLiquidityEvent is a per-event fact row for the pool-side
// Mint / Burn / Collect emissions on a Uniswap V3 pool.
//
// Shape varies by EventKind (enforced by a CHECK constraint in the DB):
//   - Mint:    Sender + Amount + Amount0 + Amount1 populated; Recipient nil.
//   - Burn:    Amount + Amount0 + Amount1 populated; Sender + Recipient nil.
//   - Collect: Recipient + Amount0 + Amount1 populated; Sender + Amount nil.
type UniswapV3PoolLiquidityEvent struct {
	UniswapV3PoolID int64
	BlockNumber     int64
	BlockVersion    int32
	Timestamp       time.Time
	TxHash          common.Hash
	LogIndex        int32
	EventKind       string
	Owner           common.Address
	TickLower       int32
	TickUpper       int32
	Amount          *big.Int // Liquidity delta; nil for Collect.
	Amount0         *big.Int
	Amount1         *big.Int
	Sender          *common.Address // Populated for Mint only.
	Recipient       *common.Address // Populated for Collect only.
}
