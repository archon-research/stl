package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CurveLiquidityEventKind enumerates the values allowed in
// curve_pool_liquidity_event.event_kind. Matches the CHECK constraint on the
// table.
const (
	CurveLiquidityEventAddLiquidity             = "AddLiquidity"
	CurveLiquidityEventRemoveLiquidity          = "RemoveLiquidity"
	CurveLiquidityEventRemoveLiquidityOne       = "RemoveLiquidityOne"
	CurveLiquidityEventRemoveLiquidityImbalance = "RemoveLiquidityImbalance"
)

// CurvePoolLiquidityEvent is a per-event fact row for the
// AddLiquidity / RemoveLiquidity / RemoveLiquidityOne / RemoveLiquidityImbalance
// emissions on a Curve pool.
//
// Shape varies by EventKind (enforced by a CHECK constraint in the DB):
//   - AddLiquidity, RemoveLiquidity, RemoveLiquidityImbalance: TokenAmounts +
//     Fees + InvariantD + TokenSupply populated; TokenAmount / CoinIndex /
//     CoinAmount are nil.
//   - RemoveLiquidityOne: TokenAmount + CoinAmount required; CoinIndex
//     populated for NG (the event carries it) and nil for V1 unless recovered
//     from tx calldata. TokenAmounts/Fees are empty for this kind.
type CurvePoolLiquidityEvent struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int32
	Timestamp    time.Time
	TxHash       common.Hash
	LogIndex     int32
	EventKind    string
	Provider     common.Address
	TokenAmounts []*big.Int
	Fees         []*big.Int
	InvariantD   *big.Int
	TokenSupply  *big.Int
	TokenAmount  *big.Int
	CoinIndex    *int16
	CoinAmount   *big.Int
}
