package outbound

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// CurvePoolRow is the data returned by LoadPools for each pool in the registry.
type CurvePoolRow struct {
	ID           int64
	Address      common.Address
	Kind         string // matches curve_pool.pool_kind
	NCoins       int
	CoinDecimals []int // index-aligned (ordered by coin_index)
	// Precisions is index-aligned to CoinDecimals (same coin_index ordering):
	// curve_pool_coin.precision = 10^(18 - token.decimals). A nil entry means
	// the column was NULL for that coin.
	Precisions  []*big.Int
	DeployBlock int64
	// LpTokenAddress is the pool's LP/share token. For pre-NG pools this is a
	// separate contract (totalSupply lives there, not on the pool); nil when the
	// pool is its own LP token (NG pools).
	LpTokenAddress *common.Address
}

// SwapInput carries primitive values for a curve_swap insert.
type SwapInput struct {
	CurvePoolID    int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	LogIndex       int
	TxHash         common.Hash
	Buyer          common.Address
	SoldID         int
	BoughtID       int
	TokensSold     *big.Int
	TokensBought   *big.Int
	Fee            *big.Int // nullable
	IsUnderlying   bool     // true for TokenExchangeUnderlying swaps
}

// LiquidityInput carries primitive values for a curve_liquidity_event insert.
type LiquidityInput struct {
	CurvePoolID    int64
	BlockNumber    int64
	BlockVersion   int
	BlockTimestamp time.Time
	LogIndex       int
	TxHash         common.Hash
	Provider       common.Address
	Kind           string
	TokenAmounts   []*big.Int
	CoinIndex      *int       // nullable
	Fees           []*big.Int // nullable
	Invariant      *big.Int   // nullable
	TokenSupply    *big.Int   // nullable
}

// BlockWrites groups all of a block's curve-table rows for a single batched persist.
type BlockWrites struct {
	Swaps             []SwapInput
	Liquidity         []LiquidityInput
	StableStates      []*entity.CurveStableswapState
	CryptoStates      []*entity.CurveCryptoswapState
	StableswapConfigs []*entity.CurveStableswapConfig
	CryptoswapConfigs []*entity.CurveCryptoswapConfig
	ParameterEvents   []*entity.CurveParameterEvent
	LpTokenEvents     []*entity.CurveLpTokenEvent
}

// CurveRepository defines the interface for Curve DEX data persistence.
type CurveRepository interface {
	LoadPools(ctx context.Context, chainID int64) ([]CurvePoolRow, error)
	// SaveBlock persists all of a block's curve rows in one pgx.Batch within tx and
	// returns the number of state rows actually inserted (ON CONFLICT DO NOTHING means
	// a redelivery returns 0), for the curve_state_rows_written_total metric.
	SaveBlock(ctx context.Context, tx pgx.Tx, w BlockWrites) (stateRows int64, err error)
}
