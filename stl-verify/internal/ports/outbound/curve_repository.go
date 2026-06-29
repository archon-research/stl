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
	CoinTokenIDs []int64 // index-aligned (ordered by coin_index)
	CoinDecimals []int   // index-aligned (ordered by coin_index), matching CoinTokenIDs
	DeployBlock  int64
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

// CurveRepository defines the interface for Curve DEX data persistence.
type CurveRepository interface {
	// LoadPools returns all pools for a chain with their coin token IDs in coin_index order.
	LoadPools(ctx context.Context, chainID int64) ([]CurvePoolRow, error)

	// SaveSwap persists a Curve exchange event within an external transaction.
	SaveSwap(ctx context.Context, tx pgx.Tx, in SwapInput) error

	// SaveLiquidityEvent persists a Curve add/remove liquidity event within an external transaction.
	SaveLiquidityEvent(ctx context.Context, tx pgx.Tx, in LiquidityInput) error

	// SaveStableswapState persists a stableswap pool state snapshot within an
	// external transaction, returning the number of rows actually written (0 when
	// the ON CONFLICT DO NOTHING upsert is a no-op, e.g. an SQS redelivery).
	SaveStableswapState(ctx context.Context, tx pgx.Tx, s *entity.CurveStableswapState) (int64, error)

	// SaveCryptoswapState persists a cryptoswap pool state snapshot within an
	// external transaction, returning the number of rows actually written (0 when
	// the ON CONFLICT DO NOTHING upsert is a no-op, e.g. an SQS redelivery).
	SaveCryptoswapState(ctx context.Context, tx pgx.Tx, s *entity.CurveCryptoswapState) (int64, error)
}
