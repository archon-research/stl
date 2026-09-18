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
	ID         int64
	ProtocolID int64
	Address    common.Address
	Kind       string // matches curve_pool.pool_kind
	NCoins     int
	// DeployBlock is the pool's on-chain deployment block (curve_pool.deploy_block,
	// 0 when not yet backfilled), used to gate snapshot sweeps so a newly-registered
	// pool isn't multicalled before it exists on chain.
	DeployBlock  int64
	CoinDecimals []int // index-aligned (ordered by coin_index)
	// LpTokenAddress is the pool's LP/share token. For pre-NG pools this is a
	// separate contract (totalSupply lives there, not on the pool); nil when the
	// pool is its own LP token (NG pools).
	LpTokenAddress *common.Address
	// HasAPrecise is curated pool metadata (curve_pool.has_a_precise): whether the
	// pool exposes the A_precise() getter. Gates the A_precise snapshot read.
	HasAPrecise bool
	// HasNoArgOracleGetters is curated pool metadata
	// (curve_pool.has_no_arg_oracle_getters): whether this stableswap-NG pool
	// exposes the no-arg oracle getters. Gates five NG snapshot reads.
	HasNoArgOracleGetters bool
	// CalcTokenAmountDynArray is curated pool metadata
	// (curve_pool.calc_token_amount_dyn_array): whether calc_token_amount takes a
	// dynamic uint256[] (true) or a fixed uint256[N] (false). nil when the pool has
	// not been probed, which gates the read out entirely.
	CalcTokenAmountDynArray *bool
	// HasFutureFee and HasOffpegFeeMultiplier are curated pool metadata
	// (curve_pool.has_future_fee, curve_pool.has_offpeg_fee_multiplier): which
	// fee-schedule getter the pool exposes. Each gates its own snapshot read.
	HasFutureFee           bool
	HasOffpegFeeMultiplier bool
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
	SaveBlock(ctx context.Context, tx pgx.Tx, w BlockWrites) (stateRows StateRowCounts, err error)
}
