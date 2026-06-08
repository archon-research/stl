package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// CurvePoolRepository defines the interface for Curve DEX persistence.
//
// Read paths (Get/List) take only a context — they use the pool's own
// connection. Write paths take a caller-supplied pgx.Tx so that typed
// projections, the raw `protocol_event` row, and the gauge/state writes for a
// single block all commit atomically.
//
// All Save* methods on hypertables rely on the table's BEFORE INSERT
// `assign_processing_version_*` trigger plus the natural PK for replay
// safety; the adapter must NOT add ON CONFLICT to those statements.
type CurvePoolRepository interface {
	// GetCurvePool retrieves a Curve pool registry row by (chain, address).
	// Returns nil, nil if the pool is not registered.
	GetCurvePool(ctx context.Context, chainID int64, address common.Address) (*entity.CurvePool, error)

	// ListEnabledCurvePools returns every enabled Curve pool on the given chain.
	// Used at worker startup to materialise the in-process pool registry.
	ListEnabledCurvePools(ctx context.Context, chainID int64) ([]*entity.CurvePool, error)

	// GetCurveGauge retrieves the gauge attached to the given pool, if any.
	// Returns nil, nil when no gauge exists.
	GetCurveGauge(ctx context.Context, curvePoolID int64) (*entity.CurveGauge, error)

	// UpsertCurveGauge inserts or updates a gauge registry row. Conflict key is
	// (chain_id, address); returns the gauge's database id.
	UpsertCurveGauge(ctx context.Context, tx pgx.Tx, gauge *entity.CurveGauge) (int64, error)

	// SetCurveGaugeKilled flips the is_killed flag for a gauge identified by
	// (chain_id, address). Used by the GaugeController KillGauge / Killed
	// event handlers.
	SetCurveGaugeKilled(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, isKilled bool) error

	// SaveCurvePoolState writes one row to curve_pool_state within the caller's
	// transaction. processing_version is assigned by the DB trigger.
	SaveCurvePoolState(ctx context.Context, tx pgx.Tx, state *entity.CurvePoolState) error

	// SaveCurvePoolSwap writes one row to curve_pool_swap within the caller's
	// transaction.
	SaveCurvePoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.CurvePoolSwap) error

	// SaveCurvePoolLiquidityEvent writes one row to curve_pool_liquidity_event
	// within the caller's transaction.
	SaveCurvePoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.CurvePoolLiquidityEvent) error

	// SaveCurvePoolParameterEvent writes one row to curve_pool_parameter_event
	// within the caller's transaction.
	SaveCurvePoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.CurvePoolParameterEvent) error

	// SaveCurvePoolExchangeRate writes one row to curve_pool_exchange_rate
	// within the caller's transaction. One call per (i, j) directional pair.
	SaveCurvePoolExchangeRate(ctx context.Context, tx pgx.Tx, rate *entity.CurvePoolExchangeRate) error

	// SaveCurveGaugeState writes one row to curve_gauge_state within the
	// caller's transaction.
	SaveCurveGaugeState(ctx context.Context, tx pgx.Tx, state *entity.CurveGaugeState) error

	// SaveCurveUserLpPosition writes one row to curve_user_lp_position within
	// the caller's transaction. A single ERC-20 Transfer typically produces
	// two calls (one for the sender side, one for the receiver side).
	SaveCurveUserLpPosition(ctx context.Context, tx pgx.Tx, pos *entity.CurveUserLpPosition) error
}
