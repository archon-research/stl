package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// UniswapV3PoolRepository defines the interface for Uniswap V3 DEX persistence.
//
// Read paths (Get/List) take only a context — they use the pool's own
// connection. Write paths take a caller-supplied pgx.Tx so that typed
// projections, the raw `protocol_event` row, and the position/state writes
// for a single block all commit atomically.
//
// All Save* methods on hypertables rely on the table's BEFORE INSERT
// `assign_processing_version_*` trigger plus the natural PK for replay
// safety; the adapter must NOT add ON CONFLICT to those statements.
type UniswapV3PoolRepository interface {
	// GetUniswapV3Pool retrieves a Uniswap V3 pool registry row by
	// (chain, address). Returns nil, nil if the pool is not registered.
	GetUniswapV3Pool(ctx context.Context, chainID int64, address common.Address) (*entity.UniswapV3Pool, error)

	// ListEnabledUniswapV3Pools returns every enabled Uniswap V3 pool on the
	// given chain. Used at worker startup to materialise the in-process pool
	// registry.
	ListEnabledUniswapV3Pools(ctx context.Context, chainID int64) ([]*entity.UniswapV3Pool, error)

	// GetUniswapV3Position retrieves the NFPM-managed position identified by
	// (chain, NFPM address, NFT token id). Returns nil, nil if the position
	// has not been registered yet (i.e. the worker has not seen the mint).
	GetUniswapV3Position(ctx context.Context, chainID int64, nfpm common.Address, tokenID *big.Int) (*entity.UniswapV3Position, error)

	// UpsertUniswapV3Position inserts or updates a position registry row.
	// Conflict key is (chain_id, nfpm_address, token_id); ON CONFLICT only
	// refreshes the mutable owner/burned fields plus updated_at — the static
	// pool / tick / fee columns are NOT overwritten. Returns the position's
	// database id.
	UpsertUniswapV3Position(ctx context.Context, tx pgx.Tx, pos *entity.UniswapV3Position) (int64, error)

	// SetUniswapV3PositionOwner updates the owner of a registered position.
	// Driven by NFPM ERC-721 Transfer events between two non-zero addresses.
	SetUniswapV3PositionOwner(ctx context.Context, tx pgx.Tx, positionID int64, owner common.Address) error

	// SetUniswapV3PositionBurned flips the `burned` flag for a registered
	// position. The NFPM never deletes the NFT row when liquidity hits 0, so
	// we keep the registry row and just mark it as inactive for downstream
	// state-snapshot loops.
	SetUniswapV3PositionBurned(ctx context.Context, tx pgx.Tx, positionID int64) error

	// SaveUniswapV3PoolState writes one row to uniswap_v3_pool_state within
	// the caller's transaction. processing_version is assigned by the DB
	// trigger.
	SaveUniswapV3PoolState(ctx context.Context, tx pgx.Tx, state *entity.UniswapV3PoolState) error

	// SaveUniswapV3PoolSwap writes one row to uniswap_v3_pool_swap within
	// the caller's transaction.
	SaveUniswapV3PoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.UniswapV3PoolSwap) error

	// SaveUniswapV3PositionState writes one row to uniswap_v3_position_state
	// within the caller's transaction.
	SaveUniswapV3PositionState(ctx context.Context, tx pgx.Tx, state *entity.UniswapV3PositionState) error

	// SaveUniswapV3PoolLiquidityEvent writes one row to
	// uniswap_v3_pool_liquidity_event within the caller's transaction.
	SaveUniswapV3PoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.UniswapV3PoolLiquidityEvent) error

	// SaveUniswapV3PoolParameterEvent writes one row to
	// uniswap_v3_pool_parameter_event within the caller's transaction.
	SaveUniswapV3PoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.UniswapV3PoolParameterEvent) error
}
