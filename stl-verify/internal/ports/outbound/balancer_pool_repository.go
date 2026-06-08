package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// BalancerPoolRepository defines the interface for Balancer V2 DEX persistence.
//
// Read paths (Get/List) take only a context — they use the pool's own
// connection. Write paths take a caller-supplied pgx.Tx so that typed
// projections, the raw `protocol_event` row, and the per-token / state
// writes for a single block all commit atomically.
//
// All Save* methods on hypertables rely on the table's BEFORE INSERT
// `assign_processing_version_*` trigger plus the natural PK for replay
// safety; the adapter must NOT add ON CONFLICT to those statements.
type BalancerPoolRepository interface {
	// GetBalancerPool retrieves a Balancer V2 pool registry row by
	// (chain, address). Returns nil, nil if the pool is not registered.
	GetBalancerPool(ctx context.Context, chainID int64, address common.Address) (*entity.BalancerPool, error)

	// ListEnabledBalancerPools returns every enabled Balancer V2 pool on the
	// given chain. Used at worker startup to materialise the in-process pool
	// registry.
	ListEnabledBalancerPools(ctx context.Context, chainID int64) ([]*entity.BalancerPool, error)

	// ListBalancerPoolTokens returns the join-table rows for a pool in
	// token_index order, plus the parallel slice of token contract addresses
	// (resolved from the token table). Used at worker startup to materialise
	// the per-pool token-slot lookup AND the address→index map needed to
	// project Vault Swap events onto typed columns; without the addresses the
	// address→index map is empty after a restart and every Swap on a known
	// pool fails to resolve its tokenIn/tokenOut until the next getPoolTokens.
	ListBalancerPoolTokens(ctx context.Context, balancerPoolID int64) ([]*entity.BalancerPoolToken, []common.Address, error)

	// UpsertBalancerPoolToken inserts or updates a balancer_pool_token row.
	// Conflict key is (balancer_pool_id, token_index); ON CONFLICT refreshes
	// the mutable token_id / is_phantom / rate_provider columns plus
	// updated_at. Called when the worker first populates the join table
	// after a Vault.getPoolTokens read.
	UpsertBalancerPoolToken(ctx context.Context, tx pgx.Tx, t *entity.BalancerPoolToken) error

	// SaveBalancerPoolState writes one row to balancer_pool_state within the
	// caller's transaction. processing_version is assigned by the DB
	// trigger.
	SaveBalancerPoolState(ctx context.Context, tx pgx.Tx, state *entity.BalancerPoolState) error

	// SaveBalancerPoolSwap writes one row to balancer_pool_swap within the
	// caller's transaction.
	SaveBalancerPoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.BalancerPoolSwap) error

	// SaveBalancerPoolLiquidityEvent writes one row to
	// balancer_pool_liquidity_event within the caller's transaction.
	SaveBalancerPoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.BalancerPoolLiquidityEvent) error

	// SaveBalancerPoolParameterEvent writes one row to
	// balancer_pool_parameter_event within the caller's transaction.
	SaveBalancerPoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.BalancerPoolParameterEvent) error

	// SaveBalancerUserBptPosition writes one row to balancer_user_bpt_position
	// within the caller's transaction. A single ERC-20 Transfer typically
	// produces two calls (one for the sender side, one for the receiver
	// side).
	SaveBalancerUserBptPosition(ctx context.Context, tx pgx.Tx, pos *entity.BalancerUserBptPosition) error
}
