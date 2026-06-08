package entity

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// BalancerPoolToken is the join-table row that pins a token slot inside a
// Balancer V2 pool. TokenIndex is the position returned by
// Vault.getPoolTokens(poolId) and is the index used by every parallel
// NUMERIC[] column on balancer_pool_state (balances / token_rates /
// scaling_factors).
//
// IsPhantom flags the BPT slot present in composable_stable pools, where the
// pool itself appears as one of its own tokens. RateProvider is the per-slot
// IRateProvider address (NULL when the slot's token is its own unit of
// account — e.g. WETH in a wstETH/WETH pool).
type BalancerPoolToken struct {
	BalancerPoolID int64
	TokenIndex     int16
	TokenID        int64
	IsPhantom      bool
	RateProvider   *common.Address
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
