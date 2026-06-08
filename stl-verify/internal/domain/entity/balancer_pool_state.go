package entity

import (
	"math/big"
	"time"
)

// BalancerPoolStateSource enumerates the allowed values for
// balancer_pool_state.source. Mirrors the CHECK constraint in the migration.
const (
	BalancerPoolStateSourceEvent    = "event"
	BalancerPoolStateSourceSnapshot = "snapshot"
)

// BalancerPoolState is a per-block snapshot of a Balancer V2 pool's reserve
// and parameter state. Written by the event-triggered multicall on every
// Vault or pool-contract event, plus a one-time bootstrap row at worker
// startup (Source = "snapshot").
//
// Balances / TokenRates / ScalingFactors are parallel arrays indexed by
// balancer_pool_token.token_index. AmpFactor / BptRate / ActualSupply /
// TotalSupply are pool-contract reads; legacy stable pools do not expose
// getActualSupply and leave ActualSupply nil. LastChangeBlock is the Vault's
// own bookkeeping for getPoolTokens.
type BalancerPoolState struct {
	BalancerPoolID  int64
	BlockNumber     int64
	BlockVersion    int32
	Timestamp       time.Time
	Source          string
	Balances        []*big.Int // One entry per slot in balancer_pool_token order; raw token amounts.
	AmpFactor       *big.Int
	BptRate         *big.Int
	ActualSupply    *big.Int // Composable-stable only.
	TotalSupply     *big.Int
	TokenRates      []*big.Int
	ScalingFactors  []*big.Int
	LastChangeBlock *int64
}
