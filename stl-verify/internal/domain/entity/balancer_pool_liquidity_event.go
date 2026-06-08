package entity

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// BalancerPoolLiquidityEvent is a per-event fact row for the Vault
// `PoolBalanceChanged(poolId, liquidityProvider, tokens, deltas,
// protocolFeeAmounts)` emission. Both join and exit flows ride the same
// event; the sign of deltas distinguishes them (positive = into pool,
// negative = out).
//
// Deltas and ProtocolFeeAmounts are parallel arrays in the Vault's tokens
// order — the worker projects them onto balancer_pool_token.token_index
// before writing so downstream queries can index by slot directly.
type BalancerPoolLiquidityEvent struct {
	BalancerPoolID     int64
	BlockNumber        int64
	BlockVersion       int32
	Timestamp          time.Time
	TxHash             common.Hash
	LogIndex           int32
	LiquidityProvider  common.Address
	Deltas             []*big.Int // Signed; positive = into pool, negative = out.
	ProtocolFeeAmounts []*big.Int
}
