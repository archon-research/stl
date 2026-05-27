package entity

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// BalancerPoolKind enumerates the on-chain Balancer V2 pool implementations
// the worker supports. Matches the `pool_kind` CHECK constraint on the
// balancer_pool table.
const (
	BalancerPoolKindStable           = "stable"
	BalancerPoolKindComposableStable = "composable_stable"
	BalancerPoolKindWeighted         = "weighted"
)

// BalancerPool is the registry row for a Balancer V2 pool we index. Token
// composition is variable-N and lives in the balancer_pool_token join table
// indexed by token_index (unlike the Uniswap V3 registry which inlines its
// two-token slots as scalar FKs).
//
// PoolID is the 32-byte Vault pool identifier — Balancer V2 convention
// encodes the pool address in the leading 20 bytes plus specialization +
// nonce. The post-seed assertion in the migration enforces that.
type BalancerPool struct {
	ID              int64
	ChainID         int64
	PoolKind        string
	Address         common.Address
	PoolID          common.Hash // 32-byte Vault pool id; encodes Address in leading 20 bytes.
	VaultAddress    common.Address
	Label           string
	DeploymentBlock *int64
	Enabled         bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
