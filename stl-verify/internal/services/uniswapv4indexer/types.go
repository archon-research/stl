package uniswapv4indexer

import (
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
)

// RegisteredPool is the static, registry-sourced identity of a watched
// Uniswap V4 pool.
//
// Two distinct identifiers coexist and must not be confused: ID is the
// uniswap_v4_pool surrogate key every fact row FKs, while PoolIDHash is the
// on-chain PoolId — keccak256(abi.encode(PoolKey)) — that every PoolManager log
// is indexed by. The SnapshotPool method below is named PoolID() to match the
// dexconsumer interface, so the on-chain hash carries the suffixed field name.
type RegisteredPool struct {
	ID                int64
	PoolManager       common.Address
	StateView         common.Address
	PoolIDHash        common.Hash
	Currency0         common.Address
	Currency1         common.Address
	Currency0Decimals int
	Currency1Decimals int
	Fee               int
	TickSpacing       int
	Hooks             common.Address
	DeployBlock       int64
}

// PoolID and DeployBlockNum implement dexconsumer.SnapshotPool, letting
// RegisteredPool feed the shared sweep/deploy-gate tracker without
// dexconsumer depending on uniswapv4indexer.
func (p RegisteredPool) PoolID() int64         { return p.ID }
func (p RegisteredPool) DeployBlockNum() int64 { return p.DeployBlock }

// DecodedEvents holds one receipt's typed entities and their capture-net
// mirror. Captured is a superset of the typed slices plus the singleton's
// non-pool-keyed governance logs; it deliberately excludes logs belonging to
// unregistered pools (see DecodeEvents).
type DecodedEvents struct {
	Swaps           []*entity.UniswapV4Swap
	LiquidityEvents []*entity.UniswapV4LiquidityEvent
	PoolEvents      []*entity.UniswapV4PoolEvent
	Captured        []dexconsumer.CapturedLog
}
