package uniswapv3indexer

import (
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
)

// RegisteredPool is the static, registry-sourced identity of a watched
// Uniswap V3 pool. Unlike Curve, V3 has a single pool class, so there is no
// per-class kind field here.
type RegisteredPool struct {
	ID             int64 // uniswap_v3_pool.id
	Address        common.Address
	Token0         common.Address
	Token1         common.Address
	Token0Decimals int
	Token1Decimals int
	Fee            int
	TickSpacing    int
	DeployBlock    int64
}

// PoolID and DeployBlockNum implement dexconsumer.SnapshotPool, letting
// RegisteredPool feed the shared sweep/deploy-gate tracker without
// dexconsumer depending on uniswapv3indexer.
func (p RegisteredPool) PoolID() int64         { return p.ID }
func (p RegisteredPool) DeployBlockNum() int64 { return p.DeployBlock }

// DecodedEvents holds the typed entities and the capture-net mirror produced
// by decoding a single transaction receipt for one pool. Captured is always
// a superset of Swaps, LiquidityEvents, and PoolEvents.
type DecodedEvents struct {
	Swaps           []*entity.UniswapV3Swap
	LiquidityEvents []*entity.UniswapV3LiquidityEvent
	PoolEvents      []*entity.UniswapV3PoolEvent
	Captured        []dexconsumer.CapturedLog
}
