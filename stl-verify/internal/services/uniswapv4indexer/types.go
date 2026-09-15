package uniswapv4indexer

import (
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
)

// ID is the uniswap_v4_pool surrogate key every fact row FKs; PoolIDHash is the
// on-chain PoolId every PoolManager log is indexed by.
type RegisteredPool struct {
	ID                int64
	PoolManager       common.Address
	StateView         common.Address
	PositionManagerID int64
	PositionManager   common.Address
	PoolIDHash        common.Hash
	Currency0         common.Address
	Currency1         common.Address
	Currency0Decimals int
	Currency1Decimals int
	Fee               int
	TickSpacing       int
	Hooks             common.Address
	DeployBlock       int64
	// Gates the snapshot path alone: an excluded pool is still decoded and
	// persisted for events.
	SnapshotSupported bool
}

// PoolID and DeployBlockNum implement dexconsumer.SnapshotPool.
func (p RegisteredPool) PoolID() int64         { return p.ID }
func (p RegisteredPool) DeployBlockNum() int64 { return p.DeployBlock }

type DecodedEvents struct {
	Swaps           []*entity.UniswapV4Swap
	LiquidityEvents []*entity.UniswapV4LiquidityEvent
	PoolEvents      []*entity.UniswapV4PoolEvent
	NFTTransfers    []*entity.UniswapV4PositionNFTTransfer
	Captured        []dexconsumer.CapturedLog
}

type RegisteredPositionManager struct {
	ID      int64
	Address common.Address
}
