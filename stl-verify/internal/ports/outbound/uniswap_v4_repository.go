package outbound

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type UniswapV4PoolRow struct {
	ID          int64
	ProtocolID  int64
	PoolManager common.Address
	StateView   common.Address
	// Raw on-chain PoolId — keccak256 of the abi-encoded PoolKey — that every
	// PoolManager log is indexed by; ID is the surrogate key.
	PoolIDHash        common.Hash
	Currency0         common.Address // address(0) is native ETH
	Currency1         common.Address
	Currency0Decimals int
	Currency1Decimals int
	Fee               int // 0x800000 flags a dynamic LP fee
	TickSpacing       int
	Hooks             common.Address
	DeployBlock       int64
	// Gates the state/tick snapshot path only; events are indexed either way.
	SnapshotSupported bool
}

type UniswapV4BlockWrites struct {
	States          []*entity.UniswapV4PoolState
	Swaps           []*entity.UniswapV4Swap
	LiquidityEvents []*entity.UniswapV4LiquidityEvent
	Ticks           []*entity.UniswapV4Tick
	PoolEvents      []*entity.UniswapV4PoolEvent
	Positions       []*entity.UniswapV4Position
}

type UniswapV4Repository interface {
	// Current version of every registered pool on chainID, with the chain's
	// PoolManager/StateView and token decimals; an unresolvable pool is an error.
	LoadPools(ctx context.Context, chainID int64) ([]UniswapV4PoolRow, error)
	SaveBlock(ctx context.Context, tx pgx.Tx, w UniswapV4BlockWrites) (stateRows StateRowCounts, err error)
	// SavePositions persists position rows alone, through the same
	// append-on-change path SaveBlock's position phase takes, and returns how
	// many rows it inserted — zero when every slot's stored state already matches.
	SavePositions(ctx context.Context, tx pgx.Tx, positions []*entity.UniswapV4Position) (insertedRows int64, err error)
	// On-chain PoolIds of the pools on chainID with a state row at blockNumber,
	// ascending; a reorg redelivery unions them into its due set. Natural keys, not
	// surrogate ids: a registry version appended after a worker booted must not
	// change what that worker resolves. blockTimestamp prunes chunks (VEC-541).
	PoolIDsWithStateAtBlock(ctx context.Context, chainID int64, blockNumber int64, blockTimestamp time.Time) ([]common.Hash, error)
	// Tick positions already written for pool at blockNumber, so a reorg
	// redelivery re-reads them; reads committed rows outside any transaction.
	TicksForPoolAtBlock(ctx context.Context, chainID int64, poolID int64, blockNumber int64) ([]int32, error)
	// PositionsForPoolAtBlock returns the position keys already stored for pool at
	// blockNumber, in entity.UniswapV4PositionKey.Compare order. A position is
	// discovered only from a log, so a reorg redelivery cannot name it otherwise.
	PositionsForPoolAtBlock(ctx context.Context, poolID int64, blockNumber int64) ([]entity.UniswapV4PositionKey, error)
	// Pools on chainID that ever wrote a state or tick row, ascending. Read once
	// at construction to rebuild the never-indexed and already-baselined sets.
	PoolIDsEverSnapshotted(ctx context.Context, chainID int64) ([]int64, error)
}
