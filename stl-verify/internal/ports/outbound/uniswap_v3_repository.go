package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// UniswapV3PoolRow is the data returned by LoadPools for each pool in the registry.
type UniswapV3PoolRow struct {
	ID             int64
	ProtocolID     int64
	Address        common.Address
	Token0         common.Address
	Token1         common.Address
	Token0Decimals int
	Token1Decimals int
	Fee            int
	TickSpacing    int
	DeployBlock    int64
}

// UniswapV3BlockWrites groups all of a block's uniswap_v3-table rows for a
// single batched persist.
type UniswapV3BlockWrites struct {
	States          []*entity.UniswapV3PoolState
	Swaps           []*entity.UniswapV3Swap
	LiquidityEvents []*entity.UniswapV3LiquidityEvent
	Ticks           []*entity.UniswapV3Tick
	PoolEvents      []*entity.UniswapV3PoolEvent
}

// UniswapV3Repository defines the interface for Uniswap V3 DEX data persistence.
type UniswapV3Repository interface {
	LoadPools(ctx context.Context, chainID int64) ([]UniswapV3PoolRow, error)
	// SaveBlock persists all of a block's uniswap_v3 rows in one pgx.Batch within
	// tx and returns the number of state rows actually inserted (ON CONFLICT DO
	// NOTHING means a redelivery returns 0), for the uniswap_v3_state_rows_written_total metric.
	SaveBlock(ctx context.Context, tx pgx.Tx, w UniswapV3BlockWrites) (stateRows int64, err error)
	// TicksForPoolAtBlock returns the distinct tick positions that already have a
	// row for pool at blockNumber, so a reorg redelivery can re-read exactly the
	// ticks a prior version wrote at this height (VEC-487). Reads committed rows
	// outside any transaction; safe to call before the write tx opens.
	TicksForPoolAtBlock(ctx context.Context, poolID int64, blockNumber int64) ([]int32, error)
}
