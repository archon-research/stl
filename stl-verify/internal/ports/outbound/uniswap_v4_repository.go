package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// UniswapV4PoolRow is the data returned by LoadPools for each pool in the registry.
type UniswapV4PoolRow struct {
	ID          int64
	ProtocolID  int64
	PoolManager common.Address
	StateView   common.Address
	// PoolID is the raw on-chain PoolId — keccak256 of the abi-encoded PoolKey —
	// which every PoolManager log is indexed by, not the surrogate key in ID.
	PoolID            common.Hash
	Currency0         common.Address // address(0) is native ETH
	Currency1         common.Address
	Currency0Decimals int
	Currency1Decimals int
	Fee               int // 0x800000 flags a dynamic LP fee
	TickSpacing       int
	Hooks             common.Address // zero address means the pool has no hooks
	DeployBlock       int64
}

// UniswapV4BlockWrites groups all of a block's uniswap_v4-table rows for a
// single batched persist.
type UniswapV4BlockWrites struct {
	States          []*entity.UniswapV4PoolState
	Swaps           []*entity.UniswapV4Swap
	LiquidityEvents []*entity.UniswapV4LiquidityEvent
	Ticks           []*entity.UniswapV4Tick
	PoolEvents      []*entity.UniswapV4PoolEvent
}

// UniswapV4Repository defines the interface for Uniswap V4 DEX data persistence.
type UniswapV4Repository interface {
	// LoadPools returns the current version of every registered pool on chainID,
	// with the chain's current PoolManager/StateView addresses and the currency
	// decimals from token. Both registry tables are append-only version
	// histories, so "current" means the highest processing_version per natural
	// key — per (chain_id, pool_id) for pools, per chain_id for the PoolManager.
	// A missing PoolManager, a NULL deploy_block, a NULL decimals, or a currency
	// that disagrees with its token row is an error rather than a skipped pool:
	// the deploy-block gate and amount scaling are both unrecoverable from a
	// half-populated registry. A currency matches when token.address equals it,
	// or when it is address(0) and the token row is the
	// 0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE native-ETH placeholder.
	LoadPools(ctx context.Context, chainID int64) ([]UniswapV4PoolRow, error)
	// SaveBlock persists all of a block's uniswap_v4 rows in one pgx.Batch within
	// tx and returns the number of state rows actually inserted (ON CONFLICT DO
	// NOTHING means a redelivery returns 0), for the uniswap_v4_state_rows_written_total metric.
	SaveBlock(ctx context.Context, tx pgx.Tx, w UniswapV4BlockWrites) (stateRows int64, err error)
	// TicksForPoolAtBlock returns the distinct tick positions that already have a
	// row for pool at blockNumber, so a reorg redelivery can re-read exactly the
	// ticks a prior version wrote at this height (VEC-487). Reads committed rows
	// outside any transaction; safe to call before the write tx opens.
	TicksForPoolAtBlock(ctx context.Context, poolID int64, blockNumber int64) ([]int32, error)
}
