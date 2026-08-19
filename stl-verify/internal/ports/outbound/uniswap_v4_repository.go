package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type UniswapV4PoolRow struct {
	ID          int64
	ProtocolID  int64
	PoolManager common.Address
	StateView   common.Address
	// PoolIDHash is the raw on-chain PoolId — keccak256 of the abi-encoded
	// PoolKey — which every PoolManager log is indexed by, not the surrogate key
	// in ID.
	PoolIDHash        common.Hash
	Currency0         common.Address // address(0) is native ETH
	Currency1         common.Address
	Currency0Decimals int
	Currency1Decimals int
	Fee               int // 0x800000 flags a dynamic LP fee
	TickSpacing       int
	Hooks             common.Address // zero address means the pool has no hooks
	DeployBlock       int64
}

type UniswapV4BlockWrites struct {
	States          []*entity.UniswapV4PoolState
	Swaps           []*entity.UniswapV4Swap
	LiquidityEvents []*entity.UniswapV4LiquidityEvent
	Ticks           []*entity.UniswapV4Tick
	PoolEvents      []*entity.UniswapV4PoolEvent
}

type UniswapV4Repository interface {
	// LoadPools returns the current version of every registered pool on chainID,
	// with the chain's current PoolManager/StateView addresses and the currency
	// decimals from token. Both registry tables are append-only version
	// histories, so "current" means the highest processing_version per natural
	// key — per (chain_id, pool_id) for pools, per chain_id for the PoolManager.
	// A missing PoolManager, a NULL decimals, or a currency that disagrees with
	// its token row is an error rather than a skipped pool: without the chain's
	// StateView there is nothing to snapshot, and the decimals are carried so
	// downstream consumers can scale and sanity-check the raw amounts this
	// indexer stores. A currency matches when token.address equals it, or when it
	// is address(0) and the token row is the
	// 0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE native-ETH placeholder.
	LoadPools(ctx context.Context, chainID int64) ([]UniswapV4PoolRow, error)
	// SaveBlock persists all of a block's uniswap_v4 rows within tx and returns
	// the number of state rows actually inserted (ON CONFLICT DO NOTHING means a
	// redelivery returns 0), for the uniswap_v4_state_rows_written_total metric.
	SaveBlock(ctx context.Context, tx pgx.Tx, w UniswapV4BlockWrites) (stateRows int64, err error)
	// PoolIDsWithStateAtBlock returns the uniswap_v4_pool ids that already have a
	// uniswap_v4_pool_state row at blockNumber, ascending. A reorg redelivery
	// unions them into its due set so a pool snapshotted on the orphaned fork is
	// re-snapshotted at the new version even when the in-memory snapshot tracker
	// was lost to a restart.
	PoolIDsWithStateAtBlock(ctx context.Context, blockNumber int64) ([]int64, error)
	// TicksForPoolAtBlock returns the distinct tick positions that already have a
	// row for pool at blockNumber, so a reorg redelivery can re-read exactly the
	// ticks a prior version wrote at this height. Reads committed rows outside
	// any transaction; safe to call before the write tx opens.
	TicksForPoolAtBlock(ctx context.Context, poolID int64, blockNumber int64) ([]int32, error)
}
