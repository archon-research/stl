package uniswapv4indexer

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func RegisteredPoolsFromRows(rows []outbound.UniswapV4PoolRow) []RegisteredPool {
	pools := make([]RegisteredPool, 0, len(rows))
	for _, row := range rows {
		pools = append(pools, RegisteredPool{
			ID:                row.ID,
			PoolManager:       row.PoolManager,
			StateView:         row.StateView,
			PositionManagerID: row.PositionManagerID,
			PositionManager:   row.PositionManager,
			PoolIDHash:        row.PoolIDHash,
			Currency0:         row.Currency0,
			Currency1:         row.Currency1,
			Currency0Decimals: row.Currency0Decimals,
			Currency1Decimals: row.Currency1Decimals,
			Fee:               row.Fee,
			TickSpacing:       row.TickSpacing,
			Hooks:             row.Hooks,
			DeployBlock:       row.DeployBlock,
			SnapshotSupported: row.SnapshotSupported,
		})
	}
	return pools
}

var poolKeyArgsOnce = sync.OnceValues(func() (abi.Arguments, error) {
	address, err := abi.NewType("address", "", nil)
	if err != nil {
		return nil, fmt.Errorf("address type: %w", err)
	}
	uint24T, err := abi.NewType("uint24", "", nil)
	if err != nil {
		return nil, fmt.Errorf("uint24 type: %w", err)
	}
	int24T, err := abi.NewType("int24", "", nil)
	if err != nil {
		return nil, fmt.Errorf("int24 type: %w", err)
	}
	return abi.Arguments{
		{Type: address}, {Type: address}, {Type: uint24T}, {Type: int24T}, {Type: address},
	}, nil
})

// DynamicFeeFlag is LPFeeLibrary.DYNAMIC_FEE_FLAG: a PoolKey.fee sentinel meaning
// the hook sets the LP fee at runtime, so the pool has no fee rate to snapshot.
const DynamicFeeFlag = 0x800000

// Logs route by PoolId, so a pool whose stored id disagrees with its key would
// silently never match a log; refusing to boot is the only way that surfaces.
func ValidatePoolKeys(pools []RegisteredPool) error {
	seen := make(map[common.Hash]int64, len(pools))
	for _, pool := range pools {
		if other, dup := seen[pool.PoolIDHash]; dup {
			return fmt.Errorf("pools %d and %d share PoolId %s: registry bug", other, pool.ID, pool.PoolIDHash)
		}
		seen[pool.PoolIDHash] = pool.ID

		computed, err := computePoolID(pool)
		if err != nil {
			return fmt.Errorf("recomputing PoolId for pool %d: %w", pool.ID, err)
		}
		if computed != pool.PoolIDHash {
			return fmt.Errorf("pool %d PoolId is %s but its key (%s, %s, fee %d, tickSpacing %d, hooks %s) hashes to %s: registry bug",
				pool.ID, pool.PoolIDHash, pool.Currency0, pool.Currency1, pool.Fee, pool.TickSpacing, pool.Hooks, computed)
		}
	}
	return nil
}

func SnapshottablePools(pools []RegisteredPool) []RegisteredPool {
	snapshottable := make([]RegisteredPool, 0, len(pools))
	for _, pool := range pools {
		if pool.SnapshotSupported {
			snapshottable = append(snapshottable, pool)
		}
	}
	return snapshottable
}

// computePoolID derives the PoolId the way v4-core's PoolIdLibrary does:
// keccak256 over the standard abi.encode layout of the PoolKey, not a packed one.
func computePoolID(pool RegisteredPool) (common.Hash, error) {
	args, err := poolKeyArgsOnce()
	if err != nil {
		return common.Hash{}, err
	}
	encoded, err := args.Pack(
		pool.Currency0,
		pool.Currency1,
		big.NewInt(int64(pool.Fee)),
		big.NewInt(int64(pool.TickSpacing)),
		pool.Hooks,
	)
	if err != nil {
		return common.Hash{}, fmt.Errorf("encoding pool key: %w", err)
	}
	return crypto.Keccak256Hash(encoded), nil
}
