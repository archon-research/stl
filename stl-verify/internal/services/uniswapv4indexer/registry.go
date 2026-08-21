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

// RegisteredPoolsFromRows maps UniswapV4PoolRow slices to RegisteredPool slices.
func RegisteredPoolsFromRows(rows []outbound.UniswapV4PoolRow) []RegisteredPool {
	pools := make([]RegisteredPool, 0, len(rows))
	for _, row := range rows {
		pools = append(pools, RegisteredPool{
			ID:                row.ID,
			PoolManager:       row.PoolManager,
			StateView:         row.StateView,
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

// poolKeyArgsOnce builds the abi.Arguments for PoolKey exactly once. The tuple
// is (address currency0, address currency1, uint24 fee, int24 tickSpacing,
// address hooks), each padded to a 32-byte word — v4-core's PoolId is
// keccak256 over that standard abi.encode layout, not a packed encoding.
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

// DynamicFeeFlag is the PoolKey.fee sentinel (LPFeeLibrary.DYNAMIC_FEE_FLAG)
// marking a pool whose LP fee the hook sets at runtime, rather than a rate.
// Such a pool is indexed for events and excluded from snapshots through the
// registry's snapshot_supported column, never refused at boot: the fee is
// hashed into the PoolId, so no superseding registry row could repair it.
const DynamicFeeFlag = 0x800000

// ValidatePoolKeys recomputes every pool's on-chain PoolId from its registry
// key and rejects any mismatch or duplicate. A mismatch is unrecoverable at
// runtime rather than merely wrong: PoolManager logs are routed by PoolId, so a
// pool whose stored id disagrees with its key would silently never match a log
// and its fact tables would stay empty while the worker looks healthy. Refusing
// to boot is the only failure mode that surfaces it.
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

// SnapshottablePools returns the pools whose state and ticks the indexer reads.
// A pool the registry excludes (snapshot_supported = false, e.g. a dynamic-fee
// pool whose lp_fee updateDynamicLPFee rewrites with no event) stays in the
// event-decoding registry; only its snapshot schedule is dropped.
func SnapshottablePools(pools []RegisteredPool) []RegisteredPool {
	snapshottable := make([]RegisteredPool, 0, len(pools))
	for _, pool := range pools {
		if pool.SnapshotSupported {
			snapshottable = append(snapshottable, pool)
		}
	}
	return snapshottable
}

// computePoolID returns keccak256(abi.encode(currency0, currency1, fee,
// tickSpacing, hooks)) — the PoolId v4-core derives in PoolIdLibrary.
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
