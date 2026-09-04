package uniswapv4indexer

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/tickbitmap"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// StateView overloads getPositionInfo: this is the five-argument form, selector
// 0xdacf1d2f, not the two-argument poolId/positionId one.
const positionViewMethodsJSON = `[
	{"name":"getPositionInfo","type":"function","stateMutability":"view","inputs":[
		{"name":"poolId","type":"bytes32"},
		{"name":"owner","type":"address"},
		{"name":"tickLower","type":"int24"},
		{"name":"tickUpper","type":"int24"},
		{"name":"salt","type":"bytes32"}
	],"outputs":[
		{"name":"liquidity","type":"uint128"},
		{"name":"feeGrowthInside0LastX128","type":"uint256"},
		{"name":"feeGrowthInside1LastX128","type":"uint256"}
	]}
]`

var positionViewABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	parsed, err := abis.ParseABI(positionViewMethodsJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing StateView position ABI: %w", err)
	}
	return parsed, nil
})

func positionViewABI() (*abi.ABI, error) {
	return positionViewABIOnce()
}

const positionsPerCall = tickbitmap.TicksPerCall

// TouchedPositions returns the deduplicated, Compare-sorted positions this
// block's ModifyLiquidity events touched. Zero-delta pokes are INCLUDED, unlike
// TouchedTicks: v4-core's Position.update rewrites both fee checkpoints anyway.
func TouchedPositions(events []*entity.UniswapV4LiquidityEvent) []entity.UniswapV4PositionKey {
	keys := make([]entity.UniswapV4PositionKey, 0, len(events))
	for _, e := range events {
		keys = append(keys, entity.UniswapV4PositionKey{
			Owner:     e.Sender,
			TickLower: e.TickLower,
			TickUpper: e.TickUpper,
			Salt:      e.Salt,
		})
	}
	return MergePositionKeys(keys, nil)
}

// MergePositionKeys returns the deduplicated, Compare-sorted union: a block must
// read each touched position exactly once, however it was discovered.
func MergePositionKeys(a, b []entity.UniswapV4PositionKey) []entity.UniswapV4PositionKey {
	seen := make(map[entity.UniswapV4PositionKey]struct{}, len(a)+len(b))
	for _, set := range [][]entity.UniswapV4PositionKey{a, b} {
		for _, key := range set {
			seen[key] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}

	out := make([]entity.UniswapV4PositionKey, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	slices.SortFunc(out, entity.UniswapV4PositionKey.Compare)
	return out
}

func modifyLiquidityKey(data map[string]any) (entity.UniswapV4PositionKey, error) {
	fields, err := bigIntFields(data, "tickLower", "tickUpper")
	if err != nil {
		return entity.UniswapV4PositionKey{}, err
	}
	owner, err := shared.GetAddrField(data, "sender")
	if err != nil {
		return entity.UniswapV4PositionKey{}, err
	}
	salt, err := shared.GetHashField(data, "salt")
	if err != nil {
		return entity.UniswapV4PositionKey{}, err
	}
	tickLower, err := int24Value("tickLower", fields["tickLower"])
	if err != nil {
		return entity.UniswapV4PositionKey{}, err
	}
	tickUpper, err := int24Value("tickUpper", fields["tickUpper"])
	if err != nil {
		return entity.UniswapV4PositionKey{}, err
	}
	return entity.UniswapV4PositionKey{
		Owner:     owner,
		TickLower: tickLower,
		TickUpper: tickUpper,
		Salt:      salt,
	}, nil
}

// BuildPositionCalls packs one getPositionInfo(poolId, owner, tickLower,
// tickUpper, salt) call per entry in keys, in the same order as the input, so
// callers can zip results back to their originating position positionally.
func BuildPositionCalls(pool RegisteredPool, keys []entity.UniswapV4PositionKey) ([]outbound.Call, error) {
	a, err := positionViewABI()
	if err != nil {
		return nil, err
	}

	calls := make([]outbound.Call, len(keys))
	for i, key := range keys {
		data, err := a.Pack("getPositionInfo", pool.PoolIDHash, key.Owner,
			big.NewInt(int64(key.TickLower)), big.NewInt(int64(key.TickUpper)), key.Salt)
		if err != nil {
			return nil, fmt.Errorf("packing getPositionInfo(%s, %s, %d, %d, %s): %w",
				pool.PoolIDHash, key.Owner, key.TickLower, key.TickUpper, key.Salt, err)
		}
		calls[i] = outbound.Call{Target: pool.StateView, AllowFailure: false, CallData: data}
	}
	return calls, nil
}

// The live indexer and the one-shot position bootstrap share it, which is what
// keeps a backfilled row byte-identical to the live one for the same block.
func ReadPositions(
	ctx context.Context,
	multicaller outbound.Multicaller,
	pool RegisteredPool,
	keys []entity.UniswapV4PositionKey,
	blockHash common.Hash,
	blockNumber int64,
	version int,
	ts time.Time,
) ([]*entity.UniswapV4Position, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	rows := make([]*entity.UniswapV4Position, 0, len(keys))
	for chunk := range slices.Chunk(keys, positionsPerCall) {
		chunkRows, err := readPositionChunk(ctx, multicaller, pool, chunk, blockHash, blockNumber, version, ts)
		if err != nil {
			return nil, err
		}
		rows = append(rows, chunkRows...)
	}
	return rows, nil
}

func readPositionChunk(
	ctx context.Context,
	multicaller outbound.Multicaller,
	pool RegisteredPool,
	chunk []entity.UniswapV4PositionKey,
	blockHash common.Hash,
	blockNumber int64,
	version int,
	ts time.Time,
) ([]*entity.UniswapV4Position, error) {
	calls, err := BuildPositionCalls(pool, chunk)
	if err != nil {
		return nil, fmt.Errorf("building position calls for pool %s block %d: %w", pool.PoolIDHash, blockNumber, err)
	}
	results, err := multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("executing position multicall for pool %s block %d: %w", pool.PoolIDHash, blockNumber, err)
	}
	if len(results) != len(chunk) {
		return nil, fmt.Errorf("pool %s block %d: got %d position results, want %d", pool.PoolIDHash, blockNumber, len(results), len(chunk))
	}

	rows := make([]*entity.UniswapV4Position, 0, len(chunk))
	for i, key := range chunk {
		row, err := DecodePosition(pool, key, blockNumber, version, ts, results[i])
		if err != nil {
			return nil, fmt.Errorf("decoding position %+v for pool %s block %d: %w", key, pool.PoolIDHash, blockNumber, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// DecodePosition treats a revert as an error: getPositionInfo answers even a
// burned position with zeros, so a revert is never an absent position.
func DecodePosition(pool RegisteredPool, key entity.UniswapV4PositionKey, blockNumber int64, version int, ts time.Time, res outbound.Result) (*entity.UniswapV4Position, error) {
	if !res.Success {
		return nil, fmt.Errorf("getPositionInfo(%s, %s, %d, %d, %s) reverted",
			pool.PoolIDHash, key.Owner, key.TickLower, key.TickUpper, key.Salt)
	}

	values, err := unpackPositionInfo(res)
	if err != nil {
		return nil, err
	}

	result := &entity.UniswapV4Position{
		PoolID:                   pool.ID,
		Owner:                    key.Owner,
		TickLower:                key.TickLower,
		TickUpper:                key.TickUpper,
		Salt:                     key.Salt,
		BlockNumber:              blockNumber,
		BlockVersion:             version,
		BlockTimestamp:           ts,
		Liquidity:                values[0],
		FeeGrowthInside0LastX128: values[1],
		FeeGrowthInside1LastX128: values[2],
	}
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("validating position %+v on pool %s: %w", key, pool.PoolIDHash, err)
	}
	return result, nil
}

func unpackPositionInfo(res outbound.Result) ([]*big.Int, error) {
	a, err := positionViewABI()
	if err != nil {
		return nil, err
	}
	out, err := a.Unpack("getPositionInfo", res.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking getPositionInfo(): %w", err)
	}
	if len(out) != 3 {
		return nil, fmt.Errorf("getPositionInfo() returned %d values, want 3", len(out))
	}

	values := make([]*big.Int, len(out))
	for i, v := range out {
		bi, ok := v.(*big.Int)
		if !ok {
			return nil, fmt.Errorf("getPositionInfo() value %d type = %T, want *big.Int", i, v)
		}
		values[i] = bi
	}
	return values, nil
}
