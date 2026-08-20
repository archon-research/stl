package uniswapv4indexer

import (
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/tickbitmap"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// StateView overloads getPositionInfo: this is the five-argument
// (poolId, owner, tickLower, tickUpper, salt) form, selector 0xdacf1d2f, not the
// two-argument poolId/positionId form.
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

// positionViewABIOnce parses positionViewMethodsJSON exactly once: this ABI is
// on the per-position hot path (BuildPositionCalls, DecodePosition), so
// re-parsing the JSON per call is pure waste.
var positionViewABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	parsed, err := abis.ParseABI(positionViewMethodsJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing StateView position ABI: %w", err)
	}
	return parsed, nil
})

// positionViewABI is the position-level StateView getter; the pool-level ones
// live in state.go and the tick-level ones in tick.go.
func positionViewABI() (*abi.ABI, error) {
	return positionViewABIOnce()
}

// positionsPerCall bounds how many getPositionInfo sub-calls one multicall3
// aggregate carries. Same cap as the per-tick getters: one sub-call per key
// against the same RPC provider request/response limits.
const positionsPerCall = tickbitmap.TicksPerCall

// TouchedPositions returns the deduplicated, Compare-sorted set of positions
// this block's ModifyLiquidity events touched, identified by
// (sender, tickLower, tickUpper, salt).
//
// Zero-delta pokes are INCLUDED, unlike TouchedTicks which excludes them:
// v4-core's Position.update writes both feeGrowthInside*LastX128 checkpoints
// unconditionally, so a fee-collect poke changes position state even though it
// leaves every tick untouched.
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

// MergePositionKeys returns the deduplicated, Compare-sorted union of two
// position-key sets: a block must read and write each touched position exactly
// once, even where this block's own events and a prior version's persisted
// positions overlap.
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

// DecodePosition decodes one getPositionInfo() multicall result into an
// authoritative entity.UniswapV4Position. A reverted call is an error, never a
// silently dropped/zero-value position: this is an authoritative read, and
// StateView answers a burned position with explicit zeros rather than reverting.
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
