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

const tickViewMethodsJSON = `[
	{"name":"getTickInfo","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"},{"name":"tick","type":"int24"}],"outputs":[
		{"name":"liquidityGross","type":"uint128"},
		{"name":"liquidityNet","type":"int128"},
		{"name":"feeGrowthOutside0X128","type":"uint256"},
		{"name":"feeGrowthOutside1X128","type":"uint256"}
	]},
	{"name":"getTickBitmap","type":"function","stateMutability":"view","inputs":[{"name":"poolId","type":"bytes32"},{"name":"tick","type":"int16"}],"outputs":[{"name":"","type":"uint256"}]}
]`

// tickViewABIOnce parses tickViewMethodsJSON exactly once: this ABI is on the
// per-tick hot path (DecodeTick, BuildTickCalls, BaselineTicks), so re-parsing
// the JSON per call is pure waste.
var tickViewABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	parsed, err := abis.ParseABI(tickViewMethodsJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing StateView tick ABI: %w", err)
	}
	return parsed, nil
})

// tickViewABI returns the ABI fragment for the PoolId-keyed tick getters on
// StateView. The pool-level getters live in state.go.
func tickViewABI() (*abi.ABI, error) {
	return tickViewABIOnce()
}

// TouchedTicks returns the deduplicated, ascending-sorted union of every
// TickLower/TickUpper bound whose tick state this block's ModifyLiquidity
// events actually changed.
//
// Zero-delta events are excluded: v4-core's Pool.modifyLiquidity guards its
// updateTick calls on liquidityDelta != 0, so a fee-collecting poke leaves
// liquidityGross/Net and feeGrowthOutside untouched. Reading those ticks would
// be wasted work, and including them would let a permissionless zero-delta call
// with arbitrary bounds amplify into junk uninitialized-tick rows.
func TouchedTicks(evs DecodedEvents) []int32 {
	seen := make(map[int32]struct{}, len(evs.LiquidityEvents)*2)
	for _, e := range evs.LiquidityEvents {
		if e.LiquidityDelta.Sign() == 0 {
			continue
		}
		seen[int32(e.TickLower)] = struct{}{}
		seen[int32(e.TickUpper)] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}

	out := make([]int32, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// BuildTickCalls packs one getTickInfo(poolId, tick) call per entry in ticks,
// in the same order as the input, so callers can zip results back to their
// originating tick positionally.
func BuildTickCalls(pool RegisteredPool, ticks []int32) ([]outbound.Call, error) {
	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}

	calls := make([]outbound.Call, len(ticks))
	for i, tick := range ticks {
		data, err := a.Pack("getTickInfo", pool.PoolIDHash, big.NewInt(int64(tick)))
		if err != nil {
			return nil, fmt.Errorf("packing getTickInfo(%s, %d): %w", pool.PoolIDHash, tick, err)
		}
		calls[i] = outbound.Call{Target: pool.StateView, AllowFailure: false, CallData: data}
	}
	return calls, nil
}

// DecodeTick decodes one getTickInfo() multicall result into an authoritative
// entity.UniswapV4Tick. A reverted call is an error, never a silently
// dropped/zero-value tick: this is an authoritative read, and StateView answers
// a cleared tick with explicit zeros rather than reverting.
func DecodeTick(pool RegisteredPool, tick int32, blockNumber int64, version int, ts time.Time, res outbound.Result) (*entity.UniswapV4Tick, error) {
	if !res.Success {
		return nil, fmt.Errorf("getTickInfo(%s, %d) reverted", pool.PoolIDHash, tick)
	}

	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}
	out, err := a.Unpack("getTickInfo", res.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking getTickInfo(%d): %w", tick, err)
	}
	if len(out) != 4 {
		return nil, fmt.Errorf("getTickInfo(%d) returned %d values, want 4", tick, len(out))
	}

	values := make([]*big.Int, len(out))
	for i, v := range out {
		bi, ok := v.(*big.Int)
		if !ok {
			return nil, fmt.Errorf("getTickInfo(%d) value %d type = %T, want *big.Int", tick, i, v)
		}
		values[i] = bi
	}

	result := &entity.UniswapV4Tick{
		PoolID:                pool.ID,
		Tick:                  int(tick),
		BlockNumber:           blockNumber,
		BlockVersion:          version,
		BlockTimestamp:        ts,
		LiquidityGross:        values[0],
		LiquidityNet:          values[1],
		FeeGrowthOutside0X128: values[2],
		FeeGrowthOutside1X128: values[3],
	}
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("validating tick %d: %w", tick, err)
	}
	return result, nil
}

// baselineTickBitmapWordsPerCall bounds how many getTickBitmap sub-calls
// BaselineTicks packs into a single multicall3 aggregate call. At tickSpacing=1
// the full word range is ~6932 words; sending them all in one aggregate call
// risks exceeding an RPC provider's request/response/gas caps. 500 words per
// call keeps that worst case to ~14 batches.
const baselineTickBitmapWordsPerCall = 500

// BaselineTicks performs a one-time enumeration of every currently initialized
// tick on pool by scanning its bitmap across the full tickSpacing-derived word
// range. It is a pure read: callers own logging and retry policy. A reverted
// call is returned as an error immediately (no partial/best-effort baseline),
// since a silently incomplete baseline would under-report initialized ticks
// forever after.
func BaselineTicks(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockHash common.Hash) ([]int32, error) {
	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}

	minWord, maxWord := tickbitmap.WordBounds(pool.TickSpacing)

	var ticks []int32
	for chunkStart := int(minWord); chunkStart <= int(maxWord); chunkStart += baselineTickBitmapWordsPerCall {
		chunkEnd := min(chunkStart+baselineTickBitmapWordsPerCall-1, int(maxWord))
		chunkTicks, err := scanBitmapWords(ctx, mc, a, pool, blockHash, chunkStart, chunkEnd)
		if err != nil {
			return nil, err
		}
		ticks = append(ticks, chunkTicks...)
	}

	slices.Sort(ticks)
	return ticks, nil
}

// scanBitmapWords reads one bounded batch of bitmap words and expands every set
// bit back into its tick position.
func scanBitmapWords(ctx context.Context, mc outbound.Multicaller, a *abi.ABI, pool RegisteredPool, blockHash common.Hash, firstWord, lastWord int) ([]int32, error) {
	words := make([]int16, 0, lastWord-firstWord+1)
	calls := make([]outbound.Call, 0, cap(words))
	for w := firstWord; w <= lastWord; w++ {
		data, err := a.Pack("getTickBitmap", pool.PoolIDHash, int16(w))
		if err != nil {
			return nil, fmt.Errorf("packing getTickBitmap(%s, %d): %w", pool.PoolIDHash, w, err)
		}
		calls = append(calls, outbound.Call{Target: pool.StateView, AllowFailure: false, CallData: data})
		words = append(words, int16(w))
	}

	results, err := mc.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("executing getTickBitmap baseline scan (words %d..%d): %w", firstWord, lastWord, err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("unexpected getTickBitmap result count: got %d, want %d", len(results), len(calls))
	}

	var ticks []int32
	for i, res := range results {
		word, err := shared.UnpackUint(a, "getTickBitmap", res)
		if err != nil {
			return nil, fmt.Errorf("getTickBitmap(%d) on pool %s: %w", words[i], pool.PoolIDHash, err)
		}
		for bit := range 256 {
			if word.Bit(bit) == 0 {
				continue
			}
			ticks = append(ticks, tickbitmap.WordBitToTick(words[i], uint8(bit), pool.TickSpacing))
		}
	}
	return ticks, nil
}
