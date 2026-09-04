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

var tickViewABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	parsed, err := abis.ParseABI(tickViewMethodsJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing StateView tick ABI: %w", err)
	}
	return parsed, nil
})

func tickViewABI() (*abi.ABI, error) {
	return tickViewABIOnce()
}

// v4-core guards its updateTick calls on liquidityDelta != 0, so a zero-delta poke
// leaves tick state untouched and its arbitrary bounds must not become tick rows.
func TouchedTicks(events []*entity.UniswapV4LiquidityEvent) []int32 {
	seen := make(map[int32]struct{}, len(events)*2)
	for _, e := range events {
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

// BuildTickCalls packs one getTickInfo call per tick, in input order, so callers
// can zip results back positionally.
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

// A revert is an error, never a zero-value tick: StateView answers a cleared tick
// with explicit zeros rather than reverting.
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

// BaselineTicks scans the pool's whole bitmap for initialized ticks. A partial
// result would under-report them forever after, so a failed word errors out.
func BaselineTicks(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockHash common.Hash) ([]int32, error) {
	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}

	minWord, maxWord, err := tickbitmap.WordBounds(pool.TickSpacing)
	if err != nil {
		return nil, fmt.Errorf("bitmap word range for pool %d: %w", pool.ID, err)
	}

	var ticks []int32
	for chunkStart := int(minWord); chunkStart <= int(maxWord); chunkStart += tickbitmap.BitmapWordsPerCall {
		chunkEnd := min(chunkStart+tickbitmap.BitmapWordsPerCall-1, int(maxWord))
		chunkTicks, err := scanBitmapWords(ctx, mc, a, pool, blockHash, chunkStart, chunkEnd)
		if err != nil {
			return nil, err
		}
		ticks = append(ticks, chunkTicks...)
	}

	slices.Sort(ticks)
	return ticks, nil
}

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
