package uniswapv3indexer

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
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// Uniswap V3 TickMath MIN_TICK/MAX_TICK: the widest tick range any pool can
// ever report, regardless of tickSpacing. This is the TickMath usable tick
// range, distinct from entity's int24 wire bounds (-8388608/8388607) used by
// Validate() elsewhere.
const (
	minTick = -887272
	maxTick = 887272
)

const tickViewMethodsJSON = `[
	{"name":"ticks","type":"function","stateMutability":"view","inputs":[{"name":"tick","type":"int24"}],"outputs":[
		{"name":"liquidityGross","type":"uint128"},
		{"name":"liquidityNet","type":"int128"},
		{"name":"feeGrowthOutside0X128","type":"uint256"},
		{"name":"feeGrowthOutside1X128","type":"uint256"},
		{"name":"tickCumulativeOutside","type":"int56"},
		{"name":"secondsPerLiquidityOutsideX128","type":"uint160"},
		{"name":"secondsOutside","type":"uint32"},
		{"name":"initialized","type":"bool"}
	]},
	{"name":"tickBitmap","type":"function","stateMutability":"view","inputs":[{"name":"wordPosition","type":"int16"}],"outputs":[{"name":"","type":"uint256"}]}
]`

// tickViewABIOnce parses tickViewMethodsJSON exactly once: this ABI is on the
// per-tick hot path (DecodeTick runs ~116×/first-touch, plus BuildTickCalls and
// BaselineTicks), so re-parsing the JSON per call is pure waste.
var tickViewABIOnce = sync.OnceValues(func() (*abi.ABI, error) {
	parsed, err := abis.ParseABI(tickViewMethodsJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing tick view ABI: %w", err)
	}
	return parsed, nil
})

// tickViewABI returns the ABI fragment for the pool's tick-reading view
// methods (ticks, tickBitmap). These are not events, so they live apart from
// PoolABI in abi.go.
func tickViewABI() (*abi.ABI, error) {
	return tickViewABIOnce()
}

// TouchedTicks returns the deduplicated, ascending-sorted union of every
// TickLower/TickUpper bound touched by the block's Mint and Burn events.
// Collect is excluded: v3-core's collect() only withdraws owed fees and never
// mutates tick state (liquidityGross/Net, feeGrowthOutside), and (since it
// omits checkTicks) its tick range is caller-supplied and unvalidated. Reading
// those ticks is wasted work, and including them would let a permissionless
// collect() with arbitrary ticks amplify into junk uninitialized-tick reads/rows.
func TouchedTicks(evs DecodedEvents) []int32 {
	seen := make(map[int32]struct{}, len(evs.LiquidityEvents)*2)
	for _, e := range evs.LiquidityEvents {
		if e.EventName == entity.LiquidityEventCollect {
			continue
		}
		seen[int32(e.TickLower)] = struct{}{}
		seen[int32(e.TickUpper)] = struct{}{}
	}

	out := make([]int32, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// BuildTickCalls packs one ticks(int24) call per entry in ticks, in the same
// order as the input, so callers can zip results back to their originating
// tick positionally.
func BuildTickCalls(pool RegisteredPool, ticks []int32) ([]outbound.Call, error) {
	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}

	calls := make([]outbound.Call, len(ticks))
	for i, tick := range ticks {
		data, err := a.Pack("ticks", big.NewInt(int64(tick)))
		if err != nil {
			return nil, fmt.Errorf("packing ticks(%d): %w", tick, err)
		}
		calls[i] = outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data}
	}
	return calls, nil
}

// DecodeTick decodes one ticks() multicall result into an authoritative
// entity.UniswapV3Tick. A reverted call is an error, never a silently
// dropped/zero-value tick: this is an authoritative read, and the caller
// asked for a tick that must exist.
func DecodeTick(pool RegisteredPool, tick int32, blockNumber int64, version int, ts time.Time, res outbound.Result) (*entity.UniswapV3Tick, error) {
	if !res.Success {
		return nil, fmt.Errorf("ticks(%d) reverted on pool %s", tick, pool.Address)
	}

	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}
	out, err := a.Unpack("ticks", res.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking ticks(%d): %w", tick, err)
	}
	if len(out) != 8 {
		return nil, fmt.Errorf("ticks(%d) returned %d values, want 8", tick, len(out))
	}

	liquidityGross, ok := out[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("ticks(%d) liquidityGross type = %T, want *big.Int", tick, out[0])
	}
	liquidityNet, ok := out[1].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("ticks(%d) liquidityNet type = %T, want *big.Int", tick, out[1])
	}
	feeGrowthOutside0, ok := out[2].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("ticks(%d) feeGrowthOutside0X128 type = %T, want *big.Int", tick, out[2])
	}
	feeGrowthOutside1, ok := out[3].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("ticks(%d) feeGrowthOutside1X128 type = %T, want *big.Int", tick, out[3])
	}
	initialized, ok := out[7].(bool)
	if !ok {
		return nil, fmt.Errorf("ticks(%d) initialized type = %T, want bool", tick, out[7])
	}

	result := &entity.UniswapV3Tick{
		PoolID:                pool.ID,
		Tick:                  int(tick),
		BlockNumber:           blockNumber,
		BlockVersion:          version,
		BlockTimestamp:        ts,
		LiquidityGross:        liquidityGross,
		LiquidityNet:          liquidityNet,
		FeeGrowthOutside0X128: feeGrowthOutside0,
		FeeGrowthOutside1X128: feeGrowthOutside1,
		Initialized:           initialized,
	}
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("validating tick %d: %w", tick, err)
	}
	return result, nil
}

// floorDiv and floorMod implement floored (as opposed to Go's truncated)
// integer division: e.g. floorDiv(-1, 256) == -1 and floorMod(-1, 256) == 255,
// whereas Go's native -1/256 == 0 and -1%256 == -1. Uniswap V3's tick bitmap
// packs ticks using floored semantics (Solidity's compressed >> 8 on a signed
// int24, an arithmetic shift which floors), so using Go's native operators
// here would silently misplace every negative tick into the wrong bitmap word.
func floorDiv(a, b int) int {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

func floorMod(a, b int) int {
	m := a % b
	if m != 0 && ((m < 0) != (b < 0)) {
		m += b
	}
	return m
}

// tickToWordBit maps an on-chain tick (must be a multiple of tickSpacing) to
// its tickBitmap word position and bit index, inverting Solidity's
// int16(compressed >> 8) / uint8(compressed % 256) packing.
func tickToWordBit(tick int32, tickSpacing int) (int16, uint8) {
	compressed := floorDiv(int(tick), tickSpacing)
	word := floorDiv(compressed, 256)
	bit := floorMod(compressed, 256)
	return int16(word), uint8(bit)
}

// wordBitToTick is the inverse of tickToWordBit: it recovers the tick at a
// given bitmap word/bit for a pool with the given tickSpacing.
func wordBitToTick(word int16, bit uint8, tickSpacing int) int32 {
	compressed := int(word)*256 + int(bit)
	return int32(compressed * tickSpacing)
}

// wordBounds returns the inclusive [minWord, maxWord] range of tickBitmap
// word positions that can hold an initialized tick for a pool with the given
// tickSpacing, derived from Uniswap's fixed MIN_TICK/MAX_TICK. Enumerating
// only this range (rather than every int16) keeps BaselineTicks to O(tens) of
// calls instead of 65536.
func wordBounds(tickSpacing int) (int16, int16) {
	minCompressed := floorDiv(minTick, tickSpacing)
	maxCompressed := floorDiv(maxTick, tickSpacing)
	minWord := floorDiv(minCompressed, 256)
	maxWord := floorDiv(maxCompressed, 256)
	return int16(minWord), int16(maxWord)
}

// baselineTickBitmapWordsPerCall bounds how many tickBitmap(int16) sub-calls
// BaselineTicks packs into a single multicall3 aggregate call. At
// tickSpacing=1 the full word range is ~6932 words; sending them all in one
// aggregate call risks exceeding an RPC provider's request/response/gas caps.
// 500 words per call keeps that worst case to ~14 batches.
const baselineTickBitmapWordsPerCall = 500

// BaselineTicks performs a one-time enumeration of every currently
// initialized tick on pool by scanning its tickBitmap across the full
// tickSpacing-derived word range. It is a pure read: callers own logging and
// retry policy. A reverted call is returned as an error immediately (no
// partial/best-effort baseline), since a silently incomplete baseline would
// under-report initialized ticks forever after. The word range is scanned in
// bounded batches (see baselineTickBitmapWordsPerCall) rather than one
// multicall covering the whole range.
func BaselineTicks(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockHash common.Hash) ([]int32, error) {
	a, err := tickViewABI()
	if err != nil {
		return nil, err
	}

	minWord, maxWord := wordBounds(pool.TickSpacing)

	var ticks []int32
	for chunkStart := int(minWord); chunkStart <= int(maxWord); chunkStart += baselineTickBitmapWordsPerCall {
		chunkEnd := min(chunkStart+baselineTickBitmapWordsPerCall-1, int(maxWord))

		words := make([]int16, 0, chunkEnd-chunkStart+1)
		calls := make([]outbound.Call, 0, cap(words))
		for w := chunkStart; w <= chunkEnd; w++ {
			data, err := a.Pack("tickBitmap", int16(w))
			if err != nil {
				return nil, fmt.Errorf("packing tickBitmap(%d): %w", w, err)
			}
			calls = append(calls, outbound.Call{Target: pool.Address, AllowFailure: false, CallData: data})
			words = append(words, int16(w))
		}

		results, err := mc.ExecuteAtHash(ctx, calls, blockHash)
		if err != nil {
			return nil, fmt.Errorf("executing tickBitmap baseline scan (words %d..%d): %w", words[0], words[len(words)-1], err)
		}
		if len(results) != len(calls) {
			return nil, fmt.Errorf("unexpected tickBitmap result count: got %d, want %d", len(results), len(calls))
		}

		for i, res := range results {
			word, err := shared.UnpackUint(a, "tickBitmap", res)
			if err != nil {
				return nil, fmt.Errorf("tickBitmap(%d) on pool %s: %w", words[i], pool.Address, err)
			}
			for bit := range 256 {
				if word.Bit(bit) == 0 {
					continue
				}
				ticks = append(ticks, wordBitToTick(words[i], uint8(bit), pool.TickSpacing))
			}
		}
	}

	slices.Sort(ticks)
	return ticks, nil
}
