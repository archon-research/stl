// Package tickbitmap holds the concentrated-liquidity tick-bitmap arithmetic
// the Uniswap V3 and V4 indexers share: v4-core's TickBitmap is v3-core's.
package tickbitmap

import (
	"fmt"
	"slices"
)

// MinTick and MaxTick are TickMath's usable bounds, identical in v3-core and
// v4-core, and distinct from the int24 wire bounds entity validation uses.
const (
	MinTick = -887272
	MaxTick = 887272
)

// FloorDiv is floored, not Go-truncated, division: FloorDiv(-1, 256) == -1
// where Go's -1/256 == 0. The bitmap packs ticks with Solidity's arithmetic
// shift, so the native operator misplaces every negative tick.
func FloorDiv(a, b int) int {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// WordBitToTick inverts Solidity's int16(compressed >> 8) /
// uint8(compressed % 256) tick packing.
func WordBitToTick(word int16, bit uint8, tickSpacing int) int32 {
	compressed := int(word)*256 + int(bit)
	return int32(compressed * tickSpacing)
}

// WordBounds returns the inclusive word range that can hold an initialized
// tick: 6,932 words for tickSpacing 1 and 36 for 200, not 65,536. Below
// tickSpacing 1 the range inverts and empties a scan loop, so it errors.
func WordBounds(tickSpacing int) (int16, int16, error) {
	if tickSpacing < 1 {
		return 0, 0, fmt.Errorf("tick spacing %d is below 1", tickSpacing)
	}
	minWord := FloorDiv(FloorDiv(MinTick, tickSpacing), 256)
	maxWord := FloorDiv(FloorDiv(MaxTick, tickSpacing), 256)
	return int16(minWord), int16(maxWord), nil
}

// TicksPerCall and BitmapWordsPerCall cap one multicall3 aggregate: a dense
// pool's first touch enumerates O(10³) ticks and ~6932 bitmap words at
// tickSpacing 1, past an RPC provider's request/response/gas caps in one call.
const (
	TicksPerCall       = 500
	BitmapWordsPerCall = 500
)

// MergeTickSets returns the deduplicated, ascending union: a first-touch
// persist must write every initialized tick exactly once, even where the
// bitmap baseline and the block's own event bounds overlap.
func MergeTickSets(a, b []int32) []int32 {
	seen := make(map[int32]struct{}, len(a)+len(b))
	out := make([]int32, 0, len(a)+len(b))
	for _, set := range [][]int32{a, b} {
		for _, tick := range set {
			if _, ok := seen[tick]; ok {
				continue
			}
			seen[tick] = struct{}{}
			out = append(out, tick)
		}
	}
	slices.Sort(out)
	return out
}
