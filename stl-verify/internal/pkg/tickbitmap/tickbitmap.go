// Package tickbitmap holds the concentrated-liquidity tick-bitmap arithmetic
// shared by the Uniswap V3 and V4 indexers. Both protocols pack initialized
// ticks identically (v4-core's TickBitmap is v3-core's, unchanged), so the
// floored-division math lives here once instead of per indexer.
package tickbitmap

import "slices"

// MinTick and MaxTick are TickMath's usable tick bounds, identical in v3-core
// and v4-core. They are the widest range any pool can report regardless of
// tickSpacing, and are distinct from the int24 wire bounds entity validation
// uses.
const (
	MinTick = -887272
	MaxTick = 887272
)

// FloorDiv implements floored (as opposed to Go's truncated) integer division:
// FloorDiv(-1, 256) == -1, whereas Go's native -1/256 == 0. The tick bitmap
// packs ticks with floored semantics (Solidity's arithmetic shift on a signed
// int24), so Go's native operator would silently misplace every negative tick
// into the wrong bitmap word.
func FloorDiv(a, b int) int {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// WordBitToTick recovers the tick at a given bitmap word/bit for a pool with
// the given tickSpacing, inverting Solidity's int16(compressed >> 8) /
// uint8(compressed % 256) packing.
func WordBitToTick(word int16, bit uint8, tickSpacing int) int32 {
	compressed := int(word)*256 + int(bit)
	return int32(compressed * tickSpacing)
}

// WordBounds returns the inclusive [minWord, maxWord] range of bitmap word
// positions that can hold an initialized tick for a pool with the given
// tickSpacing. Enumerating only this range (rather than every int16) bounds a
// full bitmap scan at 6,932 words for tickSpacing 1 and 36 for 200, instead of
// 65,536.
//
// tickSpacing must be >= 1, which every pool registry enforces (a v4-core
// PoolKey with tickSpacing 0 cannot initialize); 0 divides by zero here.
func WordBounds(tickSpacing int) (int16, int16) {
	minWord := FloorDiv(FloorDiv(MinTick, tickSpacing), 256)
	maxWord := FloorDiv(FloorDiv(MaxTick, tickSpacing), 256)
	return int16(minWord), int16(maxWord)
}

// TicksPerCall and BitmapWordsPerCall bound how many per-tick getter and how
// many bitmap-word sub-calls an indexer packs into one multicall3 aggregate
// call. A dense pool's first touch enumerates O(10³) initialized ticks and, at
// tickSpacing 1, ~6932 bitmap words; sending either set in one aggregate call
// risks exceeding an RPC provider's request/response/gas caps. 500 keeps the
// worst case to ~14 batches.
const (
	TicksPerCall       = 500
	BitmapWordsPerCall = 500
)

// MergeTickSets returns the deduplicated, ascending-sorted union of two tick
// sets: a pool's first-touch persist must write every initialized tick exactly
// once, even where the bitmap baseline and the block's own event bounds overlap.
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
