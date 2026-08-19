// Package tickbitmap holds the concentrated-liquidity tick-bitmap arithmetic
// shared by the Uniswap V3 and V4 indexers. Both protocols pack initialized
// ticks identically (v4-core's TickBitmap is v3-core's, unchanged), so the
// floored-division math lives here once instead of per indexer.
package tickbitmap

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
// tickSpacing. Enumerating only this range (rather than every int16) keeps a
// full bitmap scan to O(tens) of calls instead of 65536.
func WordBounds(tickSpacing int) (int16, int16) {
	minWord := FloorDiv(FloorDiv(MinTick, tickSpacing), 256)
	maxWord := FloorDiv(FloorDiv(MaxTick, tickSpacing), 256)
	return int16(minWord), int16(maxWord)
}
