package tickbitmap

import (
	"fmt"
	"slices"
	"testing"
)

// floorMod implements floored (as opposed to Go's truncated) modulo:
// floorMod(-1, 256) == 255, whereas Go's native -1%256 == -1.
func floorMod(a, b int) int {
	m := a % b
	if m != 0 && ((m < 0) != (b < 0)) {
		m += b
	}
	return m
}

// tickToWordBit is the test-local reference inverse of WordBitToTick.
func tickToWordBit(tick int32, tickSpacing int) (int16, uint8) {
	compressed := FloorDiv(int(tick), tickSpacing)
	return int16(FloorDiv(compressed, 256)), uint8(floorMod(compressed, 256))
}

func TestFloorDiv(t *testing.T) {
	tests := []struct {
		name string
		a, b int
		want int
	}{
		{name: "exact positive", a: 512, b: 256, want: 2},
		{name: "truncating positive", a: 300, b: 256, want: 1},
		{name: "zero", a: 0, b: 256, want: 0},
		{name: "negative floors down, not toward zero", a: -1, b: 256, want: -1},
		{name: "exact negative does not over-floor", a: -256, b: 256, want: -1},
		{name: "negative one past a boundary", a: -257, b: 256, want: -2},
		{name: "negative with spacing", a: -887272, b: 60, want: -14788},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FloorDiv(tt.a, tt.b); got != tt.want {
				t.Errorf("FloorDiv(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestWordBitToTick_RoundTrips(t *testing.T) {
	tests := []struct {
		name        string
		tick        int32
		tickSpacing int
		wantWord    int16
		wantBit     uint8
	}{
		{name: "zero tick", tick: 0, tickSpacing: 60, wantWord: 0, wantBit: 0},
		{name: "first bit of word 0", tick: 60, tickSpacing: 60, wantWord: 0, wantBit: 1},
		{name: "crossing into word 1", tick: 60 * 256, tickSpacing: 60, wantWord: 1, wantBit: 0},
		{name: "highest bit of word -1", tick: -60, tickSpacing: 60, wantWord: -1, wantBit: 255},
		{name: "lowest bit of word -1", tick: -60 * 256, tickSpacing: 60, wantWord: -1, wantBit: 0},
		{name: "one past word -1 falls to word -2", tick: -60 * 257, tickSpacing: 60, wantWord: -2, wantBit: 255},
		{name: "tick spacing 1", tick: -1, tickSpacing: 1, wantWord: -1, wantBit: 255},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWord, gotBit := tickToWordBit(tt.tick, tt.tickSpacing)
			if gotWord != tt.wantWord || gotBit != tt.wantBit {
				t.Errorf("tickToWordBit(%d, %d) = (%d, %d), want (%d, %d)", tt.tick, tt.tickSpacing, gotWord, gotBit, tt.wantWord, tt.wantBit)
			}
			if got := WordBitToTick(gotWord, gotBit, tt.tickSpacing); got != tt.tick {
				t.Errorf("WordBitToTick(%d, %d, %d) = %d, want %d (round-trip)", gotWord, gotBit, tt.tickSpacing, got, tt.tick)
			}
		})
	}
}

// TestWordBounds_CoversUsableRange checks the enumerated word range really
// brackets MIN_TICK/MAX_TICK for each tick spacing in use, and that it stays
// far below the 65536 words a naive full-int16 scan would issue.
func TestWordBounds_CoversUsableRange(t *testing.T) {
	for _, tickSpacing := range []int{1, 10, 50, 60, 200} {
		t.Run(fmt.Sprintf("tickSpacing=%d", tickSpacing), func(t *testing.T) {
			minWord, maxWord := WordBounds(tickSpacing)

			gotMin, _ := tickToWordBit(int32(MinTick), tickSpacing)
			gotMax, _ := tickToWordBit(int32(MaxTick), tickSpacing)
			if minWord != gotMin {
				t.Errorf("WordBounds(%d) min = %d, want %d (the word holding MIN_TICK)", tickSpacing, minWord, gotMin)
			}
			if maxWord != gotMax {
				t.Errorf("WordBounds(%d) max = %d, want %d (the word holding MAX_TICK)", tickSpacing, maxWord, gotMax)
			}
			if words := int(maxWord) - int(minWord) + 1; words >= 65536 {
				t.Errorf("WordBounds(%d) spans %d words, want far fewer than a full int16 scan", tickSpacing, words)
			}
		})
	}
}

// TestMergeTickSets_DedupsAndSortsOverlappingRanges pins the union both
// indexers rely on: a first-touch persist must write each initialized tick once
// even when the bitmap baseline and the block's own bounds interleave.
func TestMergeTickSets_DedupsAndSortsOverlappingRanges(t *testing.T) {
	touched := []int32{180, -120}
	baseline := []int32{60, -120, 300}

	got := MergeTickSets(touched, baseline)

	want := []int32{-120, 60, 180, 300}
	if !slices.Equal(got, want) {
		t.Errorf("MergeTickSets() = %v, want %v", got, want)
	}
}

func TestMergeTickSets_EmptyInputs(t *testing.T) {
	if got := MergeTickSets(nil, nil); len(got) != 0 {
		t.Errorf("MergeTickSets(nil, nil) = %v, want empty", got)
	}
}
