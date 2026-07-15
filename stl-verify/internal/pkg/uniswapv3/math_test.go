package uniswapv3

import (
	"math/big"
	"testing"
)

// pow2 returns 2^n as a big.Int.
func pow2(n uint) *big.Int {
	return new(big.Int).Lsh(big.NewInt(1), n)
}

// mustBig parses a base-10 big.Int literal.
func mustBig(t *testing.T, s string) *big.Int {
	t.Helper()
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("bad big.Int literal %q", s)
	}
	return v
}

// All expected values are hand-computed from the raw-unit price identity
// price(token1-raw per token0-raw) = (sqrtPriceX96 / 2^96)^2, using powers of
// two so every conversion is exact.
func TestPositionAmounts_ValueInToken1(t *testing.T) {
	tests := []struct {
		name         string
		amount0      string
		amount1      string
		sqrtPriceX96 *big.Int
		want         string
	}{
		{"price 1 adds both sides", "5", "7", pow2(96), "12"},
		{"price 4 scales token0 up", "10", "3", pow2(97), "43"},
		{"one-sided all token1 passes through", "0", "9", pow2(97), "9"},
		{"one-sided all token0 converts", "10", "0", pow2(97), "40"},
		{"tiny price 2^-20 with large amount0", "1000000000000000000000000000000", "0", pow2(86), "953674316406250000000000"},
		{"zero position", "0", "0", pow2(96), "0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := PositionAmounts{Amount0: mustBig(t, tc.amount0), Amount1: mustBig(t, tc.amount1)}
			got, err := a.ValueInToken1(tc.sqrtPriceX96)
			if err != nil {
				t.Fatalf("ValueInToken1: %v", err)
			}
			if got.String() != tc.want {
				t.Fatalf("ValueInToken1 = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestPositionAmounts_ValueInToken0(t *testing.T) {
	tests := []struct {
		name         string
		amount0      string
		amount1      string
		sqrtPriceX96 *big.Int
		want         string
	}{
		{"price 1 adds both sides", "5", "7", pow2(96), "12"},
		{"price 4 scales token1 down", "10", "40", pow2(97), "20"},
		{"one-sided all token0 passes through", "10", "0", pow2(97), "10"},
		{"one-sided all token1 converts", "0", "40", pow2(97), "10"},
		{"division truncates toward zero", "0", "3", pow2(97), "0"},
		{"zero position", "0", "0", pow2(96), "0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := PositionAmounts{Amount0: mustBig(t, tc.amount0), Amount1: mustBig(t, tc.amount1)}
			got, err := a.ValueInToken0(tc.sqrtPriceX96)
			if err != nil {
				t.Fatalf("ValueInToken0: %v", err)
			}
			if got.String() != tc.want {
				t.Fatalf("ValueInToken0 = %s, want %s", got, tc.want)
			}
		})
	}
}

// A zero, negative, or nil sqrtPriceX96 means the pool state read is broken
// (an initialized V3 pool always has a positive sqrt price); converting with
// it would silently zero one side, so both directions must reject it.
func TestPositionAmounts_ValueIn_RejectsInvalidSqrtPrice(t *testing.T) {
	invalid := []struct {
		name         string
		sqrtPriceX96 *big.Int
	}{
		{"zero", big.NewInt(0)},
		{"negative", big.NewInt(-1)},
		{"nil", nil},
	}
	a := PositionAmounts{Amount0: big.NewInt(1), Amount1: big.NewInt(1)}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := a.ValueInToken0(tc.sqrtPriceX96); err == nil {
				t.Error("ValueInToken0: expected error, got nil")
			}
			if _, err := a.ValueInToken1(tc.sqrtPriceX96); err == nil {
				t.Error("ValueInToken1: expected error, got nil")
			}
		})
	}
}

// A nil amount means the position amounts were never computed; a negative
// amount cannot come from ComputePositionAmounts, so it signals a broken
// caller whose result would be plausible-but-wrong.
func TestPositionAmounts_ValueIn_RejectsInvalidAmounts(t *testing.T) {
	tests := []struct {
		name string
		a    PositionAmounts
	}{
		{"nil amount0", PositionAmounts{Amount0: nil, Amount1: big.NewInt(1)}},
		{"nil amount1", PositionAmounts{Amount0: big.NewInt(1), Amount1: nil}},
		{"negative amount0", PositionAmounts{Amount0: big.NewInt(-1), Amount1: big.NewInt(1)}},
		{"negative amount1", PositionAmounts{Amount0: big.NewInt(1), Amount1: big.NewInt(-1)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.a.ValueInToken0(pow2(96)); err == nil {
				t.Error("ValueInToken0: expected error, got nil")
			}
			if _, err := tc.a.ValueInToken1(pow2(96)); err == nil {
				t.Error("ValueInToken1: expected error, got nil")
			}
		})
	}
}
