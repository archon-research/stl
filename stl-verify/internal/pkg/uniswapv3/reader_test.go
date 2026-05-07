package uniswapv3

import (
	"math/big"
	"testing"
)

// ---------------------------------------------------------------------------
// Tick math tests
// ---------------------------------------------------------------------------

func TestGetSqrtRatioAtTick_Zero(t *testing.T) {
	// tick 0 → sqrt(1.0001^0) = 1.0 → 1 * 2^96
	result := GetSqrtRatioAtTick(0)
	if result.Cmp(q96) != 0 {
		t.Errorf("GetSqrtRatioAtTick(0) = %s, want %s (2^96)", result, q96)
	}
}

func TestGetSqrtRatioAtTick_Symmetric(t *testing.T) {
	// sqrt(1.0001^tick) * sqrt(1.0001^-tick) should ≈ (2^96)^2
	pos := GetSqrtRatioAtTick(100)
	neg := GetSqrtRatioAtTick(-100)

	product := new(big.Int).Mul(pos, neg)
	q96Squared := new(big.Int).Mul(q96, q96)

	// Allow 0.01% tolerance for float imprecision.
	diff := new(big.Int).Sub(product, q96Squared)
	diff.Abs(diff)

	tolerance := new(big.Int).Div(q96Squared, big.NewInt(10000))
	if diff.Cmp(tolerance) > 0 {
		t.Errorf("tick symmetry violated: pos*neg = %s, want ≈ %s", product, q96Squared)
	}
}

func TestComputePositionAmounts_InRange(t *testing.T) {
	// Stablecoin pair: ticks -1 to 1, price at tick 0 (1:1).
	sqrtPriceX96 := GetSqrtRatioAtTick(0)
	liquidity := big.NewInt(250012499687515624)

	amounts := ComputePositionAmounts(sqrtPriceX96, -1, 1, liquidity)

	if amounts.Amount0.Sign() <= 0 {
		t.Errorf("amount0 should be positive in-range, got %s", amounts.Amount0)
	}
	if amounts.Amount1.Sign() <= 0 {
		t.Errorf("amount1 should be positive in-range, got %s", amounts.Amount1)
	}
}

func TestComputePositionAmounts_BelowRange(t *testing.T) {
	// Price below range — should be all token0, zero token1.
	sqrtPriceX96 := GetSqrtRatioAtTick(-10)
	liquidity := big.NewInt(1000000)

	amounts := ComputePositionAmounts(sqrtPriceX96, -1, 1, liquidity)

	if amounts.Amount0.Sign() <= 0 {
		t.Errorf("amount0 should be positive below range, got %s", amounts.Amount0)
	}
	if amounts.Amount1.Sign() != 0 {
		t.Errorf("amount1 should be zero below range, got %s", amounts.Amount1)
	}
}

func TestComputePositionAmounts_AboveRange(t *testing.T) {
	// Price above range — should be all token1, zero token0.
	sqrtPriceX96 := GetSqrtRatioAtTick(10)
	liquidity := big.NewInt(1000000)

	amounts := ComputePositionAmounts(sqrtPriceX96, -1, 1, liquidity)

	if amounts.Amount0.Sign() != 0 {
		t.Errorf("amount0 should be zero above range, got %s", amounts.Amount0)
	}
	if amounts.Amount1.Sign() <= 0 {
		t.Errorf("amount1 should be positive above range, got %s", amounts.Amount1)
	}
}

func TestComputePositionAmounts_ZeroLiquidity(t *testing.T) {
	sqrtPriceX96 := GetSqrtRatioAtTick(0)
	liquidity := big.NewInt(0)

	amounts := ComputePositionAmounts(sqrtPriceX96, -1, 1, liquidity)

	if amounts.Amount0.Sign() != 0 {
		t.Errorf("amount0 should be zero for zero liquidity, got %s", amounts.Amount0)
	}
	if amounts.Amount1.Sign() != 0 {
		t.Errorf("amount1 should be zero for zero liquidity, got %s", amounts.Amount1)
	}
}

func TestGetAmount0ForLiquidity_OrderIndependent(t *testing.T) {
	a := GetSqrtRatioAtTick(-1)
	b := GetSqrtRatioAtTick(1)
	liq := big.NewInt(1000000)

	result1 := getAmount0ForLiquidity(a, b, liq)
	result2 := getAmount0ForLiquidity(b, a, liq)

	if result1.Cmp(result2) != 0 {
		t.Errorf("getAmount0ForLiquidity should be order-independent: %s vs %s", result1, result2)
	}
}

func TestGetAmount1ForLiquidity_OrderIndependent(t *testing.T) {
	a := GetSqrtRatioAtTick(-1)
	b := GetSqrtRatioAtTick(1)
	liq := big.NewInt(1000000)

	result1 := getAmount1ForLiquidity(a, b, liq)
	result2 := getAmount1ForLiquidity(b, a, liq)

	if result1.Cmp(result2) != 0 {
		t.Errorf("getAmount1ForLiquidity should be order-independent: %s vs %s", result1, result2)
	}
}
