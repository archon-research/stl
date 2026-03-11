package allocation_tracker

import (
	"math"
	"math/big"
)

// Q96 is 2^96, used as the fixed-point scaling factor in Uniswap V3.
var q96 = new(big.Int).Lsh(big.NewInt(1), 96)

// getSqrtRatioAtTick computes sqrt(1.0001^tick) * 2^96.
// This is the Go equivalent of Uniswap V3's TickMath.getSqrtRatioAtTick.
func getSqrtRatioAtTick(tick int) *big.Int {
	// Use high-precision float to compute sqrt(1.0001^tick) * 2^96.
	// For production use with extreme ticks, a lookup-table approach
	// (matching Solidity's TickMath) would be more precise, but this
	// is accurate to ~15 significant digits which is sufficient for
	// balance computation.
	ratio := math.Pow(1.0001, float64(tick)/2.0)

	// Scale by 2^96.
	ratioF := new(big.Float).SetPrec(256).SetFloat64(ratio)
	q96F := new(big.Float).SetPrec(256).SetInt(q96)
	ratioF.Mul(ratioF, q96F)

	result, _ := ratioF.Int(new(big.Int))
	return result
}

// getAmount0ForLiquidity computes the amount of token0 for a given liquidity
// and sqrt price range. Matches Uniswap V3's LiquidityAmounts.getAmount0ForLiquidity.
func getAmount0ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity *big.Int) *big.Int {
	if sqrtRatioAX96.Cmp(sqrtRatioBX96) > 0 {
		sqrtRatioAX96, sqrtRatioBX96 = sqrtRatioBX96, sqrtRatioAX96
	}

	diff := new(big.Int).Sub(sqrtRatioBX96, sqrtRatioAX96)

	// liquidity * 2^96 * (sqrtB - sqrtA) / sqrtB / sqrtA
	numerator := new(big.Int).Mul(liquidity, q96)
	numerator.Mul(numerator, diff)

	denominator := new(big.Int).Mul(sqrtRatioAX96, sqrtRatioBX96)

	return new(big.Int).Div(numerator, denominator)
}

// getAmount1ForLiquidity computes the amount of token1 for a given liquidity
// and sqrt price range. Matches Uniswap V3's LiquidityAmounts.getAmount1ForLiquidity.
func getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity *big.Int) *big.Int {
	if sqrtRatioAX96.Cmp(sqrtRatioBX96) > 0 {
		sqrtRatioAX96, sqrtRatioBX96 = sqrtRatioBX96, sqrtRatioAX96
	}

	diff := new(big.Int).Sub(sqrtRatioBX96, sqrtRatioAX96)

	// liquidity * (sqrtB - sqrtA) / 2^96
	numerator := new(big.Int).Mul(liquidity, diff)

	return new(big.Int).Div(numerator, q96)
}

// computePositionAmounts returns (amount0, amount1) for a V3 position
// given the current sqrt price and the position's tick range + liquidity.
func computePositionAmounts(sqrtPriceX96 *big.Int, tickLower, tickUpper int, liquidity *big.Int) (amount0, amount1 *big.Int) {
	sqrtRatioAX96 := getSqrtRatioAtTick(tickLower)
	sqrtRatioBX96 := getSqrtRatioAtTick(tickUpper)

	if sqrtPriceX96.Cmp(sqrtRatioAX96) <= 0 {
		// Current price below range — all token0.
		return getAmount0ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity), big.NewInt(0)
	}

	if sqrtPriceX96.Cmp(sqrtRatioBX96) >= 0 {
		// Current price above range — all token1.
		return big.NewInt(0), getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)
	}

	// In range — split.
	return getAmount0ForLiquidity(sqrtPriceX96, sqrtRatioBX96, liquidity),
		getAmount1ForLiquidity(sqrtRatioAX96, sqrtPriceX96, liquidity)
}
