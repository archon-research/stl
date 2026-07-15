package uniswapv3

import (
	"fmt"
	"math"
	"math/big"
)

// q96 is 2^96, the fixed-point scaling factor in Uniswap V3.
var q96 = new(big.Int).Lsh(big.NewInt(1), 96)

// GetSqrtRatioAtTick computes sqrt(1.0001^tick) * 2^96.
// This is the Go equivalent of Uniswap V3's TickMath.getSqrtRatioAtTick.
func GetSqrtRatioAtTick(tick int) *big.Int {
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

// ComputePositionAmounts returns (amount0, amount1) for a V3 position
// given the current sqrt price and the position's tick range + liquidity.
func ComputePositionAmounts(sqrtPriceX96 *big.Int, tickLower, tickUpper int, liquidity *big.Int) PositionAmounts {
	sqrtRatioAX96 := GetSqrtRatioAtTick(tickLower)
	sqrtRatioBX96 := GetSqrtRatioAtTick(tickUpper)

	var amount0, amount1 *big.Int

	if sqrtPriceX96.Cmp(sqrtRatioAX96) <= 0 {
		// Current price below range — all token0.
		amount0 = getAmount0ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)
		amount1 = big.NewInt(0)
	} else if sqrtPriceX96.Cmp(sqrtRatioBX96) >= 0 {
		// Current price above range — all token1.
		amount0 = big.NewInt(0)
		amount1 = getAmount1ForLiquidity(sqrtRatioAX96, sqrtRatioBX96, liquidity)
	} else {
		// In range — split.
		amount0 = getAmount0ForLiquidity(sqrtPriceX96, sqrtRatioBX96, liquidity)
		amount1 = getAmount1ForLiquidity(sqrtRatioAX96, sqrtPriceX96, liquidity)
	}

	return PositionAmounts{Amount0: amount0, Amount1: amount1}
}

// ValueInToken0 returns the position's total value in raw token0 units:
// amount0 plus amount1 converted at the pool's current spot price.
// See ValueInToken1 for why no decimals adjustment applies.
func (a PositionAmounts) ValueInToken0(sqrtPriceX96 *big.Int) (*big.Int, error) {
	if err := a.validateForValue(sqrtPriceX96); err != nil {
		return nil, err
	}
	// amount1 -> token0: amount1 * 2^192 / sqrtPriceX96^2 (floor).
	converted := new(big.Int).Lsh(a.Amount1, 192)
	converted.Div(converted, new(big.Int).Mul(sqrtPriceX96, sqrtPriceX96))
	return converted.Add(converted, a.Amount0), nil
}

// ValueInToken1 returns the position's total value in raw token1 units:
// amount1 plus amount0 converted at the pool's current spot price.
// sqrtPriceX96 prices RAW units (token1-raw per token0-raw = (sqrtPriceX96 /
// 2^96)^2), so both tokens' decimals are already embedded in it and no
// decimals adjustment applies here.
func (a PositionAmounts) ValueInToken1(sqrtPriceX96 *big.Int) (*big.Int, error) {
	if err := a.validateForValue(sqrtPriceX96); err != nil {
		return nil, err
	}
	// amount0 -> token1: amount0 * sqrtPriceX96^2 / 2^192 (floor).
	converted := new(big.Int).Mul(a.Amount0, sqrtPriceX96)
	converted.Mul(converted, sqrtPriceX96)
	converted.Rsh(converted, 192)
	return converted.Add(converted, a.Amount1), nil
}

// validateForValue rejects inputs that would make a spot conversion silently
// wrong: a non-positive sqrt price means the pool state read is broken (an
// initialized V3 pool always has a positive sqrt price), and a nil amount
// means the position amounts were never computed.
func (a PositionAmounts) validateForValue(sqrtPriceX96 *big.Int) error {
	if a.Amount0 == nil || a.Amount1 == nil {
		return fmt.Errorf("position amounts not set (amount0=%v, amount1=%v)", a.Amount0, a.Amount1)
	}
	// Negative amounts cannot come from ComputePositionAmounts; converting one
	// would return a plausible-but-wrong value for a broken caller.
	if a.Amount0.Sign() < 0 || a.Amount1.Sign() < 0 {
		return fmt.Errorf("position amounts must be non-negative (amount0=%s, amount1=%s)", a.Amount0, a.Amount1)
	}
	if sqrtPriceX96 == nil || sqrtPriceX96.Sign() <= 0 {
		return fmt.Errorf("sqrtPriceX96 must be positive, got %v", sqrtPriceX96)
	}
	return nil
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
