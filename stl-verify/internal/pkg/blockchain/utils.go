package blockchain

import "math/big"

func ConvertToDecimalAdjusted(amount *big.Int, decimals int) *big.Float {
	if amount == nil {
		return big.NewFloat(0)
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	result := new(big.Float).SetInt(amount)
	divisorFloat := new(big.Float).SetInt(divisor)
	return result.Quo(result, divisorFloat)
}
