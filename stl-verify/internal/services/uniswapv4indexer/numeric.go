package uniswapv4indexer

import (
	"fmt"
	"math/big"
)

const (
	minInt24  = -(1 << 23)
	maxInt24  = 1<<23 - 1
	maxUint24 = 1<<24 - 1
)

func int24Value(field string, v *big.Int) (int, error) {
	return boundedIntValue(field, v, minInt24, maxInt24)
}

func uint24Value(field string, v *big.Int) (int, error) {
	return boundedIntValue(field, v, 0, maxUint24)
}

func boundedIntValue(field string, v *big.Int, low, high int64) (int, error) {
	if v == nil {
		return 0, fmt.Errorf("%s must not be nil", field)
	}
	if !v.IsInt64() {
		return 0, fmt.Errorf("%s does not fit in an int64: %s", field, v)
	}
	n := v.Int64()
	if n < low || n > high {
		return 0, fmt.Errorf("%s must be within [%d, %d], got %d", field, low, high, n)
	}
	return int(n), nil
}
