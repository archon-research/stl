package uniswapv4indexer

import (
	"fmt"
	"math/big"
)

// Solidity's int24/uint24 bounds. Every v4-core tick and fee argument decodes
// to *big.Int, and big.Int.Int64 on an out-of-range value is undefined — an
// unchecked conversion would land a nonsense tick or fee in a NOT NULL column.
const (
	minInt24  = -(1 << 23)
	maxInt24  = 1<<23 - 1
	maxUint24 = 1<<24 - 1
)

// int24Value converts an ABI-decoded int24 to int, erroring on anything the
// width cannot hold.
func int24Value(field string, v *big.Int) (int, error) {
	return boundedIntValue(field, v, minInt24, maxInt24)
}

// uint24Value converts an ABI-decoded uint24 to int, erroring on anything the
// width cannot hold.
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
