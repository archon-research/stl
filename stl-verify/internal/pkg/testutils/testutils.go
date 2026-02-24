package testutils

import (
	"math/big"
	"strings"
)

// BigFromStr parses a decimal string into *big.Int.
func BigFromStr(s string) *big.Int {
	n := new(big.Int)
	n.SetString(s, 10)
	return n
}

// ContainsSubstring returns whether s contains substr.
func ContainsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}
