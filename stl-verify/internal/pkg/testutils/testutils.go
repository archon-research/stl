package testutils

import (
	"math/big"
	"strings"
	"testing"
)

// BigFromStr parses a decimal string into *big.Int.
// It calls t.Fatalf if the string is not a valid decimal number.
func BigFromStr(t testing.TB, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("BigFromStr: invalid decimal string: %q", s)
	}
	return n
}

// ContainsSubstring returns whether s contains substr.
func ContainsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}
