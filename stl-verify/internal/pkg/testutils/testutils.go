package testutils

import (
	"math/big"
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
