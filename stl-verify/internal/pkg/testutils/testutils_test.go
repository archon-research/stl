package testutils

import (
	"math/big"
	"testing"
)

func TestBigFromStr_ValidInput(t *testing.T) {
	got := BigFromStr(t, "12345")
	want := big.NewInt(12345)
	if got.Cmp(want) != 0 {
		t.Errorf("BigFromStr(t, %q) = %s, want %s", "12345", got, want)
	}
}

func TestBigFromStr_LargeNumber(t *testing.T) {
	s := "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	got := BigFromStr(t, s)
	if got.String() != s {
		t.Errorf("BigFromStr(t, %q) = %s", s, got)
	}
}
