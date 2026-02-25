package testutils

import (
	"math/big"
	"testing"
)

func TestBigFromStr(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want *big.Int
	}{
		{"positive integer", "12345", big.NewInt(12345)},
		{"zero", "0", big.NewInt(0)},
		{"negative", "-42", big.NewInt(-42)},
		{"max uint256", "115792089237316195423570985008687907853269984665640564039457584007913129639935", func() *big.Int {
			n, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
			return n
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BigFromStr(t, tt.s)
			if got.Cmp(tt.want) != 0 {
				t.Errorf("BigFromStr(t, %q) = %s, want %s", tt.s, got, tt.want)
			}
		})
	}
}
