package curveindexer

import (
	"encoding/hex"
	"math/big"
	"testing"
)

// The two selectors are what the pools actually answer on: a pool implementing
// one reverts on the other, which is why the shape is curated per pool rather
// than assumed.
func TestPackCalcTokenAmountSelectorPerArgumentShape(t *testing.T) {
	tests := []struct {
		name     string
		amounts  []*big.Int
		dynArray bool
		selector string
		bodyLen  int
	}{
		{
			name:     "fixed uint256[2]",
			amounts:  []*big.Int{big.NewInt(1), big.NewInt(2)},
			dynArray: false,
			selector: "ed8e84f3",
			bodyLen:  3 * 32, // two inline words + is_deposit
		},
		{
			name:     "fixed uint256[3]",
			amounts:  []*big.Int{big.NewInt(1), big.NewInt(2), big.NewInt(3)},
			dynArray: false,
			selector: "3883e119",
			bodyLen:  4 * 32,
		},
		{
			name:     "dynamic uint256[]",
			amounts:  []*big.Int{big.NewInt(1), big.NewInt(2)},
			dynArray: true,
			selector: "3db06dd8",
			bodyLen:  5 * 32, // offset, is_deposit, length, two words
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := packCalcTokenAmount(tc.amounts, true, tc.dynArray)
			if err != nil {
				t.Fatalf("packCalcTokenAmount: %v", err)
			}
			if got := hex.EncodeToString(data[:4]); got != tc.selector {
				t.Errorf("selector = %s, want %s", got, tc.selector)
			}
			if got := len(data) - 4; got != tc.bodyLen {
				t.Errorf("body len = %d, want %d", got, tc.bodyLen)
			}
		})
	}
}
