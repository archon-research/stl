package morpho_indexer

import (
	"math/big"
	"testing"
)

func TestBigIntFromAny(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  *big.Int
	}{
		{"big.Int", big.NewInt(42), big.NewInt(42)},
		{"nil", nil, big.NewInt(0)},
		{"string", "hello", big.NewInt(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bigIntFromAny(tt.input)
			if got.Cmp(tt.want) != 0 {
				t.Errorf("bigIntFromAny() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestBigIntFromAny_IsCopy(t *testing.T) {
	original := big.NewInt(100)
	result := bigIntFromAny(original)
	result.SetInt64(999)
	if original.Int64() != 100 {
		t.Error("bigIntFromAny should return a copy, not modify the original")
	}
}
