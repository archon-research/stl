package blockchain

import (
	"math/big"
	"testing"
)

func TestConvertToDecimalAdjusted(t *testing.T) {
	tests := []struct {
		name     string
		amount   *big.Int
		decimals int
		expected string
	}{
		{
			name:     "18 decimals - 1 ETH",
			amount:   new(big.Int).SetInt64(1000000000000000000),
			decimals: 18,
			expected: "1",
		},
		{
			name:     "6 decimals - 1 USDC",
			amount:   new(big.Int).SetInt64(1000000),
			decimals: 6,
			expected: "1",
		},
		{
			name:     "18 decimals - 0.5 ETH",
			amount:   new(big.Int).SetInt64(500000000000000000),
			decimals: 18,
			expected: "0.5",
		},
		{
			name:     "nil amount",
			amount:   nil,
			decimals: 18,
			expected: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertToDecimalAdjusted(tt.amount, tt.decimals)
			if result.String() != tt.expected {
				t.Errorf("got %s, want %s", result.String(), tt.expected)
			}
		})
	}
}
