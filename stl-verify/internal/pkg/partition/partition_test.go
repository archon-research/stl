package partition

import (
	"fmt"
	"testing"
)

func TestGetPartition(t *testing.T) {
	tests := []struct {
		blockNumber int64
		expected    string
	}{
		// First partition: 0-999 (1000 blocks)
		{0, "0-999"},
		{1, "0-999"},
		{500, "0-999"},
		{999, "0-999"},

		// Second partition: 1000-1999 (1000 blocks)
		{1000, "1000-1999"},
		{1500, "1000-1999"},
		{1999, "1000-1999"},

		// Third partition: 2000-2999 (1000 blocks)
		{2000, "2000-2999"},
		{2999, "2000-2999"},

		// Large block numbers
		{10000, "10000-10999"},
		{10999, "10000-10999"},
		{1000000, "1000000-1000999"},
		{1000999, "1000000-1000999"},

		// Edge cases at partition boundaries
		{BlockRangeSize - 1, "0-999"},
		{BlockRangeSize, "1000-1999"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("block_%d", tt.blockNumber), func(t *testing.T) {
			result := GetPartition(tt.blockNumber)
			if result != tt.expected {
				t.Errorf("GetPartition(%d) = %s, expected %s", tt.blockNumber, result, tt.expected)
			}
		})
	}
}
