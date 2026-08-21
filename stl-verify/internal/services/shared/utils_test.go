package shared

import (
	"testing"
)

// ============================================================================
// CacheKey tests
// ============================================================================

func TestCacheKey(t *testing.T) {
	tests := []struct {
		name        string
		chainID     int64
		blockNumber int64
		version     int
		dataType    string
		expected    string
	}{
		{
			name:        "block data",
			chainID:     1,
			blockNumber: 12345,
			version:     0,
			dataType:    "block",
			expected:    "stl:1:12345:0:block",
		},
		{
			name:        "receipts data",
			chainID:     1,
			blockNumber: 12345,
			version:     0,
			dataType:    "receipts",
			expected:    "stl:1:12345:0:receipts",
		},
		{
			name:        "traces data",
			chainID:     1,
			blockNumber: 12345,
			version:     0,
			dataType:    "traces",
			expected:    "stl:1:12345:0:traces",
		},
		{
			name:        "blobs data",
			chainID:     1,
			blockNumber: 12345,
			version:     0,
			dataType:    "blobs",
			expected:    "stl:1:12345:0:blobs",
		},
		{
			name:        "reorged block version 1",
			chainID:     1,
			blockNumber: 12345,
			version:     1,
			dataType:    "block",
			expected:    "stl:1:12345:1:block",
		},
		{
			name:        "different chain",
			chainID:     137,
			blockNumber: 50000000,
			version:     0,
			dataType:    "block",
			expected:    "stl:137:50000000:0:block",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := CacheKey(tc.chainID, tc.blockNumber, tc.version, tc.dataType)
			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}
