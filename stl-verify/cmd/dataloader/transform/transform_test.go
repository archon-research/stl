package transform

import (
	"math/big"
	"testing"
)

func TestGenerateID(t *testing.T) {
	// Test determinism
	id1 := GenerateID("test-key")
	id2 := GenerateID("test-key")
	if id1 != id2 {
		t.Errorf("IDs should be deterministic: %d != %d", id1, id2)
	}

	// Test uniqueness
	id3 := GenerateID("different-key")
	if id1 == id3 {
		t.Errorf("different keys should produce different IDs")
	}

	// Test positivity
	if id1 < 0 {
		t.Errorf("ID should be positive: %d", id1)
	}
}

func TestGenerateTokenID(t *testing.T) {
	id := GenerateTokenID(1, "0xabcd1234")
	if id <= 0 {
		t.Errorf("token ID should be positive: %d", id)
	}

	// Same input should give same output
	id2 := GenerateTokenID(1, "0xabcd1234")
	if id != id2 {
		t.Errorf("token IDs should be deterministic: %d != %d", id, id2)
	}

	// Different chain should give different ID
	id3 := GenerateTokenID(5, "0xabcd1234")
	if id == id3 {
		t.Errorf("different chains should produce different IDs")
	}
}

func TestHexToBytes(t *testing.T) {
	tests := []struct {
		input    string
		expected []byte
	}{
		{"0xabcd", []byte{0xab, 0xcd}},
		{"abcd", []byte{0xab, 0xcd}},
		{"0XABCD", []byte{0xab, 0xcd}},
	}

	for _, tc := range tests {
		result, err := HexToBytes(tc.input)
		if err != nil {
			t.Errorf("HexToBytes(%s) failed: %v", tc.input, err)
			continue
		}
		if len(result) != len(tc.expected) {
			t.Errorf("HexToBytes(%s) length: expected %d, got %d", tc.input, len(tc.expected), len(result))
			continue
		}
		for i := range tc.expected {
			if result[i] != tc.expected[i] {
				t.Errorf("HexToBytes(%s)[%d]: expected %x, got %x", tc.input, i, tc.expected[i], result[i])
			}
		}
	}
}

func TestHexToAddress(t *testing.T) {
	// Valid 20-byte address
	addr := "0x1234567890123456789012345678901234567890"
	result, err := HexToAddress(addr)
	if err != nil {
		t.Fatalf("HexToAddress failed: %v", err)
	}
	if len(result) != 20 {
		t.Errorf("expected 20 bytes, got %d", len(result))
	}

	// Invalid length
	_, err = HexToAddress("0x1234")
	if err == nil {
		t.Error("expected error for short address")
	}
}

func TestNormalizeAddress(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"0xABCD1234", "abcd1234"},
		{"0XABCD", "abcd"},
		{"abcd", "abcd"},
		{"ABCD", "abcd"},
	}

	for _, tc := range tests {
		result := NormalizeAddress(tc.input)
		if result != tc.expected {
			t.Errorf("NormalizeAddress(%s): expected '%s', got '%s'", tc.input, tc.expected, result)
		}
	}
}

func TestParseChainID(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		hasError bool
	}{
		{"mainnet", 1, false},
		{"MAINNET", 1, false},
		{"ethereum", 1, false},
		{"goerli", 5, false},
		{"arbitrum", 42161, false},
		{"1", 1, false},
		{"mainnet-0xabcd", 1, false}, // user_id format
		{"unknown", 0, true},
	}

	for _, tc := range tests {
		result, err := ParseChainID(tc.input)
		if tc.hasError && err == nil {
			t.Errorf("ParseChainID(%s): expected error", tc.input)
			continue
		}
		if !tc.hasError && err != nil {
			t.Errorf("ParseChainID(%s): unexpected error: %v", tc.input, err)
			continue
		}
		if !tc.hasError && result != tc.expected {
			t.Errorf("ParseChainID(%s): expected %d, got %d", tc.input, tc.expected, result)
		}
	}
}

func TestParseChainFromUserID(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		hasError bool
	}{
		{"mainnet-0x1234567890123456789012345678901234567890", 1, false},
		{"goerli-0xabcd", 5, false},
		{"invalid", 0, true},
	}

	for _, tc := range tests {
		result, err := ParseChainFromUserID(tc.input)
		if tc.hasError && err == nil {
			t.Errorf("ParseChainFromUserID(%s): expected error", tc.input)
			continue
		}
		if !tc.hasError && err != nil {
			t.Errorf("ParseChainFromUserID(%s): unexpected error: %v", tc.input, err)
			continue
		}
		if !tc.hasError && result != tc.expected {
			t.Errorf("ParseChainFromUserID(%s): expected %d, got %d", tc.input, tc.expected, result)
		}
	}
}

func TestParseBigInt(t *testing.T) {
	tests := []struct {
		input    string
		expected *big.Int
	}{
		{"123", big.NewInt(123)},
		{"0", big.NewInt(0)},
		{"", nil},
		{"\\N", nil},
		{"1000000000000000000", big.NewInt(0).SetUint64(1e18)},
	}

	for _, tc := range tests {
		result := parseBigInt(tc.input)
		if tc.expected == nil {
			if result != nil {
				t.Errorf("parseBigInt(%s): expected nil, got %v", tc.input, result)
			}
		} else if result == nil {
			t.Errorf("parseBigInt(%s): expected %v, got nil", tc.input, tc.expected)
		} else if result.Cmp(tc.expected) != 0 {
			t.Errorf("parseBigInt(%s): expected %v, got %v", tc.input, tc.expected, result)
		}
	}
}
