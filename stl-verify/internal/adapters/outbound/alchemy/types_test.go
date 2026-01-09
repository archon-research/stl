package alchemy

import (
	"strings"
	"testing"
)

// --- Test: parseBlockNumber ---

func TestParseBlockNumber_ValidHexWithPrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0x0", 0},
		{"0x1", 1},
		{"0xa", 10},
		{"0xf", 15},
		{"0x10", 16},
		{"0xff", 255},
		{"0x100", 256},
		{"0x1234", 4660},
		{"0xdeadbeef", 3735928559},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseBlockNumber(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("parseBlockNumber(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseBlockNumber_ValidHexWithoutPrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0", 0},
		{"1", 1},
		{"a", 10},
		{"ff", 255},
		{"1234", 4660},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseBlockNumber(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("parseBlockNumber(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseBlockNumber_InvalidHex(t *testing.T) {
	tests := []string{
		"0xg",
		"0xGHI",
		"xyz",
		"0x",
		"",
		"not a number",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := parseBlockNumber(input)
			if err == nil {
				t.Errorf("parseBlockNumber(%q) should return error", input)
			}
		})
	}
}

func TestParseBlockNumber_CaseInsensitive(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0xABCDEF", 11259375},
		{"0xabcdef", 11259375},
		{"0xAbCdEf", 11259375},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseBlockNumber(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("parseBlockNumber(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

// --- Test: truncateHash ---

func TestTruncateHash_ShortHash(t *testing.T) {
	// Test with short hash (<=14 chars)
	shortHash := "0x123456"
	result := truncateHash(shortHash)
	if result != shortHash {
		t.Errorf("expected short hash to be unchanged, got %s", result)
	}

	// Test with exactly 14 chars
	hash14 := "0x123456789ab"
	result = truncateHash(hash14)
	if result != hash14 {
		t.Errorf("expected 14-char hash to be unchanged, got %s", result)
	}

	// Test with long hash (>14 chars)
	longHash := "0x1234567890abcdef1234567890abcdef"
	result = truncateHash(longHash)
	if result == longHash {
		t.Error("expected long hash to be truncated")
	}
	if !strings.HasPrefix(result, "0x123456") {
		t.Errorf("expected truncated hash to start with first 8 chars, got %s", result)
	}
	if !strings.Contains(result, "...") {
		t.Errorf("expected truncated hash to contain '...', got %s", result)
	}
}

func TestTruncateHash_EmptyString(t *testing.T) {
	result := truncateHash("")
	if result != "" {
		t.Errorf("truncateHash(\"\") = %q, want empty string", result)
	}
}

func TestTruncateHash_ExactlyFifteenChars(t *testing.T) {
	// 15 chars - should be truncated (>14)
	hash := "123456789012345"
	result := truncateHash(hash)
	if result == hash {
		t.Error("expected 15-char hash to be truncated")
	}
}

func TestTruncateHash_LongHash(t *testing.T) {
	hash := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	result := truncateHash(hash)

	// Should be truncated
	if result == hash {
		t.Error("expected hash to be truncated")
	}

	// Should start with first 8 chars
	if result[:8] != "0x123456" {
		t.Errorf("expected truncated hash to start with '0x123456', got %s", result[:8])
	}

	// Should contain ellipsis
	if !strings.Contains(result, "...") {
		t.Error("expected truncated hash to contain '...'")
	}

	// Should end with last 6 chars of original
	expectedEnd := hash[len(hash)-6:]
	if result[len(result)-6:] != expectedEnd {
		t.Errorf("expected truncated hash to end with %s, got %s", expectedEnd, result[len(result)-6:])
	}
}
