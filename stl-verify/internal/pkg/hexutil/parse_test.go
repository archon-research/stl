package hexutil

import (
	"testing"
)

func TestParseInt64_ValidHexWithPrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0x0", 0},
		{"0x1", 1},
		{"0xa", 10},
		{"0xf", 15},
		{"0x10", 16},
		{"0x64", 100},
		{"0x3e8", 1000},
		{"0x186a0", 100000},
		{"0xffffffffff", 1099511627775}, // Large number
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result, err := ParseInt64(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}

func TestParseInt64_ValidHexWithoutPrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0", 0},
		{"1", 1},
		{"a", 10},
		{"64", 100},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result, err := ParseInt64(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}

func TestParseInt64_InvalidHex(t *testing.T) {
	tests := []string{
		"not_hex",
		"0xGHI",
		"xyz",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := ParseInt64(input)
			if err == nil {
				t.Errorf("expected error for invalid input: %s", input)
			}
		})
	}
}

func TestParseInt64_EmptyString(t *testing.T) {
	_, err := ParseInt64("")
	if err == nil {
		t.Error("expected error for empty string")
	}
}

func TestParseInt64_UppercaseHex(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0xA", 10},
		{"0xF", 15},
		{"0xFF", 255},
		{"0xABCD", 43981},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result, err := ParseInt64(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, result)
			}
		})
	}
}
