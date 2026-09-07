package entity

import "testing"

func TestIsCanonicalDecimal(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"0", true},
		{"100", true},
		{"100.5", true},
		{"0.000000000000001234", true},
		{"12345.678901234567890", true},
		{"00100", true}, // leading zeros pass; the check does not collapse zero-padding (see doc)
		{"0.00000000", true},
		{"", false},      // empty
		{".", false},     // dot only
		{".5", false},    // no integer digit
		{"1.", false},    // trailing dot
		{"1.2.3", false}, // two dots
		{"-1", false},    // sign
		{"+5", false},    // sign
		{"1e5", false},   // exponent
		{"1E3", false},   // exponent
		{"1.0e3", false}, // exponent
		{"NaN", false},   // not a number
		{"Inf", false},   // not finite
		{" 1", false},    // whitespace
		{"1 ", false},    // whitespace
		{"0x10", false},  // hex
	}
	for _, tt := range tests {
		if got := IsCanonicalDecimal(tt.in); got != tt.want {
			t.Errorf("IsCanonicalDecimal(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestIsZeroDecimal(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"0", true},
		{"0.0", true},
		{"0.00000000", true},
		{"00", true},
		{"00.00", true},
		{"1", false},
		{"0.0001", false},
		{"100", false},
		{"", false},
		{".", false},  // non-canonical: no integer digit
		{"0.", false}, // non-canonical: trailing dot
		{".0", false}, // non-canonical: leading dot
	}
	for _, tt := range tests {
		if got := IsZeroDecimal(tt.in); got != tt.want {
			t.Errorf("IsZeroDecimal(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestCompareDecimal(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"100", "100", 0},
		{"100", "99", 1},
		{"99", "100", -1},
		{"100.0", "100", 0},    // trailing fractional zeros don't matter
		{"100.50", "100.5", 0}, // 100.50 == 100.5
		{"0.5", "0.49", 1},     // 0.5 > 0.49
		{"0.5", "0.50", 0},     // 0.5 == 0.50
		{"0.49", "0.5", -1},
		{"0010", "10", 0}, // leading zeros don't matter
		{"9", "10", -1},   // length-aware integer compare (not lexical)
		{"10", "9", 1},
		{"1.0001", "1.001", -1}, // fractional precision past float-ish boundaries
		// Two prices differing only in the 17th significant digit: float64 would
		// treat these as equal, decimal comparison must not.
		{"100000.00000000001", "100000.00000000002", -1},
		{"100000.00000000002", "100000.00000000001", 1},
	}
	for _, tt := range tests {
		if got := CompareDecimal(tt.a, tt.b); got != tt.want {
			t.Errorf("CompareDecimal(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}
