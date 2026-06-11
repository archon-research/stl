package entity

import "strings"

// Decimal-string semantics shared by the order book and its adapters. Prices and
// sizes are kept as exact text, never float64: a float loses precision past ~15-17
// significant digits and lets two spellings of one number ("1e5" vs "100000") key
// the book map differently, stranding an un-deletable level.

// IsCanonicalDecimal reports whether s is a plain non-negative fixed-point
// decimal: one or more integer digits, optionally followed by a single dot and
// one or more fractional digits. It rejects signs, exponents ("1e5"), NaN/Inf,
// whitespace, a leading or trailing dot, and the empty string.
//
// This blocks the dangerous cross-spellings ("1e5" vs "100000") that would key
// one price under two map entries. It does NOT collapse zero-padding ("00100",
// "100.50"): the venue text is kept verbatim, so a delete matching its insert
// relies on the venue formatting a price consistently within a stream, not on
// this check normalising spellings.
func IsCanonicalDecimal(s string) bool {
	if s == "" {
		return false
	}
	var intDigits, fracDigits int
	dot := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			if dot {
				fracDigits++
			} else {
				intDigits++
			}
		case c == '.':
			if dot {
				return false // a second dot
			}
			dot = true
		default:
			return false // sign, exponent, letter, whitespace, ...
		}
	}
	if intDigits == 0 {
		return false // "", ".", ".5"
	}
	if dot && fracDigits == 0 {
		return false // trailing dot, e.g. "1."
	}
	return true
}

// IsZeroDecimal reports whether s is a canonical decimal equal to zero (e.g. "0",
// "0.0", "0.00000000"). A zero size is the exchange convention for "remove this
// level". Callers gate on IsCanonicalDecimal first; a string of only '0' and '.'
// characters is zero.
func IsZeroDecimal(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c != '0' && c != '.' {
			return false
		}
	}
	return true
}

// CompareDecimal orders two canonical non-negative decimal strings, returning -1,
// 0 or 1. It compares digit-by-digit without converting to float64, so it is
// exact at every magnitude (float64 could otherwise mis-order two levels that
// differ only past its precision, dropping the wrong one at a deep book's trim
// boundary). Inputs are assumed canonical (IsCanonicalDecimal).
func CompareDecimal(a, b string) int {
	aInt, aFrac := splitDecimal(a)
	bInt, bFrac := splitDecimal(b)
	if c := compareIntPart(aInt, bInt); c != 0 {
		return c
	}
	return compareFracPart(aFrac, bFrac)
}

// splitDecimal splits a canonical decimal into its integer and fractional digit
// strings; the fractional part is empty when there is no dot.
func splitDecimal(s string) (intPart, fracPart string) {
	if before, after, ok := strings.Cut(s, "."); ok {
		return before, after
	}
	return s, ""
}

// compareIntPart compares two integer-digit strings by numeric value: with
// leading zeros stripped, the longer string is the larger number, and equal
// lengths compare lexically (which equals numeric order for same-length digits).
func compareIntPart(a, b string) int {
	a = strings.TrimLeft(a, "0")
	b = strings.TrimLeft(b, "0")
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return strings.Compare(a, b)
}

// compareFracPart compares two fractional-digit strings, treating the shorter as
// right-padded with zeros, so "5" equals "50" (0.5 == 0.50) and "5" exceeds "49"
// (0.5 > 0.49).
func compareFracPart(a, b string) int {
	n := max(len(b), len(a))
	for i := range n {
		ca, cb := byte('0'), byte('0')
		if i < len(a) {
			ca = a[i]
		}
		if i < len(b) {
			cb = b[i]
		}
		if ca != cb {
			if ca < cb {
				return -1
			}
			return 1
		}
	}
	return 0
}
