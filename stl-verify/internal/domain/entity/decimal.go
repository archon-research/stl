package entity

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
// level". Non-canonical input (incl. ".") is not zero; a canonical decimal of
// only '0' and '.' characters is.
func IsZeroDecimal(s string) bool {
	if !IsCanonicalDecimal(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c != '0' && c != '.' {
			return false
		}
	}
	return true
}
