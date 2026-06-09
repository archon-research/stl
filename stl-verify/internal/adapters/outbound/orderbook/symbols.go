package orderbook

import (
	"fmt"
	"strings"
)

// normalizeSymbols applies normalize to each symbol, returning the normalized
// slice or the first error.
func normalizeSymbols(symbols []string, normalize func(string) (string, error)) ([]string, error) {
	out := make([]string, len(symbols))
	for i, s := range symbols {
		n, err := normalize(s)
		if err != nil {
			return nil, fmt.Errorf("symbol %q: %w", s, err)
		}
		out[i] = n
	}
	return out, nil
}

// dedupSymbols drops duplicate symbols, preserving first-seen order. Symbols are
// deduped after normalization so two spellings of one symbol ("btc-usd",
// "BTC-USD") cannot subscribe twice on the same connection.
func dedupSymbols(symbols []string) []string {
	seen := make(map[string]bool, len(symbols))
	out := make([]string, 0, len(symbols))
	for _, s := range symbols {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// isAlphanumeric reports whether s is non-empty and contains only ASCII letters
// or digits (after upper-casing, only A-Z and 0-9). Whitespace and punctuation
// are rejected so a malformed symbol cannot slip through as a subscribe payload
// or book key the venue never echoes back.
func isAlphanumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		isDigit := r >= '0' && r <= '9'
		isLetter := r >= 'A' && r <= 'Z'
		if !isDigit && !isLetter {
			return false
		}
	}
	return true
}

// normalizeSeparatedPair upper-cases symbol and requires exactly two
// alphanumeric parts split on sep (e.g. "btc-usd" -> "BTC-USD"). Parts with
// whitespace or punctuation are rejected rather than silently trimmed, since a
// padded symbol would produce a subscribe payload the venue never echoes back.
func normalizeSeparatedPair(symbol, sep string) (string, error) {
	up := strings.ToUpper(symbol)
	parts := strings.Split(up, sep)
	if len(parts) != 2 || !isAlphanumeric(parts[0]) || !isAlphanumeric(parts[1]) {
		return "", fmt.Errorf("expected two alphanumeric parts separated by %q, got %q", sep, symbol)
	}
	return up, nil
}

// chunkSymbols splits symbols into groups of at most size entries. A size of
// zero (or one group that fits) yields a single group.
func chunkSymbols(symbols []string, size int) [][]string {
	if size <= 0 || len(symbols) <= size {
		return [][]string{symbols}
	}
	groups := make([][]string, 0, (len(symbols)+size-1)/size)
	for i := 0; i < len(symbols); i += size {
		groups = append(groups, symbols[i:min(i+size, len(symbols))])
	}
	return groups
}

// symbolSet builds a lookup of normalised symbols for the unsubscribed-symbol
// guard.
func symbolSet(symbols []string) map[string]bool {
	m := make(map[string]bool, len(symbols))
	for _, s := range symbols {
		m[strings.ToUpper(s)] = true
	}
	return m
}

// symbolAllowed reports whether sym is one of the subscribed symbols. A nil set
// (handlers constructed directly in tests) allows everything, so the guard is
// active only once the engine populates it from the subscription group.
func symbolAllowed(allowed map[string]bool, sym string) bool {
	return allowed == nil || allowed[strings.ToUpper(sym)]
}
