package orderbook

import (
	"reflect"
	"testing"
)

func TestNormalizeSeparatedPair(t *testing.T) {
	tests := []struct {
		name    string
		symbol  string
		sep     string
		want    string
		wantErr bool
	}{
		{name: "valid upper-cased", symbol: "btc-usd", sep: "-", want: "BTC-USD"},
		{name: "already upper", symbol: "BTC-USD", sep: "-", want: "BTC-USD"},
		{name: "slash separator", symbol: "xbt/usd", sep: "/", want: "XBT/USD"},
		{name: "alphanumeric with digits", symbol: "1inch-usd", sep: "-", want: "1INCH-USD"},
		{name: "empty", symbol: "", sep: "-", wantErr: true},
		{name: "missing separator", symbol: "BTCUSD", sep: "-", wantErr: true},
		{name: "extra separator", symbol: "BTC-USD-X", sep: "-", wantErr: true},
		{name: "empty left part", symbol: "-USD", sep: "-", wantErr: true},
		{name: "empty right part", symbol: "BTC-", sep: "-", wantErr: true},
		{name: "wrong separator", symbol: "BTC/USD", sep: "-", wantErr: true},
		{name: "leading whitespace rejected", symbol: " btc-usd", sep: "-", wantErr: true},
		{name: "trailing whitespace rejected", symbol: "btc-usd ", sep: "-", wantErr: true},
		{name: "whitespace around separator rejected", symbol: " btc - usd ", sep: "-", wantErr: true},
		{name: "internal space in part rejected", symbol: "bt c-usd", sep: "-", wantErr: true},
		{name: "punctuation in part rejected", symbol: "btc!-usd", sep: "-", wantErr: true},
		{name: "tab whitespace rejected", symbol: "btc\t-usd", sep: "-", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeSeparatedPair(tt.symbol, tt.sep)
			if (err != nil) != tt.wantErr {
				t.Fatalf("normalizeSeparatedPair(%q, %q) err = %v, wantErr %v", tt.symbol, tt.sep, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("normalizeSeparatedPair(%q, %q) = %q, want %q", tt.symbol, tt.sep, got, tt.want)
			}
		})
	}
}

func TestChunkSymbols(t *testing.T) {
	tests := []struct {
		name    string
		symbols []string
		size    int
		want    [][]string
	}{
		{name: "single group when size 0", symbols: []string{"a", "b"}, size: 0, want: [][]string{{"a", "b"}}},
		{name: "single group when fits", symbols: []string{"a", "b"}, size: 5, want: [][]string{{"a", "b"}}},
		{name: "even split", symbols: []string{"a", "b", "c", "d"}, size: 2, want: [][]string{{"a", "b"}, {"c", "d"}}},
		{name: "uneven split", symbols: []string{"a", "b", "c"}, size: 2, want: [][]string{{"a", "b"}, {"c"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chunkSymbols(tt.symbols, tt.size)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("chunkSymbols = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDedupSymbols(t *testing.T) {
	got := dedupSymbols([]string{"A", "B", "A", "C", "B"})
	want := []string{"A", "B", "C"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dedupSymbols = %v, want %v (first-seen order preserved)", got, want)
	}
}

func TestSymbolAllowed(t *testing.T) {
	allowed := symbolSet([]string{"BTC-USD", "eth-usd"})
	if !symbolAllowed(allowed, "BTC-USD") {
		t.Error("subscribed symbol should be allowed")
	}
	if !symbolAllowed(allowed, "btc-usd") {
		t.Error("case-insensitive match should be allowed")
	}
	if !symbolAllowed(allowed, "ETH-USD") {
		t.Error("symbol subscribed in lower case should be allowed when echoed upper")
	}
	if symbolAllowed(allowed, "SOL-USD") {
		t.Error("unsubscribed symbol should not be allowed")
	}
	// A nil set (handlers built directly in tests) allows everything.
	if !symbolAllowed(nil, "ANYTHING") {
		t.Error("nil allow-set should allow everything")
	}
}
