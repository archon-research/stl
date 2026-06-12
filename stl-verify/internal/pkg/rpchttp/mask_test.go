package rpchttp

import "testing"

func TestMaskURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"alchemy key", "https://base-mainnet.g.alchemy.com/v2/secret-key", "https://base-mainnet.g.alchemy.com/***"},
		{"no path", "http://localhost:8545", "http://localhost:8545/***"},
		{"unparseable", "http://bad url\x7f", "***"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := MaskURL(tc.in); got != tc.want {
				t.Errorf("MaskURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
