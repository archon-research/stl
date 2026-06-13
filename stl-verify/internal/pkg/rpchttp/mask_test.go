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
		// userinfo lives in url.URL.User, never in u.Host, so credentials passed
		// as userinfo are dropped (not emitted) by the host-only format.
		{"userinfo credentials", "https://user:pass@rpc.example.com/v2/key", "https://rpc.example.com/***"},
		{"key as basic-auth userinfo", "https://apikey@rpc.example.com/path", "https://rpc.example.com/***"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := MaskURL(tc.in); got != tc.want {
				t.Errorf("MaskURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
