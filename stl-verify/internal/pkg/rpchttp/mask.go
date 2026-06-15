package rpchttp

import (
	"fmt"
	"net/url"
)

// MaskURL redacts the path (which typically contains API keys) from an RPC URL.
// Example: "https://eth-mainnet.g.alchemy.com/v2/abc123" → "https://eth-mainnet.g.alchemy.com/***"
func MaskURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "***"
	}
	return fmt.Sprintf("%s://%s/***", u.Scheme, u.Host)
}
