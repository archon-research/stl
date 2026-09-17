//go:build livevalidation

package testutil

import (
	"os"
	"testing"
)

// AlchemyMainnetURL builds the real mainnet Alchemy endpoint the livevalidation
// harnesses dial, from the same ALCHEMY_API_KEY the workers use. Real network
// access is the point of those tests; they are never compiled into normal
// `go test`/CI runs.
func AlchemyMainnetURL(t *testing.T) string {
	t.Helper()
	key := os.Getenv("ALCHEMY_API_KEY")
	if key == "" {
		t.Fatal("ALCHEMY_API_KEY must be set to run a livevalidation test")
	}
	return "https://eth-mainnet.g.alchemy.com/v2/" + key
}
