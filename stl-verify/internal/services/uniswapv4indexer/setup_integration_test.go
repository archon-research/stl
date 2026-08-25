//go:build integration || livevalidation

package uniswapv4indexer

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Also tagged livevalidation so the manual, Alchemy-backed gate in this package
// takes its database from this container rather than starting one.
var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
}
