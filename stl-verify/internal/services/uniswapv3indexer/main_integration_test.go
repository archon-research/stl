//go:build integration || livevalidation

package uniswapv3indexer

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Also tagged livevalidation: that manual, Alchemy-backed gate lives in this
// package and takes its database from this container rather than starting one.
var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
}
