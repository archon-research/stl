//go:build livevalidation

package curveindexer

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Tagged livevalidation only: this package has no integration tests, and the
// manual Alchemy-backed gate here needs a migrated database to read the curated
// capability columns from rather than a hand-written copy of them.
var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
}
