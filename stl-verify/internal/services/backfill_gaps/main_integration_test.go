//go:build integration

package backfill_gaps

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunTestsWithLeakCheck(m))
}
