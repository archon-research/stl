//go:build integration

package migrator_test

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.NewIntegrationMain(m).WithTimescaleDB(&sharedDSN).Run())
}
