//go:build integration

package data_validator

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
}
