//go:build integration

package sqs

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedLocalStackCfg testutil.LocalStackConfig

func TestMain(m *testing.M) {
	os.Exit(testutil.NewIntegrationMain(m).WithLocalStack("sqs", &sharedLocalStackCfg).Run())
}
