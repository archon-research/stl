//go:build integration

package rawdatabackup

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

func TestMain(m *testing.M) {
	os.Exit(testutil.NewIntegrationMain(m).
		WithRedis(&sharedRedisAddr).
		WithLocalStack("sns,sqs,s3", &sharedLocalStackCfg).
		Run())
}
