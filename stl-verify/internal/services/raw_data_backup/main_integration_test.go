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
	os.Exit(testutil.RunShared(m, testutil.Shared{
		RedisAddr:          &sharedRedisAddr,
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "sns,sqs,s3",
	}))
}
