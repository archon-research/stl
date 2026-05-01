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
	redisAddr, redisCleanup := testutil.StartRedisForMain()
	sharedRedisAddr = redisAddr

	lsCfg, lsCleanup := testutil.StartLocalStackForMain("sns,sqs,s3")
	sharedLocalStackCfg = lsCfg

	code := m.Run()

	lsCleanup()
	redisCleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}
