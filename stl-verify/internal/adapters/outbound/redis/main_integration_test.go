//go:build integration

package redis

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedRedisAddr string

func TestMain(m *testing.M) {
	os.Exit(testutil.NewIntegrationMain(m).WithRedis(&sharedRedisAddr).Run())
}
