//go:build integration

package redis

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedRedisAddr string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{RedisAddr: &sharedRedisAddr}))
}
