//go:build integration

package redis

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedRedisAddr string

func TestMain(m *testing.M) {
	addr, cleanup := testutil.StartRedisForMain()
	sharedRedisAddr = addr

	code := m.Run()

	cleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}
