//go:build integration

package postgres

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// sharedDSN is the connection string to the shared TimescaleDB container.
// Each test file creates its own database for isolation.
var sharedDSN string

// testFileSetup holds setup and cleanup functions for each test file's database.
type testFileSetup struct {
	setup   func()
	cleanup func()
}

var testFileSetups []testFileSetup

// registerTestFileSetup allows each test file to register its database setup/cleanup.
// Called from init() in each test file.
func registerTestFileSetup(setup, cleanup func()) {
	testFileSetups = append(testFileSetups, testFileSetup{setup: setup, cleanup: cleanup})
}

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	for _, ts := range testFileSetups {
		ts.setup()
	}

	code := m.Run()

	for _, ts := range testFileSetups {
		ts.cleanup()
	}

	cleanup()

	code = testutil.CheckGoroutineLeaks(code)

	os.Exit(code)
}
