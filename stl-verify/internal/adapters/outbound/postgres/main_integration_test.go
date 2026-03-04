//go:build integration

package postgres

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// sharedDSN is the connection string to the shared TimescaleDB container.
// Each test file creates its own schema for isolation.
var sharedDSN string

// testFileSetup holds setup and cleanup functions for each test file's schema.
type testFileSetup struct {
	schemaName string
	setup      func()
	cleanup    func()
}

var testFileSetups []testFileSetup

// registerTestFileSetup allows each test file to register its schema setup/cleanup.
// Called from init() in each test file.
func registerTestFileSetup(schemaName string, setup, cleanup func()) {
	testFileSetups = append(testFileSetups, testFileSetup{
		schemaName: schemaName,
		setup:      setup,
		cleanup:    cleanup,
	})
}

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	// Run all registered schema setups
	for _, ts := range testFileSetups {
		ts.setup()
	}

	code := m.Run()

	// Run all registered schema cleanups
	for _, ts := range testFileSetups {
		ts.cleanup()
	}

	cleanup()

	os.Exit(code)
}
