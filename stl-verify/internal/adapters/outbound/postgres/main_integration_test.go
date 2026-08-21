//go:build integration

package postgres

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

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

// registerTestFileSetup takes work that needs sharedDSN, for a file whose setup is
// more than one database — useFileDatabase below covers that case.
func registerTestFileSetup(setup, cleanup func()) {
	testFileSetups = append(testFileSetups, testFileSetup{setup: setup, cleanup: cleanup})
}

// useFileDatabase gives the calling file its own database and publishes a pool for
// it. Registered rather than done here because init() runs before sharedDSN exists.
func useFileDatabase(dbName string, pool **pgxpool.Pool) {
	registerTestFileSetup(func() {
		*pool = testutil.SetupDBForMain(sharedDSN, dbName)
	}, func() {
		testutil.CleanupDBForMain(sharedDSN, *pool, dbName)
	})
}

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		TimescaleDSN: &sharedDSN,
		BeforeRun:    setUpTestFileDatabases,
		AfterRun:     tearDownTestFileDatabases,
	}))
}

func setUpTestFileDatabases() {
	for _, ts := range testFileSetups {
		ts.setup()
	}
}

func tearDownTestFileDatabases() {
	for _, ts := range testFileSetups {
		ts.cleanup()
	}
}
