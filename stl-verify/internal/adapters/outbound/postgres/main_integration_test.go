//go:build integration

package postgres

import (
	"context"
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

// compressChunks columnstores every chunk of one hypertable, which is what a compression
// policy has already done to any chunk a replay writes into. compress_chunk recompresses a
// chunk that already holds compressed data, so a caller cannot end up asserting against a
// row that stayed on the rowstore side.
func compressChunks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) {
	t.Helper()
	var chunks int
	if err := pool.QueryRow(ctx,
		`SELECT count(*)::int FROM (SELECT compress_chunk(c) FROM show_chunks($1::regclass) c) s`, table,
	).Scan(&chunks); err != nil {
		t.Fatalf("compress %s chunks: %v", table, err)
	}
	if chunks == 0 {
		t.Fatalf("%s has no chunk to compress; the seed write did not land", table)
	}
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
