//go:build integration

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Compressed target chunks. Every other test in this package seeds a chunk it
// has just created, which is never in the columnstore — which is why a backfill
// that was green locally wrote nothing in staging, where all 123 chunks below
// the cutover are compressed (VEC-759).
// ---------------------------------------------------------------------------

// historicalBuildID stands in for the live indexer that wrote the pre-cutover
// rows. It must differ from the build the run under test registers, or the
// candidate query excludes the row by design — see the replay-branch test below.
const historicalBuildID = 424242

// compressAllocationChunks moves every allocation_position chunk into the
// columnstore. It reports how many it compressed so a test cannot pass against
// a hypertable that happened to have none.
func compressAllocationChunks(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var compressed int
	if err := pool.QueryRow(ctx, `
		WITH c AS (SELECT compress_chunk(s) FROM show_chunks('allocation_position') s)
		SELECT count(*) FROM c`).Scan(&compressed); err != nil {
		t.Fatalf("compress allocation_position chunks: %v", err)
	}
	if compressed == 0 {
		t.Fatal("no allocation_position chunks to compress; the fixture seeded nothing")
	}
	return compressed
}

func compressedChunkCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.chunks
		 WHERE hypertable_name = 'allocation_position' AND is_compressed`).Scan(&n); err != nil {
		t.Fatalf("count compressed chunks: %v", err)
	}
	return n
}

// seedDirectHoldingCandidate seeds the one pre-cutover row the direct-holding
// path corrects without touching an archive RPC, and returns its token id.
func seedDirectHoldingCandidate(t *testing.T, ctx context.Context, pool *pgxpool.Pool, buildID int) int64 {
	t.Helper()
	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)
	tokenID := testutil.SeedToken(t, ctx, pool, 1, "0x6B175474E89094C44Da98b954EedeAC495271d0F", "DAI", 18)

	const balanceHuman = "1402923191.117714747284001290"
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: tokenID, primeID: primeID, buildID: buildID,
		proxyAddress: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		balance:      balanceHuman, blockNumber: 25_000_000,
		txHash: "0x" + strings.Repeat("aa", 32), logIndex: 0,
		txAmount: balanceHuman, direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-01T00:00:00Z"),
	})
	return tokenID
}

func correctedRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tokenID int64) []int32 {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT processing_version FROM allocation_position
		 WHERE token_id = $1 AND underlying_value IS NOT NULL ORDER BY processing_version`, tokenID)
	if err != nil {
		t.Fatalf("query corrected rows: %v", err)
	}
	defer rows.Close()

	var versions []int32
	for rows.Next() {
		var v int32
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan corrected row: %v", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate corrected rows: %v", err)
	}
	return versions
}

// TestRunIntegration_WritesIntoACompressedTargetChunk is the staging incident as
// a test: the whole target range is in the columnstore. The correction must
// still land, at the next processing_version, without anyone decompressing
// anything.
func TestRunIntegration_WritesIntoACompressedTargetChunk(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()
	t.Setenv("BUILD_GIT_HASH", "test")

	tokenID := seedDirectHoldingCandidate(t, ctx, pool, historicalBuildID)
	compressAllocationChunks(t, ctx, pool)

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("run: %v", err)
	}

	versions := correctedRows(t, ctx, pool, tokenID)
	if len(versions) != 1 || versions[0] != 1 {
		t.Fatalf("corrected rows at processing_version %v, want exactly [1]", versions)
	}
}

// TestRunIntegration_CompressedTargetChunkStaysCompressed guards the cost side:
// the write must not quietly drag the whole target range out of the columnstore.
func TestRunIntegration_CompressedTargetChunkStaysCompressed(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()
	t.Setenv("BUILD_GIT_HASH", "test")

	seedDirectHoldingCandidate(t, ctx, pool, historicalBuildID)
	compressed := compressAllocationChunks(t, ctx, pool)

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := compressedChunkCount(t, ctx, pool); got != compressed {
		t.Errorf("compressed chunks = %d after the run, want %d", got, compressed)
	}
}

// TestRunIntegration_CompressedChunkSecondCorrectionAppends covers an identity
// whose newest row is already a correction: the next one moves to the version
// after that, not back onto a version the columnstore already holds.
func TestRunIntegration_CompressedChunkSecondCorrectionAppends(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()
	t.Setenv("BUILD_GIT_HASH", "test")

	tokenID := seedDirectHoldingCandidate(t, ctx, pool, historicalBuildID)
	// A second observation at the same natural key. The processing_version
	// trigger assigns it 1, so the candidate query selects that row and the
	// correction belongs at 2.
	seedDirectHoldingCandidate(t, ctx, pool, historicalBuildID+1)
	compressAllocationChunks(t, ctx, pool)

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if versions := correctedRows(t, ctx, pool, tokenID); len(versions) != 1 || versions[0] != 2 {
		t.Fatalf("corrected rows at processing_version %v, want exactly [2]", versions)
	}
}

// TestRunIntegration_CompressedChunkRowFromOurOwnBuildIsNotCorrected pins the
// sharp edge of supplying processing_version. When a row at the candidate's
// identity already carries this build's id, the trigger's replay branch forces
// the insert back onto that row's version — after TimescaleDB has already
// resolved the conflict against the version the insert supplied. The write then
// appends a SECOND row at an identical primary key, silently: no error, and a
// row count of 1 that looks exactly like success. The candidate query excludes
// those identities so the replay branch is unreachable; without that exclusion
// this test finds two rows sharing one primary key.
func TestRunIntegration_CompressedChunkRowFromOurOwnBuildIsNotCorrected(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()
	t.Setenv("BUILD_GIT_HASH", "test")

	// A first run establishes which build_id this binary writes under; nothing
	// else names it, and seeding the wrong one silently skips the branch.
	seedDirectHoldingCandidate(t, ctx, pool, historicalBuildID)
	compressAllocationChunks(t, ctx, pool)
	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	ourBuildID := backfillerBuildID(t, ctx, pool)

	// A second, untouched identity already carrying that build_id.
	tokenID := seedSecondIdentity(t, ctx, pool, ourBuildID)
	compressAllocationChunks(t, ctx, pool)

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if n := duplicatePrimaryKeyCount(t, ctx, pool, tokenID); n != 0 {
		t.Fatalf("%d primary key(s) carry more than one row: the replay branch overrode the "+
			"supplied processing_version onto a version the columnstore already holds", n)
	}
}

// backfillerBuildID is the build_id the run under test wrote its corrections
// with — the one the candidate query must refuse to hand back.
func backfillerBuildID(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var buildID int
	if err := pool.QueryRow(ctx,
		`SELECT DISTINCT build_id FROM allocation_position WHERE underlying_value IS NOT NULL`,
	).Scan(&buildID); err != nil {
		t.Fatalf("read the backfiller's build_id: %v", err)
	}
	if buildID == historicalBuildID {
		t.Fatalf("the run wrote under the historical build_id %d; the fixture cannot tell the two apart", buildID)
	}
	return buildID
}

// seedSecondIdentity seeds an uncorrected row at a natural key distinct from
// seedDirectHoldingCandidate's, under the given build.
func seedSecondIdentity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, buildID int) int64 {
	t.Helper()
	primeID := sparkPrimeID(t, ctx, pool)
	tokenID := testutil.SeedToken(t, ctx, pool, 1, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USDC", 6)

	const balanceHuman = "12.500000"
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: tokenID, primeID: primeID, buildID: buildID,
		proxyAddress: common.HexToAddress("0x4444444444444444444444444444444444444444"),
		balance:      balanceHuman, blockNumber: 25_500_000,
		txHash: "0x" + strings.Repeat("bb", 32), logIndex: 0,
		txAmount: balanceHuman, direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-02T00:00:00Z"),
	})
	return tokenID
}

func duplicatePrimaryKeyCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tokenID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT 1 FROM allocation_position
			WHERE token_id = $1
			GROUP BY chain_id, token_id, prime_id, proxy_address, block_number, block_version,
			         tx_hash, log_index, direction, processing_version, created_at
			HAVING count(*) > 1
		) d`, tokenID).Scan(&n); err != nil {
		t.Fatalf("count duplicate primary keys: %v", err)
	}
	return n
}
