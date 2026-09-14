//go:build integration

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Compressed target chunks. A write into one is discarded by TimescaleDB before
// the processing_version trigger runs, so the row never reaches history and no
// error is raised. Every test above this one seeds a chunk it has just created,
// which is never compressed — which is exactly why a backfill that was green
// locally wrote nothing in staging, where all 123 chunks below the cutover are
// columnstore (VEC-759).
// ---------------------------------------------------------------------------

// compressAllocationChunks moves every allocation_position chunk into the
// columnstore, reproducing a target range the compression policy has caught up
// with. It reports how many chunks it compressed so a test cannot pass against
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

func decompressAllocationChunks(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `SELECT decompress_chunk(s) FROM show_chunks('allocation_position') s`); err != nil {
		t.Fatalf("decompress allocation_position chunks: %v", err)
	}
}

func correctedRowCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tokenID int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM allocation_position WHERE token_id = $1 AND processing_version > 0`,
		tokenID,
	).Scan(&n); err != nil {
		t.Fatalf("count corrected rows: %v", err)
	}
	return n
}

// seedDirectHoldingCandidate seeds the one pre-cutover row the direct-holding
// path corrects without touching an archive RPC, and returns its token id.
func seedDirectHoldingCandidate(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)
	tokenID := testutil.SeedToken(t, ctx, pool, 1, "0x6B175474E89094C44Da98b954EedeAC495271d0F", "DAI", 18)

	const balanceHuman = "1402923191.117714747284001290"
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: tokenID, primeID: primeID,
		proxyAddress: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		balance:      balanceHuman, blockNumber: 25_000_000,
		txHash: "0x" + strings.Repeat("aa", 32), logIndex: 0,
		txAmount: balanceHuman, direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-01T00:00:00Z"),
	})
	return tokenID
}

func TestRunIntegration_CompressedTargetChunkRefusesToStart(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()
	t.Setenv("BUILD_GIT_HASH", "test")

	tokenID := seedDirectHoldingCandidate(t, ctx, pool)
	compressAllocationChunks(t, ctx, pool)

	err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"})
	if err == nil {
		t.Fatal("run succeeded against a compressed target chunk; it wrote nothing and said so was fine")
	}
	if !strings.Contains(err.Error(), "compressed allocation_position chunk") {
		t.Errorf("error = %v, want it to name the compressed chunks and the operator step", err)
	}
	if !strings.Contains(err.Error(), "decompress_chunk") {
		t.Errorf("error = %v, want it to spell out the decompress_chunk recipe", err)
	}
	if n := correctedRowCount(t, ctx, pool, tokenID); n != 0 {
		t.Errorf("corrected rows = %d, want 0: the run must not have written anything", n)
	}
}

// TestRunIntegration_DecompressedTargetChunkWrites is the other half of the
// guard: once an operator has run the recipe the refusal names, the same run
// goes through. Without it the guard could pass its own test by refusing every
// run.
func TestRunIntegration_DecompressedTargetChunkWrites(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()
	t.Setenv("BUILD_GIT_HASH", "test")

	tokenID := seedDirectHoldingCandidate(t, ctx, pool)
	compressAllocationChunks(t, ctx, pool)
	decompressAllocationChunks(t, ctx, pool)

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := correctedRowCount(t, ctx, pool, tokenID); n != 1 {
		t.Errorf("corrected rows = %d, want 1", n)
	}
}

// TestRunIntegration_CompressedChunkOutsideTheWindowDoesNotBlock keeps the
// guard scoped to what a run actually writes into: a chunk the window does not
// reach is irrelevant, and refusing on it would make the job unstartable on any
// hypertable with history.
func TestRunIntegration_CompressedChunkOutsideTheWindowDoesNotBlock(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()
	t.Setenv("BUILD_GIT_HASH", "test")

	tokenID := seedDirectHoldingCandidate(t, ctx, pool)
	compressAllocationChunks(t, ctx, pool)

	before := mustParseTime(t, "2026-01-01T00:00:00Z").Add(-24 * time.Hour)
	if err := run(ctx, []string{
		"-db", dbURL, "-dry-run=false", "-limit", "10",
		"-before", before.Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := correctedRowCount(t, ctx, pool, tokenID); n != 0 {
		t.Errorf("corrected rows = %d, want 0: the window covers no candidates", n)
	}
}
