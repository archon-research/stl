//go:build integration

package postgres

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const allocCompressedDBName = "test_alloc_compressed"

var allocCompressedPool *pgxpool.Pool

func init() {
	useFileDatabase(allocCompressedDBName, &allocCompressedPool)
}

// allocCompressedFixture is one pre-cutover row plus the collaborators needed
// to append a correction to it under a second build.
type allocCompressedFixture struct {
	repo      *AllocationRepository
	tokenAddr common.Address
	proxyAddr common.Address
	primeID   int64
	createdAt time.Time
}

// newAllocCompressedFixture writes the original observation and leaves its
// chunk in whatever state the caller then puts it in.
func newAllocCompressedFixture(t *testing.T, ctx context.Context) allocCompressedFixture {
	t.Helper()
	pool := allocCompressedPool

	if _, err := pool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES (1, 'mainnet') ON CONFLICT (chain_id) DO NOTHING`); err != nil {
		t.Fatalf("seed chain: %v", err)
	}
	var primeID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM prime WHERE name = 'spark'`).Scan(&primeID); err != nil {
		t.Fatalf("look up spark prime: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM allocation_position`); err != nil {
		t.Fatalf("delete allocation_position: %v", err)
	}
	// DELETE leaves a sibling test's chunks in the columnstore, and the whole
	// point here is which store the target chunk is in.
	if _, err := pool.Exec(ctx,
		`SELECT decompress_chunk(s, if_compressed => true) FROM show_chunks('allocation_position') s`,
	); err != nil {
		t.Fatalf("decompress allocation_position chunks: %v", err)
	}

	f := allocCompressedFixture{
		tokenAddr: common.HexToAddress("0x6b175474e89094c44da98b954eedeac495271d0f"),
		proxyAddr: common.HexToAddress("0x3333333333333333333333333333333333333333"),
		primeID:   primeID,
		createdAt: time.Date(2026, 3, 4, 9, 0, 0, 0, time.UTC),
	}

	runID := openAllocCompressedRun(t, ctx, pool)
	original := newAllocRepoForBuild(t, pool, originalBuildID, runID)
	if _, err := saveAllocationPositions(ctx, pool, original, f.position(nil)); err != nil {
		t.Fatalf("seed original position: %v", err)
	}

	f.repo = newAllocRepoForBuild(t, pool, correctionBuildID, runID)
	return f
}

// position builds the observation at the fixture's natural key, optionally
// carrying the underlying valuation a correction adds.
func (f allocCompressedFixture) position(underlyingValue *big.Int) *entity.AllocationPosition {
	pos := &entity.AllocationPosition{
		ChainID:        1,
		TokenAddress:   f.tokenAddr,
		TokenSymbol:    "DAI",
		TokenDecimals:  18,
		PrimeID:        f.primeID,
		ProxyAddress:   f.proxyAddr,
		Balance:        big.NewInt(1_000_000_000_000_000_000),
		BlockNumber:    25_100_000,
		TxAmount:       big.NewInt(1_000_000_000_000_000_000),
		Direction:      "sweep",
		CreatedAtBlock: 25_100_000,
		CreatedAt:      f.createdAt,
	}
	if underlyingValue != nil {
		pos.Underlying = &entity.UnderlyingValuation{
			Value:         underlyingValue,
			AssetAddress:  f.tokenAddr,
			AssetSymbol:   "DAI",
			AssetDecimals: 18,
		}
	}
	return pos
}

// assign_processing_version_allocation_position reuses an existing row's version
// when build_id matches too, so the correction has to come from a second build
// or it collides with the original instead of appending to it.
const (
	originalBuildID   = 1
	correctionBuildID = 2
)

func openAllocCompressedRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool) buildregistry.RunID {
	t.Helper()
	_, runID := testutil.OpenTestRun(t, ctx, pool)
	return runID
}

func newAllocRepoForBuild(
	t *testing.T, pool *pgxpool.Pool, buildID int, runID buildregistry.RunID,
) *AllocationRepository {
	t.Helper()
	tokenRepo, err := NewTokenRepository(pool, nil, 0, runID)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	txm, err := NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	return NewAllocationRepository(pool, txm, tokenRepo, nil, buildregistry.BuildID(buildID), runID)
}

func saveAllocationPositions(
	ctx context.Context, pool *pgxpool.Pool, repo *AllocationRepository, positions ...*entity.AllocationPosition,
) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := repo.SavePositions(ctx, tx, positions)
	if err != nil {
		return inserted, err
	}
	return inserted, tx.Commit(ctx)
}

func compressAllocationPositionChunks(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
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
}

// TestSavePositions_ReportsZeroInsertedWhenTheTargetChunkIsCompressed pins the
// mechanism behind VEC-759. TimescaleDB resolves the key conflict against the
// columnstore before the BEFORE INSERT trigger runs, so the tuple still carries
// processing_version 0 — the version the original row holds — and ON CONFLICT
// DO NOTHING drops it. No error is raised, which is why the count, not
// len(positions), is what a caller has to believe.
func TestSavePositions_ReportsZeroInsertedWhenTheTargetChunkIsCompressed(t *testing.T) {
	ctx := context.Background()
	f := newAllocCompressedFixture(t, ctx)
	compressAllocationPositionChunks(t, ctx, allocCompressedPool)

	inserted, err := saveAllocationPositions(ctx, allocCompressedPool, f.repo, f.position(big.NewInt(5)))
	if err != nil {
		t.Fatalf("SavePositions: %v", err)
	}
	if inserted != 0 {
		t.Fatalf("inserted = %d, want 0: TimescaleDB discards this write, and a count that claimed "+
			"otherwise would be the silent data loss VEC-759 shipped", inserted)
	}
	if n := correctionRowCount(t, ctx); n != 0 {
		t.Errorf("correction rows = %d, want 0", n)
	}
}

// TestSavePositions_ReportsEveryRowInsertedWhenTheTargetChunkIsRowstore is the
// control: the same correction against the same key lands once the chunk is not
// in the columnstore, so the zero above is the compression, not the fixture.
func TestSavePositions_ReportsEveryRowInsertedWhenTheTargetChunkIsRowstore(t *testing.T) {
	ctx := context.Background()
	f := newAllocCompressedFixture(t, ctx)

	inserted, err := saveAllocationPositions(ctx, allocCompressedPool, f.repo, f.position(big.NewInt(5)))
	if err != nil {
		t.Fatalf("SavePositions: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("inserted = %d, want 1", inserted)
	}
	if n := correctionRowCount(t, ctx); n != 1 {
		t.Errorf("correction rows = %d, want 1", n)
	}
}

func correctionRowCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	if err := allocCompressedPool.QueryRow(ctx,
		`SELECT count(*) FROM allocation_position WHERE processing_version > 0`).Scan(&n); err != nil {
		t.Fatalf("count correction rows: %v", err)
	}
	return n
}
