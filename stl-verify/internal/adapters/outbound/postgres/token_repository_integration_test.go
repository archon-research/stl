//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestGetOrCreateToken_CreatesNewToken(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	repo, err := NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := repo.GetOrCreateToken(ctx, tx, 1, addr, "TKN", 18, 500)
	if err != nil {
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero id")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var symbol string
	var decimals int
	err = pool.QueryRow(ctx,
		`SELECT symbol, decimals FROM token WHERE chain_id = $1 AND address = $2`,
		1, addr.Bytes(),
	).Scan(&symbol, &decimals)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if symbol != "TKN" {
		t.Errorf("symbol = %q, want TKN", symbol)
	}
	if decimals != 18 {
		t.Errorf("decimals = %d, want 18", decimals)
	}
}

func TestGetOrCreateToken_IdempotentReturnsSameID(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	repo, err := NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

	tx1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	id1, err := repo.GetOrCreateToken(ctx, tx1, 1, addr, "TKN", 18, 500)
	if err != nil {
		t.Fatalf("first GetOrCreateToken: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	id2, err := repo.GetOrCreateToken(ctx, tx2, 1, addr, "TKN", 18, 500)
	if err != nil {
		t.Fatalf("second GetOrCreateToken: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	if id1 != id2 {
		t.Errorf("expected same id on second call, got id1=%d id2=%d", id1, id2)
	}
}

func TestGetOrCreateToken_EmptySymbolIsPersistedAsProvided(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	repo, err := NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	if _, err := repo.GetOrCreateToken(ctx, tx, 1, addr, "", 6, 100); err != nil {
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var symbol string
	err = pool.QueryRow(ctx,
		`SELECT symbol FROM token WHERE chain_id = $1 AND address = $2`,
		1, addr.Bytes(),
	).Scan(&symbol)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if symbol != "" {
		t.Errorf("symbol = %q, want empty string", symbol)
	}
}

// TestGetOrCreateToken_CreatedAtBlockUsesLeast verifies that when the same token
// is upserted with a later block first and an earlier block second, the stored
// created_at_block is updated to the minimum (matching GetOrCreateUser behavior).
func TestGetOrCreateToken_CreatedAtBlockUsesLeast(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	repo, err := NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")

	// First call: block 500 (a later block, as if processed first out of order).
	tx1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	if _, err := repo.GetOrCreateToken(ctx, tx1, 1, addr, "TKN", 18, 500); err != nil {
		t.Fatalf("first GetOrCreateToken: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	// Second call: block 100 (the true first seen block, processed out of order).
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	if _, err := repo.GetOrCreateToken(ctx, tx2, 1, addr, "TKN", 18, 100); err != nil {
		t.Fatalf("second GetOrCreateToken: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	var createdAtBlock int64
	err = pool.QueryRow(ctx,
		`SELECT created_at_block FROM token WHERE chain_id = $1 AND address = $2`,
		1, addr.Bytes(),
	).Scan(&createdAtBlock)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if createdAtBlock != 100 {
		t.Errorf("created_at_block = %d, want 100 (LEAST of 500 and 100)", createdAtBlock)
	}
}

// TestGetOrCreateToken_ConcurrentRaceReturnsSameID simulates concurrent workers
// both racing to insert the same new token. Both must succeed without error and
// return the same token id.
func TestGetOrCreateToken_ConcurrentRaceReturnsSameID(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	repo, err := NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")

	const workers = 10
	ids := make([]int64, workers)
	errs := make([]error, workers)

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start

			tx, err := pool.Begin(ctx)
			if err != nil {
				errs[idx] = err
				return
			}
			id, err := repo.GetOrCreateToken(ctx, tx, 1, addr, "RACE", 18, int64(1000+idx))
			if err != nil {
				tx.Rollback(ctx)
				errs[idx] = err
				return
			}
			if err := tx.Commit(ctx); err != nil {
				errs[idx] = err
				return
			}
			ids[idx] = id
		}(i)
	}

	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d returned error: %v", i, err)
		}
	}

	first := ids[0]
	for i, id := range ids {
		if id != first {
			t.Errorf("worker %d returned id %d, want %d", i, id, first)
		}
	}
}
