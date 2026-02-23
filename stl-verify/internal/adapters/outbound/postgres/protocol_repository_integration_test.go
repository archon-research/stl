//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestGetOrCreateProtocol_CreatesNewProtocol(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	repo, err := NewProtocolRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}

	// Use an address that is not seeded by migrations.
	addr := common.HexToAddress("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := repo.GetOrCreateProtocol(ctx, tx, 1, addr, "TestProtocol", "lending", 1000)
	if err != nil {
		t.Fatalf("GetOrCreateProtocol: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero id")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var name string
	err = pool.QueryRow(ctx,
		`SELECT name FROM protocol WHERE chain_id = $1 AND address = $2`,
		1, addr.Bytes(),
	).Scan(&name)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if name != "TestProtocol" {
		t.Errorf("name = %q, want TestProtocol", name)
	}
}

func TestGetOrCreateProtocol_IdempotentReturnsSameID(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	repo, err := NewProtocolRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}

	addr := common.HexToAddress("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

	tx1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	id1, err := repo.GetOrCreateProtocol(ctx, tx1, 1, addr, "TestProtocol", "lending", 1000)
	if err != nil {
		t.Fatalf("first GetOrCreateProtocol: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	id2, err := repo.GetOrCreateProtocol(ctx, tx2, 1, addr, "TestProtocol", "lending", 1000)
	if err != nil {
		t.Fatalf("second GetOrCreateProtocol: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	if id1 != id2 {
		t.Errorf("expected same id on second call, got id1=%d id2=%d", id1, id2)
	}
}

// TestGetOrCreateProtocol_ConcurrentRaceReturnsSameID simulates concurrent workers
// both racing to insert the same new protocol. Both must succeed without error and
// return the same protocol id.
func TestGetOrCreateProtocol_ConcurrentRaceReturnsSameID(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	repo, err := NewProtocolRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}

	addr := common.HexToAddress("0xABCDABCDABCDABCDABCDABCDABCDABCDABCDABCD")

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
			id, err := repo.GetOrCreateProtocol(ctx, tx, 1, addr, "RaceProtocol", "lending", int64(1000+idx))
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
