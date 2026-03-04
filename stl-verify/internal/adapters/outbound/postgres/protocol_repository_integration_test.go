//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const protocolSchemaName = "test_protocol"

var protocolPool *pgxpool.Pool

func init() {
	registerTestFileSetup(protocolSchemaName, func() {
		protocolPool = testutil.SetupSchemaForMain(sharedDSN, protocolSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, protocolPool, protocolSchemaName)
	})
}

// truncateProtocol clears the protocol table for test isolation.
func truncateProtocol(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := protocolPool.Exec(ctx, `TRUNCATE protocol CASCADE`)
	if err != nil {
		t.Fatalf("failed to truncate protocol: %v", err)
	}
}

func TestGetOrCreateProtocol_CreatesNewProtocol(t *testing.T) {
	truncateProtocol(t, context.Background())
	ctx := context.Background()

	repo, err := NewProtocolRepository(protocolPool, nil, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}

	// Use an address that is not seeded by migrations.
	addr := common.HexToAddress("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")

	tx, err := protocolPool.Begin(ctx)
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
	err = protocolPool.QueryRow(ctx,
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
	truncateProtocol(t, context.Background())
	ctx := context.Background()

	repo, err := NewProtocolRepository(protocolPool, nil, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}

	addr := common.HexToAddress("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

	tx1, err := protocolPool.Begin(ctx)
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

	tx2, err := protocolPool.Begin(ctx)
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

// TestGetOrCreateProtocol_CreatedAtBlockUsesLeast verifies that when the same protocol
// is upserted with a later block first and an earlier block second, the stored
// created_at_block is updated to the minimum (matching GetOrCreateUser behavior).
func TestGetOrCreateProtocol_CreatedAtBlockUsesLeast(t *testing.T) {
	truncateProtocol(t, context.Background())
	ctx := context.Background()

	repo, err := NewProtocolRepository(protocolPool, nil, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}

	addr := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// First call: block 500 (a later block, as if processed first out of order).
	tx1, err := protocolPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	if _, err := repo.GetOrCreateProtocol(ctx, tx1, 1, addr, "TestProtocol", "lending", 500); err != nil {
		t.Fatalf("first GetOrCreateProtocol: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	// Second call: block 100 (the true first seen block, processed out of order).
	tx2, err := protocolPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	if _, err := repo.GetOrCreateProtocol(ctx, tx2, 1, addr, "TestProtocol", "lending", 100); err != nil {
		t.Fatalf("second GetOrCreateProtocol: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	var createdAtBlock int64
	err = protocolPool.QueryRow(ctx,
		`SELECT created_at_block FROM protocol WHERE chain_id = $1 AND address = $2`,
		1, addr.Bytes(),
	).Scan(&createdAtBlock)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if createdAtBlock != 100 {
		t.Errorf("created_at_block = %d, want 100 (LEAST of 500 and 100)", createdAtBlock)
	}
}

// TestGetOrCreateProtocol_ConcurrentRaceReturnsSameID simulates concurrent workers
// both racing to insert the same new protocol. Both must succeed without error and
// return the same protocol id.
func TestGetOrCreateProtocol_ConcurrentRaceReturnsSameID(t *testing.T) {
	truncateProtocol(t, context.Background())
	ctx := context.Background()

	repo, err := NewProtocolRepository(protocolPool, nil, 0)
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

			tx, err := protocolPool.Begin(ctx)
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
