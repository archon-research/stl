//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const userSchemaName = "test_user"

var userPool *pgxpool.Pool

func init() {
	registerTestFileSetup(userSchemaName, func() {
		userPool = testutil.SetupSchemaForMain(sharedDSN, userSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, userPool, userSchemaName)
	})
}

// truncateUser clears the user table for test isolation.
func truncateUser(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := userPool.Exec(ctx, `DELETE FROM "user"`)
	if err != nil {
		t.Fatalf("failed to truncate user: %v", err)
	}
}

func TestGetOrCreateUser_CreatesNewUser(t *testing.T) {
	truncateUser(t, context.Background())
	ctx := context.Background()

	repo, err := NewUserRepository(userPool, nil, 0)
	if err != nil {
		t.Fatalf("NewUserRepository: %v", err)
	}

	user := entity.User{ChainID: 1, Address: common.HexToAddress("0x1111111111111111111111111111111111111111"), FirstSeenBlock: 100}

	tx, err := userPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := repo.GetOrCreateUser(ctx, tx, user)
	if err != nil {
		t.Fatalf("GetOrCreateUser: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero id")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var firstSeenBlock int64
	err = userPool.QueryRow(ctx,
		`SELECT first_seen_block FROM "user" WHERE chain_id = $1 AND address = $2`,
		1, user.Address.Bytes(),
	).Scan(&firstSeenBlock)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if firstSeenBlock != 100 {
		t.Errorf("first_seen_block = %d, want 100", firstSeenBlock)
	}
}

func TestGetOrCreateUser_IdempotentReturnsSameID(t *testing.T) {
	truncateUser(t, context.Background())
	ctx := context.Background()

	repo, err := NewUserRepository(userPool, nil, 0)
	if err != nil {
		t.Fatalf("NewUserRepository: %v", err)
	}

	user := entity.User{ChainID: 1, Address: common.HexToAddress("0x2222222222222222222222222222222222222222"), FirstSeenBlock: 200}

	tx1, err := userPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	id1, err := repo.GetOrCreateUser(ctx, tx1, user)
	if err != nil {
		t.Fatalf("first GetOrCreateUser: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	tx2, err := userPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	id2, err := repo.GetOrCreateUser(ctx, tx2, user)
	if err != nil {
		t.Fatalf("second GetOrCreateUser: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	if id1 != id2 {
		t.Errorf("expected same id on second call, got id1=%d id2=%d", id1, id2)
	}
}

// TestGetOrCreateUser_FirstSeenBlockUsesLeast verifies that when the same user
// is upserted with a later block first and an earlier block second, the stored
// first_seen_block is updated to the minimum.
func TestGetOrCreateUser_FirstSeenBlockUsesLeast(t *testing.T) {
	truncateUser(t, context.Background())
	ctx := context.Background()

	repo, err := NewUserRepository(userPool, nil, 0)
	if err != nil {
		t.Fatalf("NewUserRepository: %v", err)
	}

	addr := common.HexToAddress("0x3333333333333333333333333333333333333333")

	// First call: block 500 (a later block, as if a later block was processed first).
	tx1, err := userPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	if _, err := repo.GetOrCreateUser(ctx, tx1, entity.User{ChainID: 1, Address: addr, FirstSeenBlock: 500}); err != nil {
		t.Fatalf("first GetOrCreateUser: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	// Second call: block 100 (the true first seen block, processed out of order).
	tx2, err := userPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	if _, err := repo.GetOrCreateUser(ctx, tx2, entity.User{ChainID: 1, Address: addr, FirstSeenBlock: 100}); err != nil {
		t.Fatalf("second GetOrCreateUser: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	var firstSeenBlock int64
	err = userPool.QueryRow(ctx,
		`SELECT first_seen_block FROM "user" WHERE chain_id = $1 AND address = $2`,
		1, addr.Bytes(),
	).Scan(&firstSeenBlock)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if firstSeenBlock != 100 {
		t.Errorf("first_seen_block = %d, want 100 (LEAST of 500 and 100)", firstSeenBlock)
	}
}

// TestGetOrCreateUser_ConcurrentRaceReturnsSameID simulates concurrent workers
// processing different blocks that both encounter the same new user for the first
// time. Both goroutines must succeed and return the same user id.
func TestGetOrCreateUser_ConcurrentRaceReturnsSameID(t *testing.T) {
	truncateUser(t, context.Background())
	ctx := context.Background()

	repo, err := NewUserRepository(userPool, nil, 0)
	if err != nil {
		t.Fatalf("NewUserRepository: %v", err)
	}

	addr := common.HexToAddress("0x4444444444444444444444444444444444444444")

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

			tx, err := userPool.Begin(ctx)
			if err != nil {
				errs[idx] = err
				return
			}
			id, err := repo.GetOrCreateUser(ctx, tx, entity.User{
				ChainID:        1,
				Address:        addr,
				FirstSeenBlock: int64(1000 + idx), // each worker thinks it saw the user at a different block
			})
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
			t.Errorf("worker %d returned error: %v", i, err)
		}
	}

	first := ids[0]
	for i, id := range ids {
		if id != first {
			t.Errorf("worker %d returned id %d, want %d", i, id, first)
		}
	}

	// first_seen_block should be the minimum across all workers (LEAST semantics).
	var firstSeenBlock int64
	err = userPool.QueryRow(ctx,
		`SELECT first_seen_block FROM "user" WHERE chain_id = $1 AND address = $2`,
		1, addr.Bytes(),
	).Scan(&firstSeenBlock)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if firstSeenBlock != 1000 {
		t.Errorf("first_seen_block = %d, want 1000 (minimum across workers)", firstSeenBlock)
	}
}
