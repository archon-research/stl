//go:build integration

package postgres

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
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

	user := entity.User{ChainID: 1, Address: common.HexToAddress("0x1111111111111111111111111111111111111111"), FirstSeenBlock: i64(100)}

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

	user := entity.User{ChainID: 1, Address: common.HexToAddress("0x2222222222222222222222222222222222222222"), FirstSeenBlock: i64(200)}

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
	if _, err := repo.GetOrCreateUser(ctx, tx1, entity.User{ChainID: 1, Address: addr, FirstSeenBlock: i64(500)}); err != nil {
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
	if _, err := repo.GetOrCreateUser(ctx, tx2, entity.User{ChainID: 1, Address: addr, FirstSeenBlock: i64(100)}); err != nil {
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

	const workers = 5
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
				FirstSeenBlock: i64(int64(1000 + idx)), // each worker thinks it saw the user at a different block
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

// TestGetOrCreateUsers_NilBlockPreservesAndDedupes covers the VEC-353 batch
// path: a nil incoming block inserts NULL and preserves an existing on-chain
// block (LEAST ignores NULL), and a duplicate address in the input is
// deduplicated.
func TestGetOrCreateUsers_NilBlockPreservesAndDedupes(t *testing.T) {
	truncateUser(t, context.Background())
	ctx := context.Background()

	repo, err := NewUserRepository(userPool, nil, 0)
	if err != nil {
		t.Fatalf("NewUserRepository: %v", err)
	}

	a := common.HexToAddress("0xA1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1")
	b := common.HexToAddress("0xB2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2B2")

	var first map[common.Address]int64
	inUserTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		// a appears twice (nil block): the batch must deduplicate it.
		first, err = repo.GetOrCreateUsers(ctx, tx, []entity.User{
			{ChainID: 1, Address: a},
			{ChainID: 1, Address: b},
			{ChainID: 1, Address: a},
		})
		return err
	})
	if len(first) != 2 {
		t.Fatalf("len(first) = %d, want 2", len(first))
	}

	// A nil-block insert stores NULL first_seen_block.
	var fsb *int64
	if err := userPool.QueryRow(ctx,
		`SELECT first_seen_block FROM "user" WHERE chain_id = 1 AND address = $1`,
		a.Bytes()).Scan(&fsb); err != nil {
		t.Fatalf("querying user: %v", err)
	}
	if fsb != nil {
		t.Errorf("first_seen_block = %v, want NULL", *fsb)
	}

	// A user previously seen on-chain at a real block must keep it when an
	// off-block-context (nil) caller re-upserts it.
	existing := common.HexToAddress("0xC3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3C3")
	var existingID int64
	if err := userPool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block, created_at, updated_at, metadata)
		 VALUES (1, $1, 12345, NOW(), NOW(), '{}'::jsonb) RETURNING id`,
		existing.Bytes()).Scan(&existingID); err != nil {
		t.Fatalf("seeding existing user: %v", err)
	}

	var second map[common.Address]int64
	inUserTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		second, err = repo.GetOrCreateUsers(ctx, tx, []entity.User{{ChainID: 1, Address: existing}})
		return err
	})
	if second[existing] != existingID {
		t.Errorf("existing user id = %d, want %d", second[existing], existingID)
	}
	var preserved int64
	if err := userPool.QueryRow(ctx,
		`SELECT first_seen_block FROM "user" WHERE id = $1`, existingID).Scan(&preserved); err != nil {
		t.Fatalf("querying preserved user: %v", err)
	}
	if preserved != 12345 {
		t.Errorf("first_seen_block = %d, want 12345 (must not be clobbered)", preserved)
	}
}

// TestGetOrCreateUsers_NullBlockSelfHeals verifies that a user first written
// with a NULL first_seen_block (off-block-context caller) is healed to a real
// block when a later on-chain observation supplies one: LEAST(NULL, N) = N.
func TestGetOrCreateUsers_NullBlockSelfHeals(t *testing.T) {
	truncateUser(t, context.Background())
	ctx := context.Background()

	repo, err := NewUserRepository(userPool, nil, 0)
	if err != nil {
		t.Fatalf("NewUserRepository: %v", err)
	}

	addr := common.HexToAddress("0xE5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5")

	// First: nil block -> stored NULL.
	inUserTx(t, ctx, func(tx pgx.Tx) error {
		_, err := repo.GetOrCreateUsers(ctx, tx, []entity.User{{ChainID: 1, Address: addr}})
		return err
	})

	// Later: a real on-chain block arrives and must heal the NULL.
	inUserTx(t, ctx, func(tx pgx.Tx) error {
		_, err := repo.GetOrCreateUsers(ctx, tx, []entity.User{{ChainID: 1, Address: addr, FirstSeenBlock: i64(900)}})
		return err
	})

	var fsb *int64
	if err := userPool.QueryRow(ctx,
		`SELECT first_seen_block FROM "user" WHERE chain_id = 1 AND address = $1`,
		addr.Bytes()).Scan(&fsb); err != nil {
		t.Fatalf("querying user: %v", err)
	}
	if fsb == nil || *fsb != 900 {
		t.Errorf("first_seen_block = %v, want 900 (LEAST should heal NULL to the real block)", fsb)
	}
}

// TestUserFirstSeenBlockCheckConstraint verifies the VEC-353 migration guard:
// a literal first_seen_block of 0 is rejected at the column level.
func TestUserFirstSeenBlockCheckConstraint(t *testing.T) {
	truncateUser(t, context.Background())
	ctx := context.Background()

	addr := common.HexToAddress("0xD4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4D4")
	_, err := userPool.Exec(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block, created_at, updated_at, metadata)
		 VALUES (1, $1, 0, NOW(), NOW(), '{}'::jsonb)`,
		addr.Bytes())
	if err == nil {
		t.Fatal("expected CHECK constraint violation for first_seen_block = 0, got nil")
	}
	if !strings.Contains(err.Error(), "user_first_seen_block_positive") {
		t.Errorf("error %q should name the check constraint", err.Error())
	}
}

// inUserTx runs fn inside a committed transaction against userPool.
func inUserTx(t *testing.T, ctx context.Context, fn func(tx pgx.Tx) error) {
	t.Helper()
	tx, err := userPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("tx fn: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
