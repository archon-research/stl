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

const tokenSchemaName = "test_token"

var tokenPool *pgxpool.Pool

func init() {
	registerTestFileSetup(tokenSchemaName, func() {
		tokenPool = testutil.SetupSchemaForMain(sharedDSN, tokenSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, tokenPool, tokenSchemaName)
	})
}

// truncateToken clears the token table for test isolation.
func truncateToken(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := tokenPool.Exec(ctx, `TRUNCATE token CASCADE`)
	if err != nil {
		t.Fatalf("failed to truncate token: %v", err)
	}
}

func TestGetOrCreateToken_CreatesNewToken(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

	tx, err := tokenPool.Begin(ctx)
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
	err = tokenPool.QueryRow(ctx,
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
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

	tx1, err := tokenPool.Begin(ctx)
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

	tx2, err := tokenPool.Begin(ctx)
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
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")

	tx, err := tokenPool.Begin(ctx)
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
	err = tokenPool.QueryRow(ctx,
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
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")

	// First call: block 500 (a later block, as if processed first out of order).
	tx1, err := tokenPool.Begin(ctx)
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
	tx2, err := tokenPool.Begin(ctx)
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
	err = tokenPool.QueryRow(ctx,
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
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
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

			tx, err := tokenPool.Begin(ctx)
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

// TestSymbolReconciliation_MarkAndList verifies that a token inserted with an empty
// symbol can be flagged as pending and then returned by ListTokensPendingSymbol.
func TestSymbolReconciliation_MarkAndList(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")
	chainID := int64(1)
	anchorBlock := int64(12345678)

	// Insert token with empty symbol.
	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr, "", 18, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken: %v", err)
	}

	// Mark symbol pending inside the same tx.
	if err := repo.MarkTokenSymbolPending(ctx, tx, chainID, addr, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("MarkTokenSymbolPending: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// ListTokensPendingSymbol must return the token with correct anchor block.
	pending, err := repo.ListTokensPendingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensPendingSymbol: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending token, got %d", len(pending))
	}
	if pending[0].Address != addr {
		t.Errorf("address = %s, want %s", pending[0].Address.Hex(), addr.Hex())
	}
	if pending[0].AnchorBlock != anchorBlock {
		t.Errorf("anchor block = %d, want %d", pending[0].AnchorBlock, anchorBlock)
	}
}

// TestSymbolReconciliation_MarkPendingPreservesEarliestAnchor verifies that
// re-flagging a still-unresolved token in a later block keeps the EARLIEST anchor
// block, so the reconciliation backstop horizon (anchor + K) cannot be pushed
// forward indefinitely.
func TestSymbolReconciliation_MarkPendingPreservesEarliestAnchor(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")
	chainID := int64(1)
	const earliest = int64(100)
	const later = int64(200)

	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr, "", 18, earliest); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	// First sighting at the earliest block.
	if err := repo.MarkTokenSymbolPending(ctx, tx, chainID, addr, earliest); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("MarkTokenSymbolPending(earliest): %v", err)
	}
	// Re-flag at a later block while the symbol is still unresolved.
	if err := repo.MarkTokenSymbolPending(ctx, tx, chainID, addr, later); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("MarkTokenSymbolPending(later): %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	pending, err := repo.ListTokensPendingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensPendingSymbol: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending token, got %d", len(pending))
	}
	if pending[0].AnchorBlock != earliest {
		t.Errorf("anchor block = %d, want %d (earliest preserved, not moved to %d)", pending[0].AnchorBlock, earliest, later)
	}
}

// TestSymbolReconciliation_ResolveRemovesFromPendingAndSetsSymbol verifies that after
// calling ResolveTokenSymbol the token is no longer listed as pending and its symbol
// column is updated.
func TestSymbolReconciliation_ResolveRemovesFromPendingAndSetsSymbol(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")
	chainID := int64(1)
	anchorBlock := int64(12345678)

	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr, "", 18, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	if err := repo.MarkTokenSymbolPending(ctx, tx, chainID, addr, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("MarkTokenSymbolPending: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Resolve the symbol.
	if err := repo.ResolveTokenSymbol(ctx, chainID, addr, "frxUSD"); err != nil {
		t.Fatalf("ResolveTokenSymbol: %v", err)
	}

	// Must no longer be in the pending list.
	pending, err := repo.ListTokensPendingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensPendingSymbol: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending tokens after resolve, got %d", len(pending))
	}

	// Symbol column must be updated.
	var symbol string
	if err := tokenPool.QueryRow(ctx,
		`SELECT symbol FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes(),
	).Scan(&symbol); err != nil {
		t.Fatalf("query symbol: %v", err)
	}
	if symbol != "frxUSD" {
		t.Errorf("symbol = %q, want frxUSD", symbol)
	}
}

// TestSymbolReconciliation_MarkPendingIsNoOpWhenSymbolPresent verifies that
// MarkTokenSymbolPending is a no-op when the token already has a non-empty symbol.
func TestSymbolReconciliation_MarkPendingIsNoOpWhenSymbolPresent(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")
	chainID := int64(1)
	anchorBlock := int64(12345678)

	// Insert token WITH a non-empty symbol.
	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr, "frxUSD", 18, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	// Attempt to mark symbol pending — must be a no-op because symbol is non-empty.
	if err := repo.MarkTokenSymbolPending(ctx, tx, chainID, addr, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("MarkTokenSymbolPending: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Must not appear in the pending list.
	pending, err := repo.ListTokensPendingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensPendingSymbol: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending tokens when symbol is non-empty, got %d", len(pending))
	}
}

// TestSymbolReconciliation_MarkPendingHandlesNullMetadata verifies that
// MarkTokenSymbolPending correctly sets the pending flag even when the token row
// was inserted with a NULL metadata column (as legacy seed-migration rows have).
// Without COALESCE, `NULL || jsonb_build_object(...)` evaluates to NULL and the
// flag is silently lost, causing ListTokensPendingSymbol to return no rows.
func TestSymbolReconciliation_MarkPendingHandlesNullMetadata(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
	chainID := int64(1)
	anchorBlock := int64(100)

	// Insert a token row with metadata explicitly NULL (simulating legacy seed rows
	// that pre-date the '{}' default).
	_, err = tokenPool.Exec(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, updated_at)
		 VALUES ($1, $2, '', 18, $3, NOW())`,
		chainID, addr.Bytes(), anchorBlock)
	if err != nil {
		t.Fatalf("insert null-metadata token: %v", err)
	}

	// Confirm metadata is actually NULL before the fix is exercised.
	var metadataIsNull bool
	if err := tokenPool.QueryRow(ctx,
		`SELECT metadata IS NULL FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes(),
	).Scan(&metadataIsNull); err != nil {
		t.Fatalf("query metadata null check: %v", err)
	}
	if !metadataIsNull {
		t.Fatal("precondition failed: expected metadata to be NULL after direct insert")
	}

	// Mark symbol pending -- this is the operation that silently breaks without COALESCE.
	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := repo.MarkTokenSymbolPending(ctx, tx, chainID, addr, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("MarkTokenSymbolPending: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// The token must now appear in the pending list with the correct anchor block.
	pending, err := repo.ListTokensPendingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensPendingSymbol: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending token (null-metadata path), got %d", len(pending))
	}
	if pending[0].Address != addr {
		t.Errorf("address = %s, want %s", pending[0].Address.Hex(), addr.Hex())
	}
	if pending[0].AnchorBlock != anchorBlock {
		t.Errorf("anchor block = %d, want %d", pending[0].AnchorBlock, anchorBlock)
	}
}

// TestSymbolReconciliation_MarkUnresolvedClearsPending verifies that
// MarkTokenSymbolUnresolved clears the pending flag (no longer listed) and leaves
// the symbol empty.
func TestSymbolReconciliation_MarkUnresolvedClearsPending(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")
	chainID := int64(1)
	anchorBlock := int64(12345678)

	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr, "", 18, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	if err := repo.MarkTokenSymbolPending(ctx, tx, chainID, addr, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("MarkTokenSymbolPending: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Mark as unresolved.
	if err := repo.MarkTokenSymbolUnresolved(ctx, chainID, addr); err != nil {
		t.Fatalf("MarkTokenSymbolUnresolved: %v", err)
	}

	// Must no longer appear in the pending list.
	pending, err := repo.ListTokensPendingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensPendingSymbol: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending tokens after unresolved, got %d", len(pending))
	}

	// Symbol must still be empty.
	var symbol string
	if err := tokenPool.QueryRow(ctx,
		`SELECT symbol FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes(),
	).Scan(&symbol); err != nil {
		t.Fatalf("query symbol: %v", err)
	}
	if symbol != "" {
		t.Errorf("symbol = %q, want empty string after unresolved", symbol)
	}

	// symbol_anchor_block must also be absent from metadata.
	var anchorBlockPresent bool
	if err := tokenPool.QueryRow(ctx,
		`SELECT metadata ? 'symbol_anchor_block' FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes(),
	).Scan(&anchorBlockPresent); err != nil {
		t.Fatalf("query symbol_anchor_block presence: %v", err)
	}
	if anchorBlockPresent {
		t.Error("expected symbol_anchor_block to be absent from metadata after MarkTokenSymbolUnresolved")
	}
}

// TestSymbolReconciliation_ResolveNonexistentReturnsError verifies that
// ResolveTokenSymbol and MarkTokenSymbolUnresolved return an error (RowsAffected 0)
// when called for an address that has never been inserted.
func TestSymbolReconciliation_ResolveNonexistentReturnsError(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	ghost := common.HexToAddress("0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")
	chainID := int64(1)

	// ResolveTokenSymbol on a nonexistent address must return an error.
	if err := repo.ResolveTokenSymbol(ctx, chainID, ghost, "X"); err == nil {
		t.Error("expected error from ResolveTokenSymbol on nonexistent address, got nil")
	}

	// MarkTokenSymbolUnresolved on a nonexistent address must return an error.
	if err := repo.MarkTokenSymbolUnresolved(ctx, chainID, ghost); err == nil {
		t.Error("expected error from MarkTokenSymbolUnresolved on nonexistent address, got nil")
	}
}

// TestSymbolReconciliation_ResolveDoesNotClobberNonPending verifies that
// ResolveTokenSymbol does not overwrite a token whose symbol is already resolved
// (i.e. symbol_pending is absent/false). The call must return a non-nil error and
// leave the existing symbol unchanged.
func TestSymbolReconciliation_ResolveDoesNotClobberNonPending(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
	chainID := int64(1)
	anchorBlock := int64(999)

	// Insert a token that already has a resolved symbol (no pending flag).
	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr, "frxUSD", 18, anchorBlock); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Attempting to resolve a non-pending token must return an error.
	if err := repo.ResolveTokenSymbol(ctx, chainID, addr, "EVIL"); err == nil {
		t.Error("expected error from ResolveTokenSymbol on non-pending token, got nil")
	}

	// The symbol must remain unchanged.
	var symbol string
	if err := tokenPool.QueryRow(ctx,
		`SELECT symbol FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes(),
	).Scan(&symbol); err != nil {
		t.Fatalf("query symbol: %v", err)
	}
	if symbol != "frxUSD" {
		t.Errorf("symbol = %q after clobber attempt, want frxUSD", symbol)
	}
}
