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

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// i64 returns a pointer to n, for the *int64 "first known block" args. Shared
// by the token and user integration tests in this package.
func i64(n int64) *int64 { return &n }

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

	id, err := repo.GetOrCreateToken(ctx, tx, 1, addr, "TKN", 18, i64(500))
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
	id1, err := repo.GetOrCreateToken(ctx, tx1, 1, addr, "TKN", 18, i64(500))
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
	id2, err := repo.GetOrCreateToken(ctx, tx2, 1, addr, "TKN", 18, i64(500))
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

	if _, err := repo.GetOrCreateToken(ctx, tx, 1, addr, "", 6, i64(100)); err != nil {
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
	if _, err := repo.GetOrCreateToken(ctx, tx1, 1, addr, "TKN", 18, i64(500)); err != nil {
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
	if _, err := repo.GetOrCreateToken(ctx, tx2, 1, addr, "TKN", 18, i64(100)); err != nil {
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
			id, err := repo.GetOrCreateToken(ctx, tx, 1, addr, "RACE", 18, i64(int64(1000+idx)))
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

// TestListTokensMissingSymbol verifies that tokens with an empty symbol are
// returned by ListTokensMissingSymbol, while tokens with a non-empty symbol and
// the zero-address sentinel are excluded.
func TestListTokensMissingSymbol(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	chainID := int64(1)
	emptyAddr := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")
	resolvedAddr := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	zeroAddr := common.Address{}

	// Insert token with empty symbol (pending).
	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, emptyAddr, "", 18, i64(100)); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken(empty): %v", err)
	}
	// Insert token with resolved symbol (must be excluded).
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, resolvedAddr, "USDC", 6, i64(200)); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken(resolved): %v", err)
	}
	// Insert zero-address sentinel (must always be excluded).
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, zeroAddr, "", 0, nil); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken(zero): %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	missing, err := repo.ListTokensMissingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensMissingSymbol: %v", err)
	}
	if len(missing) != 1 {
		t.Fatalf("expected 1 missing-symbol token, got %d: %v", len(missing), missing)
	}
	if missing[0] != emptyAddr {
		t.Errorf("address = %s, want %s", missing[0].Hex(), emptyAddr.Hex())
	}
}

// TestListTokensMissingSymbol_RespectsLimit verifies that the limit parameter is
// honoured and that a zero or negative limit returns an error.
func TestListTokensMissingSymbol_RespectsLimit(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	chainID := int64(1)
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr1, "", 18, i64(100)); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken(addr1): %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr2, "", 18, i64(200)); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken(addr2): %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Limit of 1 must return only one row.
	missing, err := repo.ListTokensMissingSymbol(ctx, chainID, 1)
	if err != nil {
		t.Fatalf("ListTokensMissingSymbol(limit=1): %v", err)
	}
	if len(missing) != 1 {
		t.Errorf("limit=1: got %d rows, want 1", len(missing))
	}

	// Limit <= 0 must return an error.
	if _, err := repo.ListTokensMissingSymbol(ctx, chainID, 0); err == nil {
		t.Error("expected error for limit=0, got nil")
	}
}

// TestResolveTokenSymbol_FillsEmptyAndRefusesClobber verifies that
// ResolveTokenSymbol fills an empty-symbol row and returns an error (without
// clobbering) when called for a token that already has a symbol.
func TestResolveTokenSymbol_FillsEmptyAndRefusesClobber(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	chainID := int64(1)
	addr := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")

	// Insert token with empty symbol.
	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr, "", 18, i64(100)); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Resolve fills the empty symbol.
	if err := repo.ResolveTokenSymbol(ctx, chainID, addr, "frxUSD"); err != nil {
		t.Fatalf("ResolveTokenSymbol: %v", err)
	}

	// Confirm the symbol is set in the database.
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

	// Token no longer has an empty symbol, so it must no longer appear in the missing list.
	missing, err := repo.ListTokensMissingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensMissingSymbol: %v", err)
	}
	if len(missing) != 0 {
		t.Errorf("expected 0 missing-symbol tokens after resolve, got %d", len(missing))
	}

	// Attempting to resolve again (already-set symbol) must return an error and
	// leave the existing symbol unchanged.
	if err := repo.ResolveTokenSymbol(ctx, chainID, addr, "EVIL"); err == nil {
		t.Error("expected error from ResolveTokenSymbol when symbol is already set, got nil")
	}
	if err := tokenPool.QueryRow(ctx,
		`SELECT symbol FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes(),
	).Scan(&symbol); err != nil {
		t.Fatalf("query symbol after clobber attempt: %v", err)
	}
	if symbol != "frxUSD" {
		t.Errorf("symbol = %q after clobber attempt, want frxUSD", symbol)
	}
}

// TestListTokensMissingSymbol_NullSymbolIsMissing verifies that a token row with
// a NULL symbol (the column is nullable; legacy/manual rows may omit it) is
// treated the same as an empty symbol: listed by the sweep and resolvable.
func TestListTokensMissingSymbol_NullSymbolIsMissing(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	chainID := int64(1)
	addr := common.HexToAddress("0xABABABABABABABABABABABABABABABABABABABAB")

	// Insert directly with symbol omitted so the column is SQL NULL.
	if _, err := tokenPool.Exec(ctx,
		`INSERT INTO token (chain_id, address, decimals, created_at_block, updated_at)
		 VALUES ($1, $2, 18, 100, NOW())`,
		chainID, addr.Bytes()); err != nil {
		t.Fatalf("insert NULL-symbol token: %v", err)
	}
	var isNull bool
	if err := tokenPool.QueryRow(ctx,
		`SELECT symbol IS NULL FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes()).Scan(&isNull); err != nil {
		t.Fatalf("verify NULL precondition: %v", err)
	}
	if !isNull {
		t.Fatal("precondition failed: symbol is not NULL")
	}

	missing, err := repo.ListTokensMissingSymbol(ctx, chainID, 100)
	if err != nil {
		t.Fatalf("ListTokensMissingSymbol: %v", err)
	}
	if len(missing) != 1 || missing[0] != addr {
		t.Fatalf("missing = %v, want exactly the NULL-symbol token %s", missing, addr.Hex())
	}

	if err := repo.ResolveTokenSymbol(ctx, chainID, addr, "FIXED"); err != nil {
		t.Fatalf("ResolveTokenSymbol on NULL-symbol row: %v", err)
	}
	var symbol string
	if err := tokenPool.QueryRow(ctx,
		`SELECT symbol FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes()).Scan(&symbol); err != nil {
		t.Fatalf("query symbol: %v", err)
	}
	if symbol != "FIXED" {
		t.Errorf("symbol = %q, want FIXED", symbol)
	}
}

// TestResolveTokenSymbol_RejectsEmptySymbol verifies the contract guard: an
// empty resolution would match the missing-symbol predicate, report success,
// and leave the row pending, so it must be rejected outright. A pending token
// is seeded first so the failure can only come from the empty-symbol guard —
// without it, the UPDATE would match the seeded row and report success.
func TestResolveTokenSymbol_RejectsEmptySymbol(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	chainID := int64(1)
	addr := common.HexToAddress("0xCDCDCDCDCDCDCDCDCDCDCDCDCDCDCDCDCDCDCDCD")
	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := repo.GetOrCreateToken(ctx, tx, chainID, addr, "", 18, i64(100)); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("GetOrCreateToken: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := repo.ResolveTokenSymbol(ctx, chainID, addr, ""); err == nil {
		t.Fatal("expected error when resolving with an empty symbol")
	}

	// The seeded row must be untouched: still empty, still listed as missing.
	var symbol string
	if err := tokenPool.QueryRow(ctx,
		`SELECT symbol FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes()).Scan(&symbol); err != nil {
		t.Fatalf("query symbol: %v", err)
	}
	if symbol != "" {
		t.Errorf("symbol = %q, want empty (rejected resolve must not write)", symbol)
	}
}

// TestGetOrCreateTokens_DedupesBatch verifies the batch path deduplicates a
// duplicate address in the input: usdc appears twice but yields one row.
func TestGetOrCreateTokens_DedupesBatch(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	usdc := common.HexToAddress("0xE1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1")
	usdt := common.HexToAddress("0xE2E2E2E2E2E2E2E2E2E2E2E2E2E2E2E2E2E2E2E2")

	var first map[common.Address]int64
	inTokenTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		first, err = repo.GetOrCreateTokens(ctx, tx, []outbound.TokenInput{
			{ChainID: 1, Address: usdc, Symbol: "USDC", Decimals: 6},
			{ChainID: 1, Address: usdt, Symbol: "USDT", Decimals: 6},
			{ChainID: 1, Address: usdc, Symbol: "USDC", Decimals: 6},
		})
		return err
	})
	if len(first) != 2 {
		t.Fatalf("len(first) = %d, want 2", len(first))
	}
}

// TestGetOrCreateTokens_NilBlockInsertsNull verifies a nil incoming block
// inserts a NULL created_at_block rather than a sentinel.
func TestGetOrCreateTokens_NilBlockInsertsNull(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	usdc := common.HexToAddress("0xE1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1E1")

	inTokenTx(t, ctx, func(tx pgx.Tx) error {
		_, err := repo.GetOrCreateTokens(ctx, tx, []outbound.TokenInput{
			{ChainID: 1, Address: usdc, Symbol: "USDC", Decimals: 6},
		})
		return err
	})

	var cab *int64
	if err := tokenPool.QueryRow(ctx,
		`SELECT created_at_block FROM token WHERE chain_id = 1 AND address = $1`,
		usdc.Bytes()).Scan(&cab); err != nil {
		t.Fatalf("querying token: %v", err)
	}
	if cab != nil {
		t.Errorf("created_at_block = %v, want NULL", *cab)
	}
}

// TestGetOrCreateTokens_NilBlockPreservesExisting is the core VEC-353 clobber
// fix: a token seeded with a real on-chain block keeps it when an
// off-block-context (nil) caller re-upserts it (LEAST ignores NULL).
func TestGetOrCreateTokens_NilBlockPreservesExisting(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	existing := common.HexToAddress("0xE3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3")
	var existingID int64
	if err := tokenPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, metadata, updated_at)
		 VALUES (1, $1, 'WETH', 18, 12345, '{}'::jsonb, NOW()) RETURNING id`,
		existing.Bytes()).Scan(&existingID); err != nil {
		t.Fatalf("seeding existing token: %v", err)
	}

	var second map[common.Address]int64
	inTokenTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		second, err = repo.GetOrCreateTokens(ctx, tx, []outbound.TokenInput{
			{ChainID: 1, Address: existing, Symbol: "WETH", Decimals: 18},
		})
		return err
	})
	if second[existing] != existingID {
		t.Errorf("existing token id = %d, want %d", second[existing], existingID)
	}
	var preservedCAB int64
	if err := tokenPool.QueryRow(ctx,
		`SELECT created_at_block FROM token WHERE id = $1`,
		existingID).Scan(&preservedCAB); err != nil {
		t.Fatalf("querying preserved token: %v", err)
	}
	if preservedCAB != 12345 {
		t.Errorf("created_at_block = %d, want 12345 (must not be clobbered)", preservedCAB)
	}
}

// TestGetOrCreateTokens_SymbolDriftWarns verifies a differing symbol only
// warns: the call still succeeds, returns the existing id, and the stored
// symbol wins.
func TestGetOrCreateTokens_SymbolDriftWarns(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	existing := common.HexToAddress("0xE3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3E3")
	var existingID int64
	if err := tokenPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, metadata, updated_at)
		 VALUES (1, $1, 'WETH', 18, '{}'::jsonb, NOW()) RETURNING id`,
		existing.Bytes()).Scan(&existingID); err != nil {
		t.Fatalf("seeding existing token: %v", err)
	}

	var second map[common.Address]int64
	inTokenTx(t, ctx, func(tx pgx.Tx) error {
		var err error
		second, err = repo.GetOrCreateTokens(ctx, tx, []outbound.TokenInput{
			{ChainID: 1, Address: existing, Symbol: "DIFFERENT", Decimals: 18},
		})
		return err
	})
	if second[existing] != existingID {
		t.Errorf("existing token id = %d, want %d", second[existing], existingID)
	}
	var preservedSymbol string
	if err := tokenPool.QueryRow(ctx,
		`SELECT symbol FROM token WHERE id = $1`,
		existingID).Scan(&preservedSymbol); err != nil {
		t.Fatalf("querying preserved token: %v", err)
	}
	if preservedSymbol != "WETH" {
		t.Errorf("symbol = %s, want WETH (drift must warn, not clobber)", preservedSymbol)
	}
}

// TestGetOrCreateTokens_NullBlockSelfHeals verifies that a row first written
// with a NULL created_at_block (off-block-context caller) is healed to a real
// block when a later on-chain observation supplies one: LEAST(NULL, N) = N.
func TestGetOrCreateTokens_NullBlockSelfHeals(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xF1F1F1F1F1F1F1F1F1F1F1F1F1F1F1F1F1F1F1F1")

	// First: nil block -> stored NULL.
	inTokenTx(t, ctx, func(tx pgx.Tx) error {
		_, err := repo.GetOrCreateTokens(ctx, tx, []outbound.TokenInput{{ChainID: 1, Address: addr, Symbol: "USDC", Decimals: 6}})
		return err
	})

	// Later: a real on-chain block arrives and must heal the NULL.
	inTokenTx(t, ctx, func(tx pgx.Tx) error {
		_, err := repo.GetOrCreateTokens(ctx, tx, []outbound.TokenInput{{ChainID: 1, Address: addr, Symbol: "USDC", Decimals: 6, CreatedAtBlock: i64(900)}})
		return err
	})

	var cab *int64
	if err := tokenPool.QueryRow(ctx,
		`SELECT created_at_block FROM token WHERE chain_id = 1 AND address = $1`,
		addr.Bytes()).Scan(&cab); err != nil {
		t.Fatalf("querying token: %v", err)
	}
	if cab == nil || *cab != 900 {
		t.Errorf("created_at_block = %v, want 900 (LEAST should heal NULL to the real block)", cab)
	}
}

// TestGetOrCreateTokens_DecimalsDriftFails verifies the immutable-decimals
// guard moved into the shared batch upsert: a stored decimals differing from
// the incoming one fails the call rather than returning an id keyed to stale
// scaling.
func TestGetOrCreateTokens_DecimalsDriftFails(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	repo, err := NewTokenRepository(tokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	existing := common.HexToAddress("0xE4E4E4E4E4E4E4E4E4E4E4E4E4E4E4E4E4E4E4E4")
	if _, err := tokenPool.Exec(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, metadata, updated_at)
		 VALUES (1, $1, 'USDC', 6, '{}'::jsonb, NOW())`,
		existing.Bytes()); err != nil {
		t.Fatalf("seeding existing token: %v", err)
	}

	tx, err := tokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	_, driftErr := repo.GetOrCreateTokens(ctx, tx, []outbound.TokenInput{
		{ChainID: 1, Address: existing, Symbol: "USDC", Decimals: 18},
	})
	if driftErr == nil {
		t.Fatal("expected decimals-drift error, got nil")
	}
	if !strings.Contains(driftErr.Error(), "decimals changed") {
		t.Errorf("error %q should report the decimals drift", driftErr.Error())
	}
}

// TestTokenCreatedAtBlockCheckConstraint verifies the VEC-353 migration guard:
// a literal created_at_block of 0 is rejected at the column level so a future
// "unknown masquerading as genesis" write fails loudly instead of clobbering.
func TestTokenCreatedAtBlockCheckConstraint(t *testing.T) {
	truncateToken(t, context.Background())
	ctx := context.Background()

	addr := common.HexToAddress("0xE5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5E5")
	_, err := tokenPool.Exec(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, metadata, updated_at)
		 VALUES (1, $1, 'ZERO', 18, 0, '{}'::jsonb, NOW())`,
		addr.Bytes())
	if err == nil {
		t.Fatal("expected CHECK constraint violation for created_at_block = 0, got nil")
	}
	if !strings.Contains(err.Error(), "token_created_at_block_positive") {
		t.Errorf("error %q should name the check constraint", err.Error())
	}
}

// inTokenTx runs fn inside a committed transaction against tokenPool.
func inTokenTx(t *testing.T, ctx context.Context, fn func(tx pgx.Tx) error) {
	t.Helper()
	tx, err := tokenPool.Begin(ctx)
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
