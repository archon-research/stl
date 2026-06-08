//go:build integration

package postgres

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const mapleSchemaName = "test_maple"

var maplePool *pgxpool.Pool

func init() {
	registerTestFileSetup(mapleSchemaName, func() {
		maplePool = testutil.SetupSchemaForMain(sharedDSN, mapleSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, maplePool, mapleSchemaName)
	})
}

// truncateMaple clears maple-related tables for test isolation.
// CASCADE on protocol/token/user wipes maple_vault rows that FK to them,
// so each test recreates its fixtures explicitly.
func truncateMaple(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, table := range []string{
		`maple_vault_state`,
		`maple_vault_position`,
		`maple_vault`,
	} {
		if _, err := maplePool.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("failed to truncate %s: %v", table, err)
		}
	}
	if _, err := maplePool.Exec(ctx, `TRUNCATE protocol CASCADE`); err != nil {
		t.Fatalf("failed to truncate protocol: %v", err)
	}
	if _, err := maplePool.Exec(ctx, `TRUNCATE "user" CASCADE`); err != nil {
		t.Fatalf("failed to truncate user: %v", err)
	}
	if _, err := maplePool.Exec(ctx, `TRUNCATE token CASCADE`); err != nil {
		t.Fatalf("failed to truncate token: %v", err)
	}
}

type mapleTestFixture struct {
	repo         *MapleRepository
	pool         *pgxpool.Pool
	protocolID   int64
	assetTokenID int64
	vaultID      int64
	userID       int64
	vaultAddress common.Address
}

func setupMapleTest(t *testing.T) *mapleTestFixture {
	t.Helper()
	ctx := context.Background()
	truncateMaple(t, ctx)

	repo, err := NewMapleRepository(maplePool, nil, 0)
	if err != nil {
		t.Fatalf("failed to create maple repository: %v", err)
	}

	f := &mapleTestFixture{
		repo:         repo,
		pool:         maplePool,
		vaultAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
	}
	f.createTestFixtures(t, ctx)
	return f
}

func (f *mapleTestFixture) createTestFixtures(t *testing.T, ctx context.Context) {
	t.Helper()

	err := f.pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (1, '\x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b'::bytea, 'maple-syrup-v1', 'lending', 0, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
	).Scan(&f.protocolID)
	if err != nil {
		t.Fatalf("failed to create protocol: %v", err)
	}

	err = f.pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES (1, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea, 'USDC', 6)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
		 RETURNING id`,
	).Scan(&f.assetTokenID)
	if err != nil {
		t.Fatalf("failed to create asset token: %v", err)
	}

	err = f.pool.QueryRow(ctx,
		`INSERT INTO maple_vault (chain_id, protocol_id, address, name, symbol,
		                          asset_token_id, pool_address, vault_version, created_at_block)
		 VALUES (1, $1, $2, 'Syrup USDC', 'syrupUSDC',
		         $3, '\x80226fc0ee2b096224eeac085bb9a8cba1146f7d'::bytea, 1, 20231245)
		 RETURNING id`,
		f.protocolID, f.vaultAddress.Bytes(), f.assetTokenID,
	).Scan(&f.vaultID)
	if err != nil {
		t.Fatalf("failed to create maple_vault: %v", err)
	}

	err = f.pool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block)
		 VALUES (1, '\x1111111111111111111111111111111111111111'::bytea, 20231245)
		 RETURNING id`,
	).Scan(&f.userID)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}
}

func TestMapleRepository_GetAllVaults(t *testing.T) {
	f := setupMapleTest(t)
	ctx := context.Background()

	vaults, err := f.repo.GetAllVaults(ctx, 1)
	if err != nil {
		t.Fatalf("GetAllVaults: %v", err)
	}
	if len(vaults) != 1 {
		t.Fatalf("expected 1 vault, got %d", len(vaults))
	}
	v, ok := vaults[f.vaultAddress]
	if !ok {
		t.Fatalf("expected vault at %s, got keys: %v", f.vaultAddress.Hex(), keys(vaults))
	}
	if v.Symbol != "syrupUSDC" || v.AssetTokenID != f.assetTokenID || v.VaultVersion != 1 {
		t.Fatalf("vault fields mis-set: %+v", v)
	}
	if v.ChainID != 1 {
		t.Fatalf("ChainID mis-set: %d", v.ChainID)
	}
	// Decimals is joined from the underlying token row (USDC = 6).
	if v.Decimals != 6 {
		t.Fatalf("Decimals mis-set: got %d, want 6", v.Decimals)
	}
}

func TestMapleRepository_SaveVaultState_Insert(t *testing.T) {
	f := setupMapleTest(t)
	ctx := context.Background()

	ts := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	state, err := entity.NewMapleVaultState(f.vaultID, 18_500_000, 0, ts,
		big.NewInt(1_000_000_000_000), big.NewInt(900_000_000_000), big.NewInt(1_111_111))
	if err != nil {
		t.Fatalf("entity: %v", err)
	}
	state.WithPrices(big.NewInt(99_900_000), nil)

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := f.repo.SaveVaultState(ctx, tx, state); err != nil {
		t.Fatalf("save vault state: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var (
		total       string
		sharePrice  string
		ulPriceUSD  *string
		syrupUSDPtr *string
	)
	err = f.pool.QueryRow(ctx,
		`SELECT total_assets::text, share_price::text, underlying_price_usd::text, syrup_price_usd::text
		   FROM maple_vault_state
		  WHERE maple_vault_id = $1 AND block_number = $2`,
		f.vaultID, 18_500_000).Scan(&total, &sharePrice, &ulPriceUSD, &syrupUSDPtr)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if total != "1000000000000" {
		t.Fatalf("total_assets: got %s", total)
	}
	if sharePrice != "1111111" {
		t.Fatalf("share_price: got %s", sharePrice)
	}
	if ulPriceUSD == nil || *ulPriceUSD != "99900000" {
		t.Fatalf("underlying_price_usd: got %v", ulPriceUSD)
	}
	if syrupUSDPtr != nil {
		t.Fatalf("syrup_price_usd: expected NULL, got %v", *syrupUSDPtr)
	}
}

func TestMapleRepository_SaveVaultState_Idempotent_SameBuildID(t *testing.T) {
	f := setupMapleTest(t)
	ctx := context.Background()

	ts := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	state, err := entity.NewMapleVaultState(f.vaultID, 18_500_000, 0, ts,
		big.NewInt(1), big.NewInt(1), big.NewInt(1))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if err := f.repo.SaveVaultState(ctx, tx, state); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	var count int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM maple_vault_state
		   WHERE maple_vault_id = $1 AND block_number = $2`,
		f.vaultID, 18_500_000).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("trigger must dedupe same-build retries — got %d rows", count)
	}
}

func TestMapleRepository_SaveVaultState_NewVersionOnNewBuildID(t *testing.T) {
	f := setupMapleTest(t)
	ctx := context.Background()

	repoB1, err := NewMapleRepository(f.pool, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	repoB2, err := NewMapleRepository(f.pool, nil, 2)
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	state, err := entity.NewMapleVaultState(f.vaultID, 18_500_000, 0, ts,
		big.NewInt(1), big.NewInt(1), big.NewInt(1))
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range []*MapleRepository{repoB1, repoB2} {
		tx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.SaveVaultState(ctx, tx, state); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	var versions []int
	rows, err := f.pool.Query(ctx,
		`SELECT processing_version FROM maple_vault_state
		   WHERE maple_vault_id = $1 AND block_number = $2
		   ORDER BY processing_version ASC`,
		f.vaultID, 18_500_000)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, v)
	}
	if len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Fatalf("expected processing_versions [0,1], got %v", versions)
	}
}

func TestMapleRepository_SaveVaultPositions_BatchInsert(t *testing.T) {
	f := setupMapleTest(t)
	ctx := context.Background()

	// Add a second user so we batch > 1 row.
	var user2ID int64
	if err := f.pool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block)
		 VALUES (1, '\x2222222222222222222222222222222222222222'::bytea, 20231245)
		 RETURNING id`,
	).Scan(&user2ID); err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	p1, err := entity.NewMapleVaultPosition(f.userID, f.vaultID, 18_500_000, 0, ts,
		big.NewInt(500), big.NewInt(550))
	if err != nil {
		t.Fatal(err)
	}
	p2, err := entity.NewMapleVaultPosition(user2ID, f.vaultID, 18_500_000, 0, ts,
		big.NewInt(750), big.NewInt(825))
	if err != nil {
		t.Fatal(err)
	}

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repo.SaveVaultPositions(ctx, tx, []*entity.MapleVaultPosition{p1, p2}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM maple_vault_position WHERE maple_vault_id = $1`,
		f.vaultID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 position rows, got %d", count)
	}
}

func TestMapleRepository_SaveVaultPositions_EmptyNoOp(t *testing.T) {
	f := setupMapleTest(t)
	ctx := context.Background()

	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.repo.SaveVaultPositions(ctx, tx, nil); err != nil {
		t.Fatalf("expected nil error on empty input, got: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNewMapleRepository_NilPoolRejected(t *testing.T) {
	if _, err := NewMapleRepository(nil, nil, 0); err == nil {
		t.Fatal("expected error on nil pool")
	}
}

func keys(m map[common.Address]*entity.MapleVault) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k.Hex())
	}
	return out
}
