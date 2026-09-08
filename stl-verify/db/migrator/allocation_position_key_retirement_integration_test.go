//go:build integration

package migrator_test

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// recoveryStatementFile is the operator re-run that converges
// allocation_position_current. 20260825_120100 is deliberately NOT it any more: that
// one merges every key history holds, so it puts a retired key straight back.
const (
	recoveryStatementFile   = "20260908_120100_converge_allocation_position_current_past_retired_keys.sql"
	retirementMigrationFile = "20260908_120000_create_allocation_position_key_retirement.sql"
)

// retirementFixture is one (token, prime, proxy) triple plus the writes a retirement
// test drives it with. Each fixture mints its own token, so subtests never contend.
type retirementFixture struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	chainID int
	tokenID int64
	primeID int64
	proxy   []byte
}

func newRetirementFixture(ctx context.Context, t *testing.T, pool *pgxpool.Pool, label string) *retirementFixture {
	t.Helper()

	f := &retirementFixture{ctx: ctx, pool: pool, chainID: 1}
	if err := pool.QueryRow(ctx, `SELECT id FROM prime WHERE name = 'spark'`).Scan(&f.primeID); err != nil {
		t.Fatalf("read the seeded prime: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals)
		VALUES (1, substring(sha256($1::text::bytea) for 20), $1::text, 18)
		RETURNING id`, label).Scan(&f.tokenID); err != nil {
		t.Fatalf("seed the token for %s: %v", label, err)
	}
	if err := pool.QueryRow(ctx, `SELECT substring(sha256(($1::text || '-proxy')::bytea) for 20)`, label).
		Scan(&f.proxy); err != nil {
		t.Fatalf("derive the proxy for %s: %v", label, err)
	}
	return f
}

// onChainToken points the fixture at an existing token, for the keys the migration
// seeds by natural key rather than the fixture minting its own.
func (f *retirementFixture) onChainToken(chainID int, tokenID int64) *retirementFixture {
	clone := *f
	clone.chainID = chainID
	clone.tokenID = tokenID
	return &clone
}

// setRetirement appends a retirement version. valid_from is explicit because it is part
// of the key: an un-retirement must sort after the retirement it lifts.
func (f *retirementFixture) setRetirement(t *testing.T, retired bool, validFrom time.Time) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO allocation_position_key_retirement (chain_id, token_id, retired, valid_from, reason, ticket)
		VALUES ($1, $2, $3, $4, 'erc7540_vault', 'TEST-535')`,
		f.chainID, f.tokenID, retired, validFrom); err != nil {
		t.Fatalf("append retirement (retired=%v): %v", retired, err)
	}
}

func (f *retirementFixture) appendHistory(t *testing.T, blockNumber int64) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO allocation_position
			(chain_id, token_id, prime_id, proxy_address, balance, block_number, block_version,
			 tx_hash, log_index, tx_amount, direction, build_id)
		VALUES ($5, $1, $2, $3, 4200, $4, 0, sha256($3), 0, 4200, 'in', 0)`,
		f.tokenID, f.primeID, f.proxy, blockNumber, f.chainID); err != nil {
		t.Fatalf("append history at block %d: %v", blockNumber, err)
	}
}

func (f *retirementFixture) cachedRows(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, `
		SELECT count(*) FROM allocation_position_current
		WHERE chain_id = $1 AND token_id = $2 AND proxy_address = $3`, f.chainID, f.tokenID, f.proxy).Scan(&n); err != nil {
		t.Fatalf("count cache rows: %v", err)
	}
	return n
}

// runMigrationFile executes a migration as one multi-statement query. The migrator wraps
// the file in an explicit transaction (migrator.go); this gets the simple protocol's
// implicit one — either way the file's SET LOCALs take effect. The trailing
// self-registration is a no-op on a filename already applied.
func runMigrationFile(ctx context.Context, t *testing.T, pool *pgxpool.Pool, filename string) {
	t.Helper()
	sql, err := os.ReadFile(filepath.Join(getMigrationsPath(), filename))
	if err != nil {
		t.Fatalf("read %s: %v", filename, err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("re-run %s: %v", filename, err)
	}
}

// TestAllocationPositionCurrentTriggerHonoursRetirement: the AFTER INSERT trigger caches
// a key only while it is not retired. This is what carries the deploy window — the
// migrate Job is a PreSync hook, so old pods keep appending to a retired key for the
// length of the rollout.
func TestAllocationPositionCurrentTriggerHonoursRetirement(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	for _, tc := range []struct {
		name       string
		retired    bool
		wantCached int
	}{
		{"a retired key is not cached", true, 0},
		{"a live key is cached as before", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetirementFixture(ctx, t, pool, tc.name)
			f.setRetirement(t, tc.retired, utcMidnight(t, "2026-09-01"))

			f.appendHistory(t, 21000000)

			if got := f.cachedRows(t); got != tc.wantCached {
				t.Errorf("cache rows for the key = %d, want %d", got, tc.wantCached)
			}
		})
	}
}

// TestRecoveryStatementDoesNotResurrectARetiredKey: the forward-only merge is the
// documented operator re-run, and allocation_position keeps the retired key's history
// forever, so without the exclusion every re-run would undo the retirement.
func TestRecoveryStatementDoesNotResurrectARetiredKey(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	f := newRetirementFixture(ctx, t, pool, "recovery-re-run")
	f.setRetirement(t, true, utcMidnight(t, "2026-09-01"))
	f.appendHistory(t, 21000000)

	runMigrationFile(ctx, t, pool, recoveryStatementFile)

	if got := f.cachedRows(t); got != 0 {
		t.Errorf("cache rows for the retired key after the recovery re-run = %d, want 0", got)
	}
}

// TestUnretiringAKeyResumesCaching: retirement is append-on-change, so lifting one is a
// newer row rather than a DELETE, and the trigger must read the newer row.
func TestUnretiringAKeyResumesCaching(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	f := newRetirementFixture(ctx, t, pool, "un-retire")
	f.setRetirement(t, true, utcMidnight(t, "2026-09-01"))
	f.appendHistory(t, 21000000)

	f.setRetirement(t, false, utcMidnight(t, "2026-09-02"))
	f.appendHistory(t, 21000001)

	if got := f.cachedRows(t); got != 1 {
		t.Errorf("cache rows after the un-retirement = %d, want 1", got)
	}
}

// TestTheVersionInForceIsTheLatestValidFromNotTheLatestInsert: a backdated correction
// arrives after the row it predates, so physical insert order must not decide.
func TestTheVersionInForceIsTheLatestValidFromNotTheLatestInsert(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	f := newRetirementFixture(ctx, t, pool, "backdated")
	f.setRetirement(t, true, utcMidnight(t, "2026-09-02"))
	f.setRetirement(t, false, utcMidnight(t, "2026-09-01"))

	f.appendHistory(t, 21000000)

	if got := f.cachedRows(t); got != 0 {
		t.Errorf("cache rows = %d, want 0 — a backdated un-retirement must not lift the newer retirement", got)
	}
}

// TestAFutureDatedRetirementIsNotYetInForce: valid_from is when a version takes
// effect, so a retirement scheduled for next week must not blank the key today.
func TestAFutureDatedRetirementIsNotYetInForce(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	f := newRetirementFixture(ctx, t, pool, "future-dated")
	f.setRetirement(t, true, time.Now().Add(24*time.Hour))

	f.appendHistory(t, 21000000)

	if got := f.cachedRows(t); got != 1 {
		t.Errorf("cache rows = %d, want 1 — a retirement dated in the future took effect early", got)
	}
}

// TestMigrationRetiresItsSeededKeysAndClearsTheirCacheRows: on a fresh database the seed
// matches no token and inserts nothing, so re-running the file over token rows at the
// four real addresses is the only thing that exercises the addresses themselves.
func TestMigrationRetiresItsSeededKeysAndClearsTheirCacheRows(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	seeded := seedRetiredVaultTokens(ctx, t, pool, 4)
	base := newRetirementFixture(ctx, t, pool, "seeded-keys")

	retiring := make([]*retirementFixture, 0, len(seeded))
	for i, key := range seeded {
		f := base.onChainToken(key.chainID, key.tokenID)
		f.appendHistory(t, int64(21000000+i))
		if got := f.cachedRows(t); got != 1 {
			t.Fatalf("cache rows for seeded key %d before the migration = %d, want 1", i, got)
		}
		retiring = append(retiring, f)
	}
	survivor := newRetirementFixture(ctx, t, pool, "not-retired")
	survivor.appendHistory(t, 21000100)
	if got := survivor.cachedRows(t); got != 1 {
		t.Fatalf("cache rows for the un-retired key = %d, want 1", got)
	}

	runMigrationFile(ctx, t, pool, retirementMigrationFile)
	runMigrationFile(ctx, t, pool, recoveryStatementFile)

	var retired int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM allocation_position_key_retirement WHERE ticket = 'VEC-535' AND retired`,
	).Scan(&retired); err != nil {
		t.Fatalf("count seeded retirements: %v", err)
	}
	if retired != len(seeded) {
		t.Errorf("seeded retirements = %d, want %d — an address or chain_id in the seed does not match token", retired, len(seeded))
	}
	for i, f := range retiring {
		if got := f.cachedRows(t); got != 0 {
			t.Errorf("cache rows for retired key %d after the purge = %d, want 0", i, got)
		}
	}
	if got := survivor.cachedRows(t); got != 1 {
		t.Errorf("cache rows for the un-retired key = %d, want 1 — the purge is not scoped to retired keys", got)
	}
}

// TestMigrationFailsWhenASeededAddressDoesNotMatchToken: the seed resolves four
// addresses by natural key and silently retires nothing if one is wrong, so the count
// is asserted absolutely rather than against the same literal list.
func TestMigrationFailsWhenASeededAddressDoesNotMatchToken(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	seedRetiredVaultTokens(ctx, t, pool, 3)

	sql, err := os.ReadFile(filepath.Join(getMigrationsPath(), retirementMigrationFile))
	if err != nil {
		t.Fatalf("read %s: %v", retirementMigrationFile, err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err == nil {
		t.Fatal("the migration accepted a partial seed; a typo'd address would retire nothing and pass")
	}
}

// TestRecoveryStatementPurgesACacheRowRetiredLater: retiring a key after this PR must
// not need a new migration — the documented re-run converges the cache both ways.
func TestRecoveryStatementPurgesACacheRowRetiredLater(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	f := newRetirementFixture(ctx, t, pool, "retired-later")
	f.appendHistory(t, 21000000)
	if got := f.cachedRows(t); got != 1 {
		t.Fatalf("cache rows before the retirement = %d, want 1", got)
	}

	f.setRetirement(t, true, utcMidnight(t, "2026-09-01"))
	runMigrationFile(ctx, t, pool, recoveryStatementFile)

	if got := f.cachedRows(t); got != 0 {
		t.Errorf("cache rows after the recovery re-run = %d, want 0", got)
	}
}

// retiredVaultKey is one of the four ERC-7540 vault keys the migration retires.
type retiredVaultKey struct {
	chainID int
	address []byte
	tokenID int64
}

// seedRetiredVaultTokens creates the token rows the migration's seed resolves by natural
// key, so the seed runs against a database that actually holds them.
func seedRetiredVaultTokens(ctx context.Context, t *testing.T, pool *pgxpool.Pool, n int) []retiredVaultKey {
	t.Helper()

	keys := []retiredVaultKey{
		{chainID: 1, address: mustDecodeAddress(t, "4880799ee5200fc58da299e965df644fbf46780b")},
		{chainID: 43114, address: mustDecodeAddress(t, "1121f4e21ed8b9bc1bb9a2952cdd8639ac897784")},
		{chainID: 1, address: mustDecodeAddress(t, "fe6920eb6c421f1179ca8c8d4170530cdbdfd77a")},
		{chainID: 43114, address: mustDecodeAddress(t, "fe6920eb6c421f1179ca8c8d4170530cdbdfd77a")},
	}[:n]
	for i, key := range keys {
		if err := pool.QueryRow(ctx, `
			INSERT INTO token (chain_id, address, symbol, decimals)
			VALUES ($1, $2, 'VAULT', 18) RETURNING id`, key.chainID, key.address,
		).Scan(&keys[i].tokenID); err != nil {
			t.Fatalf("seed token for chain %d: %v", key.chainID, err)
		}
	}
	return keys
}

func mustDecodeAddress(t *testing.T, hexAddress string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(hexAddress)
	if err != nil {
		t.Fatalf("decode %s: %v", hexAddress, err)
	}
	return raw
}
