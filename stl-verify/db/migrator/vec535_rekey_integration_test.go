//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	vec535RekeyFile    = "20260911_120000_rekey_grove_centrifuge_vault_positions.sql"
	vec535CapStatement = "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 500000;"
	vec535RecoveryFile = "20260825_120100_backfill_allocation_position_current.sql"
)

// The addresses 20260911_120000 hard-codes, in the order it lists them.
type vec535Pair struct {
	label   string
	chainID int
	proxy   string
	vault   string
	share   string
	// Mainnet shares carried rows before the vault window; the Avalanche shares had none,
	// so only there does the re-key move created_at_block.
	preWindowShareRows bool
}

var vec535Pairs = []vec535Pair{
	{"mainnet JAAA", 1, "491edfb0b8b608044e227225c715981a30f3a44e", "4880799ee5200fc58da299e965df644fbf46780b", "5a0f93d040de44e78f251b03c43be9cf317dcf64", true},
	{"mainnet JTRSY", 1, "491edfb0b8b608044e227225c715981a30f3a44e", "fe6920eb6c421f1179ca8c8d4170530cdbdfd77a", "8c213ee79581ff4984583c6a801e5263418c4b86", true},
	{"avalanche JAAA", 43114, "7107dd8f56642327945294a18a4280c78e153644", "1121f4e21ed8b9bc1bb9a2952cdd8639ac897784", "58f93d6b1ef2f44ec379cb975657c132cbed3b6b", false},
	{"avalanche JTRSY", 43114, "7107dd8f56642327945294a18a4280c78e153644", "fe6920eb6c421f1179ca8c8d4170530cdbdfd77a", "a5d465251fbcc907f5dd6bb2145488dfc6a2627b", false},
}

const (
	// Every seeded row fires allocation_position's two row triggers and the migrator package
	// shares one ten-minute go test budget; at a ten-minute stride 2,000 rows still span 14 chunks.
	vec535RowsPerPair      = 2_000
	vec535VaultStride      = "10 minutes"
	vec535VaultFirstBlock  = 1_000_001
	vec535ShareAfterBlock  = 2_000_001
	vec535ShareBeforeBlock = 900_000
	vec535PreWindowRows    = 5
	vec535PostWindowRows   = 3
	vec535VaultBalance     = "124807013.485611"
	vec535AfterBalance     = "612895921.626227"
	vec535ForeignWallet    = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
)

type vec535Seed struct {
	primeID  int64
	vaultIDs []int64
	shareIDs []int64
}

// seedVec535 lays down what the old tracker left behind: vault-keyed sweep rows across
// several daily chunks, share rows before (mainnet) and after (all) the vault window, and
// the trigger-fed cache rows that follow from them.
func seedVec535(ctx context.Context, t *testing.T, pool *pgxpool.Pool, vaultRowsPerPair int) vec535Seed {
	t.Helper()
	s := vec535Seed{primeID: seedVec535Prime(ctx, t, pool)}
	for _, p := range vec535Pairs {
		symbol := strings.Fields(p.label)[1]
		vaultID := upsertVec535Token(ctx, t, pool, p.chainID, p.vault, symbol, vec535VaultFirstBlock)
		shareID := upsertVec535Token(ctx, t, pool, p.chainID, p.share, symbol, vec535ShareAfterBlock)
		s.vaultIDs = append(s.vaultIDs, vaultID)
		s.shareIDs = append(s.shareIDs, shareID)

		if p.preWindowShareRows {
			insertVec535Sweeps(ctx, t, pool, vec535Sweeps{p.chainID, shareID, s.primeID, p.proxy, vec535ShareBeforeBlock, "2036-04-01 00:00:00+00", "1 hour", vec535PreWindowRows, vec535VaultBalance})
		}
		insertVec535Sweeps(ctx, t, pool, vec535Sweeps{p.chainID, vaultID, s.primeID, p.proxy, vec535VaultFirstBlock, "2036-05-01 00:00:00+00", vec535VaultStride, vaultRowsPerPair, vec535VaultBalance})
		insertVec535Sweeps(ctx, t, pool, vec535Sweeps{p.chainID, shareID, s.primeID, p.proxy, vec535ShareAfterBlock, "2036-06-01 00:00:00+00", "1 hour", vec535PostWindowRows, vec535AfterBalance})
	}
	return s
}

func seedVec535Prime(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	const name = "vec535-rekey-test"
	if _, err := pool.Exec(ctx, `
		INSERT INTO prime (external_id, name, vault_address) VALUES (gen_random_uuid(), $1, decode($2, 'hex'))
		ON CONFLICT (name) DO NOTHING`, name, "5353535353535353535353535353535353535353"); err != nil {
		t.Fatalf("seed prime: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx, `SELECT id FROM prime WHERE name = $1`, name).Scan(&id); err != nil {
		t.Fatalf("read prime: %v", err)
	}
	return id
}

// The priced mainnet shares are already registered by earlier migrations, so the token rows
// are upserted; created_at_block is set either way so the LEAST assertion has a known start.
func upsertVec535Token(ctx context.Context, t *testing.T, pool *pgxpool.Pool, chainID int, addressHex, symbol string, createdAtBlock int64) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals, created_at_block)
		VALUES ($1, decode($2, 'hex'), $3, 6, $4)
		ON CONFLICT (chain_id, address) DO UPDATE SET created_at_block = EXCLUDED.created_at_block
		RETURNING id`, chainID, addressHex, symbol, createdAtBlock).Scan(&id); err != nil {
		t.Fatalf("upsert token %s on chain %d: %v", addressHex, chainID, err)
	}
	return id
}

type vec535Sweeps struct {
	chainID    int
	tokenID    int64
	primeID    int64
	proxyHex   string
	firstBlock int64
	from       string
	stride     string
	count      int
	balance    string
}

func insertVec535Sweeps(ctx context.Context, t *testing.T, pool *pgxpool.Pool, s vec535Sweeps) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO allocation_position (chain_id, token_id, prime_id, proxy_address, balance, block_number, block_version, tx_hash, log_index, tx_amount, direction, created_at, processing_version, build_id)
		SELECT $1, $2, $3, decode($4, 'hex'), $5::numeric, $6 + g, 0, '\x00'::bytea, 0, 0, 'sweep',
		       $7::timestamptz + g * $8::interval, 0, 0
		FROM generate_series(0, $9 - 1) AS g`,
		s.chainID, s.tokenID, s.primeID, s.proxyHex, s.balance, s.firstBlock, s.from, s.stride, s.count); err != nil {
		t.Fatalf("seed %d sweep rows for token %d: %v", s.count, s.tokenID, err)
	}
}

func compressAllocationPositionChunks(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('allocation_position') AS c
		) x`).Scan(&n); err != nil {
		t.Fatalf("compress chunks: %v", err)
	}
	return n
}

func readMigrationFile(t *testing.T, filename string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(getMigrationsPath(), filename))
	if err != nil {
		t.Fatalf("read %s: %v", filename, err)
	}
	return string(content)
}

// runAsMigrator executes the SQL the way applyMigrationWithTx does: one Exec of the whole
// content inside one transaction, so SET LOCAL binds to it.
func runAsMigrator(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// probeAsMigrator runs the SQL like runAsMigrator but always rolls back, so a probe that
// happens to succeed leaves the database as it found it.
func probeAsMigrator(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, sql)
	return err
}

func countRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func requireVec535SeedShape(ctx context.Context, t *testing.T, pool *pgxpool.Pool, seed vec535Seed, vaultRowsPerPair int) {
	t.Helper()
	want := int64(vaultRowsPerPair * len(vec535Pairs))
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM allocation_position WHERE token_id = ANY($1)`, seed.vaultIDs); n != want {
		t.Fatalf("seed: %d vault rows, want %d", n, want)
	}
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM allocation_position_current WHERE token_id = ANY($1)`, seed.vaultIDs); n != 4 {
		t.Fatalf("seed: %d vault cache rows, want 4", n)
	}
}

func assertNoVaultKeyLeft(ctx context.Context, t *testing.T, pool *pgxpool.Pool, seed vec535Seed) {
	t.Helper()
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM allocation_position WHERE token_id = ANY($1)`, seed.vaultIDs); n != 0 {
		t.Errorf("%d history rows still on a vault key", n)
	}
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM allocation_position_current WHERE token_id = ANY($1)`, seed.vaultIDs); n != 0 {
		t.Errorf("%d cache rows still on a vault key", n)
	}
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM token WHERE id = ANY($1)`, seed.vaultIDs); n != 0 {
		t.Errorf("%d vault token rows survived", n)
	}
}

func assertVec535PairOnShare(ctx context.Context, t *testing.T, pool *pgxpool.Pool, p vec535Pair, shareID int64, vaultRowsPerPair int) {
	t.Helper()
	wantRows := int64(vaultRowsPerPair + vec535PostWindowRows)
	wantCreatedAtBlock := int64(vec535VaultFirstBlock)
	if p.preWindowShareRows {
		wantRows += vec535PreWindowRows
		wantCreatedAtBlock = vec535ShareBeforeBlock
	}
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM allocation_position WHERE token_id = $1`, shareID); n != wantRows {
		t.Errorf("%s: %d rows on the share, want %d", p.label, n, wantRows)
	}

	var cacheBlock int64
	var cacheBalance string
	if err := pool.QueryRow(ctx, `
		SELECT block_number, balance::text FROM allocation_position_current
		WHERE chain_id = $1 AND token_id = $2 AND proxy_address = decode($3, 'hex')`,
		p.chainID, shareID, p.proxy).Scan(&cacheBlock, &cacheBalance); err != nil {
		t.Fatalf("%s: share cache row: %v", p.label, err)
	}
	newestTrackerBlock := int64(vec535ShareAfterBlock + vec535PostWindowRows - 1)
	if cacheBlock != newestTrackerBlock || cacheBalance != vec535AfterBalance {
		t.Errorf("%s: share cache row is block %d balance %s, want the newest tracker row %d / %s",
			p.label, cacheBlock, cacheBalance, newestTrackerBlock, vec535AfterBalance)
	}

	var createdAtBlock int64
	if err := pool.QueryRow(ctx, `SELECT created_at_block FROM token WHERE id = $1`, shareID).Scan(&createdAtBlock); err != nil {
		t.Fatalf("%s: share token: %v", p.label, err)
	}
	if createdAtBlock != wantCreatedAtBlock {
		t.Errorf("%s: share created_at_block %d, want %d", p.label, createdAtBlock, wantCreatedAtBlock)
	}
}

// The recovery statement rebuilds the cache from history; after the re-key it must find
// nothing on a vault key to bring back.
func assertRecoveryStatementIsInert(ctx context.Context, t *testing.T, pool *pgxpool.Pool, seed vec535Seed) {
	t.Helper()
	recovery := readMigrationFile(t, vec535RecoveryFile)
	recovery = recovery[:strings.Index(recovery, "INSERT INTO migrations")]
	if err := runAsMigrator(ctx, pool, recovery); err != nil {
		t.Fatalf("re-run the cache recovery statement: %v", err)
	}
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM allocation_position_current WHERE token_id = ANY($1)`, seed.vaultIDs); n != 0 {
		t.Errorf("the recovery statement resurrected %d vault cache rows", n)
	}
}

// TestVEC535Rekey_MovesVaultHistoryOntoTheShare seeds a vault history across several
// compressed chunks and checks the file re-keys every row, drops the four cache rows and the
// four vault tokens, lowers created_at_block only where the share had no earlier row, leaves
// the cache in agreement with the recovery statement, and is a no-op on a second run.
func TestVEC535Rekey_MovesVaultHistoryOntoTheShare(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	testutil.SkipWithoutTimescaleDB(t, pool)

	seed := seedVec535(ctx, t, pool, vec535RowsPerPair)
	chunks := compressAllocationPositionChunks(ctx, t, pool)
	requireVec535SeedShape(ctx, t, pool, seed, vec535RowsPerPair)

	migration := readMigrationFile(t, vec535RekeyFile)
	started := time.Now()
	if err := runAsMigrator(ctx, pool, migration); err != nil {
		t.Fatalf("apply %s: %v", vec535RekeyFile, err)
	}
	t.Logf("re-keyed %d rows across %d compressed chunks in %s", vec535RowsPerPair*len(vec535Pairs), chunks, time.Since(started).Round(time.Millisecond))

	assertNoVaultKeyLeft(ctx, t, pool, seed)
	for i, p := range vec535Pairs {
		assertVec535PairOnShare(ctx, t, pool, p, seed.shareIDs[i], vec535RowsPerPair)
	}
	assertRecoveryStatementIsInert(ctx, t, pool, seed)

	if err := runAsMigrator(ctx, pool, migration); err != nil {
		t.Fatalf("second run, with no pair left to resolve: %v", err)
	}
}

// TestVEC535Rekey_DecompressionCapCountsPerStatement pins down what the cap the file raises
// counts: a cap below one pair's row count must trip, one above it must pass although the four
// UPDATEs together exceed it, and the file as written must pass.
func TestVEC535Rekey_DecompressionCapCountsPerStatement(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	testutil.SkipWithoutTimescaleDB(t, pool)

	seed := seedVec535(ctx, t, pool, vec535RowsPerPair)
	compressAllocationPositionChunks(ctx, t, pool)

	migration := readMigrationFile(t, vec535RekeyFile)
	stripped := strings.ReplaceAll(migration, vec535CapStatement, "")
	if stripped == migration {
		t.Fatal("the cap statement is no longer in the file; update this test")
	}
	withCap := func(n int) string {
		return "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = " + strconv.Itoa(n) + ";\n" + stripped
	}

	tripCap, passCap := vec535RowsPerPair/2, vec535RowsPerPair*3/2
	err := probeAsMigrator(ctx, pool, withCap(tripCap))
	if err == nil || !strings.Contains(err.Error(), "decompression limit") {
		t.Fatalf("cap %d under %d-row UPDATEs: got %v, want a tuple decompression limit error", tripCap, vec535RowsPerPair, err)
	}
	if err := probeAsMigrator(ctx, pool, withCap(passCap)); err != nil {
		t.Fatalf("cap %d over four %d-row UPDATEs: %v (the cap no longer counts per statement)", passCap, vec535RowsPerPair, err)
	}
	if err := runAsMigrator(ctx, pool, migration); err != nil {
		t.Fatalf("the file as written: %v", err)
	}
	assertNoVaultKeyLeft(ctx, t, pool, seed)
}

// TestVEC535Rekey_RefusesToDuplicateAShareRow: token_id is in the primary key, so a vault
// row whose natural key already exists on the share must fail the UPDATE, not double up.
func TestVEC535Rekey_RefusesToDuplicateAShareRow(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	seed := seedVec535(ctx, t, pool, 3)
	p := vec535Pairs[0]
	insertVec535Sweeps(ctx, t, pool, vec535Sweeps{p.chainID, seed.vaultIDs[0], seed.primeID, p.proxy, vec535ShareAfterBlock, "2036-06-01 00:00:00+00", "1 hour", 1, "1"})

	err := runAsMigrator(ctx, pool, readMigrationFile(t, vec535RekeyFile))
	if code := pgCode(err); code != "23505" {
		t.Fatalf("colliding row: got %v (code %q), want unique_violation 23505", err, code)
	}
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM token WHERE id = ANY($1)`, seed.vaultIDs); n != 4 {
		t.Errorf("the failed run deleted vault tokens: %d of 4 left", n)
	}
}

// TestVEC535Rekey_FailsLoudWhenAnotherWalletHoldsTheVaultKey: the contract attaches these
// vaults to Grove's wallets only, so a vault-keyed row under any other wallet means the
// file's premise is wrong. The token DELETE must then hit the FK and roll everything back
// rather than leave that wallet's history on a key without a token.
func TestVEC535Rekey_FailsLoudWhenAnotherWalletHoldsTheVaultKey(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	seed := seedVec535(ctx, t, pool, 3)
	p := vec535Pairs[2]
	insertVec535Sweeps(ctx, t, pool, vec535Sweeps{p.chainID, seed.vaultIDs[2], seed.primeID, vec535ForeignWallet, vec535VaultFirstBlock + 7, "2036-05-15 00:00:00+00", "1 hour", 1, "1"})

	err := runAsMigrator(ctx, pool, readMigrationFile(t, vec535RekeyFile))
	if code := pgCode(err); code != "23503" {
		t.Fatalf("foreign-wallet vault row: got %v (code %q), want foreign_key_violation 23503", err, code)
	}
	if n := countRows(ctx, t, pool, `SELECT count(*) FROM allocation_position WHERE token_id = ANY($1)`, seed.vaultIDs); n != 3*4+1 {
		t.Errorf("the failed run changed history: %d vault rows, want %d untouched", n, 3*4+1)
	}
}
