//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// seedOracleAsset registers one aave-style (feedless) oracle_asset version. Each caller passes
// its own oracle name, so two fixtures in one database never share a natural key.
func seedOracleAsset(ctx context.Context, t *testing.T, pool *pgxpool.Pool, oracleName string, enabled bool, validFrom string) (oracleID, tokenID int64) {
	t.Helper()

	if err := pool.QueryRow(ctx, `
		INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
		VALUES ($1::text, $1::text, 1, sha256($1::text::bytea), 'aave_oracle', 1, 8, true)
		RETURNING id`, oracleName).Scan(&oracleID); err != nil {
		t.Fatalf("seed oracle %s: %v", oracleName, err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals)
		VALUES (1, substring(sha256($1::text::bytea) for 20), $1::text, 18)
		RETURNING id`, oracleName+"-token").Scan(&tokenID); err != nil {
		t.Fatalf("seed token for %s: %v", oracleName, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, valid_from, change_reason)
		VALUES ($1, $2, $3, $4::date, 'test fixture')`, oracleID, tokenID, enabled, validFrom); err != nil {
		t.Fatalf("seed oracle_asset for %s: %v", oracleName, err)
	}
	return oracleID, tokenID
}

// setEnabled calls the append-on-change writer and returns the appended row's
// processing_version, or -1 when the call was a no-op (NULL: value unchanged).
func setEnabled(ctx context.Context, t *testing.T, pool *pgxpool.Pool, oracleID, tokenID int64, enabled bool, effectiveAt, reason string) int {
	t.Helper()
	var pv *int
	if err := pool.QueryRow(ctx,
		`SELECT oracle_asset_set_enabled($1, $2, NULL, $3, $4::date, $5)`,
		oracleID, tokenID, enabled, effectiveAt, reason).Scan(&pv); err != nil {
		t.Fatalf("oracle_asset_set_enabled(enabled=%v, %s): %v", enabled, effectiveAt, err)
	}
	if pv == nil {
		return -1
	}
	return *pv
}

// TestOracleAssetToggleAppendsNewVersion is the contract: retiring a source appends a version
// instead of overwriting the row, so the pre-toggle reference view survives.
func TestOracleAssetToggleAppendsNewVersion(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-toggle", true, "2026-01-01")

	if pv := setEnabled(ctx, t, pool, oracleID, tokenID, false, "2026-08-20", "VEC-549: retired"); pv != 1 {
		t.Fatalf("appended processing_version = %d, want 1", pv)
	}

	rows, err := pool.Query(ctx, `
		SELECT processing_version, enabled, valid_from::text, change_reason
		FROM oracle_asset
		WHERE oracle_id = $1 AND token_id = $2
		ORDER BY processing_version`, oracleID, tokenID)
	if err != nil {
		t.Fatalf("read version history: %v", err)
	}
	defer rows.Close()

	type version struct {
		pv        int
		enabled   bool
		validFrom string
		reason    string
	}
	var got []version
	for rows.Next() {
		var v version
		if err := rows.Scan(&v.pv, &v.enabled, &v.validFrom, &v.reason); err != nil {
			t.Fatalf("scan version: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate versions: %v", err)
	}

	want := []version{
		{0, true, "2026-01-01", "test fixture"},
		{1, false, "2026-08-20", "VEC-549: retired"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d versions %+v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("version %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestOracleAssetAsOfReadsTheVersionEffectiveThen is the read half: a calculation
// pinned to an effective_at before the retirement still sees the source it used.
func TestOracleAssetAsOfReadsTheVersionEffectiveThen(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-asof", true, "2026-01-01")
	setEnabled(ctx, t, pool, oracleID, tokenID, false, "2026-08-20", "VEC-549: retired")

	for _, tc := range []struct {
		name        string
		effectiveAt string
		wantRows    int
		wantEnabled bool
	}{
		{"before the first version", "2025-12-31", 0, false},
		{"while enabled", "2026-06-01", 1, true},
		{"on the retirement date", "2026-08-20", 1, false},
		{"after the retirement", "2026-12-31", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := pool.Query(ctx, `
				SELECT enabled FROM oracle_asset_as_of($1::date)
				WHERE oracle_id = $2 AND token_id = $3`, tc.effectiveAt, oracleID, tokenID)
			if err != nil {
				t.Fatalf("oracle_asset_as_of(%s): %v", tc.effectiveAt, err)
			}
			defer rows.Close()

			var enabled []bool
			for rows.Next() {
				var e bool
				if err := rows.Scan(&e); err != nil {
					t.Fatalf("scan: %v", err)
				}
				enabled = append(enabled, e)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows: %v", err)
			}
			if len(enabled) != tc.wantRows {
				t.Fatalf("as_of(%s) returned %d rows, want %d", tc.effectiveAt, len(enabled), tc.wantRows)
			}
			if tc.wantRows > 0 && enabled[0] != tc.wantEnabled {
				t.Errorf("as_of(%s) enabled = %v, want %v", tc.effectiveAt, enabled[0], tc.wantEnabled)
			}
		})
	}
}

// TestOracleAssetVersionsDerivesTheValidityWindow is the history read: half-open windows
// [valid_from, valid_to_exclusive), so "which mapping applied on D" needs no call-site logic.
func TestOracleAssetVersionsDerivesTheValidityWindow(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-versions", true, "2026-01-01")
	setEnabled(ctx, t, pool, oracleID, tokenID, false, "2026-08-20", "VEC-549: retired")

	rows, err := pool.Query(ctx, `
		SELECT enabled, valid_from::text, coalesce(valid_to_exclusive::text, ''), is_current
		FROM oracle_asset_versions
		WHERE oracle_id = $1 AND token_id = $2
		ORDER BY valid_from`, oracleID, tokenID)
	if err != nil {
		t.Fatalf("read oracle_asset_versions: %v", err)
	}
	defer rows.Close()

	type window struct {
		enabled   bool
		from      string
		toExcl    string
		isCurrent bool
	}
	var got []window
	for rows.Next() {
		var w window
		if err := rows.Scan(&w.enabled, &w.from, &w.toExcl, &w.isCurrent); err != nil {
			t.Fatalf("scan window: %v", err)
		}
		got = append(got, w)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate windows: %v", err)
	}

	want := []window{
		{true, "2026-01-01", "2026-08-20", false},
		{false, "2026-08-20", "", true},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d windows %+v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("window %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestOracleAssetCurrentIgnoresAFutureDatedVersion: a version inserted ahead of its
// effective date must not become current early. Also why _current is banned from
// calculation SQL — the same row flips the answer once its valid_from arrives.
func TestOracleAssetCurrentIgnoresAFutureDatedVersion(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-future", true, "2026-01-01")
	future := time.Now().UTC().AddDate(0, 0, 10).Format(time.DateOnly)
	setEnabled(ctx, t, pool, oracleID, tokenID, false, future, "announced retirement")

	var currentEnabled bool
	if err := pool.QueryRow(ctx, `
		SELECT enabled FROM oracle_asset_current
		WHERE oracle_id = $1 AND token_id = $2`, oracleID, tokenID).Scan(&currentEnabled); err != nil {
		t.Fatalf("read oracle_asset_current: %v", err)
	}
	if !currentEnabled {
		t.Error("oracle_asset_current shows the future-dated version as current")
	}

	var futureEnabled bool
	if err := pool.QueryRow(ctx, `
		SELECT enabled FROM oracle_asset_as_of($1::date)
		WHERE oracle_id = $2 AND token_id = $3`, future, oracleID, tokenID).Scan(&futureEnabled); err != nil {
		t.Fatalf("read oracle_asset_as_of(%s): %v", future, err)
	}
	if futureEnabled {
		t.Errorf("oracle_asset_as_of(%s) does not see the version effective that day", future)
	}
}

// TestOracleAssetSetEnabledIsANoOpWhenUnchanged keeps append-ON-CHANGE honest: re-asserting
// the current value must not manufacture a payload-identical version.
func TestOracleAssetSetEnabledIsANoOpWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-noop", true, "2026-01-01")

	if pv := setEnabled(ctx, t, pool, oracleID, tokenID, true, "2026-08-20", "no change"); pv != -1 {
		t.Errorf("re-asserting the current value appended processing_version %d, want a NULL no-op", pv)
	}

	var versions int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM oracle_asset WHERE oracle_id = $1 AND token_id = $2`,
		oracleID, tokenID).Scan(&versions); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if versions != 1 {
		t.Errorf("got %d versions after a no-op toggle, want 1", versions)
	}
}

// TestOracleAssetSetEnabledRejectsADateBeforeTheFirstVersion: appending a row with nothing to
// supersede would claim the asset was disabled before it was ever registered.
func TestOracleAssetSetEnabledRejectsADateBeforeTheFirstVersion(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-backdate", true, "2026-06-01")

	var pv *int
	err := pool.QueryRow(ctx,
		`SELECT oracle_asset_set_enabled($1, $2, NULL, false, '2026-01-01'::date, 'backdated')`,
		oracleID, tokenID).Scan(&pv)
	if err == nil {
		t.Fatal("an effective date before the first version was accepted; want an error")
	}
}

// TestOracleAssetSetEnabledComparesAgainstTheVersionEffectiveThen: with a retirement already
// recorded for next week, retiring TODAY must still append. Comparing against the newest row
// would see "already disabled" and leave the source live until next week.
func TestOracleAssetSetEnabledComparesAgainstTheVersionEffectiveThen(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-pending", true, "2026-01-01")
	nextWeek := time.Now().UTC().AddDate(0, 0, 7).Format(time.DateOnly)
	setEnabled(ctx, t, pool, oracleID, tokenID, false, nextWeek, "announced retirement")

	today := time.Now().UTC().Format(time.DateOnly)
	if pv := setEnabled(ctx, t, pool, oracleID, tokenID, false, today, "retired early"); pv != 2 {
		t.Fatalf("bringing the retirement forward appended processing_version %d, want 2", pv)
	}

	var enabled bool
	if err := pool.QueryRow(ctx, `
		SELECT enabled FROM oracle_asset_current
		WHERE oracle_id = $1 AND token_id = $2`, oracleID, tokenID).Scan(&enabled); err != nil {
		t.Fatalf("read oracle_asset_current: %v", err)
	}
	if enabled {
		t.Error("the source is still enabled today; the early retirement did not take effect")
	}
}

// TestOracleAssetNaturalKeyIsThePrimaryKey pins the key shape the pattern is built on;
// feed_key is what makes it hold for aave-style rows, whose feed_address is NULL.
func TestOracleAssetNaturalKeyIsThePrimaryKey(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-pk", true, "2026-01-01")

	_, err := pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, valid_from, change_reason)
		VALUES ($1, $2, false, '2026-02-01'::date, 'duplicate version 0')`, oracleID, tokenID)
	if err == nil {
		t.Fatal("a duplicate (oracle_id, token_id, feed_key, processing_version) was accepted")
	}

	var keyColumns string
	if err := pool.QueryRow(ctx, `
		SELECT string_agg(a.attname, ',' ORDER BY k.ord)
		FROM pg_constraint c
		JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
		WHERE c.conrelid = 'oracle_asset'::regclass AND c.contype = 'p'`).Scan(&keyColumns); err != nil {
		t.Fatalf("read the primary key: %v", err)
	}
	if want := "oracle_id,token_id,feed_key,processing_version"; keyColumns != want {
		t.Errorf("primary key = (%s), want (%s)", keyColumns, want)
	}
}

// TestOracleAssetSetEnabledRejectsAnUnknownAsset fails loudly rather than appending a
// version for a natural key that was never registered (a typo'd feed address would
// otherwise create a phantom, permanently-disabled mapping).
func TestOracleAssetSetEnabledRejectsAnUnknownAsset(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-unknown", true, "2026-01-01")

	var pv *int
	err := pool.QueryRow(ctx,
		`SELECT oracle_asset_set_enabled($1, $2, '\xdeadbeef'::bytea, false, '2026-08-20'::date, 'unknown feed')`,
		oracleID, tokenID).Scan(&pv)
	if err == nil {
		t.Fatal("setting enabled on an unregistered (oracle, token, feed) key succeeded; want an error")
	}
}
