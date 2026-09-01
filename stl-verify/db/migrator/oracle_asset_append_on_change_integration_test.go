//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
)

// seedOracleAsset registers one aave-style (feedless) version. Each caller passes its own
// oracle name, so two fixtures in one database never share a natural key.
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
		VALUES ($1, $2, $3, $4, 'test fixture')`, oracleID, tokenID, enabled, utcMidnight(t, validFrom)); err != nil {
		t.Fatalf("seed oracle_asset for %s: %v", oracleName, err)
	}
	return oracleID, tokenID
}

// appendVersion adds the next version of a feedless natural key. There is no writer function:
// an appending caller supplies processing_version, valid_from and change_reason itself.
func appendVersion(ctx context.Context, t *testing.T, pool *pgxpool.Pool, oracleID, tokenID int64, enabled bool, processingVersion int, effectiveAt, reason string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, processing_version, valid_from, change_reason)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		oracleID, tokenID, enabled, processingVersion, utcMidnight(t, effectiveAt), reason); err != nil {
		t.Fatalf("append oracle_asset version %d: %v", processingVersion, err)
	}
}

// utcMidnight binds an absolute instant, never a cast that depends on the session TimeZone.
func utcMidnight(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.DateOnly, value)
	if err != nil {
		t.Fatalf("parse date %q: %v", value, err)
	}
	return parsed.UTC()
}

// The pinned read is a SQL fragment the application interpolates, not a database object, so this
// exercises postgres.OracleAssetAsOf itself — the text every Go call site ships.
func TestOracleAssetAsOfReadsTheVersionEffectiveThen(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-asof", true, "2026-01-01")
	appendVersion(ctx, t, pool, oracleID, tokenID, false, 1, "2026-08-20", "VEC-549: retired")

	query := fmt.Sprintf(`
		SELECT oa.enabled
		FROM %s oa
		WHERE oa.oracle_id = $2 AND oa.token_id = $3
	`, postgres.OracleAssetAsOf("$1"))

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
			rows, err := pool.Query(ctx, query, utcMidnight(t, tc.effectiveAt), oracleID, tokenID)
			if err != nil {
				t.Fatalf("pinned read at %s: %v", tc.effectiveAt, err)
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
				t.Fatalf("pinned read at %s returned %d rows, want %d", tc.effectiveAt, len(enabled), tc.wantRows)
			}
			if tc.wantRows > 0 && enabled[0] != tc.wantEnabled {
				t.Errorf("pinned read at %s enabled = %v, want %v", tc.effectiveAt, enabled[0], tc.wantEnabled)
			}
		})
	}
}

func TestOracleAssetNaturalKeyIsThePrimaryKey(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "vec597-pk", true, "2026-01-01")

	_, err := pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, valid_from, change_reason)
		VALUES ($1, $2, false, '2026-02-01T00:00:00Z'::timestamptz, 'duplicate version 0')`, oracleID, tokenID)
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

func TestOracleAssetRejectsAnEmptyFeedAddress(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "empty-feed-address", true, "2026-01-01")

	_, err := pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, processing_version, valid_from, change_reason)
		VALUES ($1, $2, true, '\x'::bytea, 1, $3, 'empty feed address')`,
		oracleID, tokenID, utcMidnight(t, "2026-02-01"))
	if err == nil {
		t.Fatal("an empty feed_address was accepted; it collides with the NULL-feed row's feed_key")
	}
}

func TestOracleAssetRejectsABlankChangeReason(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	oracleID, tokenID := seedOracleAsset(ctx, t, pool, "blank-change-reason", true, "2026-01-01")

	_, err := pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, processing_version, valid_from, change_reason)
		VALUES ($1, $2, false, 1, $3, '   ')`,
		oracleID, tokenID, utcMidnight(t, "2026-08-20"))
	if err == nil {
		t.Fatal("a whitespace-only change_reason was accepted")
	}
}
