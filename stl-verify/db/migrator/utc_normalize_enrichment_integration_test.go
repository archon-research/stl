//go:build integration

package migrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestUtcNormalizeEnrichment verifies migration 20260721_120000: business "today" is UTC, not the
// session TimeZone (Simon #574 review). The enrichment/position tables default valid_from to
// (now() AT TIME ZONE 'utc')::date, and security_instrument_bridge_current bounds on the same, so a
// future-dated re-point is not yet current for any reader session.
func TestUtcNormalizeEnrichment(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// valid_from defaults to the UTC expression on every normalized table (not session CURRENT_DATE).
	for _, tbl := range []string{"security_instrument_bridge", "position_classification", "position_entity_link"} {
		t.Run("utc default "+tbl, func(t *testing.T) {
			var def string
			if err := pool.QueryRow(ctx, `
				SELECT pg_get_expr(d.adbin, d.adrelid)
				FROM pg_attrdef d
				JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
				WHERE d.adrelid = $1::regclass AND a.attname = 'valid_from'`, tbl).Scan(&def); err != nil {
				t.Fatalf("read valid_from default: %v", err)
			}
			if !strings.Contains(def, "utc") {
				t.Errorf("%s.valid_from default = %q, want the UTC expression (now() AT TIME ZONE 'utc')", tbl, def)
			}
		})
	}

	// security_instrument_bridge_current is bounded on UTC today: a future-dated re-point is excluded,
	// a mapping effective today is included.
	if _, err := pool.Exec(ctx, `
		INSERT INTO security_instrument_bridge (instrument_key, security_id, valid_from, change_reason) VALUES
		  ('utc-future', 'sm-future', (now() AT TIME ZONE 'utc')::date + 1, 'future-dated'),
		  ('utc-live',   'sm-live',   (now() AT TIME ZONE 'utc')::date,     'effective now')`); err != nil {
		t.Fatalf("seed bridge rows: %v", err)
	}

	t.Run("future-dated re-point excluded from current", func(t *testing.T) {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*)::int FROM security_instrument_bridge_current WHERE instrument_key = 'utc-future'`).Scan(&n); err != nil {
			t.Fatalf("read current: %v", err)
		}
		if n != 0 {
			t.Errorf("future-dated mapping current count = %d, want 0", n)
		}
	})

	t.Run("effective-today mapping is current", func(t *testing.T) {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*)::int FROM security_instrument_bridge_current WHERE instrument_key = 'utc-live'`).Scan(&n); err != nil {
			t.Fatalf("read current: %v", err)
		}
		if n != 1 {
			t.Errorf("effective-today mapping current count = %d, want 1", n)
		}
	})
}
