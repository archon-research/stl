//go:build integration

package migrator_test

import (
	"context"
	"testing"
)

// Every shipped projection view (position_*) must declare deal_type: the materializer's contract
// requires it once VEC-401 lands, and until then a view without it would store NULL for every row.
func TestProjectionViewsCarryDealType(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	rows, err := pool.Query(ctx, `
		SELECT c.relname,
		       EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'deal_type'
		                 AND a.attnum > 0 AND NOT a.attisdropped AND a.atttypid = 'text'::regtype)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'v' AND c.relname LIKE 'position\_%'
		ORDER BY 1`)
	if err != nil {
		t.Fatalf("list projection views: %v", err)
	}
	defer rows.Close()
	var seen int
	for rows.Next() {
		var view string
		var declaresText bool
		if err := rows.Scan(&view, &declaresText); err != nil {
			t.Fatal(err)
		}
		seen++
		if !declaresText {
			t.Errorf("%s does not declare deal_type as text", view)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen == 0 {
		t.Fatal("no position_* projection views found; this test must run where a projection exists")
	}
}
