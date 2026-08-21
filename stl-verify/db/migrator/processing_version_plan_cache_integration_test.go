//go:build integration

package migrator_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	planCacheModeValue   = "force_custom_plan"
	planCacheModeSetting = "plan_cache_mode=" + planCacheModeValue
)

// Every versioned table contributes one trigger function. The floor turns a silently-broken
// enumeration (a renamed prefix, a wrong schema filter) into a failure instead of a vacuous pass.
const minProcessingVersionTriggerFunctions = 36

type processingVersionTriggerFunction struct {
	name      string
	proconfig []string
}

// Every trigger function carries the setting, with no exemptions: 20260806_120000_processing_version_force_custom_plan.sql
// grants it to offchain_token_price and 20260806_130000_set_plan_cache_mode_on_processing_version_triggers.sql
// to the other 35. Without it a function's per-row lookup goes generic and stops pruning chunks, so
// insert cost scales with the table's total chunk count.
func TestProcessingVersionTriggersForceCustomPlan(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	for _, fn := range processingVersionTriggerFunctions(t, ctx, pool) {
		t.Run(fn.name, func(t *testing.T) {
			if !slices.Contains(fn.proconfig, planCacheModeSetting) {
				t.Errorf("%s is missing %q; proconfig = [%s]",
					fn.name, planCacheModeSetting, strings.Join(fn.proconfig, ", "))
			}
		})
	}
}

// Enumerated from the catalogue rather than a fixed list so a trigger function added for a future
// table is covered without touching this test.
func processingVersionTriggerFunctions(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []processingVersionTriggerFunction {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT p.proname, COALESCE(p.proconfig, '{}')
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'
		  AND p.proname LIKE 'assign\_processing\_version\_%'
		ORDER BY p.proname
	`)
	if err != nil {
		t.Fatalf("query processing_version trigger functions: %v", err)
	}
	defer rows.Close()

	var functions []processingVersionTriggerFunction
	for rows.Next() {
		var fn processingVersionTriggerFunction
		if err := rows.Scan(&fn.name, &fn.proconfig); err != nil {
			t.Fatalf("scan pg_proc row: %v", err)
		}
		functions = append(functions, fn)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read pg_proc rows: %v", err)
	}
	if len(functions) < minProcessingVersionTriggerFunctions {
		t.Fatalf("found %d assign_processing_version_* functions, want >= %d: either the pg_proc enumeration "+
			"stopped matching them or one was removed — raise the floor when a new versioned table adds one",
			len(functions), minProcessingVersionTriggerFunctions)
	}
	return functions
}
