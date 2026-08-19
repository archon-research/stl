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
const minProcessingVersionTriggerFunctions = 43

// The next_processing_version_* family — the rule shared by a table's INSERT and its
// trigger — starts at morpho_adapter_state (20260821_120000).
const minProcessingVersionHelperFunctions = 1

type processingVersionTriggerFunction struct {
	name       string
	proconfig  []string
	volatility string
}

// Every trigger function carries the setting, with no exemptions: 20260806_120000_processing_version_force_custom_plan.sql
// grants it to offchain_token_price, 20260806_130000_set_plan_cache_mode_on_processing_version_triggers.sql
// to the 35 that predate it, and every migration since declares it inline (VEC-475 adds 7, one per
// uniswap_v4_* table, registry tables included). Without it a function's
// per-row lookup goes generic and stops pruning chunks, so insert cost scales with the table's total
// chunk count.
func TestProcessingVersionTriggersForceCustomPlan(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	functions := processingVersionFunctions(t, ctx, pool, `assign\_processing\_version\_%`,
		minProcessingVersionTriggerFunctions)
	assertForceCustomPlan(t, functions)
}

// The helper functions the trigger bodies now delegate to run the very same per-row
// lookups, so the setting has to be on them too — and CREATE OR REPLACE resets it.
func TestProcessingVersionHelpersForceCustomPlan(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	functions := processingVersionFunctions(t, ctx, pool, `next\_processing\_version\_%`,
		minProcessingVersionHelperFunctions)
	assertForceCustomPlan(t, functions)
}

// A helper marked STABLE would read the calling statement's snapshot, so a writer
// released from the position's advisory lock would recompute the version the writer it
// waited for already took — and on compressed data the duplicate that follows is past
// anything the unique index can catch.
func TestProcessingVersionHelpersAreVolatile(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	for _, fn := range processingVersionFunctions(t, ctx, pool, `next\_processing\_version\_%`,
		minProcessingVersionHelperFunctions) {
		t.Run(fn.name, func(t *testing.T) {
			if fn.volatility != "v" {
				t.Errorf("%s is provolatile %q, want \"v\" (VOLATILE): its per-statement snapshot is what makes the version it returns current", fn.name, fn.volatility)
			}
		})
	}
}

func assertForceCustomPlan(t *testing.T, functions []processingVersionTriggerFunction) {
	t.Helper()
	for _, fn := range functions {
		t.Run(fn.name, func(t *testing.T) {
			if !slices.Contains(fn.proconfig, planCacheModeSetting) {
				t.Errorf("%s is missing %q; proconfig = [%s]",
					fn.name, planCacheModeSetting, strings.Join(fn.proconfig, ", "))
			}
		})
	}
}

// Enumerated from the catalogue rather than a fixed list so a function added for a future
// table is covered without touching this test. floor is what turns a silently-broken
// enumeration into a failure instead of a vacuous pass.
func processingVersionFunctions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, namePattern string, floor int) []processingVersionTriggerFunction {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT p.proname, COALESCE(p.proconfig, '{}'), p.provolatile
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'
		  AND p.proname LIKE $1
		ORDER BY p.proname
	`, namePattern)
	if err != nil {
		t.Fatalf("query %s functions: %v", namePattern, err)
	}
	defer rows.Close()

	var functions []processingVersionTriggerFunction
	for rows.Next() {
		var fn processingVersionTriggerFunction
		if err := rows.Scan(&fn.name, &fn.proconfig, &fn.volatility); err != nil {
			t.Fatalf("scan pg_proc row: %v", err)
		}
		functions = append(functions, fn)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read pg_proc rows: %v", err)
	}
	if len(functions) < floor {
		t.Fatalf("found %d %s functions, want >= %d: either the pg_proc enumeration stopped matching "+
			"them or one was removed — raise the floor when a new versioned table adds one",
			len(functions), namePattern, floor)
	}
	return functions
}
