//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// TestMigrations_AuditabilityOnSelfHosted verifies that the full migration set
// (including auditability migrations) applies cleanly on self-hosted TimescaleDB
// (not TigerData Cloud). This catches Cloud-only API usage like
// timescaledb.enable_columnstore that doesn't exist on self-hosted.
func TestMigrations_AuditabilityOnSelfHosted(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(pool, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed on self-hosted TimescaleDB: %v", err)
	}

	// Verify build_registry exists and has the pre-tracking seed row.
	var gitHash string
	err := pool.QueryRow(ctx, `SELECT git_hash FROM build_registry WHERE id = 0`).Scan(&gitHash)
	if err != nil {
		t.Fatalf("build_registry seed row missing: %v", err)
	}
	if gitHash != "pre-tracking" {
		t.Fatalf("expected git_hash 'pre-tracking', got %q", gitHash)
	}

	// Verify processing_version + build_id columns exist on a sample of state tables.
	tables := []string{
		"borrower", "morpho_market_state", "onchain_token_price",
		"anchorage_package_snapshot", "offchain_token_price",
	}
	for _, table := range tables {
		var hasPV, hasBID bool
		err := pool.QueryRow(ctx, `
			SELECT
				EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = 'processing_version'),
				EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = 'build_id')
		`, table).Scan(&hasPV, &hasBID)
		if err != nil {
			t.Fatalf("column check for %s: %v", table, err)
		}
		if !hasPV {
			t.Errorf("%s missing processing_version column", table)
		}
		if !hasBID {
			t.Errorf("%s missing build_id column", table)
		}
	}

	// Verify triggers exist on state tables.
	triggerTables := []string{
		"borrower", "morpho_market_state", "onchain_token_price",
		"anchorage_package_snapshot", "offchain_token_price",
	}
	for _, table := range triggerTables {
		var exists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.triggers
				WHERE event_object_table = $1
				AND trigger_name = 'trigger_assign_processing_version'
			)
		`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("trigger check for %s: %v", table, err)
		}
		if !exists {
			t.Errorf("%s missing trigger_assign_processing_version", table)
		}
	}

	// Verify the trigger works: insert a row and check processing_version was assigned.
	_, err = pool.Exec(ctx, `
		INSERT INTO build_registry (git_hash) VALUES ('test-hash-abc123')
	`)
	if err != nil {
		t.Fatalf("inserting test build: %v", err)
	}

	var buildID int
	err = pool.QueryRow(ctx, `SELECT id FROM build_registry WHERE git_hash = 'test-hash-abc123'`).Scan(&buildID)
	if err != nil {
		t.Fatalf("looking up test build: %v", err)
	}

	// Insert into offchain_token_price — no FK constraints on this table
	// (incompatible with distributed hypertables), so we can use a fake token_id.
	_, err = pool.Exec(ctx, `
		INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd, build_id)
		VALUES (999, 1, NOW(), 100.0, $1)
		ON CONFLICT (token_id, source_id, processing_version, timestamp) DO NOTHING
	`, buildID)
	if err != nil {
		t.Fatalf("inserting price: %v", err)
	}

	var pv int
	err = pool.QueryRow(ctx, `
		SELECT processing_version FROM offchain_token_price WHERE token_id = 999
	`).Scan(&pv)
	if err != nil {
		t.Fatalf("reading processing_version: %v", err)
	}
	if pv != 0 {
		t.Fatalf("expected processing_version 0, got %d", pv)
	}
	t.Logf("trigger assigned processing_version = %d", pv)
}
