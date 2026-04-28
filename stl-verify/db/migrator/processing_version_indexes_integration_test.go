//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/jackc/pgx/v5/pgxpool"
)

type processingVersionIndexCase struct {
	tableName      string
	indexName      string
	indexFragment  string
	prepareSQL     string
	executeArgsSQL string
	maxExecBuffers int
}

func processingVersionIndexCases() []processingVersionIndexCase {
	return []processingVersionIndexCase{
		{
			tableName:     "borrower",
			indexName:     "idx_borrower_pv_lookup",
			indexFragment: "(user_id, protocol_id, token_id, block_number, block_version, created_at, processing_version DESC)",
			prepareSQL: `PREPARE pv_borrower(bigint, bigint, bigint, bigint, int, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM borrower
				WHERE user_id = $1 AND protocol_id = $2 AND token_id = $3
				  AND block_number = $4 AND block_version = $5 AND created_at = $6`,
			executeArgsSQL: "1, 1, 1, 1000000, 0, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "borrower_collateral",
			indexName:     "idx_borrower_collateral_pv_lookup",
			indexFragment: "(user_id, protocol_id, token_id, block_number, block_version, created_at, processing_version DESC)",
			prepareSQL: `PREPARE pv_borrower_collateral(bigint, bigint, bigint, bigint, int, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM borrower_collateral
				WHERE user_id = $1 AND protocol_id = $2 AND token_id = $3
				  AND block_number = $4 AND block_version = $5 AND created_at = $6`,
			executeArgsSQL: "1, 1, 1, 1000000, 0, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "sparklend_reserve_data",
			indexName:     "idx_sparklend_reserve_data_pv_lookup",
			indexFragment: "(protocol_id, token_id, block_number, block_version, processing_version DESC)",
			prepareSQL: `PREPARE pv_sparklend_reserve_data(bigint, bigint, bigint, int) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM sparklend_reserve_data
				WHERE protocol_id = $1 AND token_id = $2 AND block_number = $3 AND block_version = $4`,
			executeArgsSQL: "1, 1, 1000000, 0",
			maxExecBuffers: 200,
		},
		{
			tableName:     "onchain_token_price",
			indexName:     "idx_onchain_token_price_pv_lookup",
			indexFragment: `(token_id, oracle_id, block_number, block_version, "timestamp", processing_version DESC)`,
			prepareSQL: `PREPARE pv_onchain_token_price(bigint, smallint, bigint, smallint, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM onchain_token_price
				WHERE token_id = $1 AND oracle_id = $2 AND block_number = $3
				  AND block_version = $4 AND timestamp = $5`,
			executeArgsSQL: "1, 1, 1000000, 0, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "morpho_market_state",
			indexName:     "idx_morpho_market_state_pv_lookup",
			indexFragment: `(morpho_market_id, block_number, block_version, "timestamp", processing_version DESC)`,
			prepareSQL: `PREPARE pv_morpho_market_state(bigint, bigint, int, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM morpho_market_state
				WHERE morpho_market_id = $1 AND block_number = $2
				  AND block_version = $3 AND timestamp = $4`,
			executeArgsSQL: "1, 1000000, 0, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "morpho_market_position",
			indexName:     "idx_morpho_market_position_pv_lookup",
			indexFragment: `(user_id, morpho_market_id, block_number, block_version, "timestamp", processing_version DESC)`,
			prepareSQL: `PREPARE pv_morpho_market_position(bigint, bigint, bigint, int, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM morpho_market_position
				WHERE user_id = $1 AND morpho_market_id = $2 AND block_number = $3
				  AND block_version = $4 AND timestamp = $5`,
			executeArgsSQL: "1, 1, 1000000, 0, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "morpho_vault_state",
			indexName:     "idx_morpho_vault_state_pv_lookup",
			indexFragment: `(morpho_vault_id, block_number, block_version, "timestamp", processing_version DESC)`,
			prepareSQL: `PREPARE pv_morpho_vault_state(bigint, bigint, int, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM morpho_vault_state
				WHERE morpho_vault_id = $1 AND block_number = $2
				  AND block_version = $3 AND timestamp = $4`,
			executeArgsSQL: "1, 1000000, 0, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "morpho_vault_position",
			indexName:     "idx_morpho_vault_position_pv_lookup",
			indexFragment: `(user_id, morpho_vault_id, block_number, block_version, "timestamp", processing_version DESC)`,
			prepareSQL: `PREPARE pv_morpho_vault_position(bigint, bigint, bigint, int, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM morpho_vault_position
				WHERE user_id = $1 AND morpho_vault_id = $2 AND block_number = $3
				  AND block_version = $4 AND timestamp = $5`,
			executeArgsSQL: "1, 1, 1000000, 0, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "prime_debt",
			indexName:     "idx_prime_debt_pv_lookup",
			indexFragment: "(prime_id, block_number, block_version, synced_at, processing_version DESC)",
			prepareSQL: `PREPARE pv_prime_debt(bigint, bigint, int, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM prime_debt
				WHERE prime_id = $1 AND block_number = $2 AND block_version = $3 AND synced_at = $4`,
			executeArgsSQL: "1, 1000000, 0, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "allocation_position",
			indexName:     "idx_allocation_position_pv_lookup",
			indexFragment: "(chain_id, token_id, prime_id, proxy_address, block_number, block_version, tx_hash, log_index, direction, created_at, processing_version DESC)",
			prepareSQL: `PREPARE pv_allocation_position(int, bigint, bigint, bytea, bigint, int, bytea, int, text, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM allocation_position
				WHERE chain_id = $1 AND token_id = $2 AND prime_id = $3 AND proxy_address = $4
				  AND block_number = $5 AND block_version = $6 AND tx_hash = $7
				  AND log_index = $8 AND direction = $9 AND created_at = $10`,
			executeArgsSQL: "1, 1, 1, '\\x01'::bytea, 1000000, 0, '\\x01'::bytea, 1, 'in', timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "protocol_event",
			indexName:     "idx_protocol_event_pv_lookup",
			indexFragment: "(chain_id, block_number, block_version, tx_hash, log_index, created_at, processing_version DESC)",
			prepareSQL: `PREPARE pv_protocol_event(int, bigint, int, bytea, int, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM protocol_event
				WHERE chain_id = $1 AND block_number = $2 AND block_version = $3
				  AND tx_hash = $4 AND log_index = $5 AND created_at = $6`,
			executeArgsSQL: "1, 1000000, 0, '\\x01'::bytea, 1, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "anchorage_package_snapshot",
			indexName:     "idx_anchorage_package_snapshot_pv_lookup",
			indexFragment: "(prime_id, package_id, asset_type, custody_type, snapshot_time, processing_version DESC)",
			prepareSQL: `PREPARE pv_anchorage_package_snapshot(bigint, text, text, text, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM anchorage_package_snapshot
				WHERE prime_id = $1 AND package_id = $2 AND asset_type = $3
				  AND custody_type = $4 AND snapshot_time = $5`,
			executeArgsSQL: "1, 'pkg', 'BTC', 'AnchorageCustody', timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "anchorage_operation",
			indexName:     "idx_anchorage_operation_pv_lookup",
			indexFragment: "(operation_id, created_at, processing_version DESC)",
			prepareSQL: `PREPARE pv_anchorage_operation(text, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM anchorage_operation
				WHERE operation_id = $1 AND created_at = $2`,
			executeArgsSQL: "'operation', timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
		{
			tableName:     "offchain_token_price",
			indexName:     "idx_offchain_token_price_pv_lookup",
			indexFragment: `(token_id, source_id, "timestamp", processing_version DESC)`,
			prepareSQL: `PREPARE pv_offchain_token_price(bigint, smallint, timestamptz) AS
				SELECT COALESCE(MAX(processing_version), -1)
				FROM offchain_token_price
				WHERE token_id = $1 AND source_id = $2 AND timestamp = $3`,
			executeArgsSQL: "1, 1, timestamptz '2035-01-03 00:00:00.5+00'",
			maxExecBuffers: 200,
		},
	}
}

func TestProcessingVersionCoveringIndexesExist(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(pool, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	for _, tc := range processingVersionIndexCases() {
		t.Run(tc.tableName, func(t *testing.T) {
			var indexDef string
			err := pool.QueryRow(ctx, `
				SELECT indexdef
				FROM pg_indexes
				WHERE schemaname = 'public'
				  AND tablename = $1
				  AND indexname = $2
			`, tc.tableName, tc.indexName).Scan(&indexDef)
			if err != nil {
				t.Fatalf("index %s missing on %s: %v", tc.indexName, tc.tableName, err)
			}
			if !strings.Contains(indexDef, tc.indexFragment) {
				t.Fatalf("index %s has unexpected definition\nwant fragment: %s\ngot: %s",
					tc.indexName, tc.indexFragment, indexDef)
			}
		})
	}
}

func TestProcessingVersionTriggerQueryPlansAreEfficient(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()

	m := migrator.New(pool, getMigrationsPath())
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}
	seedProcessingVersionPlanRows(t, ctx, pool, 2_000)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SET plan_cache_mode = force_generic_plan"); err != nil {
		t.Fatalf("force generic plan: %v", err)
	}
	defer conn.Exec(ctx, "RESET plan_cache_mode")

	for _, tc := range processingVersionIndexCases() {
		t.Run(tc.tableName, func(t *testing.T) {
			planText := explainProcessingVersionPlan(t, ctx, conn, tc)
			if strings.Contains(planText, "Seq Scan") {
				t.Fatalf("processing_version trigger query fell back to a sequential scan on %s\nplan:\n%s",
					tc.tableName, planText)
			}
			buffers := parseTotalBuffersFromExplain(planText)
			if buffers > tc.maxExecBuffers {
				t.Fatalf("processing_version trigger query for %s touched %d buffers; expected <= %d\nplan:\n%s",
					tc.tableName, buffers, tc.maxExecBuffers, planText)
			}
		})
	}
}

func explainProcessingVersionPlan(t *testing.T, ctx context.Context, conn *pgxpool.Conn, tc processingVersionIndexCase) string {
	t.Helper()

	if _, err := conn.Exec(ctx, "DEALLOCATE ALL"); err != nil {
		t.Fatalf("deallocate prepared statements: %v", err)
	}
	if _, err := conn.Exec(ctx, tc.prepareSQL); err != nil {
		t.Fatalf("prepare %s: %v", tc.tableName, err)
	}

	explainSQL := fmt.Sprintf("EXPLAIN (ANALYZE, BUFFERS) EXECUTE %s(%s)",
		preparedStatementName(tc.prepareSQL), tc.executeArgsSQL)
	rows, err := conn.Query(ctx, explainSQL)
	if err != nil {
		t.Fatalf("explain %s: %v", tc.tableName, err)
	}
	defer rows.Close()

	var planLines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan EXPLAIN row: %v", err)
		}
		planLines = append(planLines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read EXPLAIN rows: %v", err)
	}
	return strings.Join(planLines, "\n")
}

func preparedStatementName(prepareSQL string) string {
	fields := strings.Fields(prepareSQL)
	if len(fields) < 2 || fields[0] != "PREPARE" {
		return ""
	}
	if idx := strings.Index(fields[1], "("); idx >= 0 {
		return fields[1][:idx]
	}
	return fields[1]
}

func seedProcessingVersionPlanRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rows int) {
	t.Helper()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire seed conn: %v", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed tx: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			if err := tx.Rollback(ctx); err != nil {
				t.Logf("rollback seed tx: %v", err)
			}
		}
	}()

	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = 'replica'"); err != nil {
		t.Fatalf("disable triggers and FK checks: %v", err)
	}

	for _, stmt := range processingVersionSeedStatements() {
		if _, err := tx.Exec(ctx, stmt, rows); err != nil {
			t.Fatalf("seed processing_version plan rows: %v\nstatement:\n%s", err, stmt)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}
	committed = true

	for _, tc := range processingVersionIndexCases() {
		if _, err := pool.Exec(ctx, fmt.Sprintf("ANALYZE %s", tc.tableName)); err != nil {
			t.Fatalf("analyze %s: %v", tc.tableName, err)
		}
	}
}

func processingVersionSeedStatements() []string {
	const tsExpr = `timestamptz '2035-01-01 00:00:00+00' + ((g % 5) * interval '1 day') + (g * interval '1 second')`
	const hashExpr = `decode(lpad(to_hex(g), 64, '0'), 'hex')`

	return []string{
		fmt.Sprintf(`
			INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, created_at, processing_version, build_id)
			SELECT 1, 1, 1, 1000000, 0, 1, 1, 'borrow', %s, %s, 0, 0
			FROM generate_series(1, $1) AS g`, hashExpr, tsExpr),
		fmt.Sprintf(`
			INSERT INTO borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash, collateral_enabled, created_at, processing_version, build_id)
			SELECT 1, 1, 1, 1000000, 0, 1, 1, 'collateral', %s, true, %s, 0, 0
			FROM generate_series(1, $1) AS g`, hashExpr, tsExpr),
		`
			INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version, total_a_token, processing_version, build_id)
			SELECT 1, 1, 1000000, 0, 1, g, 0
			FROM generate_series(1, $1) AS g`,
		fmt.Sprintf(`
			INSERT INTO onchain_token_price (token_id, oracle_id, block_number, block_version, timestamp, price_usd, processing_version, build_id)
			SELECT 1, 1, 1000000, 0, %s, 1, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO morpho_market_state (morpho_market_id, block_number, block_version, timestamp, total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares, last_update, fee, processing_version, build_id)
			SELECT 1, 1000000, 0, %s, 1, 1, 1, 1, 1, 0, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, processing_version, build_id)
			SELECT 1, 1, 1000000, 0, %s, 1, 1, 1, 1, 1, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO morpho_vault_state (morpho_vault_id, block_number, block_version, timestamp, total_assets, total_shares, processing_version, build_id)
			SELECT 1, 1000000, 0, %s, 1, 1, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, shares, assets, processing_version, build_id)
			SELECT 1, 1, 1000000, 0, %s, 1, 1, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id)
			SELECT 1, 'ilk', 1, 1000000, 0, %s, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO allocation_position (chain_id, token_id, prime_id, proxy_address, balance, scaled_balance, block_number, block_version, tx_hash, log_index, tx_amount, direction, created_at, processing_version, build_id)
			SELECT 1, 1, 1, '\x01'::bytea, 1, 1, 1000000, 0, '\x01'::bytea, 1, 1, 'in', %s, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO protocol_event (chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data, created_at, processing_version, build_id)
			SELECT 1, 1, 1000000, 0, '\x01'::bytea, 1, '\x01'::bytea, 'event', '{}'::jsonb, %s, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO anchorage_package_snapshot (prime_id, package_id, pledgor_id, secured_party_id, active, state, current_ltv, exposure_value, package_value, margin_call_ltv, critical_ltv, margin_return_ltv, asset_type, custody_type, asset_price, asset_quantity, asset_weighted_value, ltv_timestamp, snapshot_time, processing_version, build_id)
			SELECT 1, 'pkg', 'pledgor', 'secured', true, 'active', 1, 1, 1, 1, 1, 1, 'BTC', 'AnchorageCustody', 1, 1, 1, %s, %s, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr, tsExpr),
		fmt.Sprintf(`
			INSERT INTO anchorage_operation (prime_id, operation_id, action, operation_type, type_id, asset_type, custody_type, quantity, created_at, processing_version, build_id)
			SELECT 1, 'operation', 'deposit', 'package', 'type', 'BTC', 'AnchorageCustody', 1, %s, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
		fmt.Sprintf(`
			INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd, processing_version, build_id)
			SELECT 1, 1, %s, 1, 0, 0
			FROM generate_series(1, $1) AS g`, tsExpr),
	}
}

func parseTotalBuffersFromExplain(plan string) int {
	if idx := strings.Index(plan, "\nPlanning:"); idx >= 0 {
		plan = plan[:idx]
	}
	re := regexp.MustCompile(`(?:hit|read)=(\d+)`)
	total := 0
	for _, m := range re.FindAllStringSubmatch(plan, -1) {
		n, _ := strconv.Atoi(m[1])
		total += n
	}
	return total
}
