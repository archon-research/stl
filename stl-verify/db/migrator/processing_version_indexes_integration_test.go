//go:build integration

package migrator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// prime_debt carries the many-chunk fixture: chunk fan-out only shows up once a table has far more
// chunks than a handful, and spreading every table that wide would cost the suite more than the one
// signal is worth. Its trigger has the same shape as the other 35, so what holds here holds for them.
const (
	manyChunkTable = "prime_debt"
	manyChunkDays  = 400
	// A fixture that quietly stopped creating chunks would neuter every fan-out assertion below.
	minManyChunkCount = 100
)

// Floor for every other fixture table: enough chunks that pruning to one is a real narrowing.
const minFixtureChunkCount = 5

// Per-lookup budget, planning included: force_custom_plan re-plans on every execution, so planning
// is part of the per-row trigger cost and has to stay flat in chunk count too. Measured 25-59 buffers
// across all 14 tables with the many-chunk table at 401 chunks, and 37-95 once the probe key's chunk
// is in the columnstore, where planning also has to reach the compress_hyper_* twin; the same lookup
// costs 406 buffers once a generic plan stops pruning.
const maxTriggerLookupBuffers = 120

// Per-row insert budget for the real trigger on the many-chunk table, in the steady state a
// long-lived worker connection runs in. Measured 98 buffers/row at 401 chunks under the trigger's
// configured plan_cache_mode, against 423 once the same trigger plans generically.
const (
	triggerInsertRows      = 20
	maxInsertBuffersPerRow = 200
)

const processingVersionProbeTimestamp = "timestamptz '2035-01-03 00:00:00.5+00'"

// No seeded row carries this build_id, so the retry lookup has to exhaust the key's index entries
// exactly as it does on a first write — the case that has to stay cheap.
const probeBuildID = 7

// A natural-key column of a processing_version lookup, with the PREPARE parameter type and the
// literal the test probes it with. One declaration feeds both statements the trigger runs per row.
type processingVersionKeyColumn struct {
	name    string
	pgType  string
	literal string
}

type processingVersionIndexCase struct {
	tableName     string
	indexName     string
	indexFragment string
	keyColumns    []processingVersionKeyColumn
}

// The probe key deliberately lands in an existing chunk without matching a row the retry lookup can
// return: that is the production shape (a key this build has not written yet), and it stops LIMIT 1
// from short-circuiting on the first chunk, which would hide fan-out.
func processingVersionIndexCases() []processingVersionIndexCase {
	ts := processingVersionProbeTimestamp
	return []processingVersionIndexCase{
		{
			tableName:     "borrower",
			indexName:     "idx_borrower_pv_lookup",
			indexFragment: "(user_id, protocol_id, token_id, block_number, block_version, created_at, processing_version DESC)",
			keyColumns: []processingVersionKeyColumn{
				{"user_id", "bigint", "1"},
				{"protocol_id", "bigint", "1"},
				{"token_id", "bigint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{"created_at", "timestamptz", ts},
			},
		},
		{
			tableName:     "borrower_collateral",
			indexName:     "idx_borrower_collateral_pv_lookup",
			indexFragment: "(user_id, protocol_id, token_id, block_number, block_version, created_at, processing_version DESC)",
			keyColumns: []processingVersionKeyColumn{
				{"user_id", "bigint", "1"},
				{"protocol_id", "bigint", "1"},
				{"token_id", "bigint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{"created_at", "timestamptz", ts},
			},
		},
		{
			tableName:     "sparklend_reserve_data",
			indexName:     "idx_sparklend_reserve_data_pv_lookup",
			indexFragment: "(protocol_id, token_id, block_number, block_version, processing_version DESC)",
			keyColumns: []processingVersionKeyColumn{
				{"protocol_id", "bigint", "1"},
				{"token_id", "bigint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
			},
		},
		{
			tableName:     "onchain_token_price",
			indexName:     "idx_onchain_token_price_pv_lookup",
			indexFragment: `(token_id, oracle_id, block_number, block_version, "timestamp", processing_version DESC)`,
			keyColumns: []processingVersionKeyColumn{
				{"token_id", "bigint", "1"},
				{"oracle_id", "smallint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "smallint", "0"},
				{`"timestamp"`, "timestamptz", ts},
			},
		},
		{
			tableName:     "morpho_market_state",
			indexName:     "idx_morpho_market_state_pv_lookup",
			indexFragment: `(morpho_market_id, block_number, block_version, "timestamp", processing_version DESC)`,
			keyColumns: []processingVersionKeyColumn{
				{"morpho_market_id", "bigint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{`"timestamp"`, "timestamptz", ts},
			},
		},
		{
			tableName:     "morpho_market_position",
			indexName:     "idx_morpho_market_position_pv_lookup",
			indexFragment: `(user_id, morpho_market_id, block_number, block_version, "timestamp", processing_version DESC)`,
			keyColumns: []processingVersionKeyColumn{
				{"user_id", "bigint", "1"},
				{"morpho_market_id", "bigint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{`"timestamp"`, "timestamptz", ts},
			},
		},
		{
			tableName:     "morpho_vault_state",
			indexName:     "idx_morpho_vault_state_pv_lookup",
			indexFragment: `(morpho_vault_id, block_number, block_version, "timestamp", processing_version DESC)`,
			keyColumns: []processingVersionKeyColumn{
				{"morpho_vault_id", "bigint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{`"timestamp"`, "timestamptz", ts},
			},
		},
		{
			tableName:     "morpho_vault_position",
			indexName:     "idx_morpho_vault_position_pv_lookup",
			indexFragment: `(user_id, morpho_vault_id, block_number, block_version, "timestamp", processing_version DESC)`,
			keyColumns: []processingVersionKeyColumn{
				{"user_id", "bigint", "1"},
				{"morpho_vault_id", "bigint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{`"timestamp"`, "timestamptz", ts},
			},
		},
		{
			tableName:     manyChunkTable,
			indexName:     "idx_prime_debt_pv_lookup",
			indexFragment: "(prime_id, block_number, block_version, synced_at, processing_version DESC)",
			keyColumns: []processingVersionKeyColumn{
				{"prime_id", "bigint", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{"synced_at", "timestamptz", ts},
			},
		},
		{
			tableName:     "allocation_position",
			indexName:     "idx_allocation_position_pv_lookup",
			indexFragment: "(chain_id, token_id, prime_id, proxy_address, block_number, block_version, tx_hash, log_index, direction, created_at, processing_version DESC)",
			keyColumns: []processingVersionKeyColumn{
				{"chain_id", "int", "1"},
				{"token_id", "bigint", "1"},
				{"prime_id", "bigint", "1"},
				{"proxy_address", "bytea", `'\x01'::bytea`},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{"tx_hash", "bytea", `'\x01'::bytea`},
				{"log_index", "int", "1"},
				{"direction", "text", "'in'"},
				{"created_at", "timestamptz", ts},
			},
		},
		{
			tableName:     "protocol_event",
			indexName:     "idx_protocol_event_pv_lookup",
			indexFragment: "(chain_id, block_number, block_version, tx_hash, log_index, created_at, processing_version DESC)",
			keyColumns: []processingVersionKeyColumn{
				{"chain_id", "int", "1"},
				{"block_number", "bigint", "1000000"},
				{"block_version", "int", "0"},
				{"tx_hash", "bytea", `'\x01'::bytea`},
				{"log_index", "int", "1"},
				{"created_at", "timestamptz", ts},
			},
		},
		{
			tableName:     "anchorage_package_snapshot",
			indexName:     "idx_anchorage_package_snapshot_pv_lookup",
			indexFragment: "(prime_id, package_id, asset_type, custody_type, snapshot_time, processing_version DESC)",
			keyColumns: []processingVersionKeyColumn{
				{"prime_id", "bigint", "1"},
				{"package_id", "text", "'pkg'"},
				{"asset_type", "text", "'BTC'"},
				{"custody_type", "text", "'AnchorageCustody'"},
				{"snapshot_time", "timestamptz", ts},
			},
		},
		{
			tableName:     "anchorage_operation",
			indexName:     "idx_anchorage_operation_pv_lookup",
			indexFragment: "(operation_id, created_at, processing_version DESC)",
			keyColumns: []processingVersionKeyColumn{
				{"operation_id", "text", "'operation'"},
				{"created_at", "timestamptz", ts},
			},
		},
		{
			tableName:     "offchain_token_price",
			indexName:     "idx_offchain_token_price_pv_lookup",
			indexFragment: `(token_id, source_id, "timestamp", processing_version DESC)`,
			keyColumns: []processingVersionKeyColumn{
				{"token_id", "bigint", "1"},
				{"source_id", "smallint", "1"},
				{`"timestamp"`, "timestamptz", ts},
			},
		},
	}
}

// One of the two statements an assign_processing_version_* trigger runs per row.
type processingVersionLookup struct {
	label      string
	prepareSQL string
	executeSQL string
}

// Both statements are generated from the same key declaration so they cannot drift apart, and so a
// table added to the fixture is covered by every assertion below at once.
func (c processingVersionIndexCase) triggerLookups() []processingVersionLookup {
	return []processingVersionLookup{c.buildRetryLookup(), c.latestVersionLookup()}
}

// Q1: has this build already written this key? First statement in every trigger function, and the
// one that carries build_id — a column no processing_version index covers.
func (c processingVersionIndexCase) buildRetryLookup() processingVersionLookup {
	name := "pv_retry_" + c.tableName
	buildIDParam := len(c.keyColumns) + 1
	return processingVersionLookup{
		label: "build_id_retry",
		prepareSQL: fmt.Sprintf(
			`PREPARE %s(%s, int) AS SELECT processing_version FROM %s WHERE %s AND build_id = $%d LIMIT 1`,
			name, strings.Join(c.keyTypes(), ", "), c.tableName, c.keyPredicates(), buildIDParam),
		executeSQL: fmt.Sprintf("%s(%s, %d)", name, strings.Join(c.keyLiterals(), ", "), probeBuildID),
	}
}

// Q2: what is the highest version already stored at this key? Runs whenever Q1 misses.
func (c processingVersionIndexCase) latestVersionLookup() processingVersionLookup {
	name := "pv_max_" + c.tableName
	return processingVersionLookup{
		label: "max_version",
		prepareSQL: fmt.Sprintf(
			`PREPARE %s(%s) AS SELECT COALESCE(MAX(processing_version), -1) FROM %s WHERE %s`,
			name, strings.Join(c.keyTypes(), ", "), c.tableName, c.keyPredicates()),
		executeSQL: fmt.Sprintf("%s(%s)", name, strings.Join(c.keyLiterals(), ", ")),
	}
}

func (c processingVersionIndexCase) keyPredicates() string {
	preds := make([]string, len(c.keyColumns))
	for i, col := range c.keyColumns {
		preds[i] = fmt.Sprintf("%s = $%d", col.name, i+1)
	}
	return strings.Join(preds, " AND ")
}

func (c processingVersionIndexCase) keyTypes() []string {
	types := make([]string, len(c.keyColumns))
	for i, col := range c.keyColumns {
		types[i] = col.pgType
	}
	return types
}

func (c processingVersionIndexCase) keyLiterals() []string {
	literals := make([]string, len(c.keyColumns))
	for i, col := range c.keyColumns {
		literals[i] = col.literal
	}
	return literals
}

func TestProcessingVersionCoveringIndexesExist(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

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

// Both per-row trigger statements must prune to the single chunk that can hold the key, under the
// plan_cache_mode the trigger functions actually run with (VEC-541). The session SET mirrors the
// functions' proconfig: a PREPARE/EXECUTE pair goes through the same plancache that decides plpgsql's
// SPI plans, and the GUC forces the mode outright, so no execution count is needed to reach the
// decision production makes.
func TestProcessingVersionTriggerLookupsPruneToOneChunk(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	seedProcessingVersionPlanRows(t, ctx, pool, 2_000)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SET plan_cache_mode = "+planCacheModeValue); err != nil {
		t.Fatalf("set plan_cache_mode: %v", err)
	}
	defer resetSessionState(t, ctx, conn, "RESET plan_cache_mode")

	for _, tc := range processingVersionIndexCases() {
		for _, lookup := range tc.triggerLookups() {
			t.Run(tc.tableName+"/"+lookup.label, func(t *testing.T) {
				plan := explainWarmedLookup(t, ctx, conn, lookup)
				assertLookupPrunedToOneChunk(t, plan, tc.tableName+" "+lookup.label)
				if twins := plan.compressedChunkNames(); len(twins) != 0 {
					t.Fatalf("%s %s lookup reached the columnstore twins %v on a fixture that compresses "+
						"nothing, so the policy jobs setupMigratedPostgres switches off ran anyway and the "+
						"row-store chunk count above is no longer measuring what it names\nplan:\n%s",
						tc.tableName, lookup.label, twins, plan.raw)
				}
			})
		}
	}
}

// The negative control for the assertion above: with a generic plan the max-version lookup degrades
// into a Merge Append across every chunk, which is the cost force_custom_plan exists to avoid. The
// session GUC forces that mode outright rather than warming the plancache into it. Only the
// max-version lookup is checked here: the build-id lookup degrades in a different shape and has its
// own control below.
func TestProcessingVersionLatestVersionLookupFansOutUnderGenericPlan(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	seedProcessingVersionPlanRows(t, ctx, pool, 2_000)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SET plan_cache_mode = force_generic_plan"); err != nil {
		t.Fatalf("set plan_cache_mode: %v", err)
	}
	defer resetSessionState(t, ctx, conn, "RESET plan_cache_mode")

	plan := explainWarmedLookup(t, ctx, conn, manyChunkCase(t).latestVersionLookup())
	chunkNodes := len(plan.chunkNames())
	total := chunkCount(t, ctx, pool, manyChunkTable)
	if chunkNodes < total/2 {
		t.Fatalf("generic plan on %s scanned %d of %d chunks; expected it to fan out over most of them, "+
			"which is the premise of the force_custom_plan setting. If TimescaleDB now prunes generic plans "+
			"here, this control is obsolete: re-measure the per-row insert budget and delete this test, do not "+
			"loosen the threshold\nplan:\n%s",
			manyChunkTable, chunkNodes, total, plan.raw)
	}
	if buffers := plan.totalBuffers(); buffers <= maxTriggerLookupBuffers {
		t.Fatalf("generic plan on %s touched only %d buffers (<= the %d pruned budget), so the pruned "+
			"assertion has nothing to catch\nplan:\n%s",
			manyChunkTable, buffers, maxTriggerLookupBuffers, plan.raw)
	}
}

// The control for the deferred-exclusion assertion, and the reason that assertion has to exist. The
// build-id lookup does not fan out visibly under a generic plan: it keeps a ChunkAppend, reports the
// one surviving chunk, and costs almost nothing in buffers — so the chunk-count and buffer checks in
// the pruned test both pass on it while every execution still re-derives exclusion over the whole
// table. Only the startup-exclusion counter tells the two apart, which is what this pins.
func TestProcessingVersionBuildIDLookupDefersChunkExclusionUnderGenericPlan(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	seedProcessingVersionPlanRows(t, ctx, pool, 2_000)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SET plan_cache_mode = force_generic_plan"); err != nil {
		t.Fatalf("set plan_cache_mode: %v", err)
	}
	defer resetSessionState(t, ctx, conn, "RESET plan_cache_mode")

	plan := explainWarmedLookup(t, ctx, conn, manyChunkCase(t).buildRetryLookup())
	total := chunkCount(t, ctx, pool, manyChunkTable)
	if excluded := plan.deferredChunkExclusions(); excluded < total/2 {
		t.Fatalf("generic plan on the %s build-id lookup deferred exclusion of only %d of %d chunks to "+
			"startup; the pruned test's deferred-exclusion assertion then has nothing to catch. If "+
			"TimescaleDB now prunes generic plans at plan time, force_custom_plan is no longer needed: "+
			"re-measure and retire the setting rather than loosening this\nplan:\n%s",
			manyChunkTable, excluded, total, plan.raw)
	}
	if chunks := plan.chunkNames(); len(chunks) != 1 {
		t.Fatalf("generic plan on the %s build-id lookup named %d chunks; this control exists because it "+
			"names exactly 1, indistinguishable from a pruned plan\nplan:\n%s",
			manyChunkTable, len(chunks), plan.raw)
	}
}

// End-to-end guard on the real path: rows inserted through the trigger on a many-chunk table, with
// the trigger function running under its own configured plan_cache_mode. Buffers reported for an
// INSERT include what its BEFORE trigger reads, so this catches any regression that reintroduces the
// fan-out — the setting removed, the covering index dropped, or a lookup reshaped past pruning.
func TestProcessingVersionTriggerInsertStaysUnderPerRowBudget(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	seedProcessingVersionPlanRows(t, ctx, pool, 2_000)
	primeID := upsertFixturePrime(t, ctx, pool)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()

	// The session asks for generic plans, so the trigger function's own proconfig is the only thing
	// that can keep the lookup pruning — which is exactly the regression this test has to catch.
	// Without this, a fresh session's plancache serves custom plans for the handful of executions
	// below and the assertion passes even with the migration reverted.
	if _, err := conn.Exec(ctx, "SET plan_cache_mode = force_generic_plan"); err != nil {
		t.Fatalf("set plan_cache_mode: %v", err)
	}
	defer resetSessionState(t, ctx, conn, "RESET plan_cache_mode")

	// Both batches run on one connection and only the second is measured: the plancache lives there,
	// so the measurement is the steady state of a worker that inserts for hours, not a first execution.
	insertThroughTrigger(t, ctx, conn, primeID, warmupInsertHour)
	plan := insertThroughTrigger(t, ctx, conn, primeID, measuredInsertHour)

	perRow := plan.totalBuffers() / triggerInsertRows
	if perRow > maxInsertBuffersPerRow {
		t.Fatalf("inserting through the %s trigger cost %d buffers/row across %d chunks; expected <= %d\nplan:\n%s",
			manyChunkTable, perRow, chunkCount(t, ctx, pool, manyChunkTable), maxInsertBuffersPerRow, plan.raw)
	}
}

func insertThroughTrigger(t *testing.T, ctx context.Context, conn *pgxpool.Conn, primeID int64, hour int) explainPlan {
	t.Helper()
	return explainJSON(t, ctx, conn,
		fmt.Sprintf(manyChunkTriggerInsertStatement, primeID, hour, triggerInsertRows, probeBuildID))
}

func manyChunkCase(t *testing.T) processingVersionIndexCase {
	t.Helper()
	for _, tc := range processingVersionIndexCases() {
		if tc.tableName == manyChunkTable {
			return tc
		}
	}
	t.Fatalf("no fixture case for the many-chunk table %s", manyChunkTable)
	return processingVersionIndexCase{}
}

// What a trigger lookup has to keep whatever the storage layout of the chunk it lands in, so the
// row-store fixture and the columnstore one are held to one standard instead of two.
func assertLookupPrunedToOneChunk(t *testing.T, plan explainPlan, label string) {
	t.Helper()

	if plan.hasNodeType("Seq Scan") {
		t.Fatalf("%s lookup fell back to a sequential scan\nplan:\n%s", label, plan.raw)
	}
	if chunks := plan.chunkNames(); len(chunks) != 1 {
		t.Fatalf("%s lookup scanned %d chunks, want exactly 1\nplan:\n%s", label, len(chunks), plan.raw)
	}
	// The assertion above cannot stand alone: a plan that kept every chunk and excluded them at
	// startup also scans exactly one, and costs few buffers doing it, so it satisfies both of the
	// other checks while paying the fan-out per row.
	if excluded := plan.deferredChunkExclusions(); excluded > 0 {
		t.Fatalf("%s lookup deferred exclusion of %d chunks to startup, so it re-derives them on every "+
			"execution instead of pruning once when the plan is built\nplan:\n%s", label, excluded, plan.raw)
	}
	if buffers := plan.totalBuffers(); buffers > maxTriggerLookupBuffers {
		t.Fatalf("%s lookup touched %d buffers; expected <= %d\nplan:\n%s",
			label, buffers, maxTriggerLookupBuffers, plan.raw)
	}
}

// The measured EXPLAIN is the second execution, not the first. Planning a hypertable's chunks on a cold
// session reads catalog pages that are not yet in shared buffers — 1,050 planning buffers on borrower's
// first execution against 30 on every one after it — and that is a session start-up cost, not the
// per-row cost a worker pays for hours. One warm-up execution settles it; the count is stable from the
// second onward. It buys nothing plancache-related here: the session GUC forces the plan mode outright,
// so no execution count is needed to reach it.
func explainWarmedLookup(t *testing.T, ctx context.Context, conn *pgxpool.Conn, lookup processingVersionLookup) explainPlan {
	t.Helper()

	if _, err := conn.Exec(ctx, lookup.prepareSQL); err != nil {
		t.Fatalf("prepare %s: %v", lookup.label, err)
	}
	if _, err := conn.Exec(ctx, "EXECUTE "+lookup.executeSQL); err != nil {
		t.Fatalf("execute %s: %v", lookup.label, err)
	}
	return explainJSON(t, ctx, conn, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE "+lookup.executeSQL)
}

// The buffer numbers a plan exposes, plus enough of its shape to tell pruning from fan-out.
type explainPlan struct {
	raw   string
	roots []explainRoot
}

type explainRoot struct {
	Plan     explainNode `json:"Plan"`
	Planning explainBufs `json:"Planning"`
}

type explainBufs struct {
	SharedHit  int `json:"Shared Hit Blocks"`
	SharedRead int `json:"Shared Read Blocks"`
}

type explainNode struct {
	NodeType       string        `json:"Node Type"`
	RelationName   string        `json:"Relation Name"`
	SharedHit      int           `json:"Shared Hit Blocks"`
	SharedRead     int           `json:"Shared Read Blocks"`
	ChunksExcluded int           `json:"Chunks excluded during startup"`
	Plans          []explainNode `json:"Plans"`
}

// Planning buffers count: force_custom_plan re-plans on every execution, so they are part of the
// per-row cost, and the whole point of the setting is that they stay flat as chunks accumulate.
// A node's own counters already include its children's, so only the roots are summed.
func (p explainPlan) totalBuffers() int {
	total := 0
	for _, root := range p.roots {
		total += root.Plan.SharedHit + root.Plan.SharedRead + root.Planning.SharedHit + root.Planning.SharedRead
	}
	return total
}

// A compressed chunk is two relations, and the two answer different questions: the row-store chunk
// is what pruning either kept or discarded, and its columnstore twin is where a columnar scan reads
// the compressed batches from. Counting them together would let a twin stand in for a second chunk
// the plan failed to prune, so each pattern gets its own accessor and every caller says which it
// means. TimescaleDB names both, and only these two shapes are chunks.
var (
	chunkRelationPattern           = regexp.MustCompile(`^_hyper_\d+_\d+_chunk$`)
	compressedChunkRelationPattern = regexp.MustCompile(`^compress_hyper_\d+_\d+_chunk$`)
)

// The row-store chunks the plan names — one per chunk that survived pruning, whether its rows are
// compressed or not.
func (p explainPlan) chunkNames() []string {
	return p.relationNamesMatching(chunkRelationPattern)
}

// The columnstore twins the plan names, one per compressed chunk it reads batches from.
func (p explainPlan) compressedChunkNames() []string {
	return p.relationNamesMatching(compressedChunkRelationPattern)
}

func (p explainPlan) relationNamesMatching(pattern *regexp.Regexp) []string {
	seen := map[string]bool{}
	var names []string
	p.walk(func(n explainNode) {
		if pattern.MatchString(n.RelationName) && !seen[n.RelationName] {
			seen[n.RelationName] = true
			names = append(names, n.RelationName)
		}
	})
	return names
}

// How many chunks the plan discarded at execution time instead of when it was built. A plan that
// prunes as it is built names the surviving chunk and nothing else; one that defers to startup keeps
// every chunk in the plan and re-derives the survivor on each execution, which is the per-row cost
// force_custom_plan exists to remove. Both plans then report the same single chunk scanned, so this
// is the only counter that separates them.
func (p explainPlan) deferredChunkExclusions() int {
	total := 0
	p.walk(func(n explainNode) { total += n.ChunksExcluded })
	return total
}

func (p explainPlan) hasNodeType(nodeType string) bool {
	found := false
	p.walk(func(n explainNode) {
		if n.NodeType == nodeType {
			found = true
		}
	})
	return found
}

func (p explainPlan) walk(visit func(explainNode)) {
	var descend func(explainNode)
	descend = func(n explainNode) {
		visit(n)
		for _, child := range n.Plans {
			descend(child)
		}
	}
	for _, root := range p.roots {
		descend(root.Plan)
	}
}

// Undoes a session GUC before the connection goes back to the pool, and fails the test if it cannot:
// a forced plan_cache_mode is a property of one measurement, and a connection that carries it onward
// silently changes how whatever runs next is planned.
func resetSessionState(t *testing.T, ctx context.Context, conn *pgxpool.Conn, sql string) {
	t.Helper()

	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Errorf("%q failed, so this connection returns to the pool with altered session state: %v", sql, err)
	}
}

// Pinned to one connection: every caller either runs a prepared statement, which only exists on the
// connection that declared it, or measures a plancache whose state is per-connection.
func explainJSON(t *testing.T, ctx context.Context, conn *pgxpool.Conn, sql string) explainPlan {
	t.Helper()

	var raw string
	if err := conn.QueryRow(ctx, sql).Scan(&raw); err != nil {
		t.Fatalf("explain: %v\nsql: %s", err, sql)
	}
	var roots []explainRoot
	if err := json.Unmarshal([]byte(raw), &roots); err != nil {
		t.Fatalf("decode EXPLAIN json: %v\nraw: %s", err, raw)
	}
	if len(roots) == 0 {
		t.Fatalf("EXPLAIN returned no plan\nraw: %s", raw)
	}
	return explainPlan{raw: raw, roots: roots}
}

func chunkCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()

	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = $1`, table).Scan(&n); err != nil {
		t.Fatalf("count chunks of %s: %v", table, err)
	}
	return n
}

// prime_debt's only FK parent. A prime of its own keeps the measured insert on keys nothing else
// touches, clear of the migration-seeded prime_id = 1 the fan-out fixture writes under.
func upsertFixturePrime(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()

	var id int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO prime (name, vault_address)
		VALUES ('pv-plan-cache-fixture', '\xdeadbeef00000000000000000000000000000001')
		ON CONFLICT (name) DO UPDATE SET vault_address = EXCLUDED.vault_address
		RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("upsert fixture prime: %v", err)
	}
	return id
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
	if _, err := tx.Exec(ctx, manyChunkSeedStatement, manyChunkDays); err != nil {
		t.Fatalf("spread %s across chunks: %v", manyChunkTable, err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed tx: %v", err)
	}
	committed = true

	for _, tc := range processingVersionIndexCases() {
		if _, err := pool.Exec(ctx, fmt.Sprintf("ANALYZE %s", tc.tableName)); err != nil {
			t.Fatalf("analyze %s: %v", tc.tableName, err)
		}
		// "Pruned to exactly one chunk" asserts nothing about a table that only has one chunk, so every
		// fixture table has to come out of the seed with several.
		if n := chunkCount(t, ctx, pool, tc.tableName); n < minFixtureChunkCount {
			t.Fatalf("%s fixture has %d chunks, want >= %d — the prune assertion is vacuous below that",
				tc.tableName, n, minFixtureChunkCount)
		}
	}
	if n := chunkCount(t, ctx, pool, manyChunkTable); n < minManyChunkCount {
		t.Fatalf("%s fixture has %d chunks, want >= %d — fan-out assertions need a wide table",
			manyChunkTable, n, minManyChunkCount)
	}
}

// Rows all land in one existing chunk, so any buffer cost beyond that chunk comes from the trigger
// failing to prune. Distinct synced_at values keep the trigger on its first-write path per row.
const manyChunkTriggerInsertStatement = `
	EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
	INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, build_id)
	SELECT %[1]d, 'plan-cache-fixture', 1, 2000000, 0,
	       timestamptz '2035-01-03 00:00:00+00' + (%[2]d * interval '1 hour') + (g * interval '1 millisecond'), %[4]d
	FROM generate_series(1, %[3]d) AS g`

// Two batches an hour apart, both inside the probe key's chunk.
const (
	warmupInsertHour   = 12
	measuredInsertHour = 13
)

// One row per day, each landing in its own daily chunk.
const manyChunkSeedStatement = `
	INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id)
	SELECT 1, 'ilk', 1, 1000000, 0,
	       timestamptz '2035-01-01 06:00:00+00' + (g * interval '1 day'), 0, 0
	FROM generate_series(1, $1) AS g`

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
		// This table partitions on block_number (100,000 blocks to a chunk) and its natural key has no
		// time column, so the column that varies per row is also the column that picks the chunk — where
		// the other tables vary a timestamp. Striding one chunk interval every fifth row spreads the
		// fixture over five chunks, and g = 1 puts a row on the probe key itself inside the first of them.
		`
			INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version, total_a_token, processing_version, build_id)
			SELECT 1, 1, 1000000 + ((g - 1) % 5) * 100000 + ((g - 1) / 5), 0, 1, 0, 0
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
