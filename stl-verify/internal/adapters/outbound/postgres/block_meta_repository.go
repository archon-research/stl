package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that BlockMetaRepository implements outbound.BlockMetaRepository.
var _ outbound.BlockMetaRepository = (*BlockMetaRepository)(nil)

// BlockMetaRepository is a PostgreSQL implementation of the outbound.BlockMetaRepository port.
type BlockMetaRepository struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	buildID buildregistry.BuildID
	runID   buildregistry.RunID
}

// NewBlockMetaRepository creates a new PostgreSQL block_meta repository. buildID and runID are the
// build and the writer run
// opened by the process; it stamps every row the loader writes (ADR-0006 §2).
func NewBlockMetaRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID, runID buildregistry.RunID) (*BlockMetaRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if runID == 0 {
		return nil, fmt.Errorf("run id cannot be zero")
	}
	// 0 is the column default, which ADR-0006 reads as pre-tracking data, so a caller that lost the
	// registry would write rows indistinguishable from untracked ones. Refused here for the same
	// reason the run is: at the boundary, not after a run has written.
	if buildID == 0 {
		return nil, fmt.Errorf("build id cannot be zero")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BlockMetaRepository{pool: pool, logger: logger, buildID: buildID, runID: runID}, nil
}

// The arms are DERIVED from schema_master.json, not listed here. Its block_meta fills are the declared
// answer to "which tables resolve a column by joining block_meta", and a hand-kept second list drifts
// from it silently: a table gaining a fill and no arm is never enumerated, every one of its values
// resolves NULL, and the conformance check still passes because the declaration alone satisfies it.
//
// Chain resolution comes from the same register, in the three shapes a chain_id fill can take: through
// a config parent (borrower -> protocol.chain_id), as a literal for a table whose chain is fixed
// (prime_debt is Sky on chain 1 and has no chain column to join), or natively when there is no fill at
// all. The partition column is read from the live catalogue rather than declared, so a window can never
// be expressed on a column the table is no longer partitioned by.
type workListArm struct {
	table   string // the referencing table, and the hypertable whose chunks give the windows
	partCol string // its partition column, read from the catalogue; the window is expressed on it alone
	sql     string // $1 = chain id; %s = the window predicate on partCol
}

// armSQL builds one arm, taking chain from the table's chain_id fill: a join to the config parent, the
// fill's literal, or the table's own column when it declares no fill.
//
// The same expression is both selected and compared to $1, so a constant arm reads "1 = $1" and
// contributes nothing to another chain's run rather than labelling Sky's blocks with that chain.
//
// A shape this does not build is an error, not a best effort: rendering a two-hop fill as its first hop
// joins a table that has no chain_id, which fails as SQL inside a run rather than here.
func armSQL(table string, chain schemamaster.Fill, declared bool) (string, error) {
	const shape = `
		INSERT INTO block_meta_worklist (chain_id, block_number, block_version)
		SELECT %s, t.block_number, t.block_version
		  FROM %s t%s
		 WHERE %s = $1 AND %%s
		ON CONFLICT DO NOTHING`
	chainExpr, join := "t.chain_id", ""
	switch {
	case chain.ThenParent != "":
		return "", fmt.Errorf("%s resolves chain_id through two hops (%s then %s), which the work list does not build", table, chain.Parent, chain.ThenParent)
	case chain.Parent != "" && chain.Const != nil:
		return "", fmt.Errorf("%s declares chain_id as both a %s join and a constant", table, chain.Parent)
	case chain.Const != nil && *chain.Const <= 0:
		return "", fmt.Errorf("%s declares chain_id as the constant %d; no chain has that id, so the arm would match no run and enumerate nothing", table, *chain.Const)
	case declared && chain.Parent == "" && chain.Const == nil:
		return "", fmt.Errorf("%s declares a chain_id fill with neither a parent nor a constant; a fill exists because the column is not native, so t.chain_id would not resolve", table)
	case chain.Parent != "":
		chainExpr = "p.chain_id"
		join = fmt.Sprintf(" JOIN %s p ON p.%s = t.%s", quoteIdent(chain.Parent), quoteIdent(chain.Ref), quoteIdent(chain.Key))
	case chain.Const != nil:
		chainExpr = strconv.Itoa(*chain.Const)
	}
	return fmt.Sprintf(shape, chainExpr, quoteIdent(table), join, chainExpr), nil
}

// quoteIdent quotes a catalogue identifier. Every value reaching it comes from schema_master.json or
// the live catalogue rather than a caller, but the arms are built by string formatting, so an
// identifier that ever stops being a bare word must not change the shape of the statement.
func quoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// workListArms derives the arms for one run. The partition column is looked up per table, so a table
// that is not a hypertable, or whose dimension changed, fails loudly here rather than enumerating on a
// column that no longer partitions it.
func (r *BlockMetaRepository) workListArms(ctx context.Context) ([]workListArm, error) {
	register, err := schemamaster.Load()
	if err != nil {
		return nil, fmt.Errorf("loading the column register: %w", err)
	}
	chainFill := map[string]schemamaster.Fill{}
	var tables []string
	for _, f := range register.Fills {
		if f.BlockMeta {
			tables = append(tables, f.Table)
		}
	}
	for _, f := range register.Fills {
		if f.Column == "chain_id" {
			chainFill[f.Table] = f
		}
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("no table declares a block_meta fill; the register did not load as expected")
	}
	slices.Sort(tables)
	tables = slices.Compact(tables)

	arms := make([]workListArm, 0, len(tables))
	for _, table := range tables {
		partCol, err := r.partitionColumn(ctx, table)
		if err != nil {
			return nil, err
		}
		chain, declared := chainFill[table]
		if !declared {
			if err := r.requireNativeChainColumn(ctx, table); err != nil {
				return nil, err
			}
		}
		sql, err := armSQL(table, chain, declared)
		if err != nil {
			return nil, err
		}
		arms = append(arms, workListArm{
			table:   table,
			partCol: "t." + quoteIdent(partCol),
			sql:     sql,
		})
	}
	return arms, nil
}

// requireNativeChainColumn refuses a table that resolves chain neither by fill nor by its own column.
// The register exempts several tables from the chain key entirely, and a block_meta fill added to one
// of those would otherwise build an arm on t.chain_id and fail as SQL part way through a run.
func (r *BlockMetaRepository) requireNativeChainColumn(ctx context.Context, table string) error {
	var present bool
	if err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		                WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'chain_id')`,
		table).Scan(&present); err != nil {
		return fmt.Errorf("reading %s's columns: %w", table, err)
	}
	if !present {
		return fmt.Errorf("%s declares a block_meta fill but resolves chain_id neither natively nor by a fill", table)
	}
	return nil
}

// partitionColumn reads a hypertable's primary dimension from the catalogue.
func (r *BlockMetaRepository) partitionColumn(ctx context.Context, table string) (string, error) {
	var col string
	if err := r.pool.QueryRow(ctx, `
		SELECT column_name FROM timescaledb_information.dimensions
		 WHERE hypertable_schema = 'public' AND hypertable_name = $1 AND dimension_number = 1`, table).Scan(&col); err != nil {
		return "", fmt.Errorf("reading %s's partition column: %w", table, err)
	}
	return col, nil
}

// chunksPerWindow bounds how many of a table's chunks one statement may open. Planning cost tracks
// chunks opened at roughly 6 MB each, so this is the knob that keeps a statement under the guard.
const chunksPerWindow = 16

// chunkRangeSQL reads the windows' bounds. timescaledb_information.chunks lists LOCAL chunks only: a
// tiered chunk leaves it, so once tiering starts the oldest bound visible here jumps forward to the
// oldest untiered chunk and no window is ever built over the range below it. Those blocks then drop out
// of the work list with no error, and enable_tiered_reads cannot rescue a range no statement scans.
// timescaledb_osm.tiered_chunks carries the same four range columns for the tiered ones.
//
// The OSM view is absent wherever the tiering extension is not installed, including the local harness,
// and a missing relation is a parse error rather than an empty result — hence the probe rather than a
// LEFT JOIN or a to_regclass inside the query.
//
// Both halves are read as public only. The transformed layer names its hypertables after the raw tables
// they canonicalise, so an unqualified lookup unions a table's chunks with its twin's and groups ranges
// that are not disjoint.
const chunkRangeSQL = `
		SELECT range_start_integer, range_end_integer, range_start, range_end
		  FROM timescaledb_information.chunks
		 WHERE hypertable_schema = 'public' AND hypertable_name = $1`

const chunkRangeWithTieredSQL = chunkRangeSQL + `
		 UNION ALL
		SELECT range_start_integer, range_end_integer, range_start, range_end
		  FROM timescaledb_osm.tiered_chunks
		 WHERE hypertable_schema = 'public' AND hypertable_name = $1`

func (r *BlockMetaRepository) windowPredicates(ctx context.Context, table, partCol string) ([]string, error) {
	var tieredVisible bool
	if err := r.pool.QueryRow(ctx,
		`SELECT to_regclass('timescaledb_osm.tiered_chunks') IS NOT NULL`).Scan(&tieredVisible); err != nil {
		return nil, fmt.Errorf("probing for the tiered-chunk catalogue: %w", err)
	}
	query := chunkRangeSQL
	if tieredVisible {
		query = chunkRangeWithTieredSQL
	}
	rows, err := r.pool.Query(ctx,
		query+` ORDER BY 1 NULLS LAST, 3 NULLS LAST`, table)
	if err != nil {
		return nil, fmt.Errorf("reading %s chunk ranges: %w", table, err)
	}
	defer rows.Close()
	type bound struct{ lo, hi string }
	var bounds []bound
	for rows.Next() {
		var loInt, hiInt *int64
		var loTS, hiTS *time.Time
		if err := rows.Scan(&loInt, &hiInt, &loTS, &hiTS); err != nil {
			return nil, fmt.Errorf("scanning a %s chunk range: %w", table, err)
		}
		switch {
		case loInt != nil && hiInt != nil:
			bounds = append(bounds, bound{strconv.FormatInt(*loInt, 10), strconv.FormatInt(*hiInt, 10)})
		case loTS != nil && hiTS != nil:
			bounds = append(bounds, bound{quoteTimestamp(*loTS), quoteTimestamp(*hiTS)})
		default:
			return nil, fmt.Errorf("%s has a chunk with neither an integer nor a time range", table)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating %s chunk ranges: %w", table, err)
	}
	var out []string
	for i := 0; i < len(bounds); i += chunksPerWindow {
		j := min(i+chunksPerWindow, len(bounds))
		out = append(out, fmt.Sprintf("%s >= %s AND %s < %s", partCol, bounds[i].lo, partCol, bounds[j-1].hi))
	}
	return out, nil
}

// enumerateWindow runs one arm over one window, in its own transaction, with tiered reads on.
//
// Four of the six source tables carry a one-year tiering policy. The chunk catalogue still lists a
// tiered chunk, so a window would be built for it and then read with timescaledb.enable_tiered_reads
// at its default of off — returning nothing, silently, for exactly the deep-tail blocks this loader
// exists to cover. Nothing is a year old yet; this is set before that becomes true rather than after.
// SET LOCAL, so it lasts the statement's transaction and no longer.
func (r *BlockMetaRepository) enumerateWindow(ctx context.Context, arm workListArm, where string, chainID int64) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin enumeration of %s: %w", arm.table, err)
	}
	defer rollback(ctx, tx, r.logger)

	if _, err := tx.Exec(ctx, `SET LOCAL timescaledb.enable_tiered_reads = on`); err != nil {
		return fmt.Errorf("enabling tiered reads for %s: %w", arm.table, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(arm.sql, where), chainID); err != nil {
		return fmt.Errorf("enumerating %s for chain %d: %w", arm.table, chainID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing enumeration of %s: %w", arm.table, err)
	}
	return nil
}

// quoteTimestamp renders t as a literal PostgreSQL will read back exactly, in UTC.
func quoteTimestamp(t time.Time) string {
	return "'" + t.UTC().Format("2006-01-02 15:04:05.999999-07") + "'::timestamptz"
}

// blockWorkList pages the run's work list. The list is a committed table, so nothing is held open
// between batches: each page is its own pooled query, and a run that dies leaves the list behind for
// the next one to resume from rather than discarding hours of enumeration; a pass that reaches the
// end clears its own chain, so surviving rows always mean an interrupted run.
type blockWorkList struct {
	pool    *pgxpool.Pool
	logger  *slog.Logger
	chainID int64
	after   outbound.BlockRef
}

// OpenWorkList enumerates the blocks chainID references that block_meta lacks, once, into
// block_meta_worklist, and returns a cursor over it.
//
// Every statement here commits on its own. The previous shape held one transaction open for the whole
// run because its temp table was ON COMMIT DROP, and that transaction's backend_xid pins VACUUM's
// removable cutoff database-wide even with no snapshot held -- for chain 1 that is hours.
func (r *BlockMetaRepository) OpenWorkList(ctx context.Context, chainID int64, headMargin int64) (outbound.BlockWorkList, error) {
	// Every run enumerates from scratch. Rows surviving a previous run say nothing about whether its
	// enumeration FINISHED -- windows commit one at a time, so a run killed part way leaves the arms it
	// reached and none of the rest, and resuming that reads a partial list as a complete one. Resuming
	// the expensive half is the anti-join's job below, and it works whether or not this table survived.
	if _, err := r.pool.Exec(ctx,
		`DELETE FROM block_meta_worklist WHERE chain_id = $1`, chainID); err != nil {
		return nil, fmt.Errorf("clearing the work list for chain %d: %w", chainID, err)
	}
	arms, err := r.workListArms(ctx)
	if err != nil {
		return nil, err
	}
	for _, arm := range arms {
		windows, err := r.windowPredicates(ctx, arm.table, arm.partCol)
		if err != nil {
			return nil, err
		}
		for _, where := range windows {
			if err := r.enumerateWindow(ctx, arm, where, chainID); err != nil {
				return nil, err
			}
		}
		r.logger.Debug("work list arm enumerated", "table", arm.table, "chain", chainID, "windows", len(windows))
	}
	// The pending set is what the arms found minus what block_meta already has. Subtracting here rather
	// than inside every arm keeps block_meta out of six plans, and it is a plain table, so this is one
	// relation-level pass instead of six.
	if _, err := r.pool.Exec(ctx, `
		DELETE FROM block_meta_worklist w
		 WHERE w.chain_id = $1
		   AND EXISTS (SELECT 1 FROM block_meta m
		                WHERE m.chain_id = w.chain_id
		                  AND m.block_number = w.block_number
		                  AND m.block_version = w.block_version)`, chainID); err != nil {
		return nil, fmt.Errorf("removing already-loaded blocks for chain %d: %w", chainID, err)
	}
	// The head margin holds back the newest blocks while the archive catches up, so it measures from
	// the chain's head: the highest block either still pending or already loaded. Measured from the
	// pending set alone, a gap narrower than the margin sits entirely inside it and is deleted whole.
	if headMargin > 0 {
		if _, err := r.pool.Exec(ctx, `
			DELETE FROM block_meta_worklist w
			 WHERE w.chain_id = $1
			   AND w.block_number > (
			        SELECT max(head) - $2 FROM (
			          SELECT max(block_number) AS head FROM block_meta_worklist WHERE chain_id = $1
			          UNION ALL
			          SELECT max(block_number) FROM block_meta WHERE chain_id = $1) t)`,
			chainID, headMargin); err != nil {
			return nil, fmt.Errorf("applying the head margin for chain %d: %w", chainID, err)
		}
	}
	return &blockWorkList{pool: r.pool, logger: r.logger, chainID: chainID,
		after: outbound.BlockRef{Number: -1, Version: -1}}, nil
}

// Next pages the work-list with a keyset cursor, so the ordered read is not restarted per batch.
func (w *blockWorkList) Next(ctx context.Context, limit int) ([]outbound.BlockRef, error) {
	if w.pool == nil {
		return nil, fmt.Errorf("work list is closed")
	}
	rows, err := w.pool.Query(ctx, `
		SELECT block_number, block_version FROM block_meta_worklist
		 WHERE chain_id = $1 AND (block_number, block_version) > ($2, $3)
		 ORDER BY block_number, block_version
		 LIMIT $4`, w.chainID, w.after.Number, w.after.Version, limit)
	if err != nil {
		return nil, fmt.Errorf("reading the work list: %w", err)
	}
	defer rows.Close()
	var out []outbound.BlockRef
	for rows.Next() {
		var b outbound.BlockRef
		if err := rows.Scan(&b.Number, &b.Version); err != nil {
			return nil, fmt.Errorf("scanning a work-list row: %w", err)
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating the work list: %w", err)
	}
	if len(out) > 0 {
		w.after = out[len(out)-1]
	}
	return out, nil
}

func (w *blockWorkList) Close(ctx context.Context) {
	// Nothing to clear: the chain is cleared at the start of every run, which is the one boundary, so
	// rows left here are scratch the next Open discards rather than state anything depends on.
	w.pool = nil
}

// Upsert writes the batch as one INSERT ... SELECT FROM unnest(...) ON CONFLICT DO NOTHING, with no
// transaction and no stage table: a temp table per batch is catalogue churn that a 500-row batch
// cannot repay against S3 reads dominating it by three orders of magnitude.
//
// The arbiter is block_meta's primary key (chain_id, block_number, block_version, processing_version).
// The loader always writes processing_version 0, so a re-run is a no-op; a mis-parsed header is
// corrected by appending the same block at a higher processing_version, which this path never
// touches and never overwrites. Every row carries the process's run_id.
func (r *BlockMetaRepository) Upsert(ctx context.Context, rows []outbound.BlockMetaRow) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	numbers := make([]int64, len(rows))
	versions := make([]int32, len(rows))
	stamps := make([]time.Time, len(rows))
	chainID := rows[0].ChainID
	for i, row := range rows {
		if row.ChainID != chainID {
			return 0, fmt.Errorf("batch mixes chains %d and %d; one run writes one chain", chainID, row.ChainID)
		}
		numbers[i] = row.BlockNumber
		versions[i] = int32(row.BlockVersion)
		stamps[i] = row.BlockTimestamp
	}

	ct, err := r.pool.Exec(ctx, `
INSERT INTO block_meta (chain_id, block_number, block_version, processing_version, block_timestamp, build_id, run_id)
SELECT $1, s.n, s.v, 0, s.ts, $2, $3
  FROM unnest($4::bigint[], $5::int[], $6::timestamptz[]) AS s(n, v, ts)
ON CONFLICT (chain_id, block_number, block_version, processing_version) DO NOTHING`,
		chainID, int32(r.buildID), int64(r.runID), numbers, versions, stamps)
	if err != nil {
		return 0, fmt.Errorf("insert block_meta batch: %w", err)
	}
	return ct.RowsAffected(), nil
}
