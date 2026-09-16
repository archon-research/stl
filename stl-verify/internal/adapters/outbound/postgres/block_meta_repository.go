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
// build and the writer run opened by the process; it stamps every row the loader writes (ADR-0006 §2).
//
// runID must belong to this logical run and no other: it also owns the run's work-list slice, and a
// run id shared with a second concurrent pass means each clears the other's list at its own start.
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
// Chain resolution comes from the same register. A table with a chain_id fill reaches chain through its
// parent (borrower -> protocol.chain_id); one without carries chain_id natively. The partition column
// is read from the live catalogue rather than declared, so a window can never be expressed on a column
// the table is no longer partitioned by.
type workListArm struct {
	table   string // the referencing table, and the hypertable whose chunks give the windows
	partCol string // its partition column, read from the catalogue; the window is expressed on it alone
	sql     string // $1 = chain id, $2 = run id; %s = the window predicate on partCol
}

// armSQL builds one arm. parent is empty for a table carrying chain_id natively; otherwise the arm
// joins parent on parentRef = table.parentKey and takes chain from there. Every row is stamped with
// the enumerating run, which is the only writer that may delete it.
func armSQL(table, parent, parentKey, parentRef string) string {
	const shape = `
		INSERT INTO block_meta_worklist (chain_id, run_id, block_number, block_version)
		SELECT %s.chain_id, $2, t.block_number, t.block_version
		  FROM %s t%s
		 WHERE %s.chain_id = $1 AND %%s
		ON CONFLICT DO NOTHING`
	src, join := "t", ""
	if parent != "" {
		src = "p"
		join = fmt.Sprintf(" JOIN %s p ON p.%s = t.%s", quoteIdent(parent), quoteIdent(parentRef), quoteIdent(parentKey))
	}
	return fmt.Sprintf(shape, src, quoteIdent(table), join, src)
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
	chainParent := map[string]schemamaster.Fill{}
	var tables []string
	for _, f := range register.Fills {
		if f.BlockMeta {
			tables = append(tables, f.Table)
		}
	}
	for _, f := range register.Fills {
		if f.Column == "chain_id" && f.Parent != "" {
			chainParent[f.Table] = f
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
		f := chainParent[table]
		arms = append(arms, workListArm{
			table:   table,
			partCol: "t." + quoteIdent(partCol),
			sql:     armSQL(table, f.Parent, f.Key, f.Ref),
		})
	}
	return arms, nil
}

// partitionColumn reads a hypertable's primary dimension from the catalogue.
func (r *BlockMetaRepository) partitionColumn(ctx context.Context, table string) (string, error) {
	var col string
	if err := r.pool.QueryRow(ctx, `
		SELECT column_name FROM timescaledb_information.dimensions
		 WHERE hypertable_name = $1 AND dimension_number = 1`, table).Scan(&col); err != nil {
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
const chunkRangeSQL = `
		SELECT range_start_integer, range_end_integer, range_start, range_end
		  FROM timescaledb_information.chunks
		 WHERE hypertable_name = $1`

const chunkRangeWithTieredSQL = chunkRangeSQL + `
		 UNION ALL
		SELECT range_start_integer, range_end_integer, range_start, range_end
		  FROM timescaledb_osm.tiered_chunks
		 WHERE hypertable_name = $1`

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
	if _, err := tx.Exec(ctx, fmt.Sprintf(arm.sql, where), chainID, r.runID); err != nil {
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

// blockWorkList pages one run's slice of the work list. The list is a committed table, so nothing is
// held open between batches: each page is its own pooled query. Every statement here is scoped to the
// run that enumerated the slice, so a second run on the same chain -- the scheduled top-up overlapping
// an operator's pass -- reads and deletes its own rows only.
type blockWorkList struct {
	pool       *pgxpool.Pool
	logger     *slog.Logger
	chainID    int64
	runID      buildregistry.RunID
	enumerated int64 // rows this run put on the list; the cursor must account for every one of them
	after      outbound.BlockRef
}

// OpenWorkList enumerates the blocks chainID references that block_meta lacks, once, into
// block_meta_worklist, and returns a cursor over it.
//
// Every statement here commits on its own. The previous shape held one transaction open for the whole
// run because its temp table was ON COMMIT DROP, and that transaction's backend_xid pins VACUUM's
// removable cutoff database-wide even with no snapshot held -- for chain 1 that is hours.
func (r *BlockMetaRepository) OpenWorkList(ctx context.Context, chainID int64, headMargin int64) (outbound.BlockWorkList, error) {
	if err := r.clearOwnSlice(ctx, chainID); err != nil {
		return nil, err
	}
	// Fatal, deliberately: the sweep is a DELETE on the same pool every statement below uses, so one
	// that fails is a database this run cannot finish against either.
	if err := r.sweepAbandonedSlices(ctx); err != nil {
		return nil, fmt.Errorf("opening the work list for chain %d: %w", chainID, err)
	}
	if err := r.enumerateArms(ctx, chainID); err != nil {
		return nil, err
	}
	if err := r.removeAlreadyLoaded(ctx, chainID); err != nil {
		return nil, err
	}
	if err := r.applyHeadMargin(ctx, chainID, headMargin); err != nil {
		return nil, err
	}
	enumerated, err := r.measureSlice(ctx, chainID)
	if err != nil {
		return nil, fmt.Errorf("opening the work list for chain %d: %w", chainID, err)
	}
	return &blockWorkList{pool: r.pool, logger: r.logger, chainID: chainID, runID: r.runID,
		enumerated: enumerated, after: outbound.BlockRef{Number: -1, Version: -1}}, nil
}

// clearOwnSlice restarts this run's slice. Isolation comes from the run id, not from this DELETE,
// which only matters to a caller that opens two lists under one run. Rows left by a previous run say
// nothing about whether its enumeration FINISHED -- windows commit one at a time, so a run killed part
// way leaves the arms it reached and none of the rest. Resuming the expensive half is the anti-join's
// job, and that works whether or not this table survived.
func (r *BlockMetaRepository) clearOwnSlice(ctx context.Context, chainID int64) error {
	if _, err := r.pool.Exec(ctx,
		`DELETE FROM block_meta_worklist WHERE chain_id = $1 AND run_id = $2`, chainID, r.runID); err != nil {
		return fmt.Errorf("clearing the work list for chain %d: %w", chainID, err)
	}
	return nil
}

// enumerateArms runs every arm the register declares, one partition-column window at a time.
func (r *BlockMetaRepository) enumerateArms(ctx context.Context, chainID int64) error {
	arms, err := r.workListArms(ctx)
	if err != nil {
		return err
	}
	for _, arm := range arms {
		windows, err := r.windowPredicates(ctx, arm.table, arm.partCol)
		if err != nil {
			return err
		}
		for _, where := range windows {
			if err := r.enumerateWindow(ctx, arm, where, chainID); err != nil {
				return err
			}
		}
		r.logger.Debug("work list arm enumerated", "table", arm.table, "chain", chainID, "windows", len(windows))
	}
	return nil
}

// removeAlreadyLoaded subtracts what block_meta already holds. Subtracting here rather than inside
// every arm keeps block_meta out of six plans, and it is a plain table, so this is one relation-level
// pass instead of six.
func (r *BlockMetaRepository) removeAlreadyLoaded(ctx context.Context, chainID int64) error {
	if _, err := r.pool.Exec(ctx, `
		DELETE FROM block_meta_worklist w
		 WHERE w.chain_id = $1 AND w.run_id = $2
		   AND EXISTS (SELECT 1 FROM block_meta m
		                WHERE m.chain_id = w.chain_id
		                  AND m.block_number = w.block_number
		                  AND m.block_version = w.block_version)`, chainID, r.runID); err != nil {
		return fmt.Errorf("removing already-loaded blocks for chain %d: %w", chainID, err)
	}
	return nil
}

// applyHeadMargin holds back the newest blocks while the archive catches up, measured from the chain's
// head: the highest block either still pending or already loaded. Measured from the pending set alone,
// a gap narrower than the margin sits entirely inside it and is deleted whole. The head is read across
// runs on purpose -- it is the chain's, not this run's.
func (r *BlockMetaRepository) applyHeadMargin(ctx context.Context, chainID, headMargin int64) error {
	if headMargin <= 0 {
		return nil
	}
	if _, err := r.pool.Exec(ctx, `
		DELETE FROM block_meta_worklist w
		 WHERE w.chain_id = $1 AND w.run_id = $3
		   AND w.block_number > (
		        SELECT max(head) - $2 FROM (
		          SELECT max(block_number) AS head FROM block_meta_worklist
		           WHERE chain_id = $1 AND run_id = $3
		          UNION ALL
		          SELECT max(block_number) FROM block_meta WHERE chain_id = $1) t)`,
		chainID, headMargin, r.runID); err != nil {
		return fmt.Errorf("applying the head margin for chain %d: %w", chainID, err)
	}
	return nil
}

// abandonedSliceAge is how old a run must be before another run may delete its rows. A slice belongs
// to its run until then, whether or not that run is still alive: writer_run records no end, so age is
// the only signal.
//
// It must exceed the longest attempt the system permits -- the loader's activityTimeout, 24h
// (cmd/cronjobs/block-meta-loader/load.go) -- or a pass still running at its own ceiling has its slice
// taken. Doubling that is the margin; sweeping a slice whose run is alive is caught by the cursor
// rather than silently ending the run, but it still costs that run its work.
//
// A literal, not a bind parameter, so the planner sees the window it is filtering on.
const abandonedSliceAge = "48 hours"

// closeTimeout bounds the slice delete on the shutdown path, which runs outside the run's context.
const closeTimeout = 30 * time.Second

// sweepAbandonedSlices removes rows left by runs old enough that no live run can own them. Without it
// a killed run's slice stays forever, because nothing else deletes another run's rows.
//
// Every chain, not just this one: a chain whose deployment is retired never opens another list, so a
// slice abandoned there would have no other sweeper. The age predicate already excludes any run that
// could still be alive, which is what makes the wider scope safe.
//
// The join to writer_run is what dates a slice. A row whose run is gone is therefore never swept; the
// foreign key makes that unreachable, and it stays unreachable only while writer_run has no retention.
func (r *BlockMetaRepository) sweepAbandonedSlices(ctx context.Context) error {
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM block_meta_worklist w
		 USING writer_run run
		 WHERE w.run_id = run.id
		   AND w.run_id <> $1
		   AND run.started_at < now() - interval '`+abandonedSliceAge+`'`,
		r.runID)
	if err != nil {
		return fmt.Errorf("sweeping abandoned work-list slices: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		r.logger.Info("swept abandoned work-list rows", "rows", n)
	}
	return nil
}

// sliceSizeSQL counts one run's rows on a chain. Nothing but that run inserts them and nothing but
// that run deletes them, so the count is fixed for the run's lifetime -- which is what lets the cursor
// tell a drained list from one deleted under it.
const sliceSizeSQL = `SELECT count(*) FROM block_meta_worklist WHERE chain_id = $1 AND run_id = $2`

// measureSlice sizes this run's slice and reports whether another run is loading the same chain. An
// overlap is legal and costs both runs the blocks they read twice, so it is logged where the cost is
// paid rather than inferred afterwards from an archive bill.
func (r *BlockMetaRepository) measureSlice(ctx context.Context, chainID int64) (int64, error) {
	var mine, others int64
	if err := r.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE run_id = $2), count(*) FILTER (WHERE run_id <> $2)
		  FROM block_meta_worklist WHERE chain_id = $1`, chainID, r.runID).Scan(&mine, &others); err != nil {
		return 0, fmt.Errorf("sizing the work list for chain %d: %w", chainID, err)
	}
	if others > 0 {
		r.logger.Info("another run holds a work list on this chain; the overlap is read twice",
			"chain", chainID, "run", r.runID, "mine", mine, "theirs", others)
	}
	return mine, nil
}

// Next pages the work-list with a keyset cursor, so the ordered read is not restarted per batch.
func (w *blockWorkList) Next(ctx context.Context, limit int) ([]outbound.BlockRef, error) {
	if w.pool == nil {
		return nil, fmt.Errorf("work list is closed")
	}
	rows, err := w.pool.Query(ctx, `
		SELECT block_number, block_version FROM block_meta_worklist
		 WHERE chain_id = $1 AND run_id = $2 AND (block_number, block_version) > ($3, $4)
		 ORDER BY block_number, block_version
		 LIMIT $5`, w.chainID, w.runID, w.after.Number, w.after.Version, limit)
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
		return out, nil
	}
	// An empty page is the end of the list, or the list going missing: the sweep reclaims slices of
	// runs older than a day, and a run that outlives that is one whose slice another run may take. The
	// rows stay put until Close, so a drained slice still holds every row it enumerated; a short one
	// means this run would otherwise report the blocks it never reached as nothing left to do.
	if err := w.requireWholeSlice(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

func (w *blockWorkList) requireWholeSlice(ctx context.Context) error {
	var left int64
	if err := w.pool.QueryRow(ctx, sliceSizeSQL, w.chainID, w.runID).Scan(&left); err != nil {
		return fmt.Errorf("checking the work-list slice for chain %d: %w", w.chainID, err)
	}
	if left != w.enumerated {
		return fmt.Errorf("the work-list slice for chain %d was deleted under run %d: %d of %d rows remain, "+
			"so this run reached the end of a list it did not finish", w.chainID, w.runID, left, w.enumerated)
	}
	return nil
}

// Close drops this run's slice. SIGTERM is the ordinary stop, and it reaches here with the run's
// context already cancelled, so the delete runs on a context detached from it -- otherwise the one
// path Close exists for is the one on which it always fails. A run killed outright still leaves its
// slice behind, and a later run sweeps it once the owning run is old enough.
func (w *blockWorkList) Close(ctx context.Context) {
	if w.pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	if _, err := w.pool.Exec(ctx,
		`DELETE FROM block_meta_worklist WHERE chain_id = $1 AND run_id = $2`, w.chainID, w.runID); err != nil {
		w.logger.Warn("could not drop the work-list slice; the next run on this chain sweeps it",
			"chain", w.chainID, "run", w.runID, "error", err)
	}
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
