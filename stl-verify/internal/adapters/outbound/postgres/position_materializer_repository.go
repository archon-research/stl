package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PositionMaterializerRepository implements
// outbound.PositionMaterializer.
var _ outbound.PositionMaterializer = (*PositionMaterializerRepository)(nil)

// PositionMaterializerRepository calls one per-projection materializer function
// (materialize_<projection>, VEC-402). The function owns all correctness logic,
// including its projection's own pre-flight refusals; this adapter is the call site.
type PositionMaterializerRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPositionMaterializerRepository creates a PositionMaterializerRepository.
func NewPositionMaterializerRepository(pool *pgxpool.Pool, logger *slog.Logger) *PositionMaterializerRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PositionMaterializerRepository{pool: pool, logger: logger}
}

// Materialize calls one materializer function, retrying transient transaction
// errors.
//
// position_state carries compression and S3 tiering policies, and both are
// chunk-level actors taking AccessExclusiveLock per chunk. A policy job running
// concurrently with a materializer run can deadlock it (SQLSTATE 40P01), with the
// materializer as the victim. The migration orders its INSERT by chunk key so the
// two acquire chunk locks in the same order, which is the repo's standing answer
// to writer deadlocks (ADR-0002, as in token_repository and position_repository)
// — but ordering narrows the window rather than closing it, so the caller-side
// half of that pattern belongs here. The function is idempotent (NOT EXISTS +
// ON CONFLICT DO NOTHING), so a retry re-runs safely: a deadlocked attempt
// committed nothing, and a redundant attempt inserts zero rows.
//
// Config matches blockstate_repository.go. Only deadlocks and serialization failures are retried. A
// chunk lock held by a compression job is waited on for as long as it is held, under DefaultDBConfig's
// unset lock_timeout (TestMaterialize_WaitsOutALockLongerThanTheRetryBackoff).
func (r *PositionMaterializerRepository) Materialize(ctx context.Context, materializer string, buildID int, runID int64) (int64, error) {
	cfg := retry.Config{
		MaxRetries:     10,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         true,
	}

	onRetry := func(attempt int, err error, backoff time.Duration) {
		r.logger.Debug("retryable tx error, retrying materialization",
			"attempt", attempt,
			"materializer", materializer,
			"build_id", buildID,
			"run_id", runID,
			"backoff", backoff)
	}

	return retry.Do(ctx, cfg, isRetryableTxError, onRetry, func() (int64, error) {
		return r.materializeOnce(ctx, materializer, buildID, runID)
	})
}

// withheldPairView reports, per projection, the inputs whose wrapper cannot key them. Those pairs are
// recorded after the run row is written, so positions_refused — the one level the withholding alert
// reads — can never count them, and a permanently withheld position would page nobody.
//
// Read live rather than from position_projection_refusal: that table is append-only, so a count taken
// from it would outlive the defect and the alert would never clear.
const withheldPairView = "public.projection_withheld_pair"

// DISTINCT because the view emits one row per pair per reason, and a pair failing two checks is still
// one withheld position.
const withheldPairQuery = `SELECT projection, count(DISTINCT pair) FROM public.projection_withheld_pair GROUP BY projection`

// RefusedByProjection reads the level the withholding alert watches: positions_refused from the latest
// run row of each projection runID wrote within the last `within`, plus the pairs that projection is
// currently withholding as unkeyable. created_at is set by the database clock, so the bound is taken
// from it too.
//
// The withheld count is added only to a projection that completed a run in this span. A projection
// absent from the run table stays absent, so "did not complete this tick" is still distinguishable
// from "completed, withholding nothing".
func (r *PositionMaterializerRepository) RefusedByProjection(ctx context.Context, runID int64, within time.Duration) (map[string]int64, error) {
	out, err := r.refusedByCompletedRun(ctx, runID, within)
	if err != nil {
		return nil, err
	}
	withheld, err := r.withheldPairsByProjection(ctx)
	if err != nil {
		return nil, err
	}
	for projection, pairs := range withheld {
		// Only onto a projection that completed a run: absence has to keep meaning "did not complete",
		// which is what ViewFailing and ViewNotCompleting read.
		if _, ran := out[projection]; ran {
			out[projection] += pairs
		}
	}
	return out, nil
}

// refusedByCompletedRun reads positions_refused from the latest run row of each projection runID wrote
// within the last `within`.
func (r *PositionMaterializerRepository) refusedByCompletedRun(ctx context.Context, runID int64, within time.Duration) (map[string]int64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (projection) projection, positions_refused
		  FROM position_projection_run
		 WHERE run_id = $1 AND created_at >= clock_timestamp() - make_interval(secs => $2)
		 ORDER BY projection, created_at DESC`, runID, within.Seconds())
	if err != nil {
		return nil, fmt.Errorf("reading positions_refused: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var projection string
		var refused int64
		if err := rows.Scan(&projection, &refused); err != nil {
			return nil, fmt.Errorf("scanning positions_refused: %w", err)
		}
		out[projection] = refused
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating positions_refused: %w", err)
	}
	return out, nil
}

// withheldPairsByProjection counts the pairs each projection currently withholds, or nothing at all
// when the view has not been migrated yet — the runner ships independently of the wrappers that define
// it, so its absence is a deployment order, not a fault.
func (r *PositionMaterializerRepository) withheldPairsByProjection(ctx context.Context) (map[string]int64, error) {
	var present bool
	if err := r.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, withheldPairView).Scan(&present); err != nil {
		return nil, fmt.Errorf("resolving %s: %w", withheldPairView, err)
	}
	if !present {
		return nil, nil
	}

	rows, err := r.pool.Query(ctx, withheldPairQuery)
	if err != nil {
		return nil, fmt.Errorf("reading withheld pairs: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var projection string
		var pairs int64
		if err := rows.Scan(&projection, &pairs); err != nil {
			return nil, fmt.Errorf("scanning withheld pairs: %w", err)
		}
		out[projection] = pairs
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating withheld pairs: %w", err)
	}
	return out, nil
}

// MissingMaterializers returns the configured names materializeOnce cannot call: it needs exactly one
// public function of that name, executable, returning one bigint, with no OUT parameters, p_build_id
// integer and p_run_id bigint, and a default for every other argument.
func (r *PositionMaterializerRepository) MissingMaterializers(ctx context.Context, materializers []string) ([]string, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT c.m
		  FROM unnest($1::text[]) WITH ORDINALITY AS c(m, ord)
		 WHERE (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		         WHERE n.nspname = 'public' AND p.proname = c.m AND p.prokind = 'f') <> 1
		    OR NOT EXISTS (
		       SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		        WHERE n.nspname = 'public' AND p.proname = c.m AND p.prokind = 'f'
		          AND has_function_privilege(p.oid, 'EXECUTE')
		          AND p.prorettype = 'bigint'::regtype AND NOT p.proretset AND p.proallargtypes IS NULL
		          AND EXISTS (SELECT 1 FROM unnest(p.proargnames, p.proargtypes::oid[]) a(name, typ)
		                       WHERE a.name = 'p_build_id' AND a.typ = 'integer'::regtype)
		          AND EXISTS (SELECT 1 FROM unnest(p.proargnames, p.proargtypes::oid[]) a(name, typ)
		                       WHERE a.name = 'p_run_id' AND a.typ = 'bigint'::regtype)
		          AND NOT EXISTS (SELECT 1 FROM unnest(p.proargnames[1:p.pronargs - p.pronargdefaults]) a(name)
		                           WHERE a.name NOT IN ('p_build_id', 'p_run_id')))
		 ORDER BY c.ord`, materializers)
	if err != nil {
		return nil, fmt.Errorf("looking up materializer functions: %w", err)
	}
	missing, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("reading missing materializer functions: %w", err)
	}
	return missing, nil
}

// positionStateCaches are the trigger-fed caches derived from position_state, named explicitly.
// TestPositionStateCaches_NamesEveryTriggerFedCache fails when a trigger on position_state writes a
// table missing here.
var positionStateCaches = []string{"position_current"}

// CacheRowEstimates reads each cache's estimated row count.
//
// approximate_row_count reads planner statistics, so watching a large table costs no scan, and it sums
// a hypertable's chunks, so the level survives the conversion the runbook prescribes (measured: 5,000
// before and after, where the root's reltuples fell to 0). A never-analyzed table reads 0.
func (r *PositionMaterializerRepository) CacheRowEstimates(ctx context.Context) (map[string]int64, error) {
	return r.rowEstimates(ctx, positionStateCaches)
}

// rowEstimates is CacheRowEstimates over an explicit table list. A name with no relation is absent
// from the result rather than an error, so one missing table cannot silence the level for the rest.
func (r *PositionMaterializerRepository) rowEstimates(ctx context.Context, tables []string) (map[string]int64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT c.relname, approximate_row_count(c.oid)
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = 'public' AND c.relname = ANY($1)`, tables)
	if err != nil {
		return nil, fmt.Errorf("reading cache row estimates: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var table string
		var estimate int64
		if err := rows.Scan(&table, &estimate); err != nil {
			return nil, fmt.Errorf("scanning cache row estimate: %w", err)
		}
		out[table] = estimate
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating cache row estimates: %w", err)
	}
	return out, nil
}

// materializeOnce is a single materialization attempt. The SELECT is its own
// transaction, honoring the one-projection-per-transaction contract documented on
// the shared function (per-view advisory xact lock). The function name is quoted as
// an identifier, so a misconfigured entry fails loudly as an unknown function and
// cannot be silently skipped or read as SQL.
// The arguments are named, not positional: the wrappers do not share an argument list
// (materialize_maple_loan takes a skew tolerance between the two provenance parameters), so a
// positional second argument would land on whatever that projection declares there.
func (r *PositionMaterializerRepository) materializeOnce(ctx context.Context, materializer string, buildID int, runID int64) (int64, error) {
	var changed int64
	q := fmt.Sprintf(`SELECT %s(p_build_id => $1, p_run_id => $2)`, pgx.Identifier{"public", materializer}.Sanitize())
	// buildregistry.RunID, not the bare int64: its Valuer maps 0 to NULL, because a zero would name a
	// writer_run row that does not exist and run_id carries no FK to catch it.
	if err := r.pool.QueryRow(ctx, q, buildID, buildregistry.RunID(runID)).Scan(&changed); err != nil {
		return 0, fmt.Errorf("running %s: %w", materializer, err)
	}
	return changed, nil
}
