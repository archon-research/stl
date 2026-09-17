package outbound

import (
	"context"
	"time"
)

// PositionMaterializer runs one per-projection materializer function
// (materialize_morpho_market, materialize_sky_prime_debt, materialize_aave_lending, ...).
//
// Each wrapper owns its projection's pre-flight checks and delegates to the shared
// materialize_position_projection (VEC-402), which validates the position_state column
// contract and appends the observations. Calling the wrapper, not the shared function by
// view, is what makes a projection's own refusals run under the scheduler.
type PositionMaterializer interface {
	// Materialize calls one materializer function with buildID and runID, and returns the
	// number of position_state rows appended. Both are stamped on every row: buildID is the
	// ADR-0002 code-provenance record (build_registry.id; 0 means pre-tracking) and runID the
	// ADR-0006 §2 writer run (writer_run.id). The call is a single statement, so it is one
	// transaction: callers must materialize AT MOST ONE projection per transaction (the shared
	// function's per-view advisory lock is held to commit, and two callers locking different
	// views in different orders would deadlock).
	Materialize(ctx context.Context, materializer string, buildID int, runID int64) (int64, error)

	// RefusedByProjection returns positions_refused from the latest run of each projection that runID
	// completed within the last `within`, measured on the database clock. A projection that withholds
	// positions still reports success, so this count is what shows it. A projection with no run in that
	// span is absent: retired, run by hand, or failed this tick.
	RefusedByProjection(ctx context.Context, runID int64, within time.Duration) (map[string]int64, error)

	// MissingMaterializers returns the names in materializers that Materialize could not call: absent,
	// ambiguous, not returning one bigint, or not accepting a build and a writer run.
	MissingMaterializers(ctx context.Context, materializers []string) ([]string, error)

	// CacheRowEstimates returns the estimated row count of each trigger-fed cache derived from
	// position_state, keyed by table name. db/migrations/AGENTS.md requires a row-growth tripwire
	// on every plain table, and these caches are written by database triggers, so no process can
	// count what they persisted — the level is the only signal available. Estimates, from the
	// planner's statistics rather than a count, so reading them cannot cost a scan of a table the
	// caller is watching precisely because it may be large.
	CacheRowEstimates(ctx context.Context) (map[string]int64, error)
}
