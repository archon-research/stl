package outbound

import "context"

// PositionMaterializer runs one per-projection materializer function
// (materialize_morpho_market, materialize_sky_prime_debt, materialize_aave_lending, ...).
//
// Each wrapper owns its projection's pre-flight checks and delegates to the shared
// materialize_position_projection (VEC-402), which validates the position_state column
// contract and appends the observations. Calling the wrapper, not the shared function by
// view, is what makes a projection's own refusals run under the scheduler.
type PositionMaterializer interface {
	// Materialize calls one materializer function with buildID and returns the number of
	// position_state rows appended. buildID is stamped on every row as the ADR-0002
	// code-provenance record (build_registry.id; 0 means pre-tracking). The call is a
	// single statement, so it is one transaction: callers must materialize AT MOST ONE
	// projection per transaction (the shared function's per-view advisory lock is held to
	// commit, and two callers locking different views in different orders would deadlock).
	Materialize(ctx context.Context, materializer string, buildID int) (int64, error)
}
