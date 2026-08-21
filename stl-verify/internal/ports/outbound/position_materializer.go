package outbound

import "context"

// PositionMaterializer runs one position projection through the shared
// materializer function (materialize_position_projection, VEC-402).
//
// Each per-protocol projection view (position_morpho_market, position_morpho_vault,
// position_sky_prime_debt, ...) holds its own projection logic and emits the
// position_state column contract; the shared database function validates the
// contract and appends the observations to position_state. It writes no
// classification. All correctness logic (contract checks, double-emit and
// cross-view ownership guards, advisory locking) lives in that function; this port
// just invokes it for one view.
type PositionMaterializer interface {
	// Materialize runs the shared materializer for one projection view and returns
	// the number of position_state rows appended. buildID is stamped on every row as
	// the ADR-0002 code-provenance record (build_registry.id; 0 means pre-tracking).
	// The call is a single statement, so it is one transaction —
	// callers must materialize AT MOST ONE view per transaction (the function's
	// per-view advisory lock is held to commit, and two callers locking different
	// views in different orders would deadlock).
	Materialize(ctx context.Context, view string, buildID int) (int64, error)
}
