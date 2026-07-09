package outbound

import "context"

// StateReader issues block-pinned contract reads. It is the single home of the
// hash-vs-number dispatch that VEC-471 threaded by hand through every indexer:
// which block a read observes, and how it is pinned, travel inside the
// BlockPin, so no call site re-decides them and no archive can key at block 0.
// Implementations verify len(results) == len(calls) and return results
// index-aligned with calls, so callers skip the count check. Per-call Success
// gating (AllowFailure policy) stays with the caller.
type StateReader interface {
	Read(ctx context.Context, pin BlockPin, calls []Call) ([]Result, error)
}
