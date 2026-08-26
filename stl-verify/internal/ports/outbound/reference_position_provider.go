package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ReferencePositionRow is one of a prime's balance-sheet positions from an
// upstream feed, as decimal strings exactly as reported. Parsing belongs to the
// consumer so an encoding change surfaces as a parse failure rather than a
// silently wrong number.
type ReferencePositionRow struct {
	Star string

	Protocol string
	Network  string
	// Nil for a network the vendor vocabulary has no chain id for.
	ChainID *int64

	TokenSymbol string
	// Nil when the feed omits it.
	TokenName    *string
	TokenAddress string

	Assets string
	// Nil when the feed omits them, which is distinct from zero.
	AllocatedAssets *string
	IdleAssets      *string
}

// ReferencePositionProvider fetches per-prime balance-sheet positions from an
// upstream feed.
type ReferencePositionProvider interface {
	// FetchPositions returns every position the feed holds for each of `stars`.
	// The caller must pass only stars whose coverage is already established
	// (the feed answers an unknown star with 200 and an empty list, so an empty
	// result cannot be told apart from a prime that holds nothing); a passed
	// star returning zero rows therefore fails rather than reading as empty.
	FetchPositions(ctx context.Context, stars []string) ([]ReferencePositionRow, error)
}

// PrimeReferencePositionRepository persists per-prime balance-sheet positions.
type PrimeReferencePositionRepository interface {
	SaveReferencePositions(ctx context.Context, positions []entity.PrimeReferencePosition) error
}
