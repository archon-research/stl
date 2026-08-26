package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"

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
	// The feed answers an unknown star with 200 and an empty list, so a passed
	// star returning zero rows fails the fetch rather than reading as empty.
	// Callers pass stars the Star risk monitor covers — a different feed — so a
	// prime the monitor covers but this feed does not carry will fail every
	// cycle loudly until someone decides otherwise (see the PositionsZero
	// runbook).
	FetchPositions(ctx context.Context, stars []string) ([]ReferencePositionRow, error)
}

// PrimeReferencePositionRepository persists per-prime balance-sheet positions.
type PrimeReferencePositionRepository interface {
	// SaveReferencePositions writes within the caller's transaction, so the
	// caller controls what else commits or rolls back with it.
	SaveReferencePositions(ctx context.Context, tx pgx.Tx, positions []entity.PrimeReferencePosition) error
}
