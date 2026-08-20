package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// BalanceSheetDay is one prime's balance sheet on one day, as decimal strings
// exactly as reported. Parsing belongs to the consumer so an encoding change
// surfaces as a parse failure rather than a silently wrong number.
type BalanceSheetDay struct {
	Star string
	// Date as reported, YYYY-MM-DD. Converted to a timestamp by the consumer,
	// which owns the convention that a day is stamped at its midnight UTC.
	Date string

	TreasuryBalance string
	Assets          string
	AllocatedAssets string
	IdleAssets      string
	Debt            string
	BackstopCapital string
}

// BalanceSheetProvider fetches per-prime daily balance sheets from an upstream
// feed.
type BalanceSheetProvider interface {
	// FetchHistory returns every day the feed holds within `daysAgo` for each of
	// `stars`. A star the feed does not cover is simply absent from the result.
	FetchHistory(ctx context.Context, stars []string, daysAgo int) ([]BalanceSheetDay, error)
}

// PrimeBalanceSheetRepository persists per-prime daily balance sheets.
type PrimeBalanceSheetRepository interface {
	SaveBalanceSheetSnapshots(ctx context.Context, snapshots []entity.PrimeBalanceSheetSnapshot) error
}
