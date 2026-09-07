package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// TestBuildOrderbookSnapshotInsertParity guards the drift bug where the per-row
// placeholder count and the bound-arg count disagree: pgx would then reject every
// INSERT at runtime, and the indexer would silently persist nothing. This runs
// under `make test` (no DB), unlike the build-tagged integration test that is the
// only other thing exercising the SQL.
func TestBuildOrderbookSnapshotInsertParity(t *testing.T) {
	now := time.Now()
	snaps := []entity.OrderbookSnapshot{
		{
			Exchange: "coinbase", Symbol: "BTC-USD", IngestedAt: now, PersistedAt: now,
			Bids: []entity.PriceLevel{{Price: "1", Size: "2"}},
			Asks: []entity.PriceLevel{{Price: "3", Size: "4"}},
		},
		{Exchange: "coinbase", Symbol: "ETH-USD", IngestedAt: now, PersistedAt: now}, // empty sides
	}

	sql, args, err := buildOrderbookSnapshotInsert(snaps)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Every "$" in the SQL is one placeholder (JSONB content lives in args, not the
	// SQL), so the placeholder count must equal the arg count.
	if got, want := strings.Count(sql, "$"), len(args); got != want {
		t.Fatalf("placeholders=%d, args=%d — mismatch makes every INSERT fail at runtime", got, want)
	}
	if want := len(snaps) * len(orderbookSnapshotColumns); len(args) != want {
		t.Fatalf("args=%d, want %d (%d rows x %d cols)", len(args), want, len(snaps), len(orderbookSnapshotColumns))
	}
}
