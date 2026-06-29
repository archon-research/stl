//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const orderbookSnapshotSchemaName = "test_cex_orderbook"

var orderbookSnapshotPool *pgxpool.Pool

func init() {
	registerTestFileSetup(orderbookSnapshotSchemaName, func() {
		orderbookSnapshotPool = testutil.SetupSchemaForMain(sharedDSN, orderbookSnapshotSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, orderbookSnapshotPool, orderbookSnapshotSchemaName)
	})
}

func truncateOrderbookSnapshots(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := orderbookSnapshotPool.Exec(ctx, `DELETE FROM cex_orderbook_snapshots`); err != nil {
		t.Fatalf("truncate cex_orderbook_snapshots: %v", err)
	}
}

func newOrderbookSnapshotRepo(t *testing.T) *OrderbookSnapshotRepository {
	t.Helper()
	repo, err := NewOrderbookSnapshotRepository(orderbookSnapshotPool, nil)
	if err != nil {
		t.Fatalf("NewOrderbookSnapshotRepository: %v", err)
	}
	return repo
}

// TestSaveOrderbookSnapshots round-trips a batch: it stores the exact decimal
// strings as JSONB tuples, persists a present event_time, and stores SQL NULL for
// a nil event_time.
func TestSaveOrderbookSnapshots(t *testing.T) {
	repo := newOrderbookSnapshotRepo(t)
	ctx := context.Background()
	truncateOrderbookSnapshots(t, ctx)

	venueTime := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	ingested := time.Date(2026, 6, 19, 12, 0, 1, 0, time.UTC)
	persisted := time.Date(2026, 6, 19, 12, 0, 5, 0, time.UTC)

	snapshots := []entity.OrderbookSnapshot{
		{
			Exchange:    "coinbase",
			Symbol:      "BTC-USD",
			EventTime:   &venueTime,
			IngestedAt:  ingested,
			PersistedAt: persisted,
			Bids:        []entity.PriceLevel{{Price: "65000.10", Size: "0.5"}, {Price: "65000.00", Size: "1.20000000"}},
			Asks:        []entity.PriceLevel{{Price: "65001.00", Size: "2"}},
		},
		{
			Exchange:    "coinbase",
			Symbol:      "ETH-USD",
			EventTime:   nil, // no venue time -> SQL NULL
			IngestedAt:  ingested,
			PersistedAt: persisted,
			Bids:        []entity.PriceLevel{{Price: "3500.5", Size: "10"}},
			Asks:        []entity.PriceLevel{}, // empty side -> "[]"
		},
	}

	if err := repo.Save(ctx, snapshots); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// BTC row: event_time present, exact JSONB tuples preserved.
	var (
		eventTime *time.Time
		bidsJSON  string
		asksJSON  string
	)
	err := orderbookSnapshotPool.QueryRow(ctx, `
		SELECT event_time, bids::text, asks::text
		FROM cex_orderbook_snapshots
		WHERE exchange = 'coinbase' AND symbol = 'BTC-USD'
	`).Scan(&eventTime, &bidsJSON, &asksJSON)
	if err != nil {
		t.Fatalf("query BTC row: %v", err)
	}
	if eventTime == nil || !eventTime.Equal(venueTime) {
		t.Errorf("BTC event_time = %v, want %v", eventTime, venueTime)
	}
	assertTuples(t, "BTC bids", bidsJSON, [][2]string{{"65000.10", "0.5"}, {"65000.00", "1.20000000"}})
	assertTuples(t, "BTC asks", asksJSON, [][2]string{{"65001.00", "2"}})

	// ETH row: event_time NULL, empty ask side stored as "[]" (never NULL).
	var ethEventTime *time.Time
	var ethAsks string
	err = orderbookSnapshotPool.QueryRow(ctx, `
		SELECT event_time, asks::text
		FROM cex_orderbook_snapshots
		WHERE exchange = 'coinbase' AND symbol = 'ETH-USD'
	`).Scan(&ethEventTime, &ethAsks)
	if err != nil {
		t.Fatalf("query ETH row: %v", err)
	}
	if ethEventTime != nil {
		t.Errorf("ETH event_time = %v, want NULL", ethEventTime)
	}
	assertTuples(t, "ETH asks", ethAsks, [][2]string{})
}

// assertTuples decodes a stored JSONB tuple array and compares it to want,
// tolerating Postgres's canonical whitespace in the rendered text.
func assertTuples(t *testing.T, label, jsonText string, want [][2]string) {
	t.Helper()
	var got [][2]string
	if err := json.Unmarshal([]byte(jsonText), &got); err != nil {
		t.Fatalf("%s: decoding %q: %v", label, jsonText, err)
	}
	if len(want) == 0 && len(got) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v, want %v", label, got, want)
	}
}

// TestSaveOrderbookSnapshotsAppendOnly verifies repeated saves of the same
// (exchange, symbol, persisted_at) append new rows rather than replacing.
func TestSaveOrderbookSnapshotsAppendOnly(t *testing.T) {
	repo := newOrderbookSnapshotRepo(t)
	ctx := context.Background()
	truncateOrderbookSnapshots(t, ctx)

	persisted := time.Date(2026, 6, 19, 12, 0, 5, 0, time.UTC)
	snap := entity.OrderbookSnapshot{
		Exchange:    "okx",
		Symbol:      "BTC-USDT",
		IngestedAt:  persisted,
		PersistedAt: persisted,
		Bids:        []entity.PriceLevel{{Price: "1", Size: "1"}},
		Asks:        []entity.PriceLevel{{Price: "2", Size: "1"}},
	}
	if err := repo.Save(ctx, []entity.OrderbookSnapshot{snap}); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if err := repo.Save(ctx, []entity.OrderbookSnapshot{snap}); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	var count int
	if err := orderbookSnapshotPool.QueryRow(ctx, `
		SELECT COUNT(*) FROM cex_orderbook_snapshots WHERE exchange = 'okx' AND symbol = 'BTC-USDT'
	`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("row count = %d, want 2 (append-only)", count)
	}
}

func TestSaveOrderbookSnapshotsEmptyIsNoop(t *testing.T) {
	repo := newOrderbookSnapshotRepo(t)
	if err := repo.Save(context.Background(), nil); err != nil {
		t.Fatalf("Save(nil): %v", err)
	}
}
