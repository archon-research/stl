package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that OrderbookSnapshotRepository implements the port.
var _ outbound.OrderbookSnapshotRepository = (*OrderbookSnapshotRepository)(nil)

// orderbookSnapshotColumns are the inserted columns in bind order. The
// per-row placeholder count is derived from this slice (len), so the VALUES
// tuples and the bound args cannot drift apart.
var orderbookSnapshotColumns = []string{
	"exchange", "symbol", "event_time", "ingested_at", "persisted_at", "bids", "asks",
}

// OrderbookSnapshotRepository is the PostgreSQL/TimescaleDB implementation of the
// OrderbookSnapshotRepository port. It writes one row per snapshot into the
// cex_orderbook_snapshots hypertable, append-only (INSERT only, never
// UPDATE/DELETE).
type OrderbookSnapshotRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewOrderbookSnapshotRepository(pool *pgxpool.Pool, logger *slog.Logger) (*OrderbookSnapshotRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OrderbookSnapshotRepository{pool: pool, logger: logger}, nil
}

// Save writes every snapshot as one row in a single multi-row INSERT, which is
// atomic on its own. A tick produces at most one row per symbol, so the row
// count is tiny and needs no chunking.
func (r *OrderbookSnapshotRepository) Save(ctx context.Context, snapshots []entity.OrderbookSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}
	sql, args, err := buildOrderbookSnapshotInsert(snapshots)
	if err != nil {
		return err
	}
	if _, err := r.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("inserting order book snapshots: %w", err)
	}
	return nil
}

// buildOrderbookSnapshotInsert assembles the multi-row INSERT and its bound args.
// event_time is a nullable *time.Time so a snapshot with no venue time stores SQL
// NULL; bids/asks are JSONB ["price","size"] tuple arrays. It is split out from
// Save so the placeholder-vs-arg count is unit-testable without a database.
func buildOrderbookSnapshotInsert(snapshots []entity.OrderbookSnapshot) (string, []any, error) {
	cols := len(orderbookSnapshotColumns)
	var sb strings.Builder
	sb.WriteString("INSERT INTO cex_orderbook_snapshots (")
	sb.WriteString(strings.Join(orderbookSnapshotColumns, ", "))
	sb.WriteString(") VALUES ")

	args := make([]any, 0, len(snapshots)*cols)
	for i, snap := range snapshots {
		writeValuesPlaceholders(&sb, i, cols)

		bids, err := encodeLevels(snap.Bids)
		if err != nil {
			return "", nil, fmt.Errorf("encoding bids for %s %s: %w", snap.Exchange, snap.Symbol, err)
		}
		asks, err := encodeLevels(snap.Asks)
		if err != nil {
			return "", nil, fmt.Errorf("encoding asks for %s %s: %w", snap.Exchange, snap.Symbol, err)
		}
		// Order must match orderbookSnapshotColumns.
		args = append(args, snap.Exchange, snap.Symbol, snap.EventTime, snap.IngestedAt, snap.PersistedAt, bids, asks)
	}
	return sb.String(), args, nil
}

// encodeLevels marshals one side to the JSONB shape stored in the column: an array
// of ["price","size"] string tuples, preserving the exact exchange decimal text.
// A non-nil empty slice encodes as "[]" (an empty book side), never SQL NULL.
func encodeLevels(levels []entity.PriceLevel) ([]byte, error) {
	tuples := make([][2]string, len(levels))
	for i, lvl := range levels {
		tuples[i] = [2]string{lvl.Price, lvl.Size}
	}
	return json.Marshal(tuples)
}
