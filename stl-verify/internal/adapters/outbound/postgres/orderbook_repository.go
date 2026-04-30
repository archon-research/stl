package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.OrderbookRepository = (*OrderbookRepository)(nil)

type OrderbookRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewOrderbookRepository(pool *pgxpool.Pool, logger *slog.Logger) *OrderbookRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &OrderbookRepository{pool: pool, logger: logger.With("component", "orderbook-repo")}
}

func (r *OrderbookRepository) SaveSnapshots(ctx context.Context, snapshots []*entity.OrderbookSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, snap := range snapshots {
		bidJSON, err := json.Marshal(snap.Bids)
		if err != nil {
			return fmt.Errorf("marshal bids: %w", err)
		}
		askJSON, err := json.Marshal(snap.Asks)
		if err != nil {
			return fmt.Errorf("marshal asks: %w", err)
		}
		batch.Queue(
			`INSERT INTO cex_orderbook_snapshots (exchange, symbol, captured_at, bid_data, ask_data, bid_levels, ask_levels, latency_ms)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			snap.Exchange, snap.Symbol, snap.CapturedAt,
			bidJSON, askJSON, len(snap.Bids), len(snap.Asks), snap.LatencyMs,
		)
	}
	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range snapshots {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("exec batch: %w", err)
		}
	}
	return nil
}

func (r *OrderbookRepository) GetLatestSnapshot(ctx context.Context, exchange, symbol string) (*entity.OrderbookSnapshot, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT exchange, symbol, captured_at, bid_data, ask_data, latency_ms
		 FROM cex_orderbook_snapshots
		 WHERE exchange = $1 AND symbol = $2
		 ORDER BY captured_at DESC LIMIT 1`, exchange, symbol)
	return scanSnapshot(row)
}

func (r *OrderbookRepository) GetLatestSnapshotsForSymbol(ctx context.Context, symbol string) ([]*entity.OrderbookSnapshot, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT ON (exchange) exchange, symbol, captured_at, bid_data, ask_data, latency_ms
		 FROM cex_orderbook_snapshots
		 WHERE symbol = $1
		 ORDER BY exchange, captured_at DESC`, symbol)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	return collectSnapshots(rows)
}

func (r *OrderbookRepository) GetSnapshotsInRange(ctx context.Context, symbol string, from, to time.Time) ([]*entity.OrderbookSnapshot, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT exchange, symbol, captured_at, bid_data, ask_data, latency_ms
		 FROM cex_orderbook_snapshots
		 WHERE symbol = $1 AND captured_at BETWEEN $2 AND $3
		 ORDER BY captured_at DESC`, symbol, from, to)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	return collectSnapshots(rows)
}

func scanSnapshot(row pgx.Row) (*entity.OrderbookSnapshot, error) {
	var snap entity.OrderbookSnapshot
	var bidJSON, askJSON []byte
	err := row.Scan(&snap.Exchange, &snap.Symbol, &snap.CapturedAt, &bidJSON, &askJSON, &snap.LatencyMs)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	if err := json.Unmarshal(bidJSON, &snap.Bids); err != nil {
		return nil, fmt.Errorf("unmarshal bids: %w", err)
	}
	if err := json.Unmarshal(askJSON, &snap.Asks); err != nil {
		return nil, fmt.Errorf("unmarshal asks: %w", err)
	}
	return &snap, nil
}

func collectSnapshots(rows pgx.Rows) ([]*entity.OrderbookSnapshot, error) {
	var result []*entity.OrderbookSnapshot
	for rows.Next() {
		var snap entity.OrderbookSnapshot
		var bidJSON, askJSON []byte
		err := rows.Scan(&snap.Exchange, &snap.Symbol, &snap.CapturedAt, &bidJSON, &askJSON, &snap.LatencyMs)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if err := json.Unmarshal(bidJSON, &snap.Bids); err != nil {
			return nil, fmt.Errorf("unmarshal bids: %w", err)
		}
		if err := json.Unmarshal(askJSON, &snap.Asks); err != nil {
			return nil, fmt.Errorf("unmarshal asks: %w", err)
		}
		result = append(result, &snap)
	}
	return result, rows.Err()
}
