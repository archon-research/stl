package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PriceRepository implements outbound.PriceRepository.
var _ outbound.PriceRepository = (*PriceRepository)(nil)

// PriceRepository is a PostgreSQL implementation of the outbound.PriceRepository port.
type PriceRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewPriceRepository creates a new PostgreSQL Price repository.
// If batchSize is <= 0, a default batch size of 1000 is used.
func NewPriceRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*PriceRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = 1000 // Prices are simple records, can handle larger batches
	}
	return &PriceRepository{
		pool:      pool,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// GetSourceByName retrieves a price source by its name.
func (r *PriceRepository) GetSourceByName(ctx context.Context, name string) (*entity.PriceSource, error) {
	var ps entity.PriceSource
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, display_name, base_url, rate_limit_per_min,
		       supports_historical, enabled, created_at, updated_at
		FROM price_source
		WHERE name = $1
	`, name).Scan(
		&ps.ID, &ps.Name, &ps.DisplayName, &ps.BaseURL, &ps.RateLimitPerMin,
		&ps.SupportsHistorical, &ps.Enabled, &ps.CreatedAt, &ps.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("price source not found: %s", name)
	}
	if err != nil {
		return nil, fmt.Errorf("querying price source: %w", err)
	}
	return &ps, nil
}

// GetEnabledAssets retrieves all enabled assets for a given source.
func (r *PriceRepository) GetEnabledAssets(ctx context.Context, sourceID int64) ([]*entity.PriceAsset, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, source_id, source_asset_id, token_id, name, symbol, enabled, created_at, updated_at
		FROM price_asset
		WHERE source_id = $1 AND enabled = true
		ORDER BY id
	`, sourceID)
	if err != nil {
		return nil, fmt.Errorf("querying enabled assets: %w", err)
	}
	defer rows.Close()

	return scanPriceAssets(rows)
}

// GetAssetsBySourceAssetIDs retrieves assets by their source-specific IDs.
func (r *PriceRepository) GetAssetsBySourceAssetIDs(ctx context.Context, sourceID int64, sourceAssetIDs []string) ([]*entity.PriceAsset, error) {
	if len(sourceAssetIDs) == 0 {
		return nil, nil
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, source_id, source_asset_id, token_id, name, symbol, enabled, created_at, updated_at
		FROM price_asset
		WHERE source_id = $1 AND source_asset_id = ANY($2)
		ORDER BY id
	`, sourceID, sourceAssetIDs)
	if err != nil {
		return nil, fmt.Errorf("querying assets by source asset IDs: %w", err)
	}
	defer rows.Close()

	return scanPriceAssets(rows)
}

func scanPriceAssets(rows pgx.Rows) ([]*entity.PriceAsset, error) {
	var assets []*entity.PriceAsset
	for rows.Next() {
		var pa entity.PriceAsset
		if err := rows.Scan(
			&pa.ID, &pa.SourceID, &pa.SourceAssetID, &pa.TokenID, &pa.Name, &pa.Symbol, &pa.Enabled, &pa.CreatedAt, &pa.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scanning price asset: %w", err)
		}
		assets = append(assets, &pa)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating price assets: %w", err)
	}
	return assets, nil
}

// UpsertPrices inserts price records in batches.
// Uses ON CONFLICT to handle duplicates based on source, asset ID, and timestamp.
func (r *PriceRepository) UpsertPrices(ctx context.Context, prices []*entity.TokenPrice) error {
	if len(prices) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(prices); i += r.batchSize {
		end := i + r.batchSize
		if end > len(prices) {
			end = len(prices)
		}
		batch := prices[i:end]

		if err := r.upsertPriceBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (r *PriceRepository) upsertPriceBatch(ctx context.Context, tx pgx.Tx, prices []*entity.TokenPrice) error {
	if len(prices) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO token_price (token_id, source_id, timestamp, price_usd, market_cap_usd)
		VALUES `)

	args := make([]any, 0, len(prices)*5)
	for i, price := range prices {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 5
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5))

		args = append(args, price.TokenID, price.SourceID, price.Timestamp, price.PriceUSD, price.MarketCapUSD)
	}

	sb.WriteString(` ON CONFLICT (token_id, source_id, timestamp) DO NOTHING`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upserting price batch: %w", err)
	}
	return nil
}

// GetLatestPrice retrieves the most recent price for a given token.
func (r *PriceRepository) GetLatestPrice(ctx context.Context, tokenID int64) (*entity.TokenPrice, error) {
	var tp entity.TokenPrice
	err := r.pool.QueryRow(ctx, `
		SELECT token_id, source_id, timestamp, price_usd, market_cap_usd
		FROM token_price
		WHERE token_id = $1
		ORDER BY timestamp DESC
		LIMIT 1
	`, tokenID).Scan(
		&tp.TokenID, &tp.SourceID, &tp.Timestamp, &tp.PriceUSD, &tp.MarketCapUSD,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying latest price: %w", err)
	}
	return &tp, nil
}

// UpsertVolumes inserts volume records in batches.
func (r *PriceRepository) UpsertVolumes(ctx context.Context, volumes []*entity.TokenVolume) error {
	if len(volumes) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(volumes); i += r.batchSize {
		end := i + r.batchSize
		if end > len(volumes) {
			end = len(volumes)
		}
		batch := volumes[i:end]

		if err := r.upsertVolumeBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (r *PriceRepository) upsertVolumeBatch(ctx context.Context, tx pgx.Tx, volumes []*entity.TokenVolume) error {
	if len(volumes) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO token_volume (token_id, source_id, timestamp, volume_usd)
		VALUES `)

	args := make([]any, 0, len(volumes)*4)
	for i, vol := range volumes {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 4
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4))

		args = append(args, vol.TokenID, vol.SourceID, vol.Timestamp, vol.VolumeUSD)
	}

	sb.WriteString(` ON CONFLICT (token_id, source_id, timestamp) DO NOTHING`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upserting volume batch: %w", err)
	}
	return nil
}
