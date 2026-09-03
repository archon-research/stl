package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PriceRepository implements outbound.PriceRepository.
var _ outbound.PriceRepository = (*PriceRepository)(nil)

// PriceRepository is a PostgreSQL implementation of the outbound.PriceRepository port.
type PriceRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	buildID   buildregistry.BuildID
	batchSize int
}

// NewPriceRepository creates a new PostgreSQL Price repository.
// If batchSize is <= 0, a default batch size of 1000 is used.
func NewPriceRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID, batchSize int) (*PriceRepository, error) {
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
		buildID:   buildID,
		batchSize: batchSize,
	}, nil
}

// GetSourceByName retrieves a price source by its name.
func (r *PriceRepository) GetSourceByName(ctx context.Context, name string) (*entity.PriceSource, error) {
	var ps entity.PriceSource
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, display_name, base_url, rate_limit_per_min,
		       supports_historical, enabled, created_at, updated_at
		FROM offchain_price_source
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
		SELECT id, source_id, source_asset_id, token_id, offchain_only, name, symbol, enabled, created_at, updated_at
		FROM offchain_price_asset
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
		SELECT id, source_id, source_asset_id, token_id, offchain_only, name, symbol, enabled, created_at, updated_at
		FROM offchain_price_asset
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
			&pa.ID, &pa.SourceID, &pa.SourceAssetID, &pa.TokenID, &pa.OffchainOnly, &pa.Name, &pa.Symbol, &pa.Enabled, &pa.CreatedAt, &pa.UpdatedAt,
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

	// Sort by natural key once, before chunking, so the per-row advisory lock
	// in assign_processing_version_offchain_token_price is acquired in a
	// transaction-stable order across concurrent callers — including across
	// chunks within the same transaction. See ADR-0002 §3.
	slices.SortFunc(prices, func(a, b *entity.TokenPrice) int {
		return cmp.Or(
			cmp.Compare(a.TokenID, b.TokenID),
			cmp.Compare(a.SourceID, b.SourceID),
			a.Timestamp.Compare(b.Timestamp),
		)
	})
	return upsertInBatches(ctx, r, prices, r.upsertPriceBatch)
}

// UpsertAssetPrices inserts asset-keyed price records (offchain_asset_price) in
// batches — the store for assets with no token row, where UpsertPrices cannot
// write. Same idempotency contract: ON CONFLICT DO NOTHING on the primary key,
// with the build-aware version rule deciding processing_version.
func (r *PriceRepository) UpsertAssetPrices(ctx context.Context, prices []*entity.AssetPrice) error {
	// Sorted for the same reason as UpsertPrices: the per-row advisory lock in
	// next_processing_version_offchain_asset_price must be acquired in a
	// transaction-stable order across concurrent callers (ADR-0002 §3).
	slices.SortFunc(prices, func(a, b *entity.AssetPrice) int {
		return cmp.Or(
			cmp.Compare(a.AssetID, b.AssetID),
			cmp.Compare(a.SourceID, b.SourceID),
			a.Timestamp.Compare(b.Timestamp),
		)
	})

	// Two rows sharing the natural key inside one statement would race the
	// version rule against itself and ON CONFLICT would drop the second row
	// silently. Duplicates are adjacent after the sort: collapse agreeing ones,
	// refuse disagreeing ones rather than silently keep one observation.
	deduped := prices[:0]
	for _, p := range prices {
		if len(deduped) > 0 {
			last := deduped[len(deduped)-1]
			if last.AssetID == p.AssetID && last.SourceID == p.SourceID && last.Timestamp.Equal(p.Timestamp) {
				if last.PriceUSD != p.PriceUSD {
					return fmt.Errorf("conflicting prices for asset %d at %s in one batch: %v vs %v",
						p.AssetID, p.Timestamp.Format("2006-01-02T15:04:05Z07:00"), last.PriceUSD, p.PriceUSD)
				}
				continue
			}
		}
		deduped = append(deduped, p)
	}
	return upsertInBatches(ctx, r, deduped, r.upsertAssetPriceBatch)
}

// upsertInBatches runs batchFn over fixed-size slices of rows inside one
// transaction, so a mid-run failure rolls the whole upsert back.
func upsertInBatches[T any](ctx context.Context, r *PriceRepository, rows []T, batchFn func(context.Context, pgx.Tx, []T) error) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(rows); i += r.batchSize {
		end := min(i+r.batchSize, len(rows))
		if err := batchFn(ctx, tx, rows[i:end]); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (r *PriceRepository) upsertAssetPriceBatch(ctx context.Context, tx pgx.Tx, prices []*entity.AssetPrice) error {
	if len(prices) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO offchain_asset_price (asset_id, source_id, timestamp, price_usd, market_cap_usd, volume_usd, processing_version, build_id)
		VALUES `)

	args := make([]any, 0, len(prices)*7)
	for i, price := range prices {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 7
		// The INSERT, not the trigger, decides processing_version: on a
		// columnstored chunk the ON CONFLICT arbiter resolves before row
		// triggers fire, and a trigger-assigned version reaches it as DEFAULT 0,
		// silently discarding corrections (ADR-0002 §3; see the migration).
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, next_processing_version_offchain_asset_price($%d, $%d, $%d, $%d), $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6,
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+7, baseIdx+7))

		args = append(args, price.AssetID, price.SourceID, price.Timestamp, price.PriceUSD, price.MarketCapUSD, price.VolumeUSD, int(r.buildID))
	}

	sb.WriteString(` ON CONFLICT (asset_id, source_id, processing_version, timestamp) DO NOTHING`)

	if _, err := tx.Exec(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("upserting asset price batch: %w", err)
	}
	return nil
}

func (r *PriceRepository) upsertPriceBatch(ctx context.Context, tx pgx.Tx, prices []*entity.TokenPrice) error {
	if len(prices) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd, market_cap_usd, volume_usd, build_id)
		VALUES `)

	args := make([]any, 0, len(prices)*7)
	for i, price := range prices {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 7
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7))

		args = append(args, price.TokenID, price.SourceID, price.Timestamp, price.PriceUSD, price.MarketCapUSD, price.VolumeUSD, int(r.buildID))
	}

	sb.WriteString(` ON CONFLICT (token_id, source_id, processing_version, timestamp) DO NOTHING`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upserting price batch: %w", err)
	}
	return nil
}
