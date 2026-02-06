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

// Compile-time check that OnchainPriceRepository implements outbound.OnchainPriceRepository.
var _ outbound.OnchainPriceRepository = (*OnchainPriceRepository)(nil)

// OnchainPriceRepository is a PostgreSQL implementation of the outbound.OnchainPriceRepository port.
type OnchainPriceRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewOnchainPriceRepository creates a new PostgreSQL onchain price repository.
// If batchSize is <= 0, a default batch size of 1000 is used.
func NewOnchainPriceRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*OnchainPriceRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &OnchainPriceRepository{
		pool:      pool,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

// GetOracleSource retrieves an oracle source by its name.
func (r *OnchainPriceRepository) GetOracleSource(ctx context.Context, name string) (*entity.OracleSource, error) {
	var os entity.OracleSource
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, display_name, chain_id, pool_address_provider,
		       deployment_block, enabled, created_at, updated_at
		FROM oracle_source
		WHERE name = $1
	`, name).Scan(
		&os.ID, &os.Name, &os.DisplayName, &os.ChainID, &os.PoolAddressProvider,
		&os.DeploymentBlock, &os.Enabled, &os.CreatedAt, &os.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("oracle source not found: %s", name)
	}
	if err != nil {
		return nil, fmt.Errorf("querying oracle source: %w", err)
	}
	return &os, nil
}

// GetEnabledAssets retrieves all enabled assets for a given oracle source.
func (r *OnchainPriceRepository) GetEnabledAssets(ctx context.Context, oracleSourceID int64) ([]*entity.OracleAsset, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, oracle_source_id, token_id, enabled, created_at
		FROM oracle_asset
		WHERE oracle_source_id = $1 AND enabled = true
		ORDER BY id
	`, oracleSourceID)
	if err != nil {
		return nil, fmt.Errorf("querying enabled oracle assets: %w", err)
	}
	defer rows.Close()

	var assets []*entity.OracleAsset
	for rows.Next() {
		var oa entity.OracleAsset
		if err := rows.Scan(&oa.ID, &oa.OracleSourceID, &oa.TokenID, &oa.Enabled, &oa.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning oracle asset: %w", err)
		}
		assets = append(assets, &oa)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating oracle assets: %w", err)
	}
	return assets, nil
}

// GetLatestPrices returns the most recent price per token for a given oracle source.
// Used for change detection: only store prices that differ from the previous block.
func (r *OnchainPriceRepository) GetLatestPrices(ctx context.Context, oracleSourceID int64) (map[int64]float64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (token_id) token_id, price_usd
		FROM onchain_token_price
		WHERE oracle_source_id = $1
		ORDER BY token_id, block_number DESC, block_version DESC
	`, oracleSourceID)
	if err != nil {
		return nil, fmt.Errorf("querying latest onchain prices: %w", err)
	}
	defer rows.Close()

	prices := make(map[int64]float64)
	for rows.Next() {
		var tokenID int64
		var priceUSD float64
		if err := rows.Scan(&tokenID, &priceUSD); err != nil {
			return nil, fmt.Errorf("scanning latest price: %w", err)
		}
		prices[tokenID] = priceUSD
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating latest prices: %w", err)
	}
	return prices, nil
}

// GetLatestBlock returns the highest block number stored for a given oracle source.
// Returns 0 if no blocks have been stored yet.
func (r *OnchainPriceRepository) GetLatestBlock(ctx context.Context, oracleSourceID int64) (int64, error) {
	var blockNumber *int64
	err := r.pool.QueryRow(ctx, `
		SELECT MAX(block_number)
		FROM onchain_token_price
		WHERE oracle_source_id = $1
	`, oracleSourceID).Scan(&blockNumber)
	if err != nil {
		return 0, fmt.Errorf("querying latest block: %w", err)
	}
	if blockNumber == nil {
		return 0, nil
	}
	return *blockNumber, nil
}

// GetTokenAddresses returns a map of token_id → on-chain address for enabled oracle assets.
func (r *OnchainPriceRepository) GetTokenAddresses(ctx context.Context, oracleSourceID int64) (map[int64][]byte, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT oa.token_id, t.address
		FROM oracle_asset oa
		JOIN token t ON t.id = oa.token_id
		WHERE oa.oracle_source_id = $1 AND oa.enabled = true
		ORDER BY oa.id
	`, oracleSourceID)
	if err != nil {
		return nil, fmt.Errorf("querying token addresses: %w", err)
	}
	defer rows.Close()

	addrs := make(map[int64][]byte)
	for rows.Next() {
		var tokenID int64
		var address []byte
		if err := rows.Scan(&tokenID, &address); err != nil {
			return nil, fmt.Errorf("scanning token address: %w", err)
		}
		addrs[tokenID] = address
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating token addresses: %w", err)
	}
	return addrs, nil
}

// UpsertPrices inserts onchain price records in batches.
// Uses ON CONFLICT DO NOTHING to handle duplicates.
func (r *OnchainPriceRepository) UpsertPrices(ctx context.Context, prices []*entity.OnchainTokenPrice) error {
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

func (r *OnchainPriceRepository) upsertPriceBatch(ctx context.Context, tx pgx.Tx, prices []*entity.OnchainTokenPrice) error {
	if len(prices) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO onchain_token_price (token_id, oracle_source_id, block_number, block_version, timestamp, oracle_address, price_usd)
		VALUES `)

	args := make([]any, 0, len(prices)*7)
	for i, price := range prices {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 7
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7))

		args = append(args, price.TokenID, price.OracleSourceID, price.BlockNumber, price.BlockVersion, price.Timestamp, price.OracleAddress, price.PriceUSD)
	}

	sb.WriteString(` ON CONFLICT (token_id, oracle_source_id, block_number, block_version, timestamp) DO NOTHING`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upserting onchain price batch: %w", err)
	}
	return nil
}
