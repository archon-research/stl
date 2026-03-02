package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that ProtocolAssetRepository implements outbound.ProtocolAssetRepository
var _ outbound.ProtocolAssetRepository = (*ProtocolAssetRepository)(nil)

// ProtocolAssetRepository is a PostgreSQL implementation of the outbound.ProtocolAssetRepository port.
type ProtocolAssetRepository struct {
	pool *pgxpool.Pool
}

// NewProtocolAssetRepository creates a new PostgreSQL ProtocolAsset repository.
func NewProtocolAssetRepository(pool *pgxpool.Pool) (*ProtocolAssetRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	return &ProtocolAssetRepository{pool: pool}, nil
}

// GetByKey returns the protocol asset for the given protocol and asset key.
// The asset_key is a lowercase hex contract address for EVM assets, or the
// symbol string (e.g. "BTC") for non-EVM / symbol-only assets.
func (r *ProtocolAssetRepository) GetByKey(ctx context.Context, protocolID int64, assetKey string) (*entity.ProtocolAsset, error) {
	var pa entity.ProtocolAsset
	err := r.pool.QueryRow(ctx,
		`SELECT id, protocol_id, asset_key, symbol, decimals, chain_id, address, token_id
		 FROM protocol_asset
		 WHERE protocol_id = $1 AND asset_key = $2`,
		protocolID, assetKey,
	).Scan(&pa.ID, &pa.ProtocolID, &pa.AssetKey, &pa.Symbol, &pa.Decimals, &pa.ChainID, &pa.Address, &pa.TokenID)
	if err != nil {
		return nil, fmt.Errorf("protocol asset not found for protocol_id=%d asset_key=%q: %w", protocolID, assetKey, err)
	}
	return &pa, nil
}
