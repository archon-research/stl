package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// OnchainPriceRepository defines the interface for onchain oracle price data persistence.
type OnchainPriceRepository interface {
	// UpsertPrices inserts onchain price records in batches.
	UpsertPrices(ctx context.Context, prices []*entity.OnchainTokenPrice) error

	// GetOracleSource retrieves an oracle source by its name.
	GetOracleSource(ctx context.Context, name string) (*entity.OracleSource, error)

	// GetEnabledAssets retrieves all enabled assets for a given oracle source.
	GetEnabledAssets(ctx context.Context, oracleSourceID int64) ([]*entity.OracleAsset, error)

	// GetLatestPrices returns the most recent price per token for a given oracle source.
	// Used for change detection: only store prices that differ from the previous block.
	GetLatestPrices(ctx context.Context, oracleSourceID int64) (map[int64]float64, error)

	// GetLatestBlock returns the highest block number stored for a given oracle source.
	// Used for resume support in backfill.
	GetLatestBlock(ctx context.Context, oracleSourceID int64) (int64, error)

	// GetTokenAddresses returns a map of token_id → on-chain address for all enabled
	// oracle assets of the given source. Used to build the asset address list for oracle calls.
	GetTokenAddresses(ctx context.Context, oracleSourceID int64) (map[int64][]byte, error)
}
