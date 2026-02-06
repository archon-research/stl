package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// OnchainPriceRepository defines the interface for onchain oracle price data persistence.
type OnchainPriceRepository interface {
	// UpsertPrices inserts onchain price records in batches.
	UpsertPrices(ctx context.Context, prices []*entity.OnchainTokenPrice) error

	// GetOracle retrieves an oracle by its name.
	GetOracle(ctx context.Context, name string) (*entity.Oracle, error)

	// GetEnabledAssets retrieves all enabled assets for a given oracle.
	GetEnabledAssets(ctx context.Context, oracleID int64) ([]*entity.OracleAsset, error)

	// GetLatestPrices returns the most recent price per token for a given oracle.
	// Used for change detection: only store prices that differ from the previous block.
	GetLatestPrices(ctx context.Context, oracleID int64) (map[int64]float64, error)

	// GetLatestBlock returns the highest block number stored for a given oracle.
	// Used for resume support in backfill.
	GetLatestBlock(ctx context.Context, oracleID int64) (int64, error)

	// GetTokenAddresses returns a map of token_id → on-chain address for all enabled
	// oracle assets of the given oracle. Used to build the asset address list for oracle calls.
	GetTokenAddresses(ctx context.Context, oracleID int64) (map[int64][]byte, error)
}
