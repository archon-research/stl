package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PriceRepository defines the interface for price data persistence.
type PriceRepository interface {
	// Source operations
	GetSourceByName(ctx context.Context, name string) (*entity.PriceSource, error)

	// Asset operations
	GetEnabledAssets(ctx context.Context, sourceID int64) ([]*entity.PriceAsset, error)
	GetAssetsBySourceAssetIDs(ctx context.Context, sourceID int64, sourceAssetIDs []string) ([]*entity.PriceAsset, error)

	// Price operations
	UpsertPrices(ctx context.Context, prices []*entity.TokenPrice) error
	GetLatestPrice(ctx context.Context, tokenID int64) (*entity.TokenPrice, error)
}
