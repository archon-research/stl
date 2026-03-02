package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ProtocolAssetRepository defines the interface for protocol asset lookups.
// Protocol assets are pre-seeded rows that bridge a protocol to the underlying
// token it uses. Assets are keyed by (protocol_id, asset_key) where:
//   - EVM assets: asset_key is the lowercase hex contract address
//   - Non-EVM / symbol-only assets (e.g. Maple BTC): asset_key is the symbol
type ProtocolAssetRepository interface {
	// GetByKey returns the protocol asset for the given protocol and asset key.
	// Returns an error if not found.
	GetByKey(ctx context.Context, protocolID int64, assetKey string) (*entity.ProtocolAsset, error)
}
