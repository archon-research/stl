package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ProtocolRepository defines the interface for protocol-related data persistence.
// This aggregate includes chains, protocols, and protocol-specific reserve data.
// Note: Protocols are pre-seeded via database migrations and are read-only at runtime.
type ProtocolRepository interface {
	// UpsertSparkLendReserveData upserts SparkLend reserve data records.
	// This stores protocol-level market data (rates, indexes, totals) per token per block.
	// Conflict resolution: ON CONFLICT (protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertSparkLendReserveData(ctx context.Context, data []*entity.SparkLendReserveData) error

	// GetProtocolByAddress retrieves a protocol by its chain ID and address.
	GetProtocolByAddress(ctx context.Context, chainID int64, address string) (*entity.Protocol, error)
}
