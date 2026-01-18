package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ProtocolRepository defines the interface for protocol-related data persistence.
// This aggregate includes chains, protocols, and protocol-specific reserve data.
type ProtocolRepository interface {
	// UpsertChains upserts chain (network) records.
	// Conflict resolution: ON CONFLICT (chain_id) DO UPDATE
	UpsertChains(ctx context.Context, chains []*entity.Chain) error

	// UpsertProtocols upserts protocol records.
	// Conflict resolution: ON CONFLICT (chain_id, address) DO UPDATE
	UpsertProtocols(ctx context.Context, protocols []*entity.Protocol) error

	// UpsertSparkLendReserveData upserts SparkLend reserve data records.
	// This stores protocol-level market data (rates, indexes, totals) per token per block.
	// Conflict resolution: ON CONFLICT (protocol_id, token_id, block_number) DO UPDATE
	UpsertSparkLendReserveData(ctx context.Context, data []*entity.SparkLendReserveData) error
}
