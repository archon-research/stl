package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// ProtocolRepository defines the interface for protocol-related data persistence.
// This aggregate includes chains, protocols, and protocol-specific reserve data.
// Note: Protocols are pre-seeded via database migrations and are read-only at runtime.
type ProtocolRepository interface {
	// UpsertReserveData upserts SparkLend/aave reserve data records.
	// This stores protocol-level market data (rates, indexes, totals) per token per block.
	// Conflict resolution: ON CONFLICT (protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertReserveData(ctx context.Context, tx pgx.Tx, data []*entity.SparkLendReserveData) error
	// GetProtocolByAddress retrieves a protocol by its chain ID and address.
	GetProtocolByAddress(ctx context.Context, chainID int64, address common.Address) (*entity.Protocol, error)
}
