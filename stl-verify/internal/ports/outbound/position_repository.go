package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/jackc/pgx/v5"
)

// CollateralRecord represents a single collateral record for batch insertion.
type CollateralRecord struct {
	UserID            int64
	ProtocolID        int64
	TokenID           int64
	BlockNumber       int64
	BlockVersion      int
	Amount            string // decimal-adjusted amount string
	EventType         string
	TxHash            []byte
	CollateralEnabled bool
}

// PositionRepository defines the interface for user position data persistence.
// This aggregate includes borrower (debt) positions and collateral positions.
type PositionRepository interface {
	// UpsertBorrowers upserts borrower (debt) position records atomically.
	// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error

	// UpsertBorrowerCollateral upserts collateral position records atomically.
	// All records are inserted in a single transaction - if any batch fails, all changes are rolled back.
	// Conflict resolution: ON CONFLICT (user_id, protocol_id, token_id, block_number, block_version) DO UPDATE
	UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error

	// SaveBorrower saves a single borrower position record within an external transaction.
	SaveBorrower(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, eventType string, txHash []byte) error

	// SaveBorrowerCollateral saves a single collateral position record within an external transaction.
	SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, eventType string, txHash []byte, collateralEnabled bool) error

	// SaveBorrowerCollaterals saves multiple borrower collateral position records in batch.
	SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, records []CollateralRecord) error
}
