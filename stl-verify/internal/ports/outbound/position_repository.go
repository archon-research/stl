package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// CollateralRecord represents a single collateral record for batch insertion.
type CollateralRecord struct {
	UserID            int64
	ProtocolID        int64
	TokenID           int64
	BlockNumber       int64
	BlockVersion      int
	Amount            string // decimal-adjusted full current collateral balance
	Change            string // decimal-adjusted event delta
	EventType         string
	TxHash            []byte
	CollateralEnabled bool
}

// PositionRepository defines the interface for user position data persistence.
// This aggregate includes borrower (debt) positions and collateral positions.
type PositionRepository interface {
	// SaveBorrower saves a single borrower position record within an external transaction.
	// amount is the full current outstanding debt (from getUserReserveData).
	// change is the decimal-adjusted event delta (how much was borrowed or repaid).
	SaveBorrower(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte) error

	// SaveBorrowerCollateral saves a single collateral position record within an external transaction.
	// amount is the full current collateral balance; change is the event delta.
	SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte, collateralEnabled bool) error

	// SaveBorrowerCollaterals saves multiple borrower collateral position records in batch.
	SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, records []CollateralRecord) error
}
