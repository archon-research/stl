package testutil

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// MockPositionRepository implements outbound.PositionRepository for testing.
type MockPositionRepository struct {
	SaveBorrowerFn             func(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte) error
	SaveBorrowerCollateralFn   func(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte, collateralEnabled bool) error
	SaveBorrowerCollateralsFn  func(ctx context.Context, tx pgx.Tx, records []outbound.CollateralRecord) error
	UpsertBorrowersFn          func(ctx context.Context, borrowers []*entity.Borrower) error
	UpsertBorrowerCollateralFn func(ctx context.Context, collateral []*entity.BorrowerCollateral) error

	// Captured calls for test assertions.
	SavedBorrowers   []SavedBorrowerCall
	SavedCollaterals []outbound.CollateralRecord
}

// SavedBorrowerCall records the arguments of a SaveBorrower invocation.
type SavedBorrowerCall struct {
	UserID       int64
	ProtocolID   int64
	TokenID      int64
	BlockNumber  int64
	BlockVersion int
	Amount       string
	Change       string
	EventType    string
	TxHash       []byte
}

func (m *MockPositionRepository) SaveBorrower(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte) error {
	m.SavedBorrowers = append(m.SavedBorrowers, SavedBorrowerCall{
		UserID:       userID,
		ProtocolID:   protocolID,
		TokenID:      tokenID,
		BlockNumber:  blockNumber,
		BlockVersion: blockVersion,
		Amount:       amount,
		Change:       change,
		EventType:    eventType,
		TxHash:       txHash,
	})
	if m.SaveBorrowerFn != nil {
		return m.SaveBorrowerFn(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, amount, change, eventType, txHash)
	}
	return nil
}

func (m *MockPositionRepository) SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte, collateralEnabled bool) error {
	m.SavedCollaterals = append(m.SavedCollaterals, outbound.CollateralRecord{
		UserID:            userID,
		ProtocolID:        protocolID,
		TokenID:           tokenID,
		BlockNumber:       blockNumber,
		BlockVersion:      blockVersion,
		Amount:            amount,
		Change:            change,
		EventType:         eventType,
		TxHash:            txHash,
		CollateralEnabled: collateralEnabled,
	})
	if m.SaveBorrowerCollateralFn != nil {
		return m.SaveBorrowerCollateralFn(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, amount, change, eventType, txHash, collateralEnabled)
	}
	return nil
}

func (m *MockPositionRepository) SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, records []outbound.CollateralRecord) error {
	m.SavedCollaterals = append(m.SavedCollaterals, records...)
	if m.SaveBorrowerCollateralsFn != nil {
		return m.SaveBorrowerCollateralsFn(ctx, tx, records)
	}
	return nil
}

func (m *MockPositionRepository) UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error {
	if m.UpsertBorrowersFn != nil {
		return m.UpsertBorrowersFn(ctx, borrowers)
	}
	return nil
}

func (m *MockPositionRepository) UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error {
	if m.UpsertBorrowerCollateralFn != nil {
		return m.UpsertBorrowerCollateralFn(ctx, collateral)
	}
	return nil
}
