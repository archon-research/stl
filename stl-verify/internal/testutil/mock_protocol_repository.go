package testutil

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockProtocolRepository implements outbound.ProtocolRepository for testing.
type MockProtocolRepository struct {
	GetOrCreateProtocolFn func(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, name string, protocolType string, createdAtBlock int64) (int64, error)
	UpsertReserveDataFn   func(ctx context.Context, tx pgx.Tx, data []*entity.SparkLendReserveData) error
}

func (m *MockProtocolRepository) GetOrCreateProtocol(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, name string, protocolType string, createdAtBlock int64) (int64, error) {
	if m.GetOrCreateProtocolFn != nil {
		return m.GetOrCreateProtocolFn(ctx, tx, chainID, address, name, protocolType, createdAtBlock)
	}
	return 1, nil
}

func (m *MockProtocolRepository) UpsertReserveData(ctx context.Context, tx pgx.Tx, data []*entity.SparkLendReserveData) error {
	if m.UpsertReserveDataFn != nil {
		return m.UpsertReserveDataFn(ctx, tx, data)
	}
	return nil
}
