package testutil

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockMorphoRepository implements outbound.MorphoRepository for testing.
type MockMorphoRepository struct {
	GetOrCreateMarketFn   func(ctx context.Context, tx pgx.Tx, market *entity.MorphoMarket) (int64, error)
	GetMarketByMarketIDFn func(ctx context.Context, marketID common.Hash) (*entity.MorphoMarket, error)
	SaveMarketStateFn     func(ctx context.Context, tx pgx.Tx, state *entity.MorphoMarketState) error
	SaveMarketPositionFn  func(ctx context.Context, tx pgx.Tx, position *entity.MorphoMarketPosition) error
	GetOrCreateVaultFn    func(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error)
	GetVaultByAddressFn   func(ctx context.Context, address common.Address) (*entity.MorphoVault, error)
	GetAllVaultsFn        func(ctx context.Context) (map[common.Address]*entity.MorphoVault, error)
	SaveVaultStateFn      func(ctx context.Context, tx pgx.Tx, state *entity.MorphoVaultState) error
	SaveVaultPositionFn   func(ctx context.Context, tx pgx.Tx, position *entity.MorphoVaultPosition) error
}

func (m *MockMorphoRepository) GetOrCreateMarket(ctx context.Context, tx pgx.Tx, market *entity.MorphoMarket) (int64, error) {
	if m.GetOrCreateMarketFn != nil {
		return m.GetOrCreateMarketFn(ctx, tx, market)
	}
	return 1, nil
}

func (m *MockMorphoRepository) GetMarketByMarketID(ctx context.Context, marketID common.Hash) (*entity.MorphoMarket, error) {
	if m.GetMarketByMarketIDFn != nil {
		return m.GetMarketByMarketIDFn(ctx, marketID)
	}
	return nil, nil
}

func (m *MockMorphoRepository) SaveMarketState(ctx context.Context, tx pgx.Tx, state *entity.MorphoMarketState) error {
	if m.SaveMarketStateFn != nil {
		return m.SaveMarketStateFn(ctx, tx, state)
	}
	return nil
}

func (m *MockMorphoRepository) SaveMarketPosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoMarketPosition) error {
	if m.SaveMarketPositionFn != nil {
		return m.SaveMarketPositionFn(ctx, tx, position)
	}
	return nil
}

func (m *MockMorphoRepository) GetOrCreateVault(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error) {
	if m.GetOrCreateVaultFn != nil {
		return m.GetOrCreateVaultFn(ctx, tx, vault)
	}
	return 1, nil
}

func (m *MockMorphoRepository) GetVaultByAddress(ctx context.Context, address common.Address) (*entity.MorphoVault, error) {
	if m.GetVaultByAddressFn != nil {
		return m.GetVaultByAddressFn(ctx, address)
	}
	return nil, nil
}

func (m *MockMorphoRepository) GetAllVaults(ctx context.Context) (map[common.Address]*entity.MorphoVault, error) {
	if m.GetAllVaultsFn != nil {
		return m.GetAllVaultsFn(ctx)
	}
	return nil, nil
}

func (m *MockMorphoRepository) SaveVaultState(ctx context.Context, tx pgx.Tx, state *entity.MorphoVaultState) error {
	if m.SaveVaultStateFn != nil {
		return m.SaveVaultStateFn(ctx, tx, state)
	}
	return nil
}

func (m *MockMorphoRepository) SaveVaultPosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoVaultPosition) error {
	if m.SaveVaultPositionFn != nil {
		return m.SaveVaultPositionFn(ctx, tx, position)
	}
	return nil
}
