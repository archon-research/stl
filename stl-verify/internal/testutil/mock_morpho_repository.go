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
	GetMarketByMarketIDFn func(ctx context.Context, chainID int64, marketID common.Hash) (*entity.MorphoMarket, error)
	SaveMarketStateFn     func(ctx context.Context, tx pgx.Tx, state *entity.MorphoMarketState) error
	SaveMarketPositionFn  func(ctx context.Context, tx pgx.Tx, position *entity.MorphoMarketPosition) error
	GetOrCreateVaultFn    func(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error)
	GetVaultByAddressFn   func(ctx context.Context, chainID int64, address common.Address) (*entity.MorphoVault, error)
	GetAllVaultsFn        func(ctx context.Context, chainID int64) (map[common.Address]*entity.MorphoVault, error)
	SaveVaultStateFn      func(ctx context.Context, tx pgx.Tx, state *entity.MorphoVaultState) error
	SaveVaultPositionFn   func(ctx context.Context, tx pgx.Tx, position *entity.MorphoVaultPosition) error

	GetOrCreateAdapterFn       func(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error)
	MarkAdapterRemovedFn       func(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64) error
	GetActiveAdapterFn         func(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte) (*entity.MorphoAdapter, error)
	GetActiveAdaptersByVaultFn func(ctx context.Context, morphoVaultID int64) ([]*entity.MorphoAdapter, error)
	SaveAdapterStateFn         func(ctx context.Context, tx pgx.Tx, state *entity.MorphoAdapterState) error
	SaveVaultCapFn             func(ctx context.Context, tx pgx.Tx, vaultCap *entity.MorphoVaultCap) error
	UpdateVaultFeeConfigFn     func(ctx context.Context, tx pgx.Tx, morphoVaultID int64, update entity.MorphoVaultFeeUpdate) error
}

func (m *MockMorphoRepository) GetOrCreateMarket(ctx context.Context, tx pgx.Tx, market *entity.MorphoMarket) (int64, error) {
	if m.GetOrCreateMarketFn != nil {
		return m.GetOrCreateMarketFn(ctx, tx, market)
	}
	return 1, nil
}

func (m *MockMorphoRepository) GetMarketByMarketID(ctx context.Context, chainID int64, marketID common.Hash) (*entity.MorphoMarket, error) {
	if m.GetMarketByMarketIDFn != nil {
		return m.GetMarketByMarketIDFn(ctx, chainID, marketID)
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

func (m *MockMorphoRepository) GetVaultByAddress(ctx context.Context, chainID int64, address common.Address) (*entity.MorphoVault, error) {
	if m.GetVaultByAddressFn != nil {
		return m.GetVaultByAddressFn(ctx, chainID, address)
	}
	return nil, nil
}

func (m *MockMorphoRepository) GetAllVaults(ctx context.Context, chainID int64) (map[common.Address]*entity.MorphoVault, error) {
	if m.GetAllVaultsFn != nil {
		return m.GetAllVaultsFn(ctx, chainID)
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

func (m *MockMorphoRepository) GetOrCreateAdapter(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error) {
	if m.GetOrCreateAdapterFn != nil {
		return m.GetOrCreateAdapterFn(ctx, tx, adapter)
	}
	return 1, nil
}

func (m *MockMorphoRepository) MarkAdapterRemoved(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64) error {
	if m.MarkAdapterRemovedFn != nil {
		return m.MarkAdapterRemovedFn(ctx, tx, morphoVaultID, address, removedAtBlock)
	}
	return nil
}

func (m *MockMorphoRepository) GetActiveAdapter(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte) (*entity.MorphoAdapter, error) {
	if m.GetActiveAdapterFn != nil {
		return m.GetActiveAdapterFn(ctx, tx, morphoVaultID, address)
	}
	return nil, nil
}

func (m *MockMorphoRepository) GetActiveAdaptersByVault(ctx context.Context, morphoVaultID int64) ([]*entity.MorphoAdapter, error) {
	if m.GetActiveAdaptersByVaultFn != nil {
		return m.GetActiveAdaptersByVaultFn(ctx, morphoVaultID)
	}
	return nil, nil
}

func (m *MockMorphoRepository) SaveAdapterState(ctx context.Context, tx pgx.Tx, state *entity.MorphoAdapterState) error {
	if m.SaveAdapterStateFn != nil {
		return m.SaveAdapterStateFn(ctx, tx, state)
	}
	return nil
}

func (m *MockMorphoRepository) SaveVaultCap(ctx context.Context, tx pgx.Tx, vaultCap *entity.MorphoVaultCap) error {
	if m.SaveVaultCapFn != nil {
		return m.SaveVaultCapFn(ctx, tx, vaultCap)
	}
	return nil
}

func (m *MockMorphoRepository) UpdateVaultFeeConfig(ctx context.Context, tx pgx.Tx, morphoVaultID int64, update entity.MorphoVaultFeeUpdate) error {
	if m.UpdateVaultFeeConfigFn != nil {
		return m.UpdateVaultFeeConfigFn(ctx, tx, morphoVaultID, update)
	}
	return nil
}
