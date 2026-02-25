package morpho_indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockBlockCache implements outbound.BlockCache for testing.
// Only GetReceipts is used by the morpho_indexer service.
type mockBlockCache struct {
	mu       sync.RWMutex
	receipts map[string]json.RawMessage // keyed by "chainID:blockNumber:version"
	err      error                      // if set, all calls return this error
}

func newMockBlockCache() *mockBlockCache {
	return &mockBlockCache{receipts: make(map[string]json.RawMessage)}
}

func (m *mockBlockCache) SetReceipts(chainID, blockNumber int64, version int, data json.RawMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := receiptsKey(chainID, blockNumber, version)
	m.receipts[key] = data
}

func (m *mockBlockCache) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func (m *mockBlockCache) GetReceipts(_ context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.err != nil {
		return nil, m.err
	}
	key := receiptsKey(chainID, blockNumber, version)
	data, ok := m.receipts[key]
	if !ok {
		return nil, nil
	}
	return data, nil
}

// Unused BlockCache methods — the service only reads receipts.

func (m *mockBlockCache) SetBlockData(context.Context, int64, int64, int, outbound.BlockDataInput) error {
	return nil
}
func (m *mockBlockCache) GetBlock(context.Context, int64, int64, int) (json.RawMessage, error) {
	return nil, nil
}
func (m *mockBlockCache) GetTraces(context.Context, int64, int64, int) (json.RawMessage, error) {
	return nil, nil
}
func (m *mockBlockCache) GetBlobs(context.Context, int64, int64, int) (json.RawMessage, error) {
	return nil, nil
}
func (m *mockBlockCache) DeleteBlock(context.Context, int64, int64, int) error { return nil }
func (m *mockBlockCache) Close() error                                         { return nil }

func receiptsKey(chainID, blockNumber int64, version int) string {
	return fmt.Sprintf("%d:%d:%d", chainID, blockNumber, version)
}

var errCacheClosed = errors.New("cache connection closed")

// mockMorphoRepo implements outbound.MorphoRepository for testing.
type mockMorphoRepo struct {
	GetOrCreateMarketFn    func(ctx context.Context, tx pgx.Tx, market *entity.MorphoMarket) (int64, error)
	GetMarketByMarketIDFn  func(ctx context.Context, marketID []byte) (*entity.MorphoMarket, error)
	SaveMarketStateFn      func(ctx context.Context, tx pgx.Tx, state *entity.MorphoMarketState) error
	SaveMarketPositionFn   func(ctx context.Context, tx pgx.Tx, position *entity.MorphoMarketPosition) error
	GetOrCreateVaultFn     func(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error)
	GetVaultByAddressFn    func(ctx context.Context, address common.Address) (*entity.MorphoVault, error)
	GetAllVaultAddressesFn func(ctx context.Context) ([]common.Address, error)
	SaveVaultStateFn       func(ctx context.Context, tx pgx.Tx, state *entity.MorphoVaultState) error
	SaveVaultPositionFn    func(ctx context.Context, tx pgx.Tx, position *entity.MorphoVaultPosition) error
}

func (m *mockMorphoRepo) GetOrCreateMarket(ctx context.Context, tx pgx.Tx, market *entity.MorphoMarket) (int64, error) {
	if m.GetOrCreateMarketFn != nil {
		return m.GetOrCreateMarketFn(ctx, tx, market)
	}
	return 1, nil
}

func (m *mockMorphoRepo) GetMarketByMarketID(ctx context.Context, marketID []byte) (*entity.MorphoMarket, error) {
	if m.GetMarketByMarketIDFn != nil {
		return m.GetMarketByMarketIDFn(ctx, marketID)
	}
	return nil, nil
}

func (m *mockMorphoRepo) SaveMarketState(ctx context.Context, tx pgx.Tx, state *entity.MorphoMarketState) error {
	if m.SaveMarketStateFn != nil {
		return m.SaveMarketStateFn(ctx, tx, state)
	}
	return nil
}

func (m *mockMorphoRepo) SaveMarketPosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoMarketPosition) error {
	if m.SaveMarketPositionFn != nil {
		return m.SaveMarketPositionFn(ctx, tx, position)
	}
	return nil
}

func (m *mockMorphoRepo) GetOrCreateVault(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error) {
	if m.GetOrCreateVaultFn != nil {
		return m.GetOrCreateVaultFn(ctx, tx, vault)
	}
	return 1, nil
}

func (m *mockMorphoRepo) GetVaultByAddress(ctx context.Context, address common.Address) (*entity.MorphoVault, error) {
	if m.GetVaultByAddressFn != nil {
		return m.GetVaultByAddressFn(ctx, address)
	}
	return nil, nil
}

func (m *mockMorphoRepo) GetAllVaultAddresses(ctx context.Context) ([]common.Address, error) {
	if m.GetAllVaultAddressesFn != nil {
		return m.GetAllVaultAddressesFn(ctx)
	}
	return nil, nil
}

func (m *mockMorphoRepo) SaveVaultState(ctx context.Context, tx pgx.Tx, state *entity.MorphoVaultState) error {
	if m.SaveVaultStateFn != nil {
		return m.SaveVaultStateFn(ctx, tx, state)
	}
	return nil
}

func (m *mockMorphoRepo) SaveVaultPosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoVaultPosition) error {
	if m.SaveVaultPositionFn != nil {
		return m.SaveVaultPositionFn(ctx, tx, position)
	}
	return nil
}

// mockSQSConsumer implements outbound.SQSConsumer for testing.
type mockSQSConsumer struct {
	ReceiveMessagesFn func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
	DeleteMessageFn   func(ctx context.Context, receiptHandle string) error
	CloseFn           func() error
}

func (m *mockSQSConsumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	if m.ReceiveMessagesFn != nil {
		return m.ReceiveMessagesFn(ctx, maxMessages)
	}
	return nil, nil
}

func (m *mockSQSConsumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	if m.DeleteMessageFn != nil {
		return m.DeleteMessageFn(ctx, receiptHandle)
	}
	return nil
}

func (m *mockSQSConsumer) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}
