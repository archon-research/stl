package fluid_vault_indexer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var errTest = errors.New("test error")

// stubFluidRepo is an in-memory FluidVaultRepository for unit tests. RecordVaults
// assigns sequential ids and records calls; SaveVaultStates accumulates states.
type stubFluidRepo struct {
	mu sync.Mutex

	vaults    map[common.Address]*entity.FluidVault
	getAllErr error
	upsertErr error
	saveErr   error

	nextID    int64
	savedRows []*entity.FluidVaultState
	upserted  [][]*entity.FluidVault
}

func newStubFluidRepo() *stubFluidRepo {
	return &stubFluidRepo{vaults: map[common.Address]*entity.FluidVault{}}
}

func (s *stubFluidRepo) RecordVaults(_ context.Context, _ pgx.Tx, vaults []*entity.FluidVault) (map[common.Address]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upsertErr != nil {
		return nil, s.upsertErr
	}
	s.upserted = append(s.upserted, vaults)
	out := make(map[common.Address]int64, len(vaults))
	if s.vaults == nil {
		s.vaults = map[common.Address]*entity.FluidVault{}
	}
	for _, v := range vaults {
		addr := common.BytesToAddress(v.Address)
		if existing, ok := s.vaults[addr]; ok {
			out[addr] = existing.ID
			continue
		}
		s.nextID++
		v.ID = s.nextID
		stored := *v
		s.vaults[addr] = &stored
		out[addr] = v.ID
	}
	return out, nil
}

func (s *stubFluidRepo) GetAllVaults(_ context.Context, _ int64) (map[common.Address]*entity.FluidVault, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getAllErr != nil {
		return nil, s.getAllErr
	}
	out := make(map[common.Address]*entity.FluidVault, len(s.vaults))
	for k, v := range s.vaults {
		cp := *v
		out[k] = &cp
	}
	return out, nil
}

func (s *stubFluidRepo) SaveVaultStates(_ context.Context, _ pgx.Tx, states []*entity.FluidVaultState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.savedRows = append(s.savedRows, states...)
	return nil
}

func (s *stubFluidRepo) savedStates() []*entity.FluidVaultState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*entity.FluidVaultState(nil), s.savedRows...)
}

// stubTxManager runs fn inline with a nil tx (the stub repo ignores it).
type stubTxManager struct {
	beginErr error
	calls    int
}

func (m *stubTxManager) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	m.calls++
	if m.beginErr != nil {
		return m.beginErr
	}
	return fn(nil)
}

// stubCache returns canned receipts JSON keyed by block number. The embedded
// BlockCacheReader is never set; only GetReceipts is exercised by the indexer,
// so the other methods would panic if called (and must not be).
type stubCache struct {
	outbound.BlockCacheReader
	receipts map[int64]json.RawMessage
	err      error
}

func (c *stubCache) GetReceipts(_ context.Context, _ int64, blockNumber int64, _ int) (json.RawMessage, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.receipts[blockNumber], nil
}

// stubTokenRepo assigns a deterministic id per (address) and records calls. Only
// GetOrCreateToken is exercised; the rest of the port is embedded and unused.
type stubTokenRepo struct {
	outbound.TokenRepository
	mu     sync.Mutex
	ids    map[common.Address]int64
	nextID int64
	err    error
}

func newStubTokenRepo() *stubTokenRepo {
	return &stubTokenRepo{ids: map[common.Address]int64{}, nextID: 1000}
}

func (r *stubTokenRepo) GetOrCreateToken(_ context.Context, _ pgx.Tx, _ int64, address common.Address, _ string, _ int, _ *int64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return 0, r.err
	}
	if id, ok := r.ids[address]; ok {
		return id, nil
	}
	r.nextID++
	r.ids[address] = r.nextID
	return r.nextID, nil
}

// stubProtocolRepo returns a fixed protocol id. Only GetOrCreateProtocol is used.
type stubProtocolRepo struct {
	outbound.ProtocolRepository
	id  int64
	err error
}

func (r *stubProtocolRepo) GetOrCreateProtocol(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ string, _ int64) (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.id == 0 {
		return 42, nil
	}
	return r.id, nil
}

// stubBlockQuerier returns a fixed head block for startup reconcile.
type stubBlockQuerier struct {
	head uint64
	err  error
}

func (q stubBlockQuerier) BlockNumber(_ context.Context) (uint64, error) {
	return q.head, q.err
}

// mockMetrics records the status labels passed to RecordBlockProcessed.
type mockMetrics struct {
	mu        sync.Mutex
	processed []string
}

var _ outbound.BackupMetricsRecorder = (*mockMetrics)(nil)

func (m *mockMetrics) RecordProcessingLatency(context.Context, time.Duration, string) {}

func (m *mockMetrics) RecordBlockProcessed(_ context.Context, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed = append(m.processed, status)
}

func (m *mockMetrics) ProcessedStatuses() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.processed...)
}

// stubConsumer is a minimal SQSConsumer; the service tests drive processBlockEvent
// directly rather than through the poll loop, so receive/delete are no-ops.
type stubConsumer struct{}

func (stubConsumer) ReceiveMessages(_ context.Context, _ int) ([]outbound.SQSMessage, error) {
	return nil, nil
}
func (stubConsumer) DeleteMessage(_ context.Context, _ string) error { return nil }
func (stubConsumer) VisibilityTimeout() time.Duration                { return 5 * time.Minute }
func (stubConsumer) Close() error                                    { return nil }
