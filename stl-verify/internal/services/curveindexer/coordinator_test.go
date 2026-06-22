package curveindexer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeCurveRepo counts Save* calls; it ignores the pgx.Tx (nil is fine).
type fakeCurveRepo struct {
	swapSaves       int
	liquiditySaves  int
	stableswapSaves int
	cryptoswapSaves int
	snapshotPoolIDs []int64
}

func (r *fakeCurveRepo) LoadPools(_ context.Context, _ int64) ([]outbound.CurvePoolRow, error) {
	return nil, nil
}

func (r *fakeCurveRepo) SaveSwap(_ context.Context, _ pgx.Tx, _ outbound.SwapInput) error {
	r.swapSaves++
	return nil
}

func (r *fakeCurveRepo) SaveLiquidityEvent(_ context.Context, _ pgx.Tx, _ outbound.LiquidityInput) error {
	r.liquiditySaves++
	return nil
}

func (r *fakeCurveRepo) SaveStableswapState(_ context.Context, _ pgx.Tx, s *entity.CurveStableswapState) error {
	r.stableswapSaves++
	r.snapshotPoolIDs = append(r.snapshotPoolIDs, s.CurvePoolID)
	return nil
}

func (r *fakeCurveRepo) SaveCryptoswapState(_ context.Context, _ pgx.Tx, s *entity.CurveCryptoswapState) error {
	r.cryptoswapSaves++
	r.snapshotPoolIDs = append(r.snapshotPoolIDs, s.CurvePoolID)
	return nil
}

// fakeTxManager calls fn with a nil pgx.Tx; sufficient since fakeCurveRepo
// ignores the tx argument.
type fakeTxManager struct{}

func (m *fakeTxManager) WithTransaction(_ context.Context, fn func(pgx.Tx) error) error {
	return fn(nil)
}

// fakeProtocolRepo returns a fixed protocol id and implements outbound.ProtocolRepository.
type fakeProtocolRepo struct{}

func (r *fakeProtocolRepo) GetOrCreateProtocol(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _, _ string, _ int64) (int64, error) {
	return 1, nil
}

func (r *fakeProtocolRepo) UpsertReserveData(_ context.Context, _ pgx.Tx, _ []*entity.SparkLendReserveData) error {
	return nil
}

// fakeEventRepo swallows saves silently.
type fakeEventRepo struct{}

func (r *fakeEventRepo) SaveEvent(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
	return nil
}

// ---------------------------------------------------------------------------
// Test fixture factory
// ---------------------------------------------------------------------------

const testChainID = int64(1)

// testPool is a 2-coin pre-NG stableswap pool used by all coordinator tests.
var testPool = RegisteredPool{
	ID:           42,
	Address:      common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
	Kind:         KindStableswapPreNG,
	NCoins:       2,
	CoinTokenIDs: []int64{1, 2},
	CoinDecimals: []int{18, 18},
	DeployBlock:  1,
}

// newTestCoordinator constructs a Coordinator with one stableswap pool, a
// fakeMulticaller returning pre-NG canned results, and the given heartbeat
// interval. Returns the coordinator and the repo fake so callers can inspect
// call counts.
func newTestCoordinator(t *testing.T, heartbeatBlocks int64) (*Coordinator, *fakeCurveRepo) {
	t.Helper()

	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}

	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}

	repo := &fakeCurveRepo{}

	protoRepo := &fakeProtocolRepo{}
	eventRepo := &fakeEventRepo{}
	resolver := dexconsumer.NewProtocolIDResolver(protoRepo, dexconsumer.ProtocolDescriptor{
		Address:      common.HexToAddress("0x0000000000000000000000000000000000000001"),
		Name:         "curve",
		ProtocolType: "dex",
		DeployBlock:  1,
	})
	writer := dexconsumer.NewProtocolEventWriter(resolver, eventRepo)

	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}

	c, err := NewCoordinator(CoordinatorDeps{
		Pools:           []RegisteredPool{testPool},
		Handlers:        handlers,
		Multicaller:     mc,
		Repo:            repo,
		EventWriter:     writer,
		TxManager:       &fakeTxManager{},
		HeartbeatBlocks: heartbeatBlocks,
		ChainID:         testChainID,
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	return c, repo
}

// blockEvent builds a minimal outbound.BlockEvent for the given block number.
func blockEvent(bn int64) outbound.BlockEvent {
	return outbound.BlockEvent{
		ChainID:        testChainID,
		BlockNumber:    bn,
		Version:        0,
		BlockTimestamp: bn,
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestCoordinator_EventBlockSnapshotsTouchedPool: a receipt touching pool A
// triggers exactly one stableswap snapshot for pool A and the swap is saved.
func TestCoordinator_EventBlockSnapshotsTouchedPool(t *testing.T) {
	// HeartbeatBlocks=0 disables heartbeat; only touched-pool snapshot fires.
	c, repo := newTestCoordinator(t, 0)
	rh := c.ReceiptHandler()
	fin := c.Finalizer()

	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	receipt := buildReceiptWithTokenExchange(
		t, a, testPool.Address,
		common.HexToAddress("0xabc"),
		1, big.NewInt(1000),
		0, big.NewInt(999),
		0,
	)

	bn := int64(200)
	ts := time.Unix(bn, 0).UTC()
	if err := rh(context.Background(), receipt, testChainID, bn, 0, ts); err != nil {
		t.Fatalf("ReceiptHandler: %v", err)
	}
	if err := fin(context.Background(), blockEvent(bn)); err != nil {
		t.Fatalf("Finalizer: %v", err)
	}

	if repo.stableswapSaves != 1 {
		t.Errorf("stableswap snapshots = %d, want 1", repo.stableswapSaves)
	}
	if repo.swapSaves != 1 {
		t.Errorf("swap saves = %d, want 1", repo.swapSaves)
	}
	if len(repo.snapshotPoolIDs) != 1 || repo.snapshotPoolIDs[0] != testPool.ID {
		t.Errorf("snapshotted pool IDs = %v, want [%d]", repo.snapshotPoolIDs, testPool.ID)
	}
}

// TestCoordinator_HeartbeatSnapshotsQuietPool: with HeartbeatBlocks=5 and no
// events, the pool is snapshotted on block 100 (initial, never snapshotted) and
// again once bn advances >= 5 past the last snapshot; it is NOT snapshotted on
// blocks 101-104.
func TestCoordinator_HeartbeatSnapshotsQuietPool(t *testing.T) {
	// HeartbeatBlocks=5. The pool has never been snapshotted, so the first
	// finalize (block 100) triggers an initial snapshot. After that, blocks
	// 101-104 are below the threshold; block 105 (>= 100+5) triggers the second.
	c, repo := newTestCoordinator(t, 5)
	fin := c.Finalizer()

	for bn := int64(100); bn <= 105; bn++ {
		if err := fin(context.Background(), blockEvent(bn)); err != nil {
			t.Fatalf("finalize %d: %v", bn, err)
		}
	}

	// Block 100: initial snapshot (pool never snapshotted).
	// Block 101-104: no snapshot (100+5=105 not yet reached).
	// Block 105: heartbeat snapshot (105-100 >= 5).
	if repo.stableswapSaves != 2 {
		t.Fatalf("stableswap snapshots = %d, want 2 (initial at 100, heartbeat at 105)", repo.stableswapSaves)
	}
}

// TestCoordinator_HeartbeatDoesNotFireBeforeInterval: granular assertion that
// blocks 101-104 produce no snapshots when last snapshot was at block 100.
func TestCoordinator_HeartbeatDoesNotFireBeforeInterval(t *testing.T) {
	c, repo := newTestCoordinator(t, 5)
	fin := c.Finalizer()

	// Block 100 fires the initial snapshot.
	if err := fin(context.Background(), blockEvent(100)); err != nil {
		t.Fatalf("finalize 100: %v", err)
	}
	snapshotsAfterInitial := repo.stableswapSaves

	// Blocks 101-104 must not snapshot.
	for bn := int64(101); bn <= 104; bn++ {
		if err := fin(context.Background(), blockEvent(bn)); err != nil {
			t.Fatalf("finalize %d: %v", bn, err)
		}
	}

	if repo.stableswapSaves != snapshotsAfterInitial {
		t.Errorf("snapshots on blocks 101-104 = %d, want 0 (last snapshot was 100, threshold is 105)",
			repo.stableswapSaves-snapshotsAfterInitial)
	}
}

// TestCoordinator_BufferResetsAcrossBlocks: events on block N are NOT persisted
// when finalize is called for block N+1 with different receipts.
func TestCoordinator_BufferResetsAcrossBlocks(t *testing.T) {
	// HeartbeatBlocks=0 keeps snapshots tied strictly to touched pools.
	c, repo := newTestCoordinator(t, 0)
	rh := c.ReceiptHandler()
	fin := c.Finalizer()

	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	receipt := buildReceiptWithTokenExchange(
		t, a, testPool.Address,
		common.HexToAddress("0xabc"),
		1, big.NewInt(500),
		0, big.NewInt(499),
		0,
	)

	// Block 10: inject a receipt but do NOT call fin(10) — simulate a gap where
	// the processor only calls ReceiptHandler, then moves directly to block 11.
	ts10 := time.Unix(10, 0).UTC()
	if err := rh(context.Background(), receipt, testChainID, 10, 0, ts10); err != nil {
		t.Fatalf("ReceiptHandler block 10: %v", err)
	}

	// Block 11: different block; ReceiptHandler must reset the buffer.
	ts11 := time.Unix(11, 0).UTC()
	if err := rh(context.Background(), receipt, testChainID, 11, 0, ts11); err != nil {
		t.Fatalf("ReceiptHandler block 11: %v", err)
	}

	// Finalize block 11 — only block-11 events should be present (1 swap).
	if err := fin(context.Background(), blockEvent(11)); err != nil {
		t.Fatalf("Finalizer block 11: %v", err)
	}

	// If the buffer leaked from block 10, we'd see 2 swaps.
	if repo.swapSaves != 1 {
		t.Errorf("swap saves = %d, want 1 (no leakage from block 10)", repo.swapSaves)
	}
}

// TestCoordinator_BufferClearedAfterFailedFinalize: a failed finalize clears
// the buffer so a subsequent redelivery re-accumulates from empty.
func TestCoordinator_BufferClearedAfterFailedFinalize(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}

	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
	}

	goodRepo := &fakeCurveRepo{}

	// txMgr that fails once then succeeds.
	var calls int
	failOnce := &callCountingTxManager{
		fn: func(ctx context.Context, fn func(pgx.Tx) error) error {
			calls++
			if calls == 1 {
				return fmt.Errorf("transient failure")
			}
			return fn(nil)
		},
	}

	protoRepo := &fakeProtocolRepo{}
	eventRepo := &fakeEventRepo{}
	resolver := dexconsumer.NewProtocolIDResolver(protoRepo, dexconsumer.ProtocolDescriptor{
		Address: common.HexToAddress("0x0000000000000000000000000000000000000001"),
		Name:    "curve", ProtocolType: "dex", DeployBlock: 1,
	})
	writer := dexconsumer.NewProtocolEventWriter(resolver, eventRepo)
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}

	c, err := NewCoordinator(CoordinatorDeps{
		Pools:           []RegisteredPool{testPool},
		Handlers:        handlers,
		Multicaller:     mc,
		Repo:            goodRepo,
		EventWriter:     writer,
		TxManager:       failOnce,
		HeartbeatBlocks: 0,
		ChainID:         testChainID,
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	receipt := buildReceiptWithTokenExchange(
		t, a, testPool.Address,
		common.HexToAddress("0xabc"),
		1, big.NewInt(1000),
		0, big.NewInt(999),
		0,
	)

	rh := c.ReceiptHandler()
	fin := c.Finalizer()
	ts := time.Unix(200, 0).UTC()

	if err := rh(context.Background(), receipt, testChainID, 200, 0, ts); err != nil {
		t.Fatalf("ReceiptHandler: %v", err)
	}

	// First finalize fails — buffer should be cleared.
	if err := fin(context.Background(), blockEvent(200)); err == nil {
		t.Fatal("expected error from first finalize, got nil")
	}

	// Re-accumulate for the redelivery.
	if err := rh(context.Background(), receipt, testChainID, 200, 0, ts); err != nil {
		t.Fatalf("ReceiptHandler redelivery: %v", err)
	}

	// Second finalize should succeed with exactly 1 swap (not 2).
	if err := fin(context.Background(), blockEvent(200)); err != nil {
		t.Fatalf("Finalizer redelivery: %v", err)
	}

	if goodRepo.swapSaves != 1 {
		t.Errorf("swap saves = %d after redelivery, want 1 (buffer cleared on first failure)", goodRepo.swapSaves)
	}
}

// TestCoordinator_NilNilSnapshotErrors: when SnapshotState returns both
// Stableswap and Cryptoswap as nil, Finalizer should return an error.
func TestCoordinator_NilNilSnapshotErrors(t *testing.T) {
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: &nilNilHandler{},
	}

	repo := &fakeCurveRepo{}
	protoRepo := &fakeProtocolRepo{}
	eventRepo := &fakeEventRepo{}
	resolver := dexconsumer.NewProtocolIDResolver(protoRepo, dexconsumer.ProtocolDescriptor{
		Address:      common.HexToAddress("0x0000000000000000000000000000000000000001"),
		Name:         "curve",
		ProtocolType: "dex",
		DeployBlock:  1,
	})
	writer := dexconsumer.NewProtocolEventWriter(resolver, eventRepo)

	c, err := NewCoordinator(CoordinatorDeps{
		Pools:           []RegisteredPool{testPool},
		Handlers:        handlers,
		Multicaller:     &fakeMulticaller{},
		Repo:            repo,
		EventWriter:     writer,
		TxManager:       &fakeTxManager{},
		HeartbeatBlocks: 1, // triggers heartbeat snapshot
		ChainID:         testChainID,
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	fin := c.Finalizer()

	// Finalizer should error because the handler returns StateSnapshot with both nil.
	if err := fin(context.Background(), blockEvent(100)); err == nil {
		t.Fatal("expected error from Finalizer, got nil")
	}

	// lastSnapshotBlock should NOT be advanced (no DB write occurred).
	if _, ok := c.lastSnapshotBlock[testPool.ID]; ok {
		t.Errorf("lastSnapshotBlock[%d] should not be set after error", testPool.ID)
	}

	// No snapshot should be persisted.
	if repo.stableswapSaves != 0 || repo.cryptoswapSaves != 0 {
		t.Errorf("snapshot saves = %d stableswap + %d cryptoswap, want 0 + 0",
			repo.stableswapSaves, repo.cryptoswapSaves)
	}
}

// nilNilHandler is a PoolClassHandler stub that returns StateSnapshot with both
// Stableswap and Cryptoswap as nil, to test the default case error handling.
type nilNilHandler struct{}

func (h *nilNilHandler) Handles(kind PoolKind) bool {
	return kind == KindStableswapPreNG
}

func (h *nilNilHandler) DecodeEvents(receipt shared.TransactionReceipt, pool RegisteredPool, chainID, blockNumber int64, version int, ts time.Time) (DecodedEvents, error) {
	return DecodedEvents{}, nil
}

func (h *nilNilHandler) SnapshotState(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockNumber int64, version int, ts time.Time) (StateSnapshot, error) {
	// Return StateSnapshot with both pointers nil.
	return StateSnapshot{
		Pool:         pool,
		BlockNumber:  blockNumber,
		BlockVersion: version,
		Timestamp:    ts,
		Stableswap:   nil,
		Cryptoswap:   nil,
	}, nil
}

// callCountingTxManager delegates to a custom fn.
type callCountingTxManager struct {
	fn func(ctx context.Context, fn func(pgx.Tx) error) error
}

func (m *callCountingTxManager) WithTransaction(ctx context.Context, fn func(pgx.Tx) error) error {
	return m.fn(ctx, fn)
}
