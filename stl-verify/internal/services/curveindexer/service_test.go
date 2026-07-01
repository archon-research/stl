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
	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeCurveRepo counts saves via SaveBlock; it ignores the pgx.Tx (nil is fine).
// stateRowsReturn controls whether SaveBlock returns 0 (simulate ON CONFLICT DO NOTHING
// no-op) or the actual count; a zero value means newTestCurveService must set it to 1.
type fakeCurveRepo struct {
	lastWrites      outbound.BlockWrites
	snapshotPoolIDs []int64
	stateRowsReturn int64
	// computed counts for test assertions
	swapSaves       int
	liquiditySaves  int
	stableswapSaves int
	cryptoswapSaves int
}

func (r *fakeCurveRepo) LoadPools(_ context.Context, _ int64) ([]outbound.CurvePoolRow, error) {
	return nil, nil
}

func (r *fakeCurveRepo) SaveBlock(_ context.Context, _ pgx.Tx, w outbound.BlockWrites) (int64, error) {
	r.lastWrites = w
	r.swapSaves += len(w.Swaps)
	r.liquiditySaves += len(w.Liquidity)
	r.stableswapSaves += len(w.StableStates)
	r.cryptoswapSaves += len(w.CryptoStates)
	for _, s := range w.StableStates {
		r.snapshotPoolIDs = append(r.snapshotPoolIDs, s.CurvePoolID)
	}
	for _, s := range w.CryptoStates {
		r.snapshotPoolIDs = append(r.snapshotPoolIDs, s.CurvePoolID)
	}
	if r.stateRowsReturn == 0 {
		return 0, nil
	}
	return int64(len(w.StableStates) + len(w.CryptoStates)), nil
}

// fakeTxManager calls fn with a nil pgx.Tx; sufficient since fakeCurveRepo
// ignores the tx argument.
type fakeTxManager struct{}

func (m *fakeTxManager) WithTransaction(_ context.Context, fn func(pgx.Tx) error) error {
	return fn(nil)
}

// inTxTrackingTxManager flips inTx for the duration of the transaction callback
// so a multicaller can assert it is invoked OUTSIDE the transaction scope.
type inTxTrackingTxManager struct {
	inTx bool
}

func (m *inTxTrackingTxManager) WithTransaction(_ context.Context, fn func(pgx.Tx) error) error {
	m.inTx = true
	defer func() { m.inTx = false }()
	return fn(nil)
}

// txCheckingMulticaller fails if a multicall runs while the tracked tx manager
// is inside a transaction, proving snapshot reads happen before the tx opens.
// Curve's snapshot path calls ExecuteAtHash (hash-pinned reads); Execute is kept
// for other Multicaller consumers that only have a block number.
type txCheckingMulticaller struct {
	tracker *inTxTrackingTxManager
	results []outbound.Result
}

func (m *txCheckingMulticaller) checkNotInTx() error {
	if m.tracker.inTx {
		return fmt.Errorf("multicall executed inside the transaction (archive-RPC latency would pin a pgx connection)")
	}
	return nil
}

func (m *txCheckingMulticaller) Execute(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	if err := m.checkNotInTx(); err != nil {
		return nil, err
	}
	return m.results, nil
}

func (m *txCheckingMulticaller) ExecuteAtHash(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
	if err := m.checkNotInTx(); err != nil {
		return nil, err
	}
	return m.results, nil
}

func (m *txCheckingMulticaller) Address() common.Address {
	return common.Address{}
}

// hashRecordingMulticaller is a test double for outbound.Multicaller that
// records the block hash it was called with via ExecuteAtHash, so tests can
// assert the coordinator pins state reads to the block hash (reorg-correctness)
// rather than the block number alone.
type hashRecordingMulticaller struct {
	results     []outbound.Result
	gotHash     common.Hash
	executedVia string // "hash" or "number", whichever method was actually called
}

func (m *hashRecordingMulticaller) Execute(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	m.executedVia = "number"
	return m.results, nil
}

func (m *hashRecordingMulticaller) ExecuteAtHash(_ context.Context, _ []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	m.executedVia = "hash"
	m.gotHash = blockHash
	return m.results, nil
}

func (m *hashRecordingMulticaller) Address() common.Address {
	return common.Address{}
}

// fakeEventRepo swallows saves silently.
type fakeEventRepo struct{}

func (r *fakeEventRepo) SaveEvent(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
	return nil
}

func (r *fakeEventRepo) SaveBatch(_ context.Context, _ pgx.Tx, _ []*entity.ProtocolEvent) error {
	return nil
}

// countingEventRepo counts saves so capture-net tests can assert forwarding.
type countingEventRepo struct {
	saves int
}

func (r *countingEventRepo) SaveEvent(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
	r.saves++
	return nil
}

func (r *countingEventRepo) SaveBatch(_ context.Context, _ pgx.Tx, evts []*entity.ProtocolEvent) error {
	r.saves += len(evts)
	return nil
}

// capturingEventRepo records the persisted events so capture-net tests can
// assert the emitting contract address per event.
type capturingEventRepo struct {
	events []*entity.ProtocolEvent
}

func (r *capturingEventRepo) SaveEvent(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
	r.events = append(r.events, e)
	return nil
}

func (r *capturingEventRepo) SaveBatch(_ context.Context, _ pgx.Tx, evts []*entity.ProtocolEvent) error {
	r.events = append(r.events, evts...)
	return nil
}

// ---------------------------------------------------------------------------
// Test fixture factory
// ---------------------------------------------------------------------------

const testChainID = int64(1)

// newTestPool returns a 2-coin pre-NG stableswap pool used by coordinator tests.
// It models stETH classic, which does expose A_precise (HasAPrecise=true), so the
// snapshot issues the gated A_precise call and the canned results stay aligned.
func newTestPool() RegisteredPool {
	return RegisteredPool{
		ID:           42,
		Address:      common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:         KindStableswapPreNG,
		NCoins:       2,
		CoinDecimals: []int{18, 18},
		HasAPrecise:  true,
	}
}

// preNGLpTokenAddr is the separate LP-token contract for newTestPoolWithLpToken,
// modelled on the stETH-classic steCRV LP token.
var preNGLpTokenAddr = common.HexToAddress("0x06325440D014e39736583c165C2963BA99fAf14E")

// newTestPoolWithLpToken returns the pre-NG pool from newTestPool but with a
// separate LP-token contract address, so LP Transfer/Approval logs arrive on a
// different address than the pool itself.
func newTestPoolWithLpToken() RegisteredPool {
	p := newTestPool()
	lp := preNGLpTokenAddr
	p.LpTokenAddress = &lp
	return p
}

// newTestCurveService constructs a CurveService with one stableswap pool, a
// fakeMulticaller returning pre-NG canned results, and the given sweep
// interval. Returns the coordinator and the repo fake so callers can inspect
// call counts.
func newTestCurveService(t *testing.T, sweepBlocks int64) (*CurveService, *fakeCurveRepo) {
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

	repo := &fakeCurveRepo{stateRowsReturn: 1}

	eventRepo := &fakeEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)

	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: sweepBlocks,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}
	return c, repo
}

// blockEvent builds a minimal outbound.BlockEvent for the given block number.
// BlockHash defaults to a non-zero test hash so the suite exercises the real
// hash-pinned snapshot path rather than the zero hash by accident.
func blockEvent(bn int64) outbound.BlockEvent {
	return outbound.BlockEvent{
		ChainID:        testChainID,
		BlockNumber:    bn,
		Version:        0,
		BlockTimestamp: bn,
		BlockHash:      common.HexToHash("0x01").Hex(),
	}
}

// swapReceipt builds a receipt containing one TokenExchange log for the given pool.
func swapReceipt(t *testing.T) shared.TransactionReceipt {
	t.Helper()
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	return buildReceiptWithTokenExchange(
		t, a, newTestPool().Address,
		common.HexToAddress("0xabc"),
		1, big.NewInt(1000),
		0, big.NewInt(999),
		0,
	)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestCurveService_EventBlockSnapshotsTouchedPool: a receipt touching pool A
// triggers exactly one stableswap snapshot for pool A and the swap is saved.
func TestCurveService_EventBlockSnapshotsTouchedPool(t *testing.T) {
	// SweepBlocks=0 disables sweep; only touched-pool snapshot fires.
	c, repo := newTestCurveService(t, 0)
	bh := c.BlockHandler()

	receipt := swapReceipt(t)
	event := blockEvent(200)
	event.BlockTimestamp = 200

	if err := bh(context.Background(), event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if repo.stableswapSaves != 1 {
		t.Errorf("stableswap snapshots = %d, want 1", repo.stableswapSaves)
	}
	if repo.swapSaves != 1 {
		t.Errorf("swap saves = %d, want 1", repo.swapSaves)
	}
	pool := newTestPool()
	if len(repo.snapshotPoolIDs) != 1 || repo.snapshotPoolIDs[0] != pool.ID {
		t.Errorf("snapshotted pool IDs = %v, want [%d]", repo.snapshotPoolIDs, pool.ID)
	}
}

// TestCurveService_SweepSnapshotsQuietPool: with SweepBlocks=5 and no
// events, the pool is snapshotted on block 100 (initial, never snapshotted) and
// again once bn advances >= 5 past the last snapshot; it is NOT snapshotted on
// blocks 101-104.
func TestCurveService_SweepSnapshotsQuietPool(t *testing.T) {
	// SweepBlocks=5. The pool has never been snapshotted, so the first
	// call (block 100) triggers an initial snapshot. After that, blocks
	// 101-104 are below the threshold; block 105 (>= 100+5) triggers the second.
	c, repo := newTestCurveService(t, 5)
	bh := c.BlockHandler()

	for bn := int64(100); bn <= 105; bn++ {
		if err := bh(context.Background(), blockEvent(bn), nil); err != nil {
			t.Fatalf("BlockHandler %d: %v", bn, err)
		}
	}

	// Block 100: initial snapshot (pool never snapshotted).
	// Block 101-104: no snapshot (100+5=105 not yet reached).
	// Block 105: sweep snapshot (105-100 >= 5).
	if repo.stableswapSaves != 2 {
		t.Fatalf("stableswap snapshots = %d, want 2 (initial at 100, sweep at 105)", repo.stableswapSaves)
	}
}

// TestCurveService_SweepDoesNotFireBeforeInterval: granular assertion that
// blocks 101-104 produce no snapshots when last snapshot was at block 100.
func TestCurveService_SweepDoesNotFireBeforeInterval(t *testing.T) {
	c, repo := newTestCurveService(t, 5)
	bh := c.BlockHandler()

	// Block 100 fires the initial snapshot.
	if err := bh(context.Background(), blockEvent(100), nil); err != nil {
		t.Fatalf("BlockHandler 100: %v", err)
	}
	snapshotsAfterInitial := repo.stableswapSaves

	// Blocks 101-104 must not snapshot.
	for bn := int64(101); bn <= 104; bn++ {
		if err := bh(context.Background(), blockEvent(bn), nil); err != nil {
			t.Fatalf("BlockHandler %d: %v", bn, err)
		}
	}

	if repo.stableswapSaves != snapshotsAfterInitial {
		t.Errorf("snapshots on blocks 101-104 = %d, want 0 (last snapshot was 100, threshold is 105)",
			repo.stableswapSaves-snapshotsAfterInitial)
	}
}

// TestCurveService_RedeliveryDoesNotDouble: calling BlockHandler twice for the
// same block+receipts must save each row set exactly once. Local vars are fresh
// each call so there is no carryover from the first invocation.
func TestCurveService_RedeliveryDoesNotDouble(t *testing.T) {
	c, repo := newTestCurveService(t, 0)
	bh := c.BlockHandler()

	receipt := swapReceipt(t)
	event := blockEvent(200)

	// First call succeeds.
	if err := bh(context.Background(), event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler first call: %v", err)
	}
	// Second call (SQS redelivery) must persist the same row set, not accumulate.
	if err := bh(context.Background(), event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler second call: %v", err)
	}

	// Each call is independent: 1 swap per call -> 2 total, not 1 (no doubling
	// within a single call) and not more (no cross-call carryover).
	if repo.swapSaves != 2 {
		t.Errorf("swap saves = %d after two independent calls, want 2 (1 per call, no cross-call carryover)", repo.swapSaves)
	}
}

// TestCurveService_NilNilSnapshotErrors: when SnapshotState returns both
// Stableswap and Cryptoswap as nil, BlockHandler should return an error.
func TestCurveService_NilNilSnapshotErrors(t *testing.T) {
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: &nilNilHandler{},
	}

	repo := &fakeCurveRepo{stateRowsReturn: 1}
	eventRepo := &fakeEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: &fakeMulticaller{},
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: 1, // triggers sweep snapshot
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	bh := c.BlockHandler()

	// BlockHandler should error because the handler returns StateSnapshot with both nil.
	if err := bh(context.Background(), blockEvent(100), nil); err == nil {
		t.Fatal("expected error from BlockHandler, got nil")
	}

	// lastSnapshot should NOT be advanced (no DB write occurred).
	if _, ok := c.lastSnapshot[newTestPool().ID]; ok {
		t.Errorf("lastSnapshot[%d] should not be set after error", newTestPool().ID)
	}

	// No snapshot should be persisted.
	if repo.stableswapSaves != 0 || repo.cryptoswapSaves != 0 {
		t.Errorf("snapshot saves = %d stableswap + %d cryptoswap, want 0 + 0",
			repo.stableswapSaves, repo.cryptoswapSaves)
	}
}

// TestCurveService_CaptureNetReachesEventWriter: a receipt with one decodable
// event produces a captured event that is forwarded to the EventWriter (Save
// is called exactly once for that event).
func TestCurveService_CaptureNetReachesEventWriter(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}

	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}

	repo := &fakeCurveRepo{stateRowsReturn: 1}
	eventRepo := &countingEventRepo{}
	pool := newTestPool()

	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{pool},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: 0,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	receipt := buildReceiptWithTokenExchange(
		t, a, pool.Address,
		common.HexToAddress("0xabc"),
		1, big.NewInt(1000),
		0, big.NewInt(999),
		0,
	)

	bh := c.BlockHandler()
	event := blockEvent(300)
	event.BlockTimestamp = 300

	if err := bh(context.Background(), event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	// One TokenExchange log -> exactly one captured event forwarded to EventWriter.
	if eventRepo.saves != 1 {
		t.Errorf("eventRepo.saves = %d, want 1 (capture-net forwarded to EventWriter)", eventRepo.saves)
	}
}

// TestCurveService_SnapshotMulticallRunsOutsideTransaction: snapshot reads must
// happen before the transaction opens so archive-RPC latency never pins a pgx
// connection.
func TestCurveService_SnapshotMulticallRunsOutsideTransaction(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}

	tracker := &inTxTrackingTxManager{}
	mc := &txCheckingMulticaller{tracker: tracker, results: stableswapPreNGResults(t, a)}

	repo := &fakeCurveRepo{stateRowsReturn: 1}
	eventRepo := &fakeEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   tracker,
		SweepBlocks: 1, // force a snapshot even with no events
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	bh := c.BlockHandler()
	if err := bh(context.Background(), blockEvent(100), nil); err != nil {
		t.Fatalf("BlockHandler: %v (multicall must run outside the tx)", err)
	}
	if repo.stableswapSaves != 1 {
		t.Errorf("stableswap snapshots = %d, want 1", repo.stableswapSaves)
	}
}

// TestCurveService_SnapshotPinsToBlockHash: the state snapshot multicall must be
// pinned to the block hash of the (blockNumber, version) being processed, not the
// block number alone. After a reorg an archive node answers eth_call-by-number
// with the new canonical state, which can silently disagree with the reorged
// receipts being processed in this event; pinning by hash makes the read
// unambiguous. See VEC-261 task A1 (fix A2).
func TestCurveService_SnapshotPinsToBlockHash(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}

	mc := &hashRecordingMulticaller{results: stableswapPreNGResults(t, a)}

	repo := &fakeCurveRepo{stateRowsReturn: 1}
	eventRepo := &fakeEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: 1, // force a snapshot even with no events
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	wantHash := common.HexToHash("0xabc123abc123abc123abc123abc123abc123abc123abc123abc123abc123ab")
	event := blockEvent(100)
	event.BlockHash = wantHash.Hex()

	bh := c.BlockHandler()
	if err := bh(context.Background(), event, nil); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if mc.executedVia != "hash" {
		t.Fatalf("multicaller invoked via %q, want the hash-pinned path", mc.executedVia)
	}
	if mc.gotHash != wantHash {
		t.Errorf("multicall block hash = %s, want %s", mc.gotHash, wantHash)
	}
}

// TestCurveService_MissingBlockHash_ReturnsError: an event with an empty
// BlockHash must fail loud before ever reaching the multicaller, instead of
// silently defaulting to the zero hash (common.HexToHash never errors).
func TestCurveService_MissingBlockHash_ReturnsError(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}

	mc := &hashRecordingMulticaller{results: stableswapPreNGResults(t, a)}

	repo := &fakeCurveRepo{stateRowsReturn: 1}
	eventRepo := &fakeEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: 1, // force a snapshot even with no events
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	event := blockEvent(100)
	event.BlockHash = ""

	bh := c.BlockHandler()
	if err := bh(context.Background(), event, nil); err == nil {
		t.Fatal("expected non-nil error from BlockHandler when event.BlockHash is empty")
	}

	if mc.executedVia != "" {
		t.Errorf("multicaller invoked via %q, want it never called", mc.executedVia)
	}
	if repo.stableswapSaves != 0 {
		t.Errorf("stableswapSaves = %d, want 0 (block must not be persisted)", repo.stableswapSaves)
	}
}

// TestCurveService_RecordsActualStateRowsNotSnapshotCount: a redelivery where the
// state insert is a no-op (ON CONFLICT DO NOTHING -> 0 rows) must record 0 state
// rows, not the snapshot-set size.
func TestCurveService_RecordsActualStateRowsNotSnapshotCount(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})

	tel, err := dextelemetry.NewTelemetry("curve", testChainID)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}

	// stateRowsReturn=0 simulates the idempotent ON CONFLICT DO NOTHING no-op.
	repo := &fakeCurveRepo{stateRowsReturn: 0}
	eventRepo := &fakeEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: 1, // force a snapshot
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Telemetry:   tel,
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	bh := c.BlockHandler()
	if err := bh(context.Background(), blockEvent(100), nil); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if got := stateRowsWritten(t, &rm); got != 0 {
		t.Errorf("state_rows_written = %d, want 0 (must reflect actual rows affected, not snapshot-set size)", got)
	}
}

// TestCurveService_HandlerError_RecordsErrorMetric: an error on a handler path
// that is not one of the individually-instrumented stages (here an invalid log
// address surfaced by poolsTouchedByReceipt) must still increment
// curve_errors_total, so VectorCurveIndexerErrorsHigh observes every
// poison-stall path, not only the decode/snapshot/persist ones.
func TestCurveService_HandlerError_RecordsErrorMetric(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})

	tel, err := dextelemetry.NewTelemetry("curve", testChainID)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}

	repo := &fakeCurveRepo{stateRowsReturn: 1}
	writer := dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{})
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
		Telemetry:   tel,
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	// A log whose address is not a valid hex address: poolsTouchedByReceipt errors
	// before any per-stage RecordError runs.
	badReceipt := shared.TransactionReceipt{
		Logs: []shared.Log{{
			Address:         "0x123", // too short to be a valid address
			Topics:          []string{"0xdeadbeef"},
			Data:            "0x",
			TransactionHash: "0xabc",
			LogIndex:        "0x0",
		}},
		TransactionHash: "0xabc",
	}

	bh := c.BlockHandler()
	if err := bh(context.Background(), blockEvent(100), []shared.TransactionReceipt{badReceipt}); err == nil {
		t.Fatal("expected non-nil error from BlockHandler on invalid log address")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if got := curveErrorsTotal(t, &rm); got != 1 {
		t.Errorf("curve.errors.total = %d, want 1 (every handler error path must record the metric)", got)
	}
}

// TestCurveService_DecodeError_ReturnsNonNil: a receipt with corrupt event data
// causes BlockHandler to return a non-nil error so the SQS message redelivers.
func TestCurveService_DecodeError_ReturnsNonNil(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}

	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
	}

	repo := &fakeCurveRepo{stateRowsReturn: 1}
	eventRepo := &fakeEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: 0,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	// Build a receipt with a known-event topic but truncated data so decode fails.
	ev := a.Events["TokenExchange"]
	pool := newTestPool()
	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	log := shared.Log{
		Address: pool.Address.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(common.HexToAddress("0xabc").Bytes()).Hex(),
		},
		Data:            "0xdead", // too short to unpack
		TransactionHash: txHash.Hex(),
		LogIndex:        "0x0",
	}
	badReceipt := shared.TransactionReceipt{
		Logs:            []shared.Log{log},
		TransactionHash: txHash.Hex(),
	}

	bh := c.BlockHandler()
	if err := bh(context.Background(), blockEvent(50), []shared.TransactionReceipt{badReceipt}); err == nil {
		t.Fatal("expected non-nil error from BlockHandler on decode failure")
	}
}

// TestCurveService_ReorgBlock_Resnapshots: when the same block number arrives
// with a different version (reorg), the pool is re-snapshotted even though
// bn == lastBn.
func TestCurveService_ReorgBlock_Resnapshots(t *testing.T) {
	c, repo := newTestCurveService(t, 5)
	bh := c.BlockHandler()

	// Block 100 version 0: initial snapshot.
	ev0 := blockEvent(100)
	if err := bh(context.Background(), ev0, nil); err != nil {
		t.Fatalf("BlockHandler v0: %v", err)
	}
	after0 := repo.stableswapSaves

	// Block 100 version 1 (reorg): same bn, new version -> must re-snapshot.
	ev1 := blockEvent(100)
	ev1.Version = 1
	if err := bh(context.Background(), ev1, nil); err != nil {
		t.Fatalf("BlockHandler v1: %v", err)
	}

	if repo.stableswapSaves != after0+1 {
		t.Errorf("snapshots after reorg = %d, want %d (reorg must trigger re-snapshot)", repo.stableswapSaves, after0+1)
	}
}

// TestCurveService_RoutesParameterAndLpEventsIntoBlockWrites verifies that a
// receipt carrying a parameter event (RampA) and an LP-token Transfer is routed
// into BlockWrites.ParameterEvents and BlockWrites.LpTokenEvents.
func TestCurveService_RoutesParameterAndLpEventsIntoBlockWrites(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}
	repo := &fakeCurveRepo{stateRowsReturn: 1}
	writer := dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{})
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}
	pool := newTestPool()

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{pool},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: 0,
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	rampLog := buildEventLog(t, a, "RampA", pool.Address, nil,
		big.NewInt(20000), big.NewInt(90000), big.NewInt(100), big.NewInt(200))
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	transferLog := buildEventLog(t, a, "Transfer", pool.Address,
		[]common.Hash{addrTopic(from), addrTopic(to)}, big.NewInt(500))
	transferLog.LogIndex = "0x6"

	receipt := shared.TransactionReceipt{
		Logs:            []shared.Log{rampLog, transferLog},
		TransactionHash: rampLog.TransactionHash,
	}

	bh := c.BlockHandler()
	event := blockEvent(800)
	if err := bh(context.Background(), event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if len(repo.lastWrites.ParameterEvents) != 1 {
		t.Errorf("ParameterEvents = %d, want 1", len(repo.lastWrites.ParameterEvents))
	}
	if len(repo.lastWrites.LpTokenEvents) != 1 {
		t.Errorf("LpTokenEvents = %d, want 1", len(repo.lastWrites.LpTokenEvents))
	}
	if len(repo.lastWrites.ParameterEvents) == 1 && repo.lastWrites.ParameterEvents[0].EventName != "ramp_a" {
		t.Errorf("parameter event_name = %q, want ramp_a", repo.lastWrites.ParameterEvents[0].EventName)
	}
}

// TestCurveService_RoutesLpTokenLogOnSeparateAddressToPool: a pre-NG pool whose
// LP token is a SEPARATE contract emits an LP Transfer on the LP-token address
// (not the pool address). The coordinator must route that log to the owning pool
// so the LP event is attributed to the pool's curve_pool_id, while the capture
// row keeps the actual emitting address (the LP-token contract).
func TestCurveService_RoutesLpTokenLogOnSeparateAddressToPool(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}
	repo := &fakeCurveRepo{stateRowsReturn: 1}
	eventRepo := &capturingEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}
	pool := newTestPoolWithLpToken()

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{pool},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   &fakeTxManager{},
		SweepBlocks: 0, // only a touched pool should snapshot
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	// Transfer emitted on the SEPARATE LP-token contract address.
	transferLog := buildEventLog(t, a, "Transfer", *pool.LpTokenAddress,
		[]common.Hash{addrTopic(from), addrTopic(to)}, big.NewInt(500))
	transferLog.LogIndex = "0x4"
	receipt := shared.TransactionReceipt{
		Logs:            []shared.Log{transferLog},
		TransactionHash: transferLog.TransactionHash,
	}

	bh := c.BlockHandler()
	if err := bh(context.Background(), blockEvent(810), []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if len(repo.lastWrites.LpTokenEvents) != 1 {
		t.Fatalf("LpTokenEvents = %d, want 1 (LP transfer on separate LP-token address must route to pool)", len(repo.lastWrites.LpTokenEvents))
	}
	if got := repo.lastWrites.LpTokenEvents[0].CurvePoolID; got != pool.ID {
		t.Errorf("LP token event curve_pool_id = %d, want %d", got, pool.ID)
	}

	// The pool was touched only by an LP-token log; it must still be snapshotted.
	if repo.stableswapSaves != 1 {
		t.Errorf("stableswap snapshots = %d, want 1 (pool touched by LP-token log must snapshot)", repo.stableswapSaves)
	}

	// The capture row must keep the actual emitting address (the LP-token
	// contract), not the pool address.
	if len(eventRepo.events) != 1 {
		t.Fatalf("captured events = %d, want 1", len(eventRepo.events))
	}
	gotAddr := common.BytesToAddress(eventRepo.events[0].ContractAddress)
	if gotAddr != *pool.LpTokenAddress {
		t.Errorf("captured event contract address = %s, want %s (LP-token contract, not pool %s)",
			gotAddr, pool.LpTokenAddress, pool.Address)
	}
}

// TestCurveService_RoutesStableswapConfigIntoBlockWrites verifies that the
// stableswap snapshot's config is routed into BlockWrites.StableswapConfigs.
func TestCurveService_RoutesStableswapConfigIntoBlockWrites(t *testing.T) {
	c, repo := newTestCurveService(t, 0)
	bh := c.BlockHandler()

	if err := bh(context.Background(), blockEvent(900), []shared.TransactionReceipt{swapReceipt(t)}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}
	if len(repo.lastWrites.StableswapConfigs) != 1 {
		t.Errorf("StableswapConfigs = %d, want 1 (touched-pool snapshot builds a config)", len(repo.lastWrites.StableswapConfigs))
	}
}

// curveErrorsTotal reads the curve.errors.total counter total across all
// operation labels, returning 0 if the metric was never recorded.
func curveErrorsTotal(t *testing.T, rm *metricdata.ResourceMetrics) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "curve.errors.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("curve.errors.total: unexpected metric type %T", m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

// stateRowsWritten reads the curve.state.rows.written counter total, returning 0
// if the metric was never recorded.
func stateRowsWritten(t *testing.T, rm *metricdata.ResourceMetrics) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "curve.state.rows.written" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("curve.state.rows.written: unexpected metric type %T", m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

// TestNewCurveService_WarmsHandlersForRegisteredPoolCoinCounts verifies the
// constructor primes each handler's per-coin-count cache for every registered
// pool, so the per-block decode path performs no lazy cache writes.
func TestNewCurveService_WarmsHandlersForRegisteredPoolCoinCounts(t *testing.T) {
	h := &nilNilHandler{}
	pool2 := newTestPool() // 2-coin pre-NG
	pool3 := newTestPool()
	pool3.ID = 777
	pool3.NCoins = 3
	pool3.Address = common.HexToAddress("0x0000000000000000000000000000000000000003")

	_, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{pool2, pool3},
		Handlers:    map[PoolKind]PoolClassHandler{KindStableswapPreNG: h},
		Multicaller: &fakeMulticaller{},
		Repo:        &fakeCurveRepo{},
		EventWriter: dexconsumer.NewProtocolEventWriter(1, &fakeEventRepo{}),
		TxManager:   &fakeTxManager{},
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	got := map[int]bool{}
	for _, n := range h.warmed {
		got[n] = true
	}
	for _, want := range []int{2, 3} {
		if !got[want] {
			t.Errorf("handler not warmed for nCoins=%d; warmed=%v", want, h.warmed)
		}
	}
}

// nilNilHandler is a PoolClassHandler stub that returns StateSnapshot with both
// Stableswap and Cryptoswap as nil, to test the default case error handling.
type nilNilHandler struct{ warmed []int }

func (h *nilNilHandler) Warm(nCoins int) { h.warmed = append(h.warmed, nCoins) }

func (h *nilNilHandler) DecodeEvents(receipt shared.TransactionReceipt, pool RegisteredPool, chainID, blockNumber int64, version int, ts time.Time) (DecodedEvents, error) {
	return DecodedEvents{}, nil
}

func (h *nilNilHandler) SnapshotState(ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, blockNumber int64, version int, blockHash common.Hash, ts time.Time) (StateSnapshot, error) {
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

// countingTxManager delegates to a real fakeTxManager but increments a counter
// each time WithTransaction is called, so tests can assert it was (or was not) invoked.
type countingTxManager struct {
	calls int
	err   error // if non-nil, returned on the FIRST call then cleared
}

func (m *countingTxManager) WithTransaction(ctx context.Context, fn func(pgx.Tx) error) error {
	m.calls++
	if m.err != nil {
		err := m.err
		m.err = nil
		return err
	}
	return fn(nil)
}

// ---------------------------------------------------------------------------
// Fix 3: quiet-block early-return must not open a transaction
// ---------------------------------------------------------------------------

// TestCurveService_QuietBlock_NoTransaction: a block with receipts that contain
// no logs for any registered pool must return nil and must NOT call
// WithTransaction (the quiet-block early-return saves the empty DB round-trip).
func TestCurveService_QuietBlock_NoTransaction(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	stable := NewStableswapHandler(a)
	handlers := map[PoolKind]PoolClassHandler{
		KindStableswapPreNG: stable,
		KindStableswapNG:    stable,
	}

	repo := &fakeCurveRepo{stateRowsReturn: 1}
	eventRepo := &fakeEventRepo{}
	writer := dexconsumer.NewProtocolEventWriter(1, eventRepo)
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}
	txMgr := &countingTxManager{}

	c, err := NewCurveService(CurveServiceDeps{
		Pools:       []RegisteredPool{newTestPool()},
		Handlers:    handlers,
		Multicaller: mc,
		Repo:        repo,
		EventWriter: writer,
		TxManager:   txMgr,
		SweepBlocks: 0, // no sweep -> no snapshot on quiet block
		ChainID:     testChainID,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}

	// A receipt whose log is from an unrelated address (not a registered pool).
	unrelatedLog := shared.Log{
		Address:         "0x0000000000000000000000000000000000001234",
		Topics:          []string{"0xdeadbeef"},
		Data:            "0x",
		TransactionHash: "0xdeadbeef",
		LogIndex:        "0x0",
	}
	quietReceipt := shared.TransactionReceipt{
		Logs:            []shared.Log{unrelatedLog},
		TransactionHash: "0xdeadbeef",
	}

	bh := c.BlockHandler()
	if err := bh(context.Background(), blockEvent(500), []shared.TransactionReceipt{quietReceipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	if txMgr.calls != 0 {
		t.Errorf("WithTransaction called %d time(s), want 0 (quiet block must skip the transaction)", txMgr.calls)
	}
	if repo.swapSaves != 0 {
		t.Errorf("swapSaves = %d, want 0", repo.swapSaves)
	}
	if repo.stableswapSaves != 0 {
		t.Errorf("stableswapSaves = %d, want 0", repo.stableswapSaves)
	}
}

// ---------------------------------------------------------------------------
// Fix 4: transient tx error then retry must persist exactly once, not double
// ---------------------------------------------------------------------------

// TestCurveService_TxErrorThenRetry_PersistsOnce: on first call WithTransaction
// returns an error (transient DB failure); BlockHandler must return non-nil and
// nothing is persisted. On second call (SQS redelivery) the tx succeeds; exactly
// one swap is saved, not two.
func TestCurveService_TxErrorThenRetry_PersistsOnce(t *testing.T) {
	c, repo := newTestCurveService(t, 0)

	txMgr := &countingTxManager{err: fmt.Errorf("transient DB failure")}
	c.txMgr = txMgr

	bh := c.BlockHandler()
	receipt := swapReceipt(t)
	event := blockEvent(700)

	// First call: WithTransaction errors.
	if err := bh(context.Background(), event, []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("expected non-nil error from first BlockHandler call (transient tx failure)")
	}
	if repo.swapSaves != 0 {
		t.Errorf("swapSaves after first (failed) call = %d, want 0", repo.swapSaves)
	}

	// Second call: WithTransaction succeeds; must save exactly once.
	if err := bh(context.Background(), event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler second call: %v", err)
	}
	if repo.swapSaves != 1 {
		t.Errorf("swapSaves after retry = %d, want 1 (no doubling from the failed first attempt)", repo.swapSaves)
	}
}
