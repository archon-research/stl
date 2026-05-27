package curve_dex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// -----------------------------------------------------------------------------
// MockCurvePoolRepository: function-pointer mock for outbound.CurvePoolRepository.
// -----------------------------------------------------------------------------

type MockCurvePoolRepository struct {
	GetCurvePoolFn                func(ctx context.Context, chainID int64, address common.Address) (*entity.CurvePool, error)
	ListEnabledCurvePoolsFn       func(ctx context.Context, chainID int64) ([]*entity.CurvePool, error)
	GetCurveGaugeFn               func(ctx context.Context, curvePoolID int64) (*entity.CurveGauge, error)
	UpsertCurveGaugeFn            func(ctx context.Context, tx pgx.Tx, gauge *entity.CurveGauge) (int64, error)
	SetCurveGaugeKilledFn         func(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, isKilled bool) error
	SaveCurvePoolStateFn          func(ctx context.Context, tx pgx.Tx, state *entity.CurvePoolState) error
	SaveCurvePoolSwapFn           func(ctx context.Context, tx pgx.Tx, swap *entity.CurvePoolSwap) error
	SaveCurvePoolLiquidityEventFn func(ctx context.Context, tx pgx.Tx, evt *entity.CurvePoolLiquidityEvent) error
	SaveCurvePoolParameterEventFn func(ctx context.Context, tx pgx.Tx, evt *entity.CurvePoolParameterEvent) error
	SaveCurvePoolExchangeRateFn   func(ctx context.Context, tx pgx.Tx, rate *entity.CurvePoolExchangeRate) error
	SaveCurveGaugeStateFn         func(ctx context.Context, tx pgx.Tx, state *entity.CurveGaugeState) error
	SaveCurveUserLpPositionFn     func(ctx context.Context, tx pgx.Tx, pos *entity.CurveUserLpPosition) error
}

func (m *MockCurvePoolRepository) GetCurvePool(ctx context.Context, chainID int64, address common.Address) (*entity.CurvePool, error) {
	if m.GetCurvePoolFn != nil {
		return m.GetCurvePoolFn(ctx, chainID, address)
	}
	return nil, nil
}
func (m *MockCurvePoolRepository) ListEnabledCurvePools(ctx context.Context, chainID int64) ([]*entity.CurvePool, error) {
	if m.ListEnabledCurvePoolsFn != nil {
		return m.ListEnabledCurvePoolsFn(ctx, chainID)
	}
	return nil, nil
}
func (m *MockCurvePoolRepository) GetCurveGauge(ctx context.Context, curvePoolID int64) (*entity.CurveGauge, error) {
	if m.GetCurveGaugeFn != nil {
		return m.GetCurveGaugeFn(ctx, curvePoolID)
	}
	return nil, nil
}
func (m *MockCurvePoolRepository) UpsertCurveGauge(ctx context.Context, tx pgx.Tx, gauge *entity.CurveGauge) (int64, error) {
	if m.UpsertCurveGaugeFn != nil {
		return m.UpsertCurveGaugeFn(ctx, tx, gauge)
	}
	return 1, nil
}
func (m *MockCurvePoolRepository) SetCurveGaugeKilled(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, isKilled bool) error {
	if m.SetCurveGaugeKilledFn != nil {
		return m.SetCurveGaugeKilledFn(ctx, tx, chainID, address, isKilled)
	}
	return nil
}
func (m *MockCurvePoolRepository) SaveCurvePoolState(ctx context.Context, tx pgx.Tx, state *entity.CurvePoolState) error {
	if m.SaveCurvePoolStateFn != nil {
		return m.SaveCurvePoolStateFn(ctx, tx, state)
	}
	return nil
}
func (m *MockCurvePoolRepository) SaveCurvePoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.CurvePoolSwap) error {
	if m.SaveCurvePoolSwapFn != nil {
		return m.SaveCurvePoolSwapFn(ctx, tx, swap)
	}
	return nil
}
func (m *MockCurvePoolRepository) SaveCurvePoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.CurvePoolLiquidityEvent) error {
	if m.SaveCurvePoolLiquidityEventFn != nil {
		return m.SaveCurvePoolLiquidityEventFn(ctx, tx, evt)
	}
	return nil
}
func (m *MockCurvePoolRepository) SaveCurvePoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.CurvePoolParameterEvent) error {
	if m.SaveCurvePoolParameterEventFn != nil {
		return m.SaveCurvePoolParameterEventFn(ctx, tx, evt)
	}
	return nil
}
func (m *MockCurvePoolRepository) SaveCurvePoolExchangeRate(ctx context.Context, tx pgx.Tx, rate *entity.CurvePoolExchangeRate) error {
	if m.SaveCurvePoolExchangeRateFn != nil {
		return m.SaveCurvePoolExchangeRateFn(ctx, tx, rate)
	}
	return nil
}
func (m *MockCurvePoolRepository) SaveCurveGaugeState(ctx context.Context, tx pgx.Tx, state *entity.CurveGaugeState) error {
	if m.SaveCurveGaugeStateFn != nil {
		return m.SaveCurveGaugeStateFn(ctx, tx, state)
	}
	return nil
}
func (m *MockCurvePoolRepository) SaveCurveUserLpPosition(ctx context.Context, tx pgx.Tx, pos *entity.CurveUserLpPosition) error {
	if m.SaveCurveUserLpPositionFn != nil {
		return m.SaveCurveUserLpPositionFn(ctx, tx, pos)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Test harness
// -----------------------------------------------------------------------------

type harness struct {
	t            *testing.T
	svc          *Service
	curveRepo    *MockCurvePoolRepository
	tokenRepo    *testutil.MockTokenRepository
	protocolRepo *testutil.MockProtocolRepository
	eventRepo    *testutil.MockEventRepository
	multicaller  *testutil.MockMulticaller
	txManager    *testutil.MockTxManager
	consumer     *testutil.MockSQSConsumer
	cache        *testutil.MockBlockCache

	v1EventsABI *abi.ABI
	ngEventsABI *abi.ABI
	lpEventsABI *abi.ABI
	gaugeABI    *abi.ABI
	gcABI       *abi.ABI

	v1ReadABI    *abi.ABI
	ngReadABI    *abi.ABI
	lpReadABI    *abi.ABI
	gaugeReadABI *abi.ABI
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	curveRepo := &MockCurvePoolRepository{}
	tokenRepo := &testutil.MockTokenRepository{}
	protocolRepo := &testutil.MockProtocolRepository{}
	eventRepo := &testutil.MockEventRepository{}
	mc := testutil.NewMockMulticaller()
	txManager := &testutil.MockTxManager{}
	consumer := &testutil.MockSQSConsumer{}
	cache := testutil.NewMockBlockCache()

	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	cfg.ChainID = 1

	svc, err := NewService(cfg, consumer, cache, mc, txManager, curveRepo, tokenRepo, protocolRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	v1ev, err := abis.GetCurveStableswapV1EventsABI()
	if err != nil {
		t.Fatalf("v1 events ABI: %v", err)
	}
	ngev, err := abis.GetCurveStableswapNGEventsABI()
	if err != nil {
		t.Fatalf("ng events ABI: %v", err)
	}
	lpev, err := abis.GetCurveLPTokenEventsABI()
	if err != nil {
		t.Fatalf("lp events ABI: %v", err)
	}
	gev, err := abis.GetCurveGaugeEventsABI()
	if err != nil {
		t.Fatalf("gauge events ABI: %v", err)
	}
	gcev, err := abis.GetCurveGaugeControllerEventsABI()
	if err != nil {
		t.Fatalf("gauge controller events ABI: %v", err)
	}
	v1rd, err := abis.GetCurveStableswapV1ReadABI()
	if err != nil {
		t.Fatalf("v1 read ABI: %v", err)
	}
	ngrd, err := abis.GetCurveStableswapNGReadABI()
	if err != nil {
		t.Fatalf("ng read ABI: %v", err)
	}
	lprd, err := abis.GetCurveLPTokenReadABI()
	if err != nil {
		t.Fatalf("lp read ABI: %v", err)
	}
	gaugeRd, err := abis.GetCurveGaugeReadABI()
	if err != nil {
		t.Fatalf("gauge read ABI: %v", err)
	}
	return &harness{
		t:            t,
		svc:          svc,
		curveRepo:    curveRepo,
		tokenRepo:    tokenRepo,
		protocolRepo: protocolRepo,
		eventRepo:    eventRepo,
		multicaller:  mc,
		txManager:    txManager,
		consumer:     consumer,
		cache:        cache,
		v1EventsABI:  v1ev,
		ngEventsABI:  ngev,
		lpEventsABI:  lpev,
		gaugeABI:     gev,
		gcABI:        gcev,
		v1ReadABI:    v1rd,
		ngReadABI:    ngrd,
		lpReadABI:    lprd,
		gaugeReadABI: gaugeRd,
	}
}

// -----------------------------------------------------------------------------
// Helpers to build logs + multicall results.
// -----------------------------------------------------------------------------

var (
	testPoolV1Addr  = common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022")
	testLPTokenAddr = common.HexToAddress("0x06325440D014e39736583c165C2963BA99fAf14E")
	testPoolNGAddr  = common.HexToAddress("0x21E27a5E5513D6e65C4f830167390997aA84843a")
	testGaugeAddr   = common.HexToAddress("0xAaaaaAAAaaaaAaAAaAaAAaAaAaaaAAaaAAaa0000")
	testUser        = common.HexToAddress("0x9999999999999999999999999999999999999999")
	testTxHash      = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func makePoolV1() *entity.CurvePool {
	lp := testLPTokenAddr
	return &entity.CurvePool{
		ID:             1,
		ChainID:        1,
		PoolKind:       entity.CurvePoolKindV1,
		Address:        testPoolV1Addr,
		LPTokenAddress: &lp,
		Label:          "stETH-classic",
		NCoins:         2,
		CoinAddresses:  []common.Address{common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"), common.HexToAddress("0xae7ab96520de3a18e5e111b5eaab095312d7fe84")},
		CoinTokenIDs:   []int64{4, 227},
		CoinDecimals:   []int16{18, 18},
		Enabled:        true,
	}
}

func makePoolNG() *entity.CurvePool {
	return &entity.CurvePool{
		ID:           2,
		ChainID:      1,
		PoolKind:     entity.CurvePoolKindNG,
		Address:      testPoolNGAddr,
		Label:        "stETH-ng",
		NCoins:       2,
		CoinDecimals: []int16{18, 18},
		Enabled:      true,
	}
}

func (h *harness) packUint256(v *big.Int) []byte {
	t, _ := abi.NewType("uint256", "", nil)
	args := abi.Arguments{{Type: t}}
	out, err := args.Pack(v)
	if err != nil {
		h.t.Fatalf("packUint256: %v", err)
	}
	return out
}

func (h *harness) packBool(v bool) []byte {
	t, _ := abi.NewType("bool", "", nil)
	args := abi.Arguments{{Type: t}}
	out, err := args.Pack(v)
	if err != nil {
		h.t.Fatalf("packBool: %v", err)
	}
	return out
}

// poolStateResults returns mock results matching readPoolState's call layout
// for a 2-coin V1 pool (balances×2 + virtual_price + A + fee + totalSupply +
// get_dy×2).
func (h *harness) poolStateResultsV1() []outbound.Result {
	return []outbound.Result{
		{Success: true, ReturnData: h.packUint256(big.NewInt(100))}, // balances(0)
		{Success: true, ReturnData: h.packUint256(big.NewInt(200))}, // balances(1)
		{Success: true, ReturnData: h.packUint256(big.NewInt(1e9))}, // get_virtual_price
		{Success: true, ReturnData: h.packUint256(big.NewInt(100))}, // A
		{Success: true, ReturnData: h.packUint256(big.NewInt(4e6))}, // fee
		{Success: true, ReturnData: h.packUint256(big.NewInt(300))}, // totalSupply
		{Success: true, ReturnData: h.packUint256(big.NewInt(99))},  // get_dy(0,1)
		{Success: true, ReturnData: h.packUint256(big.NewInt(101))}, // get_dy(1,0)
	}
}

// gaugeStateResults returns mock results matching readGaugeState's 5 calls.
func (h *harness) gaugeStateResults(rewardCount int64) []outbound.Result {
	return []outbound.Result{
		{Success: true, ReturnData: h.packUint256(big.NewInt(1e15))}, // inflation_rate
		{Success: true, ReturnData: h.packUint256(big.NewInt(500))},  // working_supply
		{Success: true, ReturnData: h.packUint256(big.NewInt(300))},  // totalSupply
		{Success: true, ReturnData: h.packBool(false)},               // is_killed
		{Success: true, ReturnData: h.packUint256(big.NewInt(rewardCount))},
	}
}

// makeTokenExchangeLog builds a V1 TokenExchange log.
func (h *harness) makeTokenExchangeLog(poolAddr common.Address, buyer common.Address, soldID, boughtID int64, sold, bought *big.Int) shared.Log {
	ev := h.v1EventsABI.Events["TokenExchange"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(soldID), sold, big.NewInt(boughtID), bought)
	if err != nil {
		h.t.Fatalf("packing TokenExchange data: %v", err)
	}
	return shared.Log{
		Address: poolAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(buyer.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x1",
	}
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

func TestConfigDefaults(t *testing.T) {
	defaults := ConfigDefaults()
	if defaults.MaxMessages != 10 {
		t.Errorf("MaxMessages = %d, want 10", defaults.MaxMessages)
	}
	if defaults.PollInterval == 0 {
		t.Error("PollInterval should not be zero")
	}
	if defaults.Logger == nil {
		t.Error("Logger should not be nil")
	}
}

func TestNewService_ValidateDependencies(t *testing.T) {
	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	cfg.ChainID = 1
	rc := testutil.NewMockBlockCache()
	mc := testutil.NewMockMulticaller()
	tm := &testutil.MockTxManager{}
	cr := &MockCurvePoolRepository{}
	tr := &testutil.MockTokenRepository{}
	pr := &testutil.MockProtocolRepository{}
	er := &testutil.MockEventRepository{}
	cons := &testutil.MockSQSConsumer{}

	cases := []struct {
		name        string
		consumer    outbound.SQSConsumer
		cache       outbound.BlockCacheReader
		mc          outbound.Multicaller
		txm         outbound.TxManager
		curveRepo   outbound.CurvePoolRepository
		tokenRepo   outbound.TokenRepository
		proto       outbound.ProtocolRepository
		evt         outbound.EventRepository
		wantErrSub  string
		wantSuccess bool
	}{
		{"nil consumer", nil, rc, mc, tm, cr, tr, pr, er, "consumer", false},
		{"nil cache", cons, nil, mc, tm, cr, tr, pr, er, "cache", false},
		{"nil multicaller", cons, rc, nil, tm, cr, tr, pr, er, "multicaller", false},
		{"nil txm", cons, rc, mc, nil, cr, tr, pr, er, "txManager", false},
		{"nil curveRepo", cons, rc, mc, tm, nil, tr, pr, er, "curveRepo", false},
		{"nil tokenRepo", cons, rc, mc, tm, cr, nil, pr, er, "tokenRepo", false},
		{"nil protocol", cons, rc, mc, tm, cr, tr, nil, er, "protocolRepo", false},
		{"nil event", cons, rc, mc, tm, cr, tr, pr, nil, "eventRepo", false},
		{"all valid", cons, rc, mc, tm, cr, tr, pr, er, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewService(cfg, c.consumer, c.cache, c.mc, c.txm, c.curveRepo, c.tokenRepo, c.proto, c.evt)
			if c.wantSuccess {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.wantErrSub)
			}
			if !strings.Contains(err.Error(), c.wantErrSub) {
				t.Fatalf("error %q does not contain %q", err.Error(), c.wantErrSub)
			}
		})
	}
}

func TestNewService_RejectsZeroChainID(t *testing.T) {
	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	_, err := NewService(cfg, &testutil.MockSQSConsumer{}, testutil.NewMockBlockCache(), testutil.NewMockMulticaller(), &testutil.MockTxManager{}, &MockCurvePoolRepository{}, &testutil.MockTokenRepository{}, &testutil.MockProtocolRepository{}, &testutil.MockEventRepository{})
	if err == nil {
		t.Fatal("expected error for zero ChainID")
	}
	if !strings.Contains(err.Error(), "ChainID") {
		t.Errorf("error %q does not mention ChainID", err.Error())
	}
}

func TestStartLoadsPoolsAndGauges(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	gauge := &entity.CurveGauge{ID: 9, CurvePoolID: pool.ID, ChainID: 1, Address: testGaugeAddr, Enabled: true}

	h.curveRepo.ListEnabledCurvePoolsFn = func(ctx context.Context, chainID int64) ([]*entity.CurvePool, error) {
		return []*entity.CurvePool{pool}, nil
	}
	h.curveRepo.GetCurveGaugeFn = func(ctx context.Context, poolID int64) (*entity.CurveGauge, error) {
		if poolID == pool.ID {
			return gauge, nil
		}
		return nil, nil
	}

	if err := h.svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.svc.Stop()

	if got := h.svc.registry.poolCount(); got != 1 {
		t.Errorf("poolCount = %d, want 1", got)
	}
	if got := h.svc.registry.gaugeCount(); got != 1 {
		t.Errorf("gaugeCount = %d, want 1", got)
	}
	if h.svc.registry.poolByAddress(pool.Address) == nil {
		t.Error("pool not registered by address")
	}
	if h.svc.registry.gaugeByPoolID(pool.ID) == nil {
		t.Error("gauge not registered by pool ID")
	}
}

// Start should bootstrap gauges from MetaRegistry.get_gauge for any tracked
// pool that doesn't yet have a curve_gauge row in the DB. This catches the
// canonical mainnet gauges that were deployed before our event subscription
// window — without it, the gauge registry starts empty.
func TestStartBootstrapsGaugesFromMetaRegistry(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	gaugeAddr := common.HexToAddress("0xAbCdEf0123456789abcDef0123456789ABcDEF01")

	h.curveRepo.ListEnabledCurvePoolsFn = func(_ context.Context, _ int64) ([]*entity.CurvePool, error) {
		return []*entity.CurvePool{pool}, nil
	}
	h.curveRepo.GetCurveGaugeFn = func(_ context.Context, _ int64) (*entity.CurveGauge, error) {
		// No gauge yet — must be bootstrapped from MetaRegistry.
		return nil, nil
	}

	var upserted *entity.CurveGauge
	h.curveRepo.UpsertCurveGaugeFn = func(_ context.Context, _ pgx.Tx, g *entity.CurveGauge) (int64, error) {
		upserted = g
		return 1, nil
	}

	// Multicaller mock: MetaRegistry.get_gauge(pool) returns the gauge address.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		out := make([]outbound.Result, len(calls))
		for i := range calls {
			out[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(gaugeAddr.Bytes(), 32)}
		}
		return out, nil
	}

	if err := h.svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.svc.Stop()

	if upserted == nil {
		t.Fatal("MetaRegistry-bootstrapped gauge was not upserted into curve_gauge")
	}
	if upserted.Address != gaugeAddr {
		t.Errorf("upserted gauge address = %s, want %s", upserted.Address.Hex(), gaugeAddr.Hex())
	}
	if upserted.CurvePoolID != pool.ID {
		t.Errorf("upserted gauge pool id = %d, want %d", upserted.CurvePoolID, pool.ID)
	}
	if h.svc.registry.gaugeByPoolID(pool.ID) == nil {
		t.Error("bootstrapped gauge not added to in-process registry")
	}
}

// MetaRegistry.get_gauge returning the zero address means the pool has no
// gauge — the bootstrap must skip it without raising or upserting.
func TestStartSkipsBootstrapWhenMetaRegistryReturnsZero(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()

	h.curveRepo.ListEnabledCurvePoolsFn = func(_ context.Context, _ int64) ([]*entity.CurvePool, error) {
		return []*entity.CurvePool{pool}, nil
	}
	h.curveRepo.GetCurveGaugeFn = func(_ context.Context, _ int64) (*entity.CurveGauge, error) {
		return nil, nil
	}
	upsertCalled := false
	h.curveRepo.UpsertCurveGaugeFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGauge) (int64, error) {
		upsertCalled = true
		return 0, nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		out := make([]outbound.Result, len(calls))
		zero := common.Address{}
		for i := range calls {
			out[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(zero.Bytes(), 32)}
		}
		return out, nil
	}

	if err := h.svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.svc.Stop()

	if upsertCalled {
		t.Error("MetaRegistry returned zero address but UpsertCurveGauge was still called")
	}
	if h.svc.registry.gaugeByPoolID(pool.ID) != nil {
		t.Error("gauge registered despite MetaRegistry returning zero")
	}
}

// B5: a single MetaRegistry.get_gauge probe failure (RPC revert, transient
// network error, MetaRegistry rejecting an exotic factory pool) must NOT
// fail-stop the entire worker. The runtime NewGauge listener will catch
// future gauges; the bootstrap can be retried on next restart. Tanking the
// whole worker because one pool's optional bootstrap probe errored is a
// disproportionate response and the reason production ops have had to
// manually intervene before.
func TestStartContinuesWhenSingleMetaRegistryBootstrapFails(t *testing.T) {
	h := newHarness(t)
	poolA := makePoolV1()
	poolB := makePoolNG()
	goodGauge := common.HexToAddress("0xAbCdEf0123456789abcDef0123456789ABcDEF02")

	h.curveRepo.ListEnabledCurvePoolsFn = func(_ context.Context, _ int64) ([]*entity.CurvePool, error) {
		return []*entity.CurvePool{poolA, poolB}, nil
	}
	h.curveRepo.GetCurveGaugeFn = func(_ context.Context, _ int64) (*entity.CurveGauge, error) {
		return nil, nil
	}

	var upserted *entity.CurveGauge
	h.curveRepo.UpsertCurveGaugeFn = func(_ context.Context, _ pgx.Tx, g *entity.CurveGauge) (int64, error) {
		upserted = g
		return 7, nil
	}

	// Multicaller fans out two get_gauge calls (one per pool). Mock the first
	// to revert (simulating MetaRegistry not knowing about a pool) and the
	// second to succeed with goodGauge.
	callIdx := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callIdx++
		if callIdx == 1 {
			out := make([]outbound.Result, len(calls))
			for i := range calls {
				out[i] = outbound.Result{Success: false, ReturnData: nil}
			}
			return out, nil
		}
		out := make([]outbound.Result, len(calls))
		for i := range calls {
			out[i] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(goodGauge.Bytes(), 32)}
		}
		return out, nil
	}

	if err := h.svc.Start(context.Background()); err != nil {
		t.Fatalf("Start should not fail when a single bootstrap probe errors: %v", err)
	}
	defer h.svc.Stop()

	if upserted == nil {
		t.Fatal("expected the second pool's MetaRegistry-bootstrapped gauge to be upserted despite the first pool's failure")
	}
	if upserted.Address != goodGauge {
		t.Errorf("upserted gauge = %s, want %s", upserted.Address.Hex(), goodGauge.Hex())
	}
	if h.svc.registry.gaugeByPoolID(poolB.ID) == nil {
		t.Error("poolB's gauge not registered after partial-failure bootstrap")
	}
	if h.svc.registry.gaugeByPoolID(poolA.ID) != nil {
		t.Error("poolA's bootstrap failed, expected no gauge registered for it")
	}
}

func TestProcessReceipt_TokenExchange_WritesSwapStateAndExchangeRates(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var savedSwap *entity.CurvePoolSwap
	var savedState *entity.CurvePoolState
	var savedRates []*entity.CurvePoolExchangeRate
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, swap *entity.CurvePoolSwap) error {
		savedSwap = swap
		return nil
	}
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolState) error {
		savedState = s
		return nil
	}
	h.curveRepo.SaveCurvePoolExchangeRateFn = func(_ context.Context, _ pgx.Tx, r *entity.CurvePoolExchangeRate) error {
		savedRates = append(savedRates, r)
		return nil
	}

	var savedEvent *entity.ProtocolEvent
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
		savedEvent = e
		return nil
	}

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs:            []shared.Log{log},
	}
	body, err := json.Marshal([]shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	h.cache.SetReceipts(1, 100, 0, body)

	if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    100,
		Version:        0,
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	if savedSwap == nil {
		t.Fatal("swap row not persisted")
	}
	if savedSwap.SoldID != 0 || savedSwap.BoughtID != 1 {
		t.Errorf("swap ids = (%d,%d), want (0,1)", savedSwap.SoldID, savedSwap.BoughtID)
	}
	if savedSwap.TokensSold.Int64() != 50 {
		t.Errorf("tokens sold = %s, want 50", savedSwap.TokensSold)
	}
	if savedState == nil {
		t.Fatal("pool state row not persisted")
	}
	if savedState.Source != entity.CurvePoolStateSourceEvent {
		t.Errorf("state source = %q, want %q", savedState.Source, entity.CurvePoolStateSourceEvent)
	}
	if len(savedState.Balances) != 2 || savedState.Balances[0].Int64() != 100 {
		t.Errorf("balances = %v, want [100, 200]", savedState.Balances)
	}
	if len(savedRates) != 2 {
		t.Errorf("expected 2 exchange-rate rows, got %d", len(savedRates))
	}
	if savedEvent == nil {
		t.Fatal("protocol_event audit row not persisted")
	}
	if savedEvent.EventName != string(EventTokenExchange) {
		t.Errorf("event_name = %q, want %q", savedEvent.EventName, EventTokenExchange)
	}
}

func TestProcessReceipt_GaugeControllerNewGauge_RegistersGauge(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	newGaugeAddr := common.HexToAddress("0xb000000000000000000000000000000000000001")
	ev := h.gcABI.Events["NewGauge"]
	data, err := ev.Inputs.NonIndexed().Pack(newGaugeAddr, big.NewInt(0), big.NewInt(0))
	if err != nil {
		t.Fatalf("pack NewGauge: %v", err)
	}
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address:         CurveGaugeControllerAddress.Hex(),
			Topics:          []string{ev.ID.Hex()},
			Data:            "0x" + common.Bytes2Hex(data),
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})
	h.cache.SetReceipts(1, 200, 0, body)

	// Multicall mock: first call is gauge_types (must return 0 = stableswap),
	// second is lp_token (returns the V1 pool's LP-token address).
	addrT, _ := abi.NewType("address", "", nil)
	int128T, _ := abi.NewType("int128", "", nil)
	gaugeTypesOut, _ := (abi.Arguments{{Type: int128T}}).Pack(big.NewInt(0))
	lpTokenOut, err := (abi.Arguments{{Type: addrT}}).Pack(testLPTokenAddr)
	if err != nil {
		t.Fatalf("pack lp_token: %v", err)
	}
	calls := 0
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		calls++
		switch calls {
		case 1:
			return []outbound.Result{{Success: true, ReturnData: gaugeTypesOut}}, nil
		case 2:
			return []outbound.Result{{Success: true, ReturnData: lpTokenOut}}, nil
		default:
			return nil, nil
		}
	}

	var saved *entity.CurveGauge
	h.curveRepo.UpsertCurveGaugeFn = func(_ context.Context, _ pgx.Tx, g *entity.CurveGauge) (int64, error) {
		saved = g
		return 42, nil
	}

	if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    200,
		Version:        0,
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	if saved == nil {
		t.Fatal("gauge not upserted")
	}
	if saved.Address != newGaugeAddr {
		t.Errorf("gauge address = %s, want %s", saved.Address.Hex(), newGaugeAddr.Hex())
	}
	if h.svc.registry.gaugeByAddress(newGaugeAddr) == nil {
		t.Error("gauge not added to in-memory registry")
	}
}

// Non-stableswap gauge_types must be skipped BEFORE the lp_token() probe, so
// crypto/tricrypto/lending gauges don't even attempt a revert-prone read.
// We assert that lp_token is never called (the multicaller sees only one call
// for the gauge_types probe) and no curve_gauge row is upserted.
func TestProcessReceipt_NewGauge_NonStableswapType_Skipped(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	newGaugeAddr := common.HexToAddress("0xb000000000000000000000000000000000000002")
	ev := h.gcABI.Events["NewGauge"]
	data, err := ev.Inputs.NonIndexed().Pack(newGaugeAddr, big.NewInt(0), big.NewInt(0))
	if err != nil {
		t.Fatalf("pack NewGauge: %v", err)
	}
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address:         CurveGaugeControllerAddress.Hex(),
			Topics:          []string{ev.ID.Hex()},
			Data:            "0x" + common.Bytes2Hex(data),
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})
	h.cache.SetReceipts(1, 200, 0, body)

	// Compute the expected selector for `gauge_types(address)`.
	gcRead, err := abis.GetCurveGaugeControllerReadABI()
	if err != nil {
		t.Fatalf("loading controller read ABI: %v", err)
	}
	gaugeTypesSel := gcRead.Methods["gauge_types"].ID

	var executeCallsSeen int
	var firstCallSelector []byte
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		executeCallsSeen++
		if executeCallsSeen == 1 && len(calls) > 0 && len(calls[0].CallData) >= 4 {
			firstCallSelector = append([]byte(nil), calls[0].CallData[:4]...)
		}
		// Return type=1 (non-stableswap) — the worker must skip and never re-Execute.
		tType, _ := abi.NewType("int128", "", nil)
		args := abi.Arguments{{Type: tType}}
		out, _ := args.Pack(big.NewInt(1))
		results := make([]outbound.Result, len(calls))
		for i := range calls {
			results[i] = outbound.Result{Success: true, ReturnData: out}
		}
		return results, nil
	}

	upsertCalled := false
	h.curveRepo.UpsertCurveGaugeFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGauge) (int64, error) {
		upsertCalled = true
		return 0, nil
	}

	if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    200,
		Version:        0,
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	if upsertCalled {
		t.Error("UpsertCurveGauge was called for a non-stableswap gauge (type=1)")
	}
	if executeCallsSeen != 1 {
		t.Errorf("multicaller Execute called %d times; expected exactly 1 (gauge_types probe only — no lp_token follow-up)", executeCallsSeen)
	}
	if !bytes.Equal(firstCallSelector, gaugeTypesSel) {
		t.Errorf("first multicall selector = %x, want gauge_types selector %x", firstCallSelector, gaugeTypesSel)
	}
}

func TestProcessReceipt_GaugeControllerKill_FlipsFlag(t *testing.T) {
	h := newHarness(t)
	gauge := &entity.CurveGauge{ID: 7, CurvePoolID: 1, ChainID: 1, Address: testGaugeAddr, Enabled: true}
	h.svc.registry.addGauge(gauge)

	ev := h.gcABI.Events["KillGauge"]
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address: CurveGaugeControllerAddress.Hex(),
			Topics: []string{
				ev.ID.Hex(),
				common.BytesToHash(testGaugeAddr.Bytes()).Hex(),
			},
			Data:            "0x",
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})
	h.cache.SetReceipts(1, 200, 0, body)

	var killed bool
	h.curveRepo.SetCurveGaugeKilledFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, isKilled bool) error {
		if addr == testGaugeAddr && isKilled {
			killed = true
		}
		return nil
	}
	if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    200,
		Version:        0,
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if !killed {
		t.Error("SetCurveGaugeKilled was not called")
	}
}

// UnkillGauge is the reversal of KillGauge on the canonical mainnet
// GaugeController. The worker must flip is_killed back to false so APY
// consumers stop excluding the gauge.
func TestProcessReceipt_GaugeControllerUnkill_FlipsFlagBack(t *testing.T) {
	h := newHarness(t)
	gauge := &entity.CurveGauge{ID: 7, CurvePoolID: 1, ChainID: 1, Address: testGaugeAddr, IsKilled: true, Enabled: true}
	h.svc.registry.addGauge(gauge)

	ev, ok := h.gcABI.Events["UnkillGauge"]
	if !ok {
		t.Fatalf("UnkillGauge event missing from gauge controller ABI")
	}
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address: CurveGaugeControllerAddress.Hex(),
			Topics: []string{
				ev.ID.Hex(),
				common.BytesToHash(testGaugeAddr.Bytes()).Hex(),
			},
			Data:            "0x",
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})
	h.cache.SetReceipts(1, 200, 0, body)

	var captured struct {
		addr     common.Address
		isKilled bool
		called   bool
	}
	h.curveRepo.SetCurveGaugeKilledFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, isKilled bool) error {
		captured.addr = addr
		captured.isKilled = isKilled
		captured.called = true
		return nil
	}
	if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    200,
		Version:        0,
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if !captured.called {
		t.Fatal("SetCurveGaugeKilled was not called")
	}
	if captured.addr != testGaugeAddr {
		t.Errorf("addr = %s, want %s", captured.addr.Hex(), testGaugeAddr.Hex())
	}
	if captured.isKilled {
		t.Errorf("isKilled = true, want false (UnkillGauge resurrects the gauge)")
	}
}

func TestProcessReceipt_LPTokenTransfer_WritesPositions(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	ev := h.lpEventsABI.Events["Transfer"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(123))
	if err != nil {
		t.Fatalf("pack transfer: %v", err)
	}
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address: testLPTokenAddr.Hex(),
			Topics: []string{
				ev.ID.Hex(),
				common.BytesToHash(from.Bytes()).Hex(),
				common.BytesToHash(to.Bytes()).Hex(),
			},
			Data:            "0x" + common.Bytes2Hex(data),
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})
	h.cache.SetReceipts(1, 300, 0, body)

	// Mock the balanceOf multicall: the worker now reads the post-event
	// balance for each non-zero side and writes it into lp_balance.
	encodeUint := func(v *big.Int) []byte { return common.LeftPadBytes(v.Bytes(), 32) }
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		out := make([]outbound.Result, len(calls))
		for i := range calls {
			// Mocked balance: 1000 wei for the sender, 500 wei for the receiver.
			amount := big.NewInt(1000)
			if i == 1 {
				amount = big.NewInt(500)
			}
			out[i] = outbound.Result{Success: true, ReturnData: encodeUint(amount)}
		}
		return out, nil
	}

	var positions []*entity.CurveUserLpPosition
	h.curveRepo.SaveCurveUserLpPositionFn = func(_ context.Context, _ pgx.Tx, pos *entity.CurveUserLpPosition) error {
		positions = append(positions, pos)
		return nil
	}

	if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    300,
		Version:        0,
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if len(positions) != 2 {
		t.Fatalf("expected 2 lp position rows, got %d", len(positions))
	}
	// sender side: delta is negative.
	if positions[0].Delta.Sign() >= 0 {
		t.Errorf("sender delta = %s, want negative", positions[0].Delta)
	}
	// receiver side: delta is positive.
	if positions[1].Delta.Sign() <= 0 {
		t.Errorf("receiver delta = %s, want positive", positions[1].Delta)
	}
	// Both rows must carry the balanceOf-derived lp_balance (no more nil placeholder).
	if positions[0].LpBalance == nil || positions[0].LpBalance.Sign() <= 0 {
		t.Errorf("sender lp_balance = %v, want positive", positions[0].LpBalance)
	}
	if positions[1].LpBalance == nil || positions[1].LpBalance.Sign() <= 0 {
		t.Errorf("receiver lp_balance = %v, want positive", positions[1].LpBalance)
	}
}

func TestProcessBlockEvent_MissingReceipts(t *testing.T) {
	h := newHarness(t)
	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 999, Version: 0,
	})
	if err == nil || !strings.Contains(err.Error(), "receipts not found") {
		t.Errorf("expected receipts-not-found error, got %v", err)
	}
}

// N-CURVE4: the MetaRegistry bootstrap is a one-shot startup call. There is
// no event block in scope; the worker must read the registry at "latest"
// (multicall convention: blockNumber=nil) so the call sees the deployed
// MetaRegistry contract. Passing big.NewInt(0) here would pin to genesis,
// where no contracts exist, and the bootstrap would silently fail.
func TestBootstrapGaugeFromMetaRegistry_UsesLatestBlock(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	gaugeAddr := common.HexToAddress("0xfeedfacefeedfacefeedfacefeedfacefeedface")

	h.curveRepo.ListEnabledCurvePoolsFn = func(_ context.Context, _ int64) ([]*entity.CurvePool, error) {
		return []*entity.CurvePool{pool}, nil
	}
	h.curveRepo.GetCurveGaugeFn = func(_ context.Context, _ int64) (*entity.CurveGauge, error) {
		return nil, nil
	}
	h.curveRepo.UpsertCurveGaugeFn = func(_ context.Context, _ pgx.Tx, g *entity.CurveGauge) (int64, error) {
		return 1, nil
	}

	var bootstrapBlockArg *big.Int
	var bootstrapBlockArgSeen bool
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		bootstrapBlockArg = blockNumber
		bootstrapBlockArgSeen = true
		return []outbound.Result{
			{Success: true, ReturnData: common.LeftPadBytes(gaugeAddr.Bytes(), 32)},
		}, nil
	}

	if err := h.svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.svc.Stop()

	if !bootstrapBlockArgSeen {
		t.Fatal("multicaller was not invoked by bootstrap")
	}
	if bootstrapBlockArg != nil {
		t.Errorf("bootstrap blockNumber = %v, want nil (== \"latest\" in multicaller convention)", bootstrapBlockArg)
	}
}

// Per-event GetOrCreateProtocol is wasted work: the Curve protocol row is
// fixed for the worker's lifetime. Resolve once at startup and cache the id.
func TestProtocolIDIsCachedAcrossEvents(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var getCalls atomic.Int32
	h.protocolRepo.GetOrCreateProtocolFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ string, _ int64) (int64, error) {
		getCalls.Add(1)
		return 99, nil
	}

	// Drive three independent log events through processBlockEvent. Without
	// caching, GetOrCreateProtocol is called once per event (= 3 times).
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolSwap) error { return nil }
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolState) error { return nil }
	h.curveRepo.SaveCurvePoolExchangeRateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolExchangeRate) error { return nil }
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error { return nil }

	for i := range 3 {
		log := h.makeTokenExchangeLog(pool.Address, common.HexToAddress("0xdeadbeef"), 0, 1, big.NewInt(int64(10+i)), big.NewInt(int64(9+i)))
		receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
		body, _ := json.Marshal([]shared.TransactionReceipt{receipt})
		h.cache.SetReceipts(1, int64(100+i), 0, body)
		if err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
			ChainID: 1, BlockNumber: int64(100 + i), Version: 0, BlockTimestamp: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("processBlockEvent[%d]: %v", i, err)
		}
	}

	if got := getCalls.Load(); got != 1 {
		t.Errorf("GetOrCreateProtocol called %d times; expected exactly 1 (cached after first resolve)", got)
	}
}

// S4: the protocol_event audit row is the source of truth and must land even
// if a downstream multicall sub-call reverts. The pre-fix worker ran every
// readPoolState/readGaugeState multicall BEFORE opening any DB transaction,
// so a single revert wiped the audit row from the world.
// When the multicall reverts BEFORE the DB transaction opens (audit + typed
// projection now share one tx — see handlePoolLog comment), processBlockEvent
// returns an error so SQS does not ack. No rows of any kind are written.
// The retry is safe: the multicall re-runs idempotently and the SQS-retried
// INSERTs are protected by ADR-0002 ON CONFLICT clauses on every typed table.
func TestProcessReceipt_MulticallFails_NoRowsWritten_NoAck(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	auditWritten := false
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		auditWritten = true
		return nil
	}
	stateWritten := false
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolState) error {
		stateWritten = true
		return nil
	}
	swapWritten := false
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolSwap) error {
		swapWritten = true
		return nil
	}

	// readPoolState reverts on the first sub-call (e.g., a degenerate pool).
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("simulated multicall revert")
	}

	log := h.makeTokenExchangeLog(pool.Address, common.HexToAddress("0xdeadbeef"), 0, 1, big.NewInt(10), big.NewInt(9))
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})
	h.cache.SetReceipts(1, 200, 0, body)

	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 200, Version: 0, BlockTimestamp: time.Now().Unix(),
	})
	if err == nil {
		t.Fatal("expected error when multicall reverts, got nil (SQS would ack and we'd lose the message)")
	}
	if auditWritten {
		t.Error("audit row should NOT be persisted when multicall reverts; SQS retry will re-run cleanly")
	}
	if stateWritten {
		t.Error("typed pool_state row should NOT be persisted when multicall reverts")
	}
	if swapWritten {
		t.Error("typed swap row should NOT be persisted when multicall reverts")
	}
}

func TestStopWithoutStart(t *testing.T) {
	h := newHarness(t)
	if err := h.svc.Stop(); err != nil {
		t.Fatalf("Stop without Start: %v", err)
	}
}

// readERC20Metadata must read symbol() and decimals() from the on-chain
// contract — registering a non-18-decimal reward token with the hard-coded
// 18 placeholder silently breaks every "amount / 10^decimals" USD conversion.
func TestReadERC20Metadata_FetchesSymbolAndDecimals(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	tokenAddr := common.HexToAddress("0xC0Ffee0000000000000000000000000000000000")

	strT, _ := abi.NewType("string", "", nil)
	uint8T, _ := abi.NewType("uint8", "", nil)
	symOut, err := (abi.Arguments{{Type: strT}}).Pack("USDC")
	if err != nil {
		t.Fatalf("pack symbol: %v", err)
	}
	decOut, err := (abi.Arguments{{Type: uint8T}}).Pack(uint8(6))
	if err != nil {
		t.Fatalf("pack decimals: %v", err)
	}
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 {
			return nil, errors.New("expected 2 calls (symbol + decimals)")
		}
		return []outbound.Result{
			{Success: true, ReturnData: symOut},
			{Success: true, ReturnData: decOut},
		}, nil
	}

	bs, err := newBlockchainService(mc, nil)
	if err != nil {
		t.Fatalf("newBlockchainService: %v", err)
	}
	sym, dec, err := bs.readERC20Metadata(context.Background(), tokenAddr, 12345)
	if err != nil {
		t.Fatalf("readERC20Metadata: %v", err)
	}
	if sym != "USDC" {
		t.Errorf("symbol = %q, want USDC", sym)
	}
	if dec != 6 {
		t.Errorf("decimals = %d, want 6", dec)
	}
}
