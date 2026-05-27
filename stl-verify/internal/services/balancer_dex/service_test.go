package balancer_dex

import (
	"context"
	"encoding/json"
	"fmt"
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
// MockBalancerPoolRepository: function-pointer mock for
// outbound.BalancerPoolRepository.
// -----------------------------------------------------------------------------

type MockBalancerPoolRepository struct {
	GetBalancerPoolFn                func(ctx context.Context, chainID int64, address common.Address) (*entity.BalancerPool, error)
	ListEnabledBalancerPoolsFn       func(ctx context.Context, chainID int64) ([]*entity.BalancerPool, error)
	ListBalancerPoolTokensFn         func(ctx context.Context, balancerPoolID int64) ([]*entity.BalancerPoolToken, error)
	UpsertBalancerPoolTokenFn        func(ctx context.Context, tx pgx.Tx, t *entity.BalancerPoolToken) error
	SaveBalancerPoolStateFn          func(ctx context.Context, tx pgx.Tx, state *entity.BalancerPoolState) error
	SaveBalancerPoolSwapFn           func(ctx context.Context, tx pgx.Tx, swap *entity.BalancerPoolSwap) error
	SaveBalancerPoolLiquidityEventFn func(ctx context.Context, tx pgx.Tx, evt *entity.BalancerPoolLiquidityEvent) error
	SaveBalancerPoolParameterEventFn func(ctx context.Context, tx pgx.Tx, evt *entity.BalancerPoolParameterEvent) error
	SaveBalancerUserBptPositionFn    func(ctx context.Context, tx pgx.Tx, pos *entity.BalancerUserBptPosition) error
}

func (m *MockBalancerPoolRepository) GetBalancerPool(ctx context.Context, chainID int64, address common.Address) (*entity.BalancerPool, error) {
	if m.GetBalancerPoolFn != nil {
		return m.GetBalancerPoolFn(ctx, chainID, address)
	}
	return nil, nil
}
func (m *MockBalancerPoolRepository) ListEnabledBalancerPools(ctx context.Context, chainID int64) ([]*entity.BalancerPool, error) {
	if m.ListEnabledBalancerPoolsFn != nil {
		return m.ListEnabledBalancerPoolsFn(ctx, chainID)
	}
	return nil, nil
}
func (m *MockBalancerPoolRepository) ListBalancerPoolTokens(ctx context.Context, balancerPoolID int64) ([]*entity.BalancerPoolToken, error) {
	if m.ListBalancerPoolTokensFn != nil {
		return m.ListBalancerPoolTokensFn(ctx, balancerPoolID)
	}
	return nil, nil
}
func (m *MockBalancerPoolRepository) UpsertBalancerPoolToken(ctx context.Context, tx pgx.Tx, t *entity.BalancerPoolToken) error {
	if m.UpsertBalancerPoolTokenFn != nil {
		return m.UpsertBalancerPoolTokenFn(ctx, tx, t)
	}
	return nil
}
func (m *MockBalancerPoolRepository) SaveBalancerPoolState(ctx context.Context, tx pgx.Tx, state *entity.BalancerPoolState) error {
	if m.SaveBalancerPoolStateFn != nil {
		return m.SaveBalancerPoolStateFn(ctx, tx, state)
	}
	return nil
}
func (m *MockBalancerPoolRepository) SaveBalancerPoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.BalancerPoolSwap) error {
	if m.SaveBalancerPoolSwapFn != nil {
		return m.SaveBalancerPoolSwapFn(ctx, tx, swap)
	}
	return nil
}
func (m *MockBalancerPoolRepository) SaveBalancerPoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.BalancerPoolLiquidityEvent) error {
	if m.SaveBalancerPoolLiquidityEventFn != nil {
		return m.SaveBalancerPoolLiquidityEventFn(ctx, tx, evt)
	}
	return nil
}
func (m *MockBalancerPoolRepository) SaveBalancerPoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.BalancerPoolParameterEvent) error {
	if m.SaveBalancerPoolParameterEventFn != nil {
		return m.SaveBalancerPoolParameterEventFn(ctx, tx, evt)
	}
	return nil
}
func (m *MockBalancerPoolRepository) SaveBalancerUserBptPosition(ctx context.Context, tx pgx.Tx, pos *entity.BalancerUserBptPosition) error {
	if m.SaveBalancerUserBptPositionFn != nil {
		return m.SaveBalancerUserBptPositionFn(ctx, tx, pos)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Test harness
// -----------------------------------------------------------------------------

type harness struct {
	t            *testing.T
	svc          *Service
	balancerRepo *MockBalancerPoolRepository
	tokenRepo    *testutil.MockTokenRepository
	protocolRepo *testutil.MockProtocolRepository
	eventRepo    *testutil.MockEventRepository
	multicaller  *testutil.MockMulticaller
	txManager    *testutil.MockTxManager
	consumer     *testutil.MockSQSConsumer
	cache        *testutil.MockBlockCache

	vaultEventsABI *abi.ABI
	poolEventsABI  *abi.ABI
	vaultReadABI   *abi.ABI
	poolReadABI    *abi.ABI
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	balancerRepo := &MockBalancerPoolRepository{}
	tokenRepo := &testutil.MockTokenRepository{}
	protocolRepo := &testutil.MockProtocolRepository{}
	eventRepo := &testutil.MockEventRepository{}
	mc := testutil.NewMockMulticaller()
	txManager := &testutil.MockTxManager{}
	consumer := &testutil.MockSQSConsumer{}
	cache := testutil.NewMockBlockCache()

	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	cfg.ChainID = 1

	svc, err := NewService(cfg, consumer, cache, mc, txManager, balancerRepo, tokenRepo, protocolRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	vev, err := abis.GetBalancerV2VaultEventsABI()
	if err != nil {
		t.Fatalf("vault events ABI: %v", err)
	}
	pev, err := abis.GetBalancerV2ComposableStableEventsABI()
	if err != nil {
		t.Fatalf("pool events ABI: %v", err)
	}
	vrd, err := abis.GetBalancerV2VaultReadABI()
	if err != nil {
		t.Fatalf("vault read ABI: %v", err)
	}
	prd, err := abis.GetBalancerV2ComposableStableReadABI()
	if err != nil {
		t.Fatalf("pool read ABI: %v", err)
	}
	return &harness{
		t:              t,
		svc:            svc,
		balancerRepo:   balancerRepo,
		tokenRepo:      tokenRepo,
		protocolRepo:   protocolRepo,
		eventRepo:      eventRepo,
		multicaller:    mc,
		txManager:      txManager,
		consumer:       consumer,
		cache:          cache,
		vaultEventsABI: vev,
		poolEventsABI:  pev,
		vaultReadABI:   vrd,
		poolReadABI:    prd,
	}
}

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

var (
	// wstETH-WETH composable_stable pool, mainnet (matches seed migration).
	testPoolAddr = common.HexToAddress("0x93d199263632a4EF4Bb438F1feB99e57b4b5f0BD")
	testPoolID   = common.HexToHash("0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd0000000000000000000005c2")
	// Foreign pool whose Swaps the worker must drop.
	foreignPoolID = common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	wstETHAddr    = common.HexToAddress("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0")
	wethAddr      = common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	testUserAddr  = common.HexToAddress("0x9999999999999999999999999999999999999999")
	testTxHash    = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testRateProv  = common.HexToAddress("0xAaaaAAaaaAaaAaAAaAAAAaAaAaaAAaAaAaAaAAa1")
)

func makePool() *entity.BalancerPool {
	return &entity.BalancerPool{
		ID:           1,
		ChainID:      1,
		PoolKind:     entity.BalancerPoolKindComposableStable,
		Address:      testPoolAddr,
		PoolID:       testPoolID,
		VaultAddress: BalancerVaultAddress,
		Label:        "wstETH-WETH-CSP",
		Enabled:      true,
	}
}

// makePoolTokens returns 3 slots: wstETH, WETH, and the phantom BPT (the
// pool address). Indices match how Vault.getPoolTokens orders them on
// mainnet for this pool.
func makePoolTokens() ([]*entity.BalancerPoolToken, []common.Address) {
	slots := []*entity.BalancerPoolToken{
		{BalancerPoolID: 1, TokenIndex: 0, TokenID: 100, IsPhantom: false},
		{BalancerPoolID: 1, TokenIndex: 1, TokenID: 101, IsPhantom: true}, // phantom BPT
		{BalancerPoolID: 1, TokenIndex: 2, TokenID: 102, IsPhantom: false},
	}
	addrs := []common.Address{wstETHAddr, testPoolAddr, wethAddr}
	return slots, addrs
}

// registerPool injects a pool + tokens into the in-memory registry without
// calling Start, so tests can drive processBlockEvent directly.
func (h *harness) registerPool() *registeredPool {
	pool := makePool()
	slots, addrs := makePoolTokens()
	h.svc.registry.addPool(pool, slots, addrs)
	return h.svc.registry.poolByAddress(pool.Address)
}

// poolStateResults returns canned successful results for the 9 calls our
// readPoolState issues for a 3-slot pool (1 phantom, so 2 getTokenRate calls):
// getPoolTokens + amp + rate + actualSupply + totalSupply + scalingFactors +
// swapFee + pausedState + getTokenRate(wstETH) + getTokenRate(WETH).
func (h *harness) poolStateResults() []outbound.Result {
	// 0: getPoolTokens(poolId) — encodes (address[], uint256[], uint256).
	addrSliceT, _ := abi.NewType("address[]", "", nil)
	uintSliceT, _ := abi.NewType("uint256[]", "", nil)
	uintT, _ := abi.NewType("uint256", "", nil)
	boolT, _ := abi.NewType("bool", "", nil)
	getPoolArgs := abi.Arguments{{Type: addrSliceT}, {Type: uintSliceT}, {Type: uintT}}
	gptData, _ := getPoolArgs.Pack(
		[]common.Address{wstETHAddr, testPoolAddr, wethAddr},
		[]*big.Int{big.NewInt(1000), big.NewInt(0), big.NewInt(2000)},
		big.NewInt(123),
	)

	// 1: getAmplificationParameter — (uint256 value, bool isUpdating, uint256 precision).
	ampArgs := abi.Arguments{{Type: uintT}, {Type: boolT}, {Type: uintT}}
	ampData, _ := ampArgs.Pack(big.NewInt(50000), false, big.NewInt(1000))

	// 7: getPausedState — (bool paused, uint256, uint256).
	pausedArgs := abi.Arguments{{Type: boolT}, {Type: uintT}, {Type: uintT}}
	pausedData, _ := pausedArgs.Pack(false, big.NewInt(0), big.NewInt(0))

	// Scaling factors slice.
	sfArgs := abi.Arguments{{Type: uintSliceT}}
	sfData, _ := sfArgs.Pack([]*big.Int{big.NewInt(1e18), big.NewInt(1e18), big.NewInt(1e18)})

	uintArgs := abi.Arguments{{Type: uintT}}
	mkUint := func(v int64) []byte {
		out, _ := uintArgs.Pack(big.NewInt(v))
		return out
	}

	return []outbound.Result{
		{Success: true, ReturnData: gptData},                // 0 getPoolTokens
		{Success: true, ReturnData: ampData},                // 1 getAmplificationParameter
		{Success: true, ReturnData: mkUint(int64(1.05e18))}, // 2 getRate
		{Success: true, ReturnData: mkUint(int64(1.0e18))},  // 3 getActualSupply
		{Success: true, ReturnData: mkUint(int64(2.5e18))},  // 4 totalSupply
		{Success: true, ReturnData: sfData},                 // 5 getScalingFactors
		{Success: true, ReturnData: mkUint(int64(4e14))},    // 6 getSwapFeePercentage
		{Success: true, ReturnData: pausedData},             // 7 getPausedState
		{Success: true, ReturnData: mkUint(int64(1.18e18))}, // 8 getTokenRate(wstETH)
		{Success: true, ReturnData: mkUint(int64(1.0e18))},  // 9 getTokenRate(WETH)
	}
}

// vaultTokensOnlyResults canned single-call result for populatePoolTokens.
func (h *harness) vaultTokensOnlyResults() []outbound.Result {
	addrSliceT, _ := abi.NewType("address[]", "", nil)
	uintSliceT, _ := abi.NewType("uint256[]", "", nil)
	uintT, _ := abi.NewType("uint256", "", nil)
	args := abi.Arguments{{Type: addrSliceT}, {Type: uintSliceT}, {Type: uintT}}
	data, _ := args.Pack(
		[]common.Address{wstETHAddr, testPoolAddr, wethAddr},
		[]*big.Int{big.NewInt(1000), big.NewInt(0), big.NewInt(2000)},
		big.NewInt(42),
	)
	return []outbound.Result{{Success: true, ReturnData: data}}
}

// -----------------------------------------------------------------------------
// Log builders.
// -----------------------------------------------------------------------------

func (h *harness) makeVaultSwapLog(poolID common.Hash, tokenIn, tokenOut common.Address, amountIn, amountOut *big.Int, logIdx string) shared.Log {
	ev := h.vaultEventsABI.Events["Swap"]
	data, err := ev.Inputs.NonIndexed().Pack(amountIn, amountOut)
	if err != nil {
		h.t.Fatalf("packing Swap data: %v", err)
	}
	return shared.Log{
		Address: BalancerVaultAddress.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			poolID.Hex(),
			common.BytesToHash(tokenIn.Bytes()).Hex(),
			common.BytesToHash(tokenOut.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        logIdx,
	}
}

func (h *harness) makeVaultPoolBalanceChangedLog(poolID common.Hash, lp common.Address, tokens []common.Address, deltas, fees []*big.Int) shared.Log {
	ev := h.vaultEventsABI.Events["PoolBalanceChanged"]
	data, err := ev.Inputs.NonIndexed().Pack(tokens, deltas, fees)
	if err != nil {
		h.t.Fatalf("packing PoolBalanceChanged data: %v", err)
	}
	return shared.Log{
		Address: BalancerVaultAddress.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			poolID.Hex(),
			common.BytesToHash(lp.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x2",
	}
}

func (h *harness) makeVaultPoolBalanceManagedLog(poolID common.Hash, manager, token common.Address, cashDelta, managedDelta *big.Int) shared.Log {
	ev := h.vaultEventsABI.Events["PoolBalanceManaged"]
	data, err := ev.Inputs.NonIndexed().Pack(cashDelta, managedDelta)
	if err != nil {
		h.t.Fatalf("packing PoolBalanceManaged data: %v", err)
	}
	return shared.Log{
		Address: BalancerVaultAddress.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			poolID.Hex(),
			common.BytesToHash(manager.Bytes()).Hex(),
			common.BytesToHash(token.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x3",
	}
}

// makePoolEventLog wraps the generic pool-emitted event packing path so the
// individual builder functions stay terse.
func (h *harness) makePoolEventLog(name string, indexedTopics []common.Hash, nonIndexedArgs []any, logIdx string) shared.Log {
	ev := h.poolEventsABI.Events[name]
	data, err := ev.Inputs.NonIndexed().Pack(nonIndexedArgs...)
	if err != nil {
		h.t.Fatalf("packing %s data: %v", name, err)
	}
	topics := make([]string, 0, len(indexedTopics)+1)
	topics = append(topics, ev.ID.Hex())
	for _, t := range indexedTopics {
		topics = append(topics, t.Hex())
	}
	return shared.Log{
		Address:         testPoolAddr.Hex(),
		Topics:          topics,
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        logIdx,
	}
}

func (h *harness) deliver(receipts []shared.TransactionReceipt, blockNumber int64, version int) error {
	body, err := json.Marshal(receipts)
	if err != nil {
		h.t.Fatalf("marshal receipts: %v", err)
	}
	h.cache.SetReceipts(1, blockNumber, version, body)
	return h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	})
}

// -----------------------------------------------------------------------------
// Construction tests
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
	br := &MockBalancerPoolRepository{}
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
		bal         outbound.BalancerPoolRepository
		tok         outbound.TokenRepository
		proto       outbound.ProtocolRepository
		evt         outbound.EventRepository
		wantErrSub  string
		wantSuccess bool
	}{
		{"nil consumer", nil, rc, mc, tm, br, tr, pr, er, "consumer", false},
		{"nil cache", cons, nil, mc, tm, br, tr, pr, er, "cache", false},
		{"nil multicaller", cons, rc, nil, tm, br, tr, pr, er, "multicaller", false},
		{"nil txm", cons, rc, mc, nil, br, tr, pr, er, "txManager", false},
		{"nil balancerRepo", cons, rc, mc, tm, nil, tr, pr, er, "balancerRepo", false},
		{"nil tokenRepo", cons, rc, mc, tm, br, nil, pr, er, "tokenRepo", false},
		{"nil protocol", cons, rc, mc, tm, br, tr, nil, er, "protocolRepo", false},
		{"nil event", cons, rc, mc, tm, br, tr, pr, nil, "eventRepo", false},
		{"all valid", cons, rc, mc, tm, br, tr, pr, er, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewService(cfg, c.consumer, c.cache, c.mc, c.txm, c.bal, c.tok, c.proto, c.evt)
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
	_, err := NewService(cfg, &testutil.MockSQSConsumer{}, testutil.NewMockBlockCache(), testutil.NewMockMulticaller(), &testutil.MockTxManager{}, &MockBalancerPoolRepository{}, &testutil.MockTokenRepository{}, &testutil.MockProtocolRepository{}, &testutil.MockEventRepository{})
	if err == nil {
		t.Fatal("expected error for zero ChainID")
	}
	if !strings.Contains(err.Error(), "ChainID") {
		t.Errorf("error %q does not mention ChainID", err.Error())
	}
}

func TestStartLoadsPoolsAndTokens(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	slots, _ := makePoolTokens()
	h.balancerRepo.ListEnabledBalancerPoolsFn = func(ctx context.Context, chainID int64) ([]*entity.BalancerPool, error) {
		return []*entity.BalancerPool{pool}, nil
	}
	h.balancerRepo.ListBalancerPoolTokensFn = func(ctx context.Context, poolID int64) ([]*entity.BalancerPoolToken, error) {
		if poolID == pool.ID {
			return slots, nil
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
	if h.svc.registry.poolByAddress(pool.Address) == nil {
		t.Error("pool not registered by address")
	}
	if h.svc.registry.poolByPoolID(pool.PoolID) == nil {
		t.Error("pool not registered by poolId")
	}
}

func TestStopWithoutStart(t *testing.T) {
	h := newHarness(t)
	if err := h.svc.Stop(); err != nil {
		t.Fatalf("Stop without Start: %v", err)
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

// -----------------------------------------------------------------------------
// Event-kind tests (one per kind).
// -----------------------------------------------------------------------------

func TestProcessReceipt_VaultSwap_WritesSwapStateAndProtocolEvent(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var savedSwap *entity.BalancerPoolSwap
	var savedState *entity.BalancerPoolState
	var savedEvent *entity.ProtocolEvent
	h.balancerRepo.SaveBalancerPoolSwapFn = func(_ context.Context, _ pgx.Tx, swap *entity.BalancerPoolSwap) error {
		savedSwap = swap
		return nil
	}
	h.balancerRepo.SaveBalancerPoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.BalancerPoolState) error {
		savedState = s
		return nil
	}
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
		savedEvent = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(100), big.NewInt(99), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	if savedSwap == nil {
		t.Fatal("swap row not persisted")
	}
	if savedSwap.TokenInIdx != 0 || savedSwap.TokenOutIdx != 2 {
		t.Errorf("swap idx (%d,%d), want (0,2)", savedSwap.TokenInIdx, savedSwap.TokenOutIdx)
	}
	if savedSwap.AmountIn.Int64() != 100 || savedSwap.AmountOut.Int64() != 99 {
		t.Errorf("swap amounts = (%s,%s)", savedSwap.AmountIn, savedSwap.AmountOut)
	}
	if savedState == nil {
		t.Fatal("pool state row not persisted")
	}
	if savedState.Source != entity.BalancerPoolStateSourceEvent {
		t.Errorf("state source = %q, want event", savedState.Source)
	}
	if len(savedState.Balances) != 3 || savedState.Balances[0].Int64() != 1000 {
		t.Errorf("balances = %v, want [1000, 0, 2000]", savedState.Balances)
	}
	if savedEvent == nil {
		t.Fatal("protocol_event audit row not persisted")
	}
	if savedEvent.EventName != string(EventSwap) {
		t.Errorf("event_name = %q, want Swap", savedEvent.EventName)
	}
}

// Phantom BPT slot must not appear as a real balance in the saved row.
// The Vault.getPoolTokens response for a ComposableStable pool includes the
// pool's own BPT in the tokens array (as a "phantom" slot whose value is the
// internal BPT supply, NOT real liquidity). Any TVL/depth sum over balances[]
// would double-count this. The projection must NIL the phantom slot.
func TestProcessReceipt_PhantomBPTSlotIsNotProjectedIntoBalances(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	// Override the fixture so the phantom slot carries a realistic BPT supply.
	bptSupply, _ := new(big.Int).SetString("2686000000000000000000000000000000", 10) // 2.686e33
	addrSliceT, _ := abi.NewType("address[]", "", nil)
	uintSliceT, _ := abi.NewType("uint256[]", "", nil)
	uintT, _ := abi.NewType("uint256", "", nil)
	getPoolArgs := abi.Arguments{{Type: addrSliceT}, {Type: uintSliceT}, {Type: uintT}}
	gptData, _ := getPoolArgs.Pack(
		[]common.Address{wstETHAddr, testPoolAddr, wethAddr},
		[]*big.Int{big.NewInt(1000), bptSupply, big.NewInt(2000)},
		big.NewInt(123),
	)
	results := h.poolStateResults()
	results[0] = outbound.Result{Success: true, ReturnData: gptData}

	var savedState *entity.BalancerPoolState
	h.balancerRepo.SaveBalancerPoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.BalancerPoolState) error {
		savedState = s
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return results, nil
	}

	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(100), big.NewInt(99), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	if savedState == nil {
		t.Fatal("pool state row not persisted")
	}
	if len(savedState.Balances) != 3 {
		t.Fatalf("balances len = %d, want 3", len(savedState.Balances))
	}
	if savedState.Balances[1] != nil {
		t.Errorf("phantom-slot balance = %s, want nil (BPT supply must not be projected as real balance)", savedState.Balances[1])
	}
	if savedState.Balances[0] == nil || savedState.Balances[0].Int64() != 1000 {
		t.Errorf("slot 0 balance = %v, want 1000", savedState.Balances[0])
	}
	if savedState.Balances[2] == nil || savedState.Balances[2].Int64() != 2000 {
		t.Errorf("slot 2 balance = %v, want 2000", savedState.Balances[2])
	}
}

// Vault.Swap for a poolId outside our registry should be dropped silently.
func TestProcessReceipt_VaultSwap_ForeignPoolIsIgnored(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	swapCalled := false
	stateCalled := false
	h.balancerRepo.SaveBalancerPoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolSwap) error {
		swapCalled = true
		return nil
	}
	h.balancerRepo.SaveBalancerPoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolState) error {
		stateCalled = true
		return nil
	}

	log := h.makeVaultSwapLog(foreignPoolID, wstETHAddr, wethAddr, big.NewInt(100), big.NewInt(99), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if swapCalled {
		t.Error("foreign pool Swap should have been dropped, but SaveBalancerPoolSwap was called")
	}
	if stateCalled {
		t.Error("foreign pool Swap should not trigger state write")
	}
}

func TestProcessReceipt_VaultPoolBalanceChanged_WritesLiquidityEvent(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var saved *entity.BalancerPoolLiquidityEvent
	h.balancerRepo.SaveBalancerPoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, evt *entity.BalancerPoolLiquidityEvent) error {
		saved = evt
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	// Tokens in vault order: wstETH, BPT (phantom), WETH.
	tokens := []common.Address{wstETHAddr, testPoolAddr, wethAddr}
	deltas := []*big.Int{big.NewInt(10), big.NewInt(0), big.NewInt(-5)}
	fees := []*big.Int{big.NewInt(0), big.NewInt(0), big.NewInt(0)}
	log := h.makeVaultPoolBalanceChangedLog(testPoolID, testUserAddr, tokens, deltas, fees)
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if saved == nil {
		t.Fatal("liquidity event row not persisted")
	}
	if saved.LiquidityProvider != testUserAddr {
		t.Errorf("lp = %s, want %s", saved.LiquidityProvider.Hex(), testUserAddr.Hex())
	}
	if len(saved.Deltas) != 3 {
		t.Fatalf("deltas len = %d, want 3", len(saved.Deltas))
	}
	if saved.Deltas[0].Int64() != 10 || saved.Deltas[2].Int64() != -5 {
		t.Errorf("deltas = %v, want [10,0,-5]", saved.Deltas)
	}
}

// PoolBalanceManaged should only write the protocol_event audit row.
func TestProcessReceipt_VaultPoolBalanceManaged_OnlyAuditRow(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var savedEvent *entity.ProtocolEvent
	swapCalled := false
	liquidityCalled := false
	stateCalled := false
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
		savedEvent = e
		return nil
	}
	h.balancerRepo.SaveBalancerPoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolSwap) error {
		swapCalled = true
		return nil
	}
	h.balancerRepo.SaveBalancerPoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolLiquidityEvent) error {
		liquidityCalled = true
		return nil
	}
	h.balancerRepo.SaveBalancerPoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolState) error {
		stateCalled = true
		return nil
	}

	log := h.makeVaultPoolBalanceManagedLog(testPoolID, testUserAddr, wstETHAddr, big.NewInt(7), big.NewInt(-3))
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if savedEvent == nil || savedEvent.EventName != string(EventPoolBalanceManaged) {
		t.Errorf("audit row not persisted (got %#v)", savedEvent)
	}
	if swapCalled || liquidityCalled || stateCalled {
		t.Errorf("PoolBalanceManaged should not produce typed projection or state (swap=%v liq=%v state=%v)", swapCalled, liquidityCalled, stateCalled)
	}
}

func TestProcessReceipt_PoolAmpUpdateStarted_WritesParameterEvent(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var saved *entity.BalancerPoolParameterEvent
	h.balancerRepo.SaveBalancerPoolParameterEventFn = func(_ context.Context, _ pgx.Tx, evt *entity.BalancerPoolParameterEvent) error {
		saved = evt
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	log := h.makePoolEventLog("AmpUpdateStarted", nil, []any{big.NewInt(1000), big.NewInt(2000), big.NewInt(100), big.NewInt(200)}, "0x4")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if saved == nil {
		t.Fatal("parameter event row not persisted")
	}
	if saved.EventKind != entity.BalancerParameterEventAmpUpdateStarted {
		t.Errorf("event_kind = %q, want AmpUpdateStarted", saved.EventKind)
	}
	if saved.StartValue.Int64() != 1000 || saved.EndValue.Int64() != 2000 {
		t.Errorf("start/end = (%s,%s), want (1000,2000)", saved.StartValue, saved.EndValue)
	}
}

func TestProcessReceipt_PoolAmpUpdateStopped_WritesParameterEvent(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var saved *entity.BalancerPoolParameterEvent
	h.balancerRepo.SaveBalancerPoolParameterEventFn = func(_ context.Context, _ pgx.Tx, evt *entity.BalancerPoolParameterEvent) error {
		saved = evt
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	log := h.makePoolEventLog("AmpUpdateStopped", nil, []any{big.NewInt(1500)}, "0x5")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if saved == nil || saved.EventKind != entity.BalancerParameterEventAmpUpdateStopped {
		t.Fatalf("parameter event missing or wrong kind: %#v", saved)
	}
	if saved.CurrentValue == nil || saved.CurrentValue.Int64() != 1500 {
		t.Errorf("current_value = %v, want 1500", saved.CurrentValue)
	}
}

func TestProcessReceipt_PoolTokenRateProviderSet_RefreshesJoinTable(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var savedParam *entity.BalancerPoolParameterEvent
	var upsertedToken *entity.BalancerPoolToken
	h.balancerRepo.SaveBalancerPoolParameterEventFn = func(_ context.Context, _ pgx.Tx, evt *entity.BalancerPoolParameterEvent) error {
		savedParam = evt
		return nil
	}
	h.balancerRepo.UpsertBalancerPoolTokenFn = func(_ context.Context, _ pgx.Tx, t *entity.BalancerPoolToken) error {
		upsertedToken = t
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	// Indexed: tokenIndex=2 (WETH slot), provider=testRateProv.
	tokenIdx := common.BigToHash(big.NewInt(2))
	provHash := common.BytesToHash(testRateProv.Bytes())
	log := h.makePoolEventLog("TokenRateProviderSet", []common.Hash{tokenIdx, provHash}, []any{big.NewInt(3600)}, "0x6")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if savedParam == nil || savedParam.EventKind != entity.BalancerParameterEventTokenRateProviderSet {
		t.Fatalf("parameter row missing or wrong kind: %#v", savedParam)
	}
	if savedParam.RateProvider == nil || *savedParam.RateProvider != testRateProv {
		t.Errorf("rate_provider = %v, want %s", savedParam.RateProvider, testRateProv.Hex())
	}
	if savedParam.TokenAddress == nil || *savedParam.TokenAddress != wethAddr {
		t.Errorf("token_address = %v, want %s", savedParam.TokenAddress, wethAddr.Hex())
	}
	if upsertedToken == nil {
		t.Fatal("balancer_pool_token row not upserted")
	}
	if upsertedToken.RateProvider == nil || *upsertedToken.RateProvider != testRateProv {
		t.Errorf("upserted rate_provider = %v, want %s", upsertedToken.RateProvider, testRateProv.Hex())
	}
}

func TestProcessReceipt_PoolTokenRateCacheUpdated_WritesRate(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var saved *entity.BalancerPoolParameterEvent
	h.balancerRepo.SaveBalancerPoolParameterEventFn = func(_ context.Context, _ pgx.Tx, evt *entity.BalancerPoolParameterEvent) error {
		saved = evt
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	tokenIdx := common.BigToHash(big.NewInt(0))
	rate := big.NewInt(1180000000000000000) // 1.18e18
	log := h.makePoolEventLog("TokenRateCacheUpdated", []common.Hash{tokenIdx}, []any{rate}, "0x7")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if saved == nil || saved.EventKind != entity.BalancerParameterEventTokenRateCacheUpdated {
		t.Fatalf("parameter row missing or wrong kind: %#v", saved)
	}
	if saved.Rate == nil || saved.Rate.Cmp(rate) != 0 {
		t.Errorf("rate = %v, want %s", saved.Rate, rate)
	}
	if saved.TokenAddress == nil || *saved.TokenAddress != wstETHAddr {
		t.Errorf("token_address = %v, want %s", saved.TokenAddress, wstETHAddr.Hex())
	}
}

func TestProcessReceipt_PoolSwapFeePercentageChanged_WritesFee(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var saved *entity.BalancerPoolParameterEvent
	h.balancerRepo.SaveBalancerPoolParameterEventFn = func(_ context.Context, _ pgx.Tx, evt *entity.BalancerPoolParameterEvent) error {
		saved = evt
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	fee := big.NewInt(1000000000000000) // 0.1%
	log := h.makePoolEventLog("SwapFeePercentageChanged", nil, []any{fee}, "0x8")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if saved == nil || saved.SwapFeePercentage == nil || saved.SwapFeePercentage.Cmp(fee) != 0 {
		t.Errorf("swap_fee = %v, want %s", saved, fee)
	}
}

func TestProcessReceipt_PoolPausedStateChanged_WritesPaused(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var saved *entity.BalancerPoolParameterEvent
	h.balancerRepo.SaveBalancerPoolParameterEventFn = func(_ context.Context, _ pgx.Tx, evt *entity.BalancerPoolParameterEvent) error {
		saved = evt
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	log := h.makePoolEventLog("PausedStateChanged", nil, []any{true}, "0x9")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if saved == nil || saved.EventKind != entity.BalancerParameterEventPausedStateChanged {
		t.Fatalf("parameter row missing or wrong kind: %#v", saved)
	}
	if len(saved.Extra) == 0 || !strings.Contains(string(saved.Extra), "true") {
		t.Errorf("extra JSONB = %q, want paused=true", string(saved.Extra))
	}
}

func TestProcessReceipt_BPTTransfer_WritesTwoPositions(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	var positions []*entity.BalancerUserBptPosition
	h.balancerRepo.SaveBalancerUserBptPositionFn = func(_ context.Context, _ pgx.Tx, pos *entity.BalancerUserBptPosition) error {
		positions = append(positions, pos)
		return nil
	}

	// Mock the balanceOf multicall: 2 sides → 2 calls → 1000 / 500 wei.
	encodeUint := func(v *big.Int) []byte { return common.LeftPadBytes(v.Bytes(), 32) }
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		out := make([]outbound.Result, len(calls))
		for i := range calls {
			amount := big.NewInt(1000)
			if i == 1 {
				amount = big.NewInt(500)
			}
			out[i] = outbound.Result{Success: true, ReturnData: encodeUint(amount)}
		}
		return out, nil
	}

	value := big.NewInt(500)
	log := h.makePoolEventLog("Transfer", []common.Hash{common.BytesToHash(from.Bytes()), common.BytesToHash(to.Bytes())}, []any{value}, "0xA")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(positions) != 2 {
		t.Fatalf("expected 2 bpt position rows, got %d", len(positions))
	}
	if positions[0].Delta.Sign() >= 0 {
		t.Errorf("sender delta = %s, want negative", positions[0].Delta)
	}
	if positions[1].Delta.Sign() <= 0 {
		t.Errorf("receiver delta = %s, want positive", positions[1].Delta)
	}
	// Each row must carry the post-event balanceOf(user) — no more zero placeholder.
	if positions[0].BptBalance == nil || positions[0].BptBalance.Sign() <= 0 {
		t.Errorf("sender BptBalance = %v, want positive (balanceOf-derived)", positions[0].BptBalance)
	}
	if positions[1].BptBalance == nil || positions[1].BptBalance.Sign() <= 0 {
		t.Errorf("receiver BptBalance = %v, want positive (balanceOf-derived)", positions[1].BptBalance)
	}
}

func TestProcessReceipt_BPTTransferMint_WritesOnePosition(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	var positions []*entity.BalancerUserBptPosition
	h.balancerRepo.SaveBalancerUserBptPositionFn = func(_ context.Context, _ pgx.Tx, pos *entity.BalancerUserBptPosition) error {
		positions = append(positions, pos)
		return nil
	}

	// Mint = only `to` is non-zero, so balanceOf multicall is one call.
	encodeUint := func(v *big.Int) []byte { return common.LeftPadBytes(v.Bytes(), 32) }
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		out := make([]outbound.Result, len(calls))
		for i := range calls {
			out[i] = outbound.Result{Success: true, ReturnData: encodeUint(big.NewInt(99))}
		}
		return out, nil
	}

	zero := common.Address{}
	log := h.makePoolEventLog("Transfer", []common.Hash{common.BytesToHash(zero.Bytes()), common.BytesToHash(to.Bytes())}, []any{big.NewInt(99)}, "0xB")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 bpt position row (mint), got %d", len(positions))
	}
	if positions[0].Delta.Sign() <= 0 {
		t.Errorf("mint delta = %s, want positive", positions[0].Delta)
	}
}

// First-run getPoolTokens hydration: an empty registry slot list triggers a
// Vault.getPoolTokens read, persists balancer_pool_token rows, and marks the
// pool's own address slot as is_phantom=true.
func TestProcessReceipt_FirstRunPopulatesPoolTokens(t *testing.T) {
	h := newHarness(t)
	// Register pool but with empty token slots.
	pool := makePool()
	h.svc.registry.addPool(pool, nil, nil)

	var upserted []*entity.BalancerPoolToken
	h.balancerRepo.UpsertBalancerPoolTokenFn = func(_ context.Context, _ pgx.Tx, tok *entity.BalancerPoolToken) error {
		upserted = append(upserted, tok)
		return nil
	}
	tokenCounter := int64(99)
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
		tokenCounter++
		return tokenCounter, nil
	}

	// Multicall sequence for populatePoolTokens + first state read:
	//   1: vault.getPoolTokens
	//   2: readERC20MetadataBatch (2N sub-calls — 6 results for 3 slots)
	//   3: readPoolState (10 calls)
	strT, _ := abi.NewType("string", "", nil)
	uint8T, _ := abi.NewType("uint8", "", nil)
	symOut, _ := (abi.Arguments{{Type: strT}}).Pack("TKN")
	decOut, _ := (abi.Arguments{{Type: uint8T}}).Pack(uint8(18))
	metaBatchResp := []outbound.Result{
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
	}
	var callIdx atomic.Int32
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		n := callIdx.Add(1)
		switch n {
		case 1:
			return h.vaultTokensOnlyResults(), nil
		case 2:
			return metaBatchResp, nil
		default:
			return h.poolStateResults(), nil
		}
	}

	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(10), big.NewInt(9), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(upserted) != 3 {
		t.Fatalf("expected 3 balancer_pool_token rows, got %d", len(upserted))
	}
	// Slot 1 holds the pool's own address — must be marked phantom.
	if !upserted[1].IsPhantom {
		t.Errorf("slot 1 (pool address) is_phantom = false, want true")
	}
	if upserted[0].IsPhantom || upserted[2].IsPhantom {
		t.Errorf("non-phantom slots flagged phantom")
	}
}

// -----------------------------------------------------------------------------
// Lessons from Curve Phase 2c — must cover.
// -----------------------------------------------------------------------------

//  1. Idempotency: deliver same block twice produces the same write set the
//     second time (writes still happen — the DB trigger handles dedup — but
//     the worker must not crash or skip).
func TestProcessReceipt_Idempotency_SameBlockTwiceSucceeds(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	swapCount := 0
	h.balancerRepo.SaveBalancerPoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolSwap) error {
		swapCount++
		return nil
	}
	stateCount := 0
	h.balancerRepo.SaveBalancerPoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolState) error {
		stateCount++
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(100), big.NewInt(99), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	for i := range 2 {
		if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
			t.Fatalf("deliver attempt %d: %v", i, err)
		}
	}
	if swapCount != 2 || stateCount != 2 {
		t.Errorf("expected idempotent writes (DB trigger dedups) — got swap=%d state=%d", swapCount, stateCount)
	}
}

//  2. Reorg: same block with bumped block_version produces a new row with the
//     new version.
func TestProcessReceipt_Reorg_NewBlockVersionProducesRow(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	var savedVersions []int32
	h.balancerRepo.SaveBalancerPoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.BalancerPoolSwap) error {
		savedVersions = append(savedVersions, s.BlockVersion)
		return nil
	}
	h.balancerRepo.SaveBalancerPoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolState) error {
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(100), big.NewInt(99), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("v0 deliver: %v", err)
	}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 1); err != nil {
		t.Fatalf("v1 deliver: %v", err)
	}
	if len(savedVersions) != 2 || savedVersions[0] != 0 || savedVersions[1] != 1 {
		t.Errorf("block_versions = %v, want [0, 1]", savedVersions)
	}
}

//  3. Transaction failure: TxManager callback returns an error, so the worker
//     must propagate it (and SQS message acker would not delete).
func TestProcessReceipt_TxFailure_PropagatesError(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	// Force the TxManager to return an error from its callback.
	h.txManager.WithTransactionFn = func(_ context.Context, _ func(tx pgx.Tx) error) error {
		return fmt.Errorf("simulated tx failure")
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(1), big.NewInt(1), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0)
	if err == nil || !strings.Contains(err.Error(), "simulated tx failure") {
		t.Errorf("expected tx failure error, got: %v", err)
	}
}

//  4. Multicall RPC failure: ExecuteFn returns an error, so processBlockEvent
//     propagates it.
func TestProcessReceipt_MulticallFailure_PropagatesError(t *testing.T) {
	h := newHarness(t)
	h.registerPool()
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, fmt.Errorf("simulated RPC down")
	}

	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(1), big.NewInt(1), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0)
	if err == nil || !strings.Contains(err.Error(), "simulated RPC down") {
		t.Errorf("expected multicall failure, got: %v", err)
	}
}

//  5. No-op block: block whose receipts contain zero Balancer-related logs is
//     a success, no DB writes happen.
func TestProcessReceipt_BlockWithNoRelevantEvents_NoOp(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	calls := atomic.Int32{}
	h.balancerRepo.SaveBalancerPoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolSwap) error {
		calls.Add(1)
		return nil
	}
	h.balancerRepo.SaveBalancerPoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolState) error {
		calls.Add(1)
		return nil
	}
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		calls.Add(1)
		return nil
	}

	// One unrelated log (random topic + random address).
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address:         "0x4444444444444444444444444444444444444444",
			Topics:          []string{"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"},
			Data:            "0x",
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if calls.Load() != 0 {
		t.Errorf("expected zero DB writes, got %d", calls.Load())
	}
}

//  6. Per-receipt dedup: 3 Swap events on the same pool produce one
//     balancer_pool_state row, not three.
func TestProcessReceipt_PerReceiptDedup_OneStateRowPerPool(t *testing.T) {
	h := newHarness(t)
	h.registerPool()

	stateCount := atomic.Int32{}
	swapCount := atomic.Int32{}
	h.balancerRepo.SaveBalancerPoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolState) error {
		stateCount.Add(1)
		return nil
	}
	h.balancerRepo.SaveBalancerPoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.BalancerPoolSwap) error {
		swapCount.Add(1)
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResults(), nil
	}

	logs := []shared.Log{
		h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(1), big.NewInt(1), "0x1"),
		h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(2), big.NewInt(2), "0x2"),
		h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(3), big.NewInt(3), "0x3"),
	}
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: logs}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := swapCount.Load(); got != 3 {
		t.Errorf("swap count = %d, want 3", got)
	}
	if got := stateCount.Load(); got != 1 {
		t.Errorf("state count = %d, want 1 (per-receipt dedup)", got)
	}
}

// -----------------------------------------------------------------------------
// Extractor smoke tests on public-API surface.
// -----------------------------------------------------------------------------

func TestExtractor_VaultEventLookup(t *testing.T) {
	h := newHarness(t)
	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(1), big.NewInt(1), "0x1")
	name, ok := h.svc.extractor.vaultEvent(log)
	if !ok || name != EventSwap {
		t.Errorf("vaultEvent = (%q, %v), want (Swap, true)", name, ok)
	}
}

func TestExtractor_PoolEventLookup(t *testing.T) {
	h := newHarness(t)
	log := h.makePoolEventLog("AmpUpdateStarted", nil, []any{big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4)}, "0x0")
	name, ok := h.svc.extractor.poolEvent(log)
	if !ok || name != EventAmpUpdateStarted {
		t.Errorf("poolEvent = (%q, %v), want (AmpUpdateStarted, true)", name, ok)
	}
}

func TestExtractor_UnknownTopicNotRecognised(t *testing.T) {
	h := newHarness(t)
	log := shared.Log{Topics: []string{"0x" + strings.Repeat("aa", 32)}}
	if _, ok := h.svc.extractor.vaultEvent(log); ok {
		t.Error("unknown topic should not match vault")
	}
	if _, ok := h.svc.extractor.poolEvent(log); ok {
		t.Error("unknown topic should not match pool")
	}
	// Also: empty topics returns false.
	empty := shared.Log{}
	if _, ok := h.svc.extractor.vaultEvent(empty); ok {
		t.Error("empty topics should not match vault")
	}
	if _, ok := h.svc.extractor.poolEvent(empty); ok {
		t.Error("empty topics should not match pool")
	}
}

// PoolRegistered topic is recognised but produces no decoded event (audit
// only — the worker does not yet auto-discover pools from PoolRegistered).
func TestExtractor_PoolRegisteredTopicReturnsNoDecodedEvent(t *testing.T) {
	h := newHarness(t)
	ev := h.vaultEventsABI.Events["PoolRegistered"]
	data, err := ev.Inputs.NonIndexed().Pack(uint8(0))
	if err != nil {
		t.Fatalf("packing PoolRegistered: %v", err)
	}
	log := shared.Log{
		Address: BalancerVaultAddress.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			testPoolID.Hex(),
			common.BytesToHash(testPoolAddr.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	decoded, err := h.svc.extractor.extractVaultEvent(log)
	if err != nil {
		t.Errorf("PoolRegistered should not error: %v", err)
	}
	if decoded != nil {
		t.Errorf("PoolRegistered should not produce a decoded event, got %+v", decoded)
	}
}

// Multicaller with no calls succeeds (defensive — exercises trivial path).
func TestExtractor_NewExtractorSucceeds(t *testing.T) {
	if _, err := newEventExtractor(); err != nil {
		t.Errorf("newEventExtractor: %v", err)
	}
}

// Vault Swap on a pool with empty token slots should trigger populatePoolTokens
// and then proceed with the swap. Already covered structurally, but also assert
// that the second-pass tokenIndices succeeds on the freshly-loaded slots.
func TestProcessReceipt_VaultSwapAfterPopulate_ResolvesIndices(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.svc.registry.addPool(pool, nil, nil)

	tokenCounter := int64(99)
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
		tokenCounter++
		return tokenCounter, nil
	}
	var savedSwap *entity.BalancerPoolSwap
	h.balancerRepo.SaveBalancerPoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.BalancerPoolSwap) error {
		savedSwap = s
		return nil
	}
	// Multicall sequence: 1=vault.getPoolTokens, 2=ERC20-metadata-batch (2N=6 results),
	// then readPoolState (one per receipt).
	strT, _ := abi.NewType("string", "", nil)
	uint8T, _ := abi.NewType("uint8", "", nil)
	symOut, _ := (abi.Arguments{{Type: strT}}).Pack("TKN")
	decOut, _ := (abi.Arguments{{Type: uint8T}}).Pack(uint8(18))
	metaBatchResp := []outbound.Result{
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
	}
	var callIdx atomic.Int32
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		n := callIdx.Add(1)
		switch n {
		case 1:
			return h.vaultTokensOnlyResults(), nil
		case 2:
			return metaBatchResp, nil
		default:
			return h.poolStateResults(), nil
		}
	}

	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(50), big.NewInt(49), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	if err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if savedSwap == nil {
		t.Fatal("swap row not persisted")
	}
	// wstETH = slot 0, WETH = slot 2 (slot 1 is phantom BPT).
	if savedSwap.TokenInIdx != 0 || savedSwap.TokenOutIdx != 2 {
		t.Errorf("after populate, idx (%d,%d), want (0,2)", savedSwap.TokenInIdx, savedSwap.TokenOutIdx)
	}
}

// negate / bigIntCopy nil-input branches.
func TestHelpers_NilSafety(t *testing.T) {
	if v := negate(nil); v == nil || v.Sign() != 0 {
		t.Errorf("negate(nil) = %v, want 0", v)
	}
	if v := bigIntCopy(nil); v != nil {
		t.Errorf("bigIntCopy(nil) = %v, want nil", v)
	}
	if v := bigIntCopy(big.NewInt(5)); v.Int64() != 5 {
		t.Errorf("bigIntCopy(5) = %v, want 5", v)
	}
	if v := bigIntToTimePtr(nil); v != nil {
		t.Errorf("bigIntToTimePtr(nil) = %v, want nil", v)
	}
	if v := bigIntToTimePtr(big.NewInt(0)); v != nil {
		t.Errorf("bigIntToTimePtr(0) = %v, want nil", v)
	}
}

// projectByToken with no registered slots falls back to input ordering.
func TestProjectByToken_NoSlotsFallback(t *testing.T) {
	rp := newRegisteredPool(&entity.BalancerPool{}, nil, nil)
	out := rp.projectByToken([]common.Address{wstETHAddr}, []*big.Int{big.NewInt(7)})
	if len(out) != 1 || out[0].Int64() != 7 {
		t.Errorf("fallback projection = %v, want [7]", out)
	}
}

// Multicall failure during getPoolTokens during populate should also propagate.
func TestProcessReceipt_PopulatePoolTokensFailure_PropagatesError(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.svc.registry.addPool(pool, nil, nil)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, fmt.Errorf("getPoolTokens RPC down")
	}
	log := h.makeVaultSwapLog(testPoolID, wstETHAddr, wethAddr, big.NewInt(1), big.NewInt(1), "0x1")
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}
	err := h.deliver([]shared.TransactionReceipt{receipt}, 100, 0)
	if err == nil || !strings.Contains(err.Error(), "getPoolTokens RPC down") {
		t.Errorf("expected populate failure, got: %v", err)
	}
}
