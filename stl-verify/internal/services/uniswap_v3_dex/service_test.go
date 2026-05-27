package uniswap_v3_dex

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"sync"
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
// MockUniswapV3PoolRepository: function-pointer mock for
// outbound.UniswapV3PoolRepository.
// -----------------------------------------------------------------------------

type MockUniswapV3PoolRepository struct {
	GetUniswapV3PoolFn                func(ctx context.Context, chainID int64, address common.Address) (*entity.UniswapV3Pool, error)
	ListEnabledUniswapV3PoolsFn       func(ctx context.Context, chainID int64) ([]*entity.UniswapV3Pool, error)
	GetUniswapV3PositionFn            func(ctx context.Context, chainID int64, nfpm common.Address, tokenID *big.Int) (*entity.UniswapV3Position, error)
	UpsertUniswapV3PositionFn         func(ctx context.Context, tx pgx.Tx, pos *entity.UniswapV3Position) (int64, error)
	SetUniswapV3PositionOwnerFn       func(ctx context.Context, tx pgx.Tx, positionID int64, owner common.Address) error
	SetUniswapV3PositionBurnedFn      func(ctx context.Context, tx pgx.Tx, positionID int64) error
	SaveUniswapV3PoolStateFn          func(ctx context.Context, tx pgx.Tx, state *entity.UniswapV3PoolState) error
	SaveUniswapV3PoolSwapFn           func(ctx context.Context, tx pgx.Tx, swap *entity.UniswapV3PoolSwap) error
	SaveUniswapV3PositionStateFn      func(ctx context.Context, tx pgx.Tx, state *entity.UniswapV3PositionState) error
	SaveUniswapV3PoolLiquidityEventFn func(ctx context.Context, tx pgx.Tx, evt *entity.UniswapV3PoolLiquidityEvent) error
	SaveUniswapV3PoolParameterEventFn func(ctx context.Context, tx pgx.Tx, evt *entity.UniswapV3PoolParameterEvent) error
}

func (m *MockUniswapV3PoolRepository) GetUniswapV3Pool(ctx context.Context, chainID int64, address common.Address) (*entity.UniswapV3Pool, error) {
	if m.GetUniswapV3PoolFn != nil {
		return m.GetUniswapV3PoolFn(ctx, chainID, address)
	}
	return nil, nil
}
func (m *MockUniswapV3PoolRepository) ListEnabledUniswapV3Pools(ctx context.Context, chainID int64) ([]*entity.UniswapV3Pool, error) {
	if m.ListEnabledUniswapV3PoolsFn != nil {
		return m.ListEnabledUniswapV3PoolsFn(ctx, chainID)
	}
	return nil, nil
}
func (m *MockUniswapV3PoolRepository) GetUniswapV3Position(ctx context.Context, chainID int64, nfpm common.Address, tokenID *big.Int) (*entity.UniswapV3Position, error) {
	if m.GetUniswapV3PositionFn != nil {
		return m.GetUniswapV3PositionFn(ctx, chainID, nfpm, tokenID)
	}
	return nil, nil
}
func (m *MockUniswapV3PoolRepository) UpsertUniswapV3Position(ctx context.Context, tx pgx.Tx, pos *entity.UniswapV3Position) (int64, error) {
	if m.UpsertUniswapV3PositionFn != nil {
		return m.UpsertUniswapV3PositionFn(ctx, tx, pos)
	}
	return 1, nil
}
func (m *MockUniswapV3PoolRepository) SetUniswapV3PositionOwner(ctx context.Context, tx pgx.Tx, positionID int64, owner common.Address) error {
	if m.SetUniswapV3PositionOwnerFn != nil {
		return m.SetUniswapV3PositionOwnerFn(ctx, tx, positionID, owner)
	}
	return nil
}
func (m *MockUniswapV3PoolRepository) SetUniswapV3PositionBurned(ctx context.Context, tx pgx.Tx, positionID int64) error {
	if m.SetUniswapV3PositionBurnedFn != nil {
		return m.SetUniswapV3PositionBurnedFn(ctx, tx, positionID)
	}
	return nil
}
func (m *MockUniswapV3PoolRepository) SaveUniswapV3PoolState(ctx context.Context, tx pgx.Tx, state *entity.UniswapV3PoolState) error {
	if m.SaveUniswapV3PoolStateFn != nil {
		return m.SaveUniswapV3PoolStateFn(ctx, tx, state)
	}
	return nil
}
func (m *MockUniswapV3PoolRepository) SaveUniswapV3PoolSwap(ctx context.Context, tx pgx.Tx, swap *entity.UniswapV3PoolSwap) error {
	if m.SaveUniswapV3PoolSwapFn != nil {
		return m.SaveUniswapV3PoolSwapFn(ctx, tx, swap)
	}
	return nil
}
func (m *MockUniswapV3PoolRepository) SaveUniswapV3PositionState(ctx context.Context, tx pgx.Tx, state *entity.UniswapV3PositionState) error {
	if m.SaveUniswapV3PositionStateFn != nil {
		return m.SaveUniswapV3PositionStateFn(ctx, tx, state)
	}
	return nil
}
func (m *MockUniswapV3PoolRepository) SaveUniswapV3PoolLiquidityEvent(ctx context.Context, tx pgx.Tx, evt *entity.UniswapV3PoolLiquidityEvent) error {
	if m.SaveUniswapV3PoolLiquidityEventFn != nil {
		return m.SaveUniswapV3PoolLiquidityEventFn(ctx, tx, evt)
	}
	return nil
}
func (m *MockUniswapV3PoolRepository) SaveUniswapV3PoolParameterEvent(ctx context.Context, tx pgx.Tx, evt *entity.UniswapV3PoolParameterEvent) error {
	if m.SaveUniswapV3PoolParameterEventFn != nil {
		return m.SaveUniswapV3PoolParameterEventFn(ctx, tx, evt)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Test fixtures and harness
// -----------------------------------------------------------------------------

var (
	testPoolAddr  = common.HexToAddress("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640") // USDC/WETH 0.05%
	testToken0    = common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48") // USDC
	testToken1    = common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2") // WETH
	testNFPMAddr  = DefaultNFPMAddress
	testUser      = common.HexToAddress("0x9999999999999999999999999999999999999999")
	testRecipient = common.HexToAddress("0x8888888888888888888888888888888888888888")
	testTxHash    = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

type harness struct {
	t            *testing.T
	svc          *Service
	uniswapRepo  *MockUniswapV3PoolRepository
	tokenRepo    *testutil.MockTokenRepository
	protocolRepo *testutil.MockProtocolRepository
	eventRepo    *testutil.MockEventRepository
	multicaller  *testutil.MockMulticaller
	txManager    *testutil.MockTxManager
	consumer     *testutil.MockSQSConsumer
	cache        *testutil.MockBlockCache

	poolEventsABI *abi.ABI
	nfpmEventsABI *abi.ABI
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	uniswapRepo := &MockUniswapV3PoolRepository{}
	tokenRepo := &testutil.MockTokenRepository{}
	protocolRepo := &testutil.MockProtocolRepository{}
	eventRepo := &testutil.MockEventRepository{}
	mc := testutil.NewMockMulticaller()
	txManager := &testutil.MockTxManager{}
	consumer := &testutil.MockSQSConsumer{}
	cache := testutil.NewMockBlockCache()

	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults(), NFPMAddress: testNFPMAddr}
	cfg.ChainID = 1

	svc, err := NewService(cfg, consumer, cache, mc, txManager, uniswapRepo, tokenRepo, protocolRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	poolEv, err := abis.GetUniswapV3PoolEventsABI()
	if err != nil {
		t.Fatalf("pool events ABI: %v", err)
	}
	nfpmEv, err := abis.GetUniswapV3NFPMEventsABI()
	if err != nil {
		t.Fatalf("nfpm events ABI: %v", err)
	}

	return &harness{
		t:             t,
		svc:           svc,
		uniswapRepo:   uniswapRepo,
		tokenRepo:     tokenRepo,
		protocolRepo:  protocolRepo,
		eventRepo:     eventRepo,
		multicaller:   mc,
		txManager:     txManager,
		consumer:      consumer,
		cache:         cache,
		poolEventsABI: poolEv,
		nfpmEventsABI: nfpmEv,
	}
}

func makePool() *entity.UniswapV3Pool {
	return &entity.UniswapV3Pool{
		ID:          1,
		ChainID:     1,
		Address:     testPoolAddr,
		Token0ID:    101,
		Token1ID:    102,
		FeeTier:     500,
		TickSpacing: 10,
		Label:       "USDC/WETH 0.05%",
		Enabled:     true,
	}
}

func (h *harness) primePool(pool *entity.UniswapV3Pool) {
	h.svc.registry.addPool(pool)
	h.svc.registry.setTokenAddresses(pool.ID, testToken0, testToken1)
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

// packSlot0 returns ABI-packed slot0() output.
func (h *harness) packSlot0(sqrt *big.Int, tick int32, obsIdx, obsCard, obsCardNext uint16, feeProto uint8, unlocked bool) []byte {
	u160, _ := abi.NewType("uint160", "", nil)
	i24, _ := abi.NewType("int24", "", nil)
	u16, _ := abi.NewType("uint16", "", nil)
	u8, _ := abi.NewType("uint8", "", nil)
	bt, _ := abi.NewType("bool", "", nil)
	args := abi.Arguments{
		{Type: u160}, {Type: i24}, {Type: u16}, {Type: u16}, {Type: u16}, {Type: u8}, {Type: bt},
	}
	out, err := args.Pack(sqrt, big.NewInt(int64(tick)), obsIdx, obsCard, obsCardNext, feeProto, unlocked)
	if err != nil {
		h.t.Fatalf("packSlot0: %v", err)
	}
	return out
}

// packObserve packs observe([0]) return data: (int56[], uint160[]).
func (h *harness) packObserve(tickCum, secsPerLiq *big.Int) []byte {
	i56arr, _ := abi.NewType("int56[]", "", nil)
	u160arr, _ := abi.NewType("uint160[]", "", nil)
	args := abi.Arguments{{Type: i56arr}, {Type: u160arr}}
	out, err := args.Pack([]*big.Int{tickCum}, []*big.Int{secsPerLiq})
	if err != nil {
		h.t.Fatalf("packObserve: %v", err)
	}
	return out
}

// packPositions encodes NFPM positions(tokenId) return data — 12 values.
func (h *harness) packPositions(token0, token1 common.Address, fee int32, tickLower, tickUpper int32, liquidity, fg0, fg1, owed0, owed1 *big.Int) []byte {
	u96, _ := abi.NewType("uint96", "", nil)
	addr, _ := abi.NewType("address", "", nil)
	u24, _ := abi.NewType("uint24", "", nil)
	i24, _ := abi.NewType("int24", "", nil)
	u128, _ := abi.NewType("uint128", "", nil)
	u256, _ := abi.NewType("uint256", "", nil)
	args := abi.Arguments{
		{Type: u96},  // nonce
		{Type: addr}, // operator
		{Type: addr}, // token0
		{Type: addr}, // token1
		{Type: u24},  // fee
		{Type: i24},  // tickLower
		{Type: i24},  // tickUpper
		{Type: u128}, // liquidity
		{Type: u256}, // feeGrowthInside0LastX128
		{Type: u256}, // feeGrowthInside1LastX128
		{Type: u128}, // tokensOwed0
		{Type: u128}, // tokensOwed1
	}
	out, err := args.Pack(
		big.NewInt(0), common.Address{}, token0, token1,
		big.NewInt(int64(fee)),
		big.NewInt(int64(tickLower)), big.NewInt(int64(tickUpper)),
		liquidity, fg0, fg1, owed0, owed1,
	)
	if err != nil {
		h.t.Fatalf("packPositions: %v", err)
	}
	return out
}

// poolStateResults returns the canned multicall results for the 5-call
// pool state read: slot0, liquidity, observe, balanceOf×2.
func (h *harness) poolStateResults() []outbound.Result {
	return []outbound.Result{
		{Success: true, ReturnData: h.packSlot0(big.NewInt(1e18), 100, 1, 10, 100, 0, true)},
		{Success: true, ReturnData: h.packUint256(big.NewInt(5000))}, // liquidity
		{Success: true, ReturnData: h.packObserve(big.NewInt(99999), big.NewInt(88888))},
		{Success: true, ReturnData: h.packUint256(big.NewInt(100000))}, // balanceOf(token0)
		{Success: true, ReturnData: h.packUint256(big.NewInt(200000))}, // balanceOf(token1)
	}
}

// dispatchMulticall multiplexes by call count: 5 calls = pool state, 1 call =
// NFPM positions (caller provides via the override map keyed by the pool/NFPM
// target address) or pool static read (3 calls).
func (h *harness) defaultMulticall() func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	return func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 5:
			return h.poolStateResults(), nil
		case 1:
			// NFPM positions(tokenId): default to the test pool's tokens/fee.
			return []outbound.Result{
				{Success: true, ReturnData: h.packPositions(testToken0, testToken1, 500, -887220, 887220,
					big.NewInt(100), big.NewInt(11), big.NewInt(22), big.NewInt(33), big.NewInt(44))},
			}, nil
		}
		return nil, errors.New("unexpected multicall arg count")
	}
}

func (h *harness) makePoolEventLog(name string, topicArgs []common.Hash, data []byte) shared.Log {
	ev := h.poolEventsABI.Events[name]
	topics := []string{ev.ID.Hex()}
	for _, t := range topicArgs {
		topics = append(topics, t.Hex())
	}
	return shared.Log{
		Address:         testPoolAddr.Hex(),
		Topics:          topics,
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x1",
	}
}

func (h *harness) makeNFPMEventLog(name string, topicArgs []common.Hash, data []byte) shared.Log {
	ev := h.nfpmEventsABI.Events[name]
	topics := []string{ev.ID.Hex()}
	for _, t := range topicArgs {
		topics = append(topics, t.Hex())
	}
	return shared.Log{
		Address:         testNFPMAddr.Hex(),
		Topics:          topics,
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x2",
	}
}

func (h *harness) buildSwapLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["Swap"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(-100), big.NewInt(101), big.NewInt(1e18), big.NewInt(5000), big.NewInt(123))
	if err != nil {
		t.Fatalf("packing Swap data: %v", err)
	}
	return h.makePoolEventLog("Swap", []common.Hash{common.BytesToHash(testUser.Bytes()), common.BytesToHash(testRecipient.Bytes())}, data)
}

func (h *harness) buildMintLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["Mint"]
	data, err := ev.Inputs.NonIndexed().Pack(testUser, big.NewInt(1000), big.NewInt(50), big.NewInt(60))
	if err != nil {
		t.Fatalf("packing Mint data: %v", err)
	}
	return h.makePoolEventLog("Mint", []common.Hash{
		common.BytesToHash(testUser.Bytes()),
		common.BigToHash(big.NewInt(-100)),
		common.BigToHash(big.NewInt(100)),
	}, data)
}

func (h *harness) buildBurnLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["Burn"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(500), big.NewInt(20), big.NewInt(30))
	if err != nil {
		t.Fatalf("packing Burn data: %v", err)
	}
	return h.makePoolEventLog("Burn", []common.Hash{
		common.BytesToHash(testUser.Bytes()),
		common.BigToHash(big.NewInt(-100)),
		common.BigToHash(big.NewInt(100)),
	}, data)
}

func (h *harness) buildCollectLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["Collect"]
	data, err := ev.Inputs.NonIndexed().Pack(testRecipient, big.NewInt(7), big.NewInt(8))
	if err != nil {
		t.Fatalf("packing Collect data: %v", err)
	}
	return h.makePoolEventLog("Collect", []common.Hash{
		common.BytesToHash(testUser.Bytes()),
		common.BigToHash(big.NewInt(-100)),
		common.BigToHash(big.NewInt(100)),
	}, data)
}

func (h *harness) buildInitializeLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["Initialize"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(1e18), big.NewInt(100))
	if err != nil {
		t.Fatalf("packing Initialize data: %v", err)
	}
	return h.makePoolEventLog("Initialize", nil, data)
}

func (h *harness) buildIOCNLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["IncreaseObservationCardinalityNext"]
	data, err := ev.Inputs.NonIndexed().Pack(uint16(10), uint16(100))
	if err != nil {
		t.Fatalf("packing IOCN data: %v", err)
	}
	return h.makePoolEventLog("IncreaseObservationCardinalityNext", nil, data)
}

func (h *harness) buildSetFeeProtocolLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["SetFeeProtocol"]
	data, err := ev.Inputs.NonIndexed().Pack(uint8(0), uint8(0), uint8(4), uint8(4))
	if err != nil {
		t.Fatalf("packing SetFeeProtocol data: %v", err)
	}
	return h.makePoolEventLog("SetFeeProtocol", nil, data)
}

func (h *harness) buildCollectProtocolLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["CollectProtocol"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(11), big.NewInt(22))
	if err != nil {
		t.Fatalf("packing CollectProtocol data: %v", err)
	}
	return h.makePoolEventLog("CollectProtocol", []common.Hash{
		common.BytesToHash(testUser.Bytes()),
		common.BytesToHash(testRecipient.Bytes()),
	}, data)
}

func (h *harness) buildFlashLog(t *testing.T) shared.Log {
	ev := h.poolEventsABI.Events["Flash"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(100), big.NewInt(200), big.NewInt(101), big.NewInt(201))
	if err != nil {
		t.Fatalf("packing Flash data: %v", err)
	}
	return h.makePoolEventLog("Flash", []common.Hash{
		common.BytesToHash(testUser.Bytes()),
		common.BytesToHash(testRecipient.Bytes()),
	}, data)
}

func (h *harness) buildNFPMIncreaseLog(t *testing.T, tokenID *big.Int) shared.Log {
	ev := h.nfpmEventsABI.Events["IncreaseLiquidity"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(1000), big.NewInt(50), big.NewInt(60))
	if err != nil {
		t.Fatalf("packing IncreaseLiquidity data: %v", err)
	}
	return h.makeNFPMEventLog("IncreaseLiquidity", []common.Hash{common.BigToHash(tokenID)}, data)
}

func (h *harness) buildNFPMDecreaseLog(t *testing.T, tokenID *big.Int, liquidity *big.Int) shared.Log {
	ev := h.nfpmEventsABI.Events["DecreaseLiquidity"]
	data, err := ev.Inputs.NonIndexed().Pack(liquidity, big.NewInt(20), big.NewInt(30))
	if err != nil {
		t.Fatalf("packing DecreaseLiquidity data: %v", err)
	}
	return h.makeNFPMEventLog("DecreaseLiquidity", []common.Hash{common.BigToHash(tokenID)}, data)
}

func (h *harness) buildNFPMCollectLog(t *testing.T, tokenID *big.Int) shared.Log {
	ev := h.nfpmEventsABI.Events["Collect"]
	data, err := ev.Inputs.NonIndexed().Pack(testRecipient, big.NewInt(11), big.NewInt(22))
	if err != nil {
		t.Fatalf("packing NFPM Collect data: %v", err)
	}
	return h.makeNFPMEventLog("Collect", []common.Hash{common.BigToHash(tokenID)}, data)
}

func (h *harness) buildNFPMTransferLog(t *testing.T, tokenID *big.Int, from, to common.Address) shared.Log {
	return h.makeNFPMEventLog("Transfer", []common.Hash{
		common.BytesToHash(from.Bytes()),
		common.BytesToHash(to.Bytes()),
		common.BigToHash(tokenID),
	}, nil)
}

func (h *harness) deliverReceipt(t *testing.T, blockNumber, blockVersion int64, logs []shared.Log) error {
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs:            logs,
	}
	body, err := json.Marshal([]shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	h.cache.SetReceipts(1, blockNumber, int(blockVersion), body)
	return h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        int(blockVersion),
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	})
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
	if defaults.NFPMAddress != DefaultNFPMAddress {
		t.Errorf("NFPMAddress = %s, want default", defaults.NFPMAddress.Hex())
	}
}

func TestNewService_ValidateDependencies(t *testing.T) {
	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	cfg.ChainID = 1
	rc := testutil.NewMockBlockCache()
	mc := testutil.NewMockMulticaller()
	tm := &testutil.MockTxManager{}
	ur := &MockUniswapV3PoolRepository{}
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
		uniswapRepo outbound.UniswapV3PoolRepository
		tokenRepo   outbound.TokenRepository
		proto       outbound.ProtocolRepository
		evt         outbound.EventRepository
		wantErrSub  string
		wantSuccess bool
	}{
		{"nil consumer", nil, rc, mc, tm, ur, tr, pr, er, "consumer", false},
		{"nil cache", cons, nil, mc, tm, ur, tr, pr, er, "cache", false},
		{"nil multicaller", cons, rc, nil, tm, ur, tr, pr, er, "multicaller", false},
		{"nil txm", cons, rc, mc, nil, ur, tr, pr, er, "txManager", false},
		{"nil uniswapRepo", cons, rc, mc, tm, nil, tr, pr, er, "uniswapRepo", false},
		{"nil tokenRepo", cons, rc, mc, tm, ur, nil, pr, er, "tokenRepo", false},
		{"nil protocol", cons, rc, mc, tm, ur, tr, nil, er, "protocolRepo", false},
		{"nil event", cons, rc, mc, tm, ur, tr, pr, nil, "eventRepo", false},
		{"all valid", cons, rc, mc, tm, ur, tr, pr, er, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewService(cfg, c.consumer, c.cache, c.mc, c.txm, c.uniswapRepo, c.tokenRepo, c.proto, c.evt)
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
	_, err := NewService(cfg, &testutil.MockSQSConsumer{}, testutil.NewMockBlockCache(), testutil.NewMockMulticaller(), &testutil.MockTxManager{}, &MockUniswapV3PoolRepository{}, &testutil.MockTokenRepository{}, &testutil.MockProtocolRepository{}, &testutil.MockEventRepository{})
	if err == nil {
		t.Fatal("expected error for zero ChainID")
	}
	if !strings.Contains(err.Error(), "ChainID") {
		t.Errorf("error %q does not mention ChainID", err.Error())
	}
}

func TestNewService_DefaultsNFPMAddress(t *testing.T) {
	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	cfg.ChainID = 1
	// NFPMAddress is zero value — service should populate the default.
	svc, err := NewService(cfg, &testutil.MockSQSConsumer{}, testutil.NewMockBlockCache(), testutil.NewMockMulticaller(), &testutil.MockTxManager{}, &MockUniswapV3PoolRepository{}, &testutil.MockTokenRepository{}, &testutil.MockProtocolRepository{}, &testutil.MockEventRepository{})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if svc.config.NFPMAddress != DefaultNFPMAddress {
		t.Errorf("NFPMAddress = %s, want %s", svc.config.NFPMAddress.Hex(), DefaultNFPMAddress.Hex())
	}
}

func TestStartLoadsPools(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.uniswapRepo.ListEnabledUniswapV3PoolsFn = func(ctx context.Context, chainID int64) ([]*entity.UniswapV3Pool, error) {
		return []*entity.UniswapV3Pool{pool}, nil
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
}

func TestStart_ListPoolsFails(t *testing.T) {
	h := newHarness(t)
	h.uniswapRepo.ListEnabledUniswapV3PoolsFn = func(_ context.Context, _ int64) ([]*entity.UniswapV3Pool, error) {
		return nil, errors.New("db boom")
	}
	if err := h.svc.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail")
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

func TestProcessBlockEvent_BadJSON(t *testing.T) {
	h := newHarness(t)
	h.cache.SetReceipts(1, 1, 0, json.RawMessage(`not-json`))
	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1, Version: 0})
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestProcessBlockEvent_CacheReadError(t *testing.T) {
	h := newHarness(t)
	h.cache.SetError(errors.New("cache down"))
	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1, Version: 0})
	if err == nil || !strings.Contains(err.Error(), "cache down") {
		t.Fatalf("expected cache error, got %v", err)
	}
}

// TestNoOpBlock — block with no relevant V3 logs writes nothing and returns nil.
func TestProcessBlockEvent_NoRelevantLogs(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)

	var savesObserved int32
	h.uniswapRepo.SaveUniswapV3PoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolState) error {
		atomic.AddInt32(&savesObserved, 1)
		return nil
	}
	// Irrelevant log: random ERC-20 transfer from an unrelated address.
	unrelated := shared.Log{Address: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef", Topics: []string{}}
	if err := h.deliverReceipt(t, 1, 0, []shared.Log{unrelated}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if got := atomic.LoadInt32(&savesObserved); got != 0 {
		t.Errorf("SavePoolState called %d times, want 0", got)
	}
}

// Test row #1: Swap.
func TestProcessReceipt_Swap_WritesAllRows(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)

	var savedSwap *entity.UniswapV3PoolSwap
	var savedState *entity.UniswapV3PoolState
	var savedEvent *entity.ProtocolEvent
	h.uniswapRepo.SaveUniswapV3PoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.UniswapV3PoolSwap) error {
		savedSwap = s
		return nil
	}
	h.uniswapRepo.SaveUniswapV3PoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.UniswapV3PoolState) error {
		savedState = s
		return nil
	}
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
		savedEvent = e
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()

	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildSwapLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if savedSwap == nil {
		t.Fatal("swap row not persisted")
	}
	if savedSwap.Amount0.Int64() != -100 {
		t.Errorf("amount0 = %s, want -100", savedSwap.Amount0)
	}
	if savedSwap.TickAfter != 123 {
		t.Errorf("TickAfter = %d, want 123", savedSwap.TickAfter)
	}
	if savedState == nil {
		t.Fatal("pool state row not persisted")
	}
	if savedState.TickCumulative == nil || savedState.TickCumulative.Int64() != 99999 {
		t.Errorf("TickCumulative = %v, want 99999", savedState.TickCumulative)
	}
	if savedState.SecsPerLiquidityCumulativeX128 == nil || savedState.SecsPerLiquidityCumulativeX128.Int64() != 88888 {
		t.Errorf("SecsPerLiquidityCumulativeX128 = %v, want 88888", savedState.SecsPerLiquidityCumulativeX128)
	}
	if savedState.Balance0 == nil || savedState.Balance0.Int64() != 100000 {
		t.Errorf("Balance0 = %v, want 100000", savedState.Balance0)
	}
	if savedEvent == nil || savedEvent.EventName != string(eventSwap) {
		t.Errorf("protocol_event missing or wrong name: %+v", savedEvent)
	}
}

// Test row #2: Mint.
func TestProcessReceipt_Mint(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var saved *entity.UniswapV3PoolLiquidityEvent
	h.uniswapRepo.SaveUniswapV3PoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, e *entity.UniswapV3PoolLiquidityEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildMintLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil || saved.EventKind != entity.UniswapV3PoolLiquidityEventMint {
		t.Fatalf("expected Mint liquidity event, got %+v", saved)
	}
	if saved.Sender == nil || *saved.Sender != testUser {
		t.Errorf("Sender = %v, want %s", saved.Sender, testUser.Hex())
	}
	if saved.Amount.Int64() != 1000 {
		t.Errorf("Amount = %s, want 1000", saved.Amount)
	}
}

// Test row #3: Burn.
func TestProcessReceipt_Burn(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var saved *entity.UniswapV3PoolLiquidityEvent
	h.uniswapRepo.SaveUniswapV3PoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, e *entity.UniswapV3PoolLiquidityEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildBurnLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil || saved.EventKind != entity.UniswapV3PoolLiquidityEventBurn {
		t.Fatalf("expected Burn liquidity event, got %+v", saved)
	}
	if saved.Sender != nil {
		t.Errorf("Burn must have nil Sender, got %v", saved.Sender)
	}
	if saved.Amount.Int64() != 500 {
		t.Errorf("Amount = %s, want 500", saved.Amount)
	}
}

// Test row #4: pool-side Collect.
func TestProcessReceipt_PoolCollect(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var saved *entity.UniswapV3PoolLiquidityEvent
	h.uniswapRepo.SaveUniswapV3PoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, e *entity.UniswapV3PoolLiquidityEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildCollectLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil || saved.EventKind != entity.UniswapV3PoolLiquidityEventCollect {
		t.Fatalf("expected Collect, got %+v", saved)
	}
	if saved.Recipient == nil || *saved.Recipient != testRecipient {
		t.Errorf("Recipient = %v, want %s", saved.Recipient, testRecipient.Hex())
	}
	if saved.Amount != nil {
		t.Errorf("Collect must have nil Amount, got %v", saved.Amount)
	}
}

// Test row #5: Initialize.
func TestProcessReceipt_Initialize(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var saved *entity.UniswapV3PoolParameterEvent
	h.uniswapRepo.SaveUniswapV3PoolParameterEventFn = func(_ context.Context, _ pgx.Tx, e *entity.UniswapV3PoolParameterEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildInitializeLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil || saved.EventKind != entity.UniswapV3PoolParameterEventInitialize {
		t.Fatalf("expected Initialize, got %+v", saved)
	}
	if saved.Tick == nil || *saved.Tick != 100 {
		t.Errorf("Tick = %v, want 100", saved.Tick)
	}
}

// Test row #6: IOCN.
func TestProcessReceipt_IncreaseObservationCardinalityNext(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var saved *entity.UniswapV3PoolParameterEvent
	h.uniswapRepo.SaveUniswapV3PoolParameterEventFn = func(_ context.Context, _ pgx.Tx, e *entity.UniswapV3PoolParameterEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildIOCNLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil || saved.EventKind != entity.UniswapV3PoolParameterEventIncreaseObservationCardinalityNext {
		t.Fatalf("expected IOCN, got %+v", saved)
	}
	if saved.ObservationCardinalityNew == nil || *saved.ObservationCardinalityNew != 100 {
		t.Errorf("New = %v, want 100", saved.ObservationCardinalityNew)
	}
}

// Test row #7: SetFeeProtocol.
func TestProcessReceipt_SetFeeProtocol(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var saved *entity.UniswapV3PoolParameterEvent
	h.uniswapRepo.SaveUniswapV3PoolParameterEventFn = func(_ context.Context, _ pgx.Tx, e *entity.UniswapV3PoolParameterEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildSetFeeProtocolLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil || saved.EventKind != entity.UniswapV3PoolParameterEventSetFeeProtocol {
		t.Fatalf("expected SetFeeProtocol, got %+v", saved)
	}
	if saved.FeeProtocol0New == nil || *saved.FeeProtocol0New != 4 {
		t.Errorf("FeeProtocol0New = %v, want 4", saved.FeeProtocol0New)
	}
}

// Test row #8: CollectProtocol.
func TestProcessReceipt_CollectProtocol(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var saved *entity.UniswapV3PoolParameterEvent
	h.uniswapRepo.SaveUniswapV3PoolParameterEventFn = func(_ context.Context, _ pgx.Tx, e *entity.UniswapV3PoolParameterEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildCollectProtocolLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil || saved.EventKind != entity.UniswapV3PoolParameterEventCollectProtocol {
		t.Fatalf("expected CollectProtocol, got %+v", saved)
	}
	if saved.Amount0 == nil || saved.Amount0.Int64() != 11 {
		t.Errorf("Amount0 = %v, want 11", saved.Amount0)
	}
}

// Flash is an intentional no-op.
func TestProcessReceipt_FlashIsNoOp(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)

	var anyWrite int32
	h.uniswapRepo.SaveUniswapV3PoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolState) error {
		atomic.AddInt32(&anyWrite, 1)
		return nil
	}
	h.uniswapRepo.SaveUniswapV3PoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolSwap) error {
		atomic.AddInt32(&anyWrite, 1)
		return nil
	}
	h.uniswapRepo.SaveUniswapV3PoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolLiquidityEvent) error {
		atomic.AddInt32(&anyWrite, 1)
		return nil
	}
	h.uniswapRepo.SaveUniswapV3PoolParameterEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolParameterEvent) error {
		atomic.AddInt32(&anyWrite, 1)
		return nil
	}
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		atomic.AddInt32(&anyWrite, 1)
		return nil
	}
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildFlashLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if got := atomic.LoadInt32(&anyWrite); got != 0 {
		t.Errorf("Flash triggered %d writes, want 0", got)
	}
}

// Per-receipt dedup: two pool events on the same pool in one receipt produce
// only one state row.
func TestProcessReceipt_PoolStateDedupedPerReceipt(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var stateWrites int32
	h.uniswapRepo.SaveUniswapV3PoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolState) error {
		atomic.AddInt32(&stateWrites, 1)
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildSwapLog(t), h.buildMintLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if got := atomic.LoadInt32(&stateWrites); got != 1 {
		t.Errorf("state writes = %d, want 1 (deduped)", got)
	}
}

// Idempotency: deliver the same block twice — the mock txManager passes through
// and the underlying Save* functions are called twice, but the DB-side trigger
// (not exercised here) would treat them as the same row. We assert: the
// processBlockEvent is safely idempotent (no error, no extra DB-side logic).
// Same block + bumped Version: writes happen at the new version (reorg path).
func TestProcessReceipt_Idempotency_SameBlockTwice(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var calls int32
	h.uniswapRepo.SaveUniswapV3PoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolSwap) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	log := h.buildSwapLog(t)
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{log}); err != nil {
		t.Fatalf("first deliver: %v", err)
	}
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{log}); err != nil {
		t.Fatalf("second deliver: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("Save calls = %d, want 2 (both invocations replay)", got)
	}
}

func TestProcessReceipt_Idempotency_BumpedBlockVersion(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	var versions []int32
	h.uniswapRepo.SaveUniswapV3PoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.UniswapV3PoolSwap) error {
		versions = append(versions, s.BlockVersion)
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	log := h.buildSwapLog(t)
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{log}); err != nil {
		t.Fatalf("v0: %v", err)
	}
	if err := h.deliverReceipt(t, 100, 1, []shared.Log{log}); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Errorf("versions = %v, want [0,1]", versions)
	}
}

// Tx failure: callback error → handler error → SQS won't ack.
func TestProcessReceipt_TxnFailure_NoDelete(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	h.txManager.WithTransactionFn = func(_ context.Context, _ func(tx pgx.Tx) error) error {
		return errors.New("tx aborted")
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildSwapLog(t)})
	if err == nil || !strings.Contains(err.Error(), "tx aborted") {
		t.Fatalf("expected tx aborted error, got %v", err)
	}
	// processBlockEvent returns an error — the SQS poll loop won't call
	// DeleteMessage in that branch.
}

// Multicall failure: surface as handler error.
func TestProcessReceipt_MulticallFailure(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc down")
	}
	err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildSwapLog(t)})
	if err == nil || !strings.Contains(err.Error(), "rpc down") {
		t.Fatalf("expected rpc down error, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// NFPM event tests
// -----------------------------------------------------------------------------

// IncreaseLiquidity on an unknown tokenId discovers the position via
// positions(tokenId), registers it, and writes uniswap_v3_position_state.
func TestProcessReceipt_NFPMIncreaseLiquidity_RegistersPosition(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	tokenID := big.NewInt(424242)

	var upserted *entity.UniswapV3Position
	h.uniswapRepo.UpsertUniswapV3PositionFn = func(_ context.Context, _ pgx.Tx, p *entity.UniswapV3Position) (int64, error) {
		upserted = p
		return 77, nil
	}
	var posState *entity.UniswapV3PositionState
	h.uniswapRepo.SaveUniswapV3PositionStateFn = func(_ context.Context, _ pgx.Tx, s *entity.UniswapV3PositionState) error {
		posState = s
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildNFPMIncreaseLog(t, tokenID)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if upserted == nil {
		t.Fatal("position not upserted")
	}
	if upserted.UniswapV3PoolID != pool.ID {
		t.Errorf("position pool = %d, want %d", upserted.UniswapV3PoolID, pool.ID)
	}
	if posState == nil {
		t.Fatal("position state not written")
	}
	if posState.UniswapV3PositionID != 77 {
		t.Errorf("posState id = %d, want 77", posState.UniswapV3PositionID)
	}
	if posState.Liquidity == nil || posState.Liquidity.Int64() != 100 {
		t.Errorf("posState Liquidity = %v, want 100", posState.Liquidity)
	}
}

// DecreaseLiquidity to liquidity=0 marks the position burned.
func TestProcessReceipt_NFPMDecreaseLiquidity_ToZero_FlipsBurned(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	tokenID := big.NewInt(424242)

	// Pre-register the position in the cache so we exercise the warm path.
	h.svc.posCache.put(&entity.UniswapV3Position{
		ID: 77, ChainID: 1, NFPMAddress: testNFPMAddr,
		TokenID: tokenID, UniswapV3PoolID: pool.ID, Owner: testUser,
		TickLower: -887220, TickUpper: 887220, Fee: 500, CreatedAtBlock: 99,
	})

	// positions() now returns liquidity = 0 (the post-decrease state).
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 5:
			return h.poolStateResults(), nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: h.packPositions(testToken0, testToken1, 500, -887220, 887220,
					big.NewInt(0), big.NewInt(11), big.NewInt(22), big.NewInt(33), big.NewInt(44))},
			}, nil
		}
		return nil, errors.New("unexpected calls")
	}

	var burnedID int64
	h.uniswapRepo.SetUniswapV3PositionBurnedFn = func(_ context.Context, _ pgx.Tx, positionID int64) error {
		burnedID = positionID
		return nil
	}
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildNFPMDecreaseLog(t, tokenID, big.NewInt(0))}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if burnedID != 77 {
		t.Errorf("SetUniswapV3PositionBurned called with %d, want 77", burnedID)
	}
}

// NFPM Collect — writes position state row (no liquidity change, so no pool
// state write).
func TestProcessReceipt_NFPMCollect_WritesPositionStateOnly(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	tokenID := big.NewInt(424242)

	h.svc.posCache.put(&entity.UniswapV3Position{
		ID: 77, ChainID: 1, NFPMAddress: testNFPMAddr,
		TokenID: tokenID, UniswapV3PoolID: pool.ID, Owner: testUser,
		TickLower: -887220, TickUpper: 887220, Fee: 500, CreatedAtBlock: 99,
	})

	var poolStateCalls int32
	h.uniswapRepo.SaveUniswapV3PoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolState) error {
		atomic.AddInt32(&poolStateCalls, 1)
		return nil
	}
	var positionStateCalls int32
	h.uniswapRepo.SaveUniswapV3PositionStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PositionState) error {
		atomic.AddInt32(&positionStateCalls, 1)
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildNFPMCollectLog(t, tokenID)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if got := atomic.LoadInt32(&positionStateCalls); got != 1 {
		t.Errorf("position state writes = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&poolStateCalls); got != 0 {
		t.Errorf("pool state writes = %d, want 0 for NFPM Collect", got)
	}
}

// NFPM Transfer on a known position updates owner.
func TestProcessReceipt_NFPMTransfer_UpdatesOwner(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	tokenID := big.NewInt(424242)

	h.svc.posCache.put(&entity.UniswapV3Position{
		ID: 77, ChainID: 1, NFPMAddress: testNFPMAddr,
		TokenID: tokenID, UniswapV3PoolID: pool.ID, Owner: testUser,
		TickLower: -887220, TickUpper: 887220, Fee: 500, CreatedAtBlock: 99,
	})

	var ownerCall struct {
		positionID int64
		newOwner   common.Address
	}
	h.uniswapRepo.SetUniswapV3PositionOwnerFn = func(_ context.Context, _ pgx.Tx, positionID int64, owner common.Address) error {
		ownerCall.positionID = positionID
		ownerCall.newOwner = owner
		return nil
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	newOwner := common.HexToAddress("0x4444444444444444444444444444444444444444")
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildNFPMTransferLog(t, tokenID, testUser, newOwner)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if ownerCall.positionID != 77 || ownerCall.newOwner != newOwner {
		t.Errorf("owner update = (%d, %s), want (77, %s)", ownerCall.positionID, ownerCall.newOwner.Hex(), newOwner.Hex())
	}
}

// NFPM event for a tokenId pointing at an untracked pool — silently drops
// after positions() lookup confirms it's out of scope.
func TestProcessReceipt_NFPM_UntrackedPool_DropsSilently(t *testing.T) {
	h := newHarness(t)
	// Don't prime any pool — registry is empty.
	tokenID := big.NewInt(999)

	var upsertCalls int32
	h.uniswapRepo.UpsertUniswapV3PositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3Position) (int64, error) {
		atomic.AddInt32(&upsertCalls, 1)
		return 1, nil
	}
	var saveStateCalls int32
	h.uniswapRepo.SaveUniswapV3PositionStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PositionState) error {
		atomic.AddInt32(&saveStateCalls, 1)
		return nil
	}
	var protocolEvents int32
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		atomic.AddInt32(&protocolEvents, 1)
		return nil
	}

	h.multicaller.ExecuteFn = h.defaultMulticall()
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildNFPMIncreaseLog(t, tokenID)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if got := atomic.LoadInt32(&upsertCalls); got != 0 {
		t.Errorf("upserts = %d, want 0 (out-of-scope)", got)
	}
	if got := atomic.LoadInt32(&saveStateCalls); got != 0 {
		t.Errorf("position state writes = %d, want 0 (out-of-scope)", got)
	}
	if got := atomic.LoadInt32(&protocolEvents); got != 1 {
		t.Errorf("protocol events = %d, want 1 (audit row still written)", got)
	}

	// Second event for the same tokenId — should hit the missing-marker cache
	// and skip the positions() read.
	preCallCount := h.multicaller.CallCount
	if err := h.deliverReceipt(t, 101, 0, []shared.Log{h.buildNFPMIncreaseLog(t, tokenID)}); err != nil {
		t.Fatalf("second deliver: %v", err)
	}
	if h.multicaller.CallCount != preCallCount {
		t.Errorf("expected no extra multicall on cached miss; before=%d after=%d", preCallCount, h.multicaller.CallCount)
	}
}

// Extractor helpers — direct error paths.
func TestExtractorHelpers_GetAddressErrors(t *testing.T) {
	d := map[string]any{"x": "not-an-address"}
	if _, err := getAddress(d, "missing"); err == nil {
		t.Error("missing key should error")
	}
	if _, err := getAddress(d, "x"); err == nil {
		t.Error("wrong type should error")
	}
}

func TestExtractorHelpers_GetBigIntAndInt32(t *testing.T) {
	d := map[string]any{
		"u16": uint16(42),
		"u8":  uint8(7),
		"bi":  big.NewInt(123),
		"bad": "not-a-number",
		"i32": int32(-5),
		"i16": int16(-3),
		"i8":  int8(-1),
		"u32": uint32(99),
	}
	for _, k := range []string{"u16", "u8", "bi"} {
		v, err := getBigInt(d, k)
		if err != nil || v == nil {
			t.Errorf("getBigInt(%q) = (%v, %v)", k, v, err)
		}
	}
	if _, err := getBigInt(d, "missing"); err == nil {
		t.Error("getBigInt missing key should error")
	}
	if _, err := getBigInt(d, "bad"); err == nil {
		t.Error("getBigInt bad type should error")
	}
	for _, k := range []string{"u16", "u8", "bi", "i32", "i16", "i8", "u32"} {
		v, err := getInt32(d, k)
		if err != nil {
			t.Errorf("getInt32(%q) = (%d, %v)", k, v, err)
		}
	}
	if _, err := getInt32(d, "missing"); err == nil {
		t.Error("getInt32 missing key should error")
	}
	if _, err := getInt32(d, "bad"); err == nil {
		t.Error("getInt32 bad type should error")
	}
}

// nfpmTokenID with empty event yields nil; isLiquidityChanging covers all branches.
func TestNfpmTokenIDAndLiquidityFlag(t *testing.T) {
	if id := nfpmTokenID(&decodedEvent{}); id != nil {
		t.Errorf("empty event tokenID = %v, want nil", id)
	}
	cases := []struct {
		name string
		d    *decodedEvent
		want bool
	}{
		{"increase", &decodedEvent{NFPMIncrease: &nfpmLiquidityEvent{}}, true},
		{"decrease", &decodedEvent{NFPMDecrease: &nfpmLiquidityEvent{}}, true},
		{"collect", &decodedEvent{NFPMCollect: &nfpmCollectEvent{}}, false},
		{"transfer", &decodedEvent{NFPMTransfer: &nfpmTransferEvent{}}, false},
	}
	for _, c := range cases {
		if got := isLiquidityChanging(c.d); got != c.want {
			t.Errorf("%s: isLiquidityChanging = %v, want %v", c.name, got, c.want)
		}
		// nfpmTokenID per branch.
		switch c.name {
		case "increase":
			c.d.NFPMIncrease.TokenID = big.NewInt(1)
		case "decrease":
			c.d.NFPMDecrease.TokenID = big.NewInt(2)
		case "collect":
			c.d.NFPMCollect.TokenID = big.NewInt(3)
		case "transfer":
			c.d.NFPMTransfer.TokenID = big.NewInt(4)
		}
		if got := nfpmTokenID(c.d); got == nil {
			t.Errorf("%s: nfpmTokenID = nil", c.name)
		}
	}
}

// N-UNI3: setting position.Burned must go through a mutex-aware setter. The
// pre-fix worker wrote `cached.Burned = true` directly on a pointer shared
// with the cache map — a data race the moment the cache is touched
// concurrently. We model the contract by asserting a dedicated
// `markBurned(tokenID)` helper exists and flips the cached entry.
func TestPositionCache_MarkBurned_SetsBurnedUnderLock(t *testing.T) {
	c := newPositionCache()
	tokenID := big.NewInt(7)
	c.put(&entity.UniswapV3Position{ID: 1, TokenID: tokenID, Burned: false})

	c.markBurned(tokenID)

	got := c.get(tokenID)
	if got == nil {
		t.Fatal("cached position lost after markBurned")
	}
	if !got.Burned {
		t.Errorf("Burned = false after markBurned, want true")
	}
	// markBurned on an unknown tokenID is a no-op (no panic, no stray entry).
	c.markBurned(big.NewInt(999))
	if c.get(big.NewInt(999)) != nil {
		t.Error("markBurned on unknown tokenID created a phantom cache entry")
	}
}

// findPoolForPosition must hold s.registry.mu exactly once. The original
// implementation RLock'd, then called tokensFor() which also RLock'd —
// sync.RWMutex is non-reentrant. If a writer queues between the two RLocks,
// the second one starves until the writer completes; combined with the outer
// RLock still held, this can deadlock under the right interleaving (writers
// excluded by readers, readers waiting for writer-blocked inner RLock).
//
// The test wedges a single writer between two consecutive findPoolForPosition
// calls and asserts both complete promptly. With the reentrant bug, the
// second call's inner RLock blocks on the queued writer that the outer RLock
// is itself blocking — classic deadlock.
func TestFindPoolForPosition_NoReentrantRLockDeadlock(t *testing.T) {
	h := newHarness(t)
	pool := &entity.UniswapV3Pool{
		ID:          1,
		ChainID:     1,
		Address:     common.HexToAddress("0xAAAa0000000000000000000000000000000000aa"),
		Token0ID:    1,
		Token1ID:    2,
		FeeTier:     500,
		TickSpacing: 10,
		Enabled:     true,
	}
	t0 := common.HexToAddress("0xBBBB000000000000000000000000000000000001")
	t1 := common.HexToAddress("0xCCCC000000000000000000000000000000000002")
	h.svc.registry.addPool(pool)
	h.svc.registry.setTokenAddresses(pool.ID, t0, t1)

	// Background writer hammers setTokenAddresses, ensuring the registry
	// write lock is contending against the readers throughout the test.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				h.svc.registry.setTokenAddresses(pool.ID, t0, t1)
				time.Sleep(time.Microsecond)
			}
		}
	}()

	// Reader: run findPoolForPosition many times. The pre-fix version
	// re-acquires the RLock internally via tokensFor — a writer queued
	// between the two acquisitions deadlocks the second one.
	done := make(chan int, 1)
	go func() {
		n := 0
		for n < 10_000 {
			if h.svc.findPoolForPosition(&nfpmPositionResult{Token0: t0, Token1: t1, Fee: 500}) == nil {
				done <- n
				return
			}
			n++
		}
		done <- n
	}()
	select {
	case n := <-done:
		close(stop)
		wg.Wait()
		if n < 10_000 {
			t.Fatalf("findPoolForPosition returned nil after %d iterations — registry inconsistent under contention", n)
		}
	case <-time.After(3 * time.Second):
		close(stop)
		wg.Wait()
		t.Fatal("findPoolForPosition deadlocked under contending writers (likely RWMutex reentrancy)")
	}
}

// Position cache: missing marker handling.
func TestPositionCache_MissingMarker(t *testing.T) {
	c := newPositionCache()
	if c.get(big.NewInt(99)) != nil {
		t.Error("empty cache should miss")
	}
	c.byTokenID["99"] = missingPositionMarker
	got := c.get(big.NewInt(99))
	if !isMissingMarker(got) {
		t.Error("expected missing marker")
	}
	if isMissingMarker(nil) {
		t.Error("nil should not be a marker")
	}
}

// putMissing must store the marker under the SUPPLIED tokenID's key (a
// concrete *big.Int), not under the marker entity's nil-TokenID stringified
// to "<nil>". And it must lock the cache like every other write.
func TestPositionCache_PutMissing_StoresUnderTokenIDKey(t *testing.T) {
	c := newPositionCache()
	tokenID := big.NewInt(42)
	c.putMissing(tokenID)

	if got := c.get(tokenID); !isMissingMarker(got) {
		t.Errorf("get(42) = %v, want missing marker", got)
	}
	// No stray "<nil>" key from blindly using marker.TokenID.String().
	if _, ok := c.byTokenID["<nil>"]; ok {
		t.Error("putMissing leaked a \"<nil>\" key into the cache")
	}
	if len(c.byTokenID) != 1 {
		t.Errorf("cache size = %d, want 1", len(c.byTokenID))
	}
}

// Extractor error paths — invalid log shape (no topics → poolTopic false).
func TestExtractor_EmptyTopics(t *testing.T) {
	ex, err := newEventExtractor()
	if err != nil {
		t.Fatalf("newEventExtractor: %v", err)
	}
	log := shared.Log{Topics: []string{}}
	if _, ok := ex.poolTopic(log); ok {
		t.Error("empty topics should yield false from poolTopic")
	}
	if _, ok := ex.nfpmTopic(log); ok {
		t.Error("empty topics should yield false from nfpmTopic")
	}
	d, err := ex.extractPoolEvent(log)
	if err != nil || d != nil {
		t.Errorf("extractPoolEvent on empty = (%v, %v), want (nil, nil)", d, err)
	}
	d, err = ex.extractNFPMEvent(log)
	if err != nil || d != nil {
		t.Errorf("extractNFPMEvent on empty = (%v, %v), want (nil, nil)", d, err)
	}
}

// parseLogIndex error path: malformed hex.
func TestParseLogIndex_Errors(t *testing.T) {
	if _, err := parseLogIndex("xyz"); err == nil {
		t.Error("expected error on malformed log index")
	}
	if v, err := parseLogIndex(""); err != nil || v != 0 {
		t.Errorf("empty index = (%d, %v), want (0, nil)", v, err)
	}
}

// Saving the protocol_event fails — the whole handler returns error.
func TestProcessReceipt_SaveEventError_PropagatesFromTransaction(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		return errors.New("event save failed")
	}
	h.multicaller.ExecuteFn = h.defaultMulticall()
	err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildSwapLog(t)})
	if err == nil || !strings.Contains(err.Error(), "event save failed") {
		t.Fatalf("expected event save failure, got %v", err)
	}
}

// processBlockEvent dispatches to multiple receipts and joins errors.
func TestProcessBlockEvent_AggregatesReceiptErrors(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.primePool(pool)
	h.multicaller.ExecuteFn = h.defaultMulticall()
	h.uniswapRepo.SaveUniswapV3PoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.UniswapV3PoolSwap) error {
		return errors.New("swap save failed")
	}
	// Two receipts each with a Swap.
	receipts := []shared.TransactionReceipt{
		{TransactionHash: testTxHash, Logs: []shared.Log{h.buildSwapLog(t)}},
		{TransactionHash: testTxHash, Logs: []shared.Log{h.buildSwapLog(t)}},
	}
	body, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	h.cache.SetReceipts(1, 100, 0, body)
	err = h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 100, Version: 0,
		BlockTimestamp: time.Now().Unix(),
	})
	if err == nil {
		t.Fatal("expected errors.Join from receipts")
	}
}

// Lazy token-address resolution: when a pool's token addresses aren't cached,
// the worker issues a static read to populate them before the pool-state
// multicall.
func TestReadPoolStateForEvent_LazyTokenResolution(t *testing.T) {
	h := newHarness(t)
	pool := makePool()
	h.svc.registry.addPool(pool) // NOTE: no setTokenAddresses → triggers static read.

	var staticReadDone bool
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			// pool static read: token0, token1, fee — return our test tokens.
			staticReadDone = true
			addrType, _ := abi.NewType("address", "", nil)
			addrArgs := abi.Arguments{{Type: addrType}}
			u24, _ := abi.NewType("uint24", "", nil)
			u24Args := abi.Arguments{{Type: u24}}
			t0, _ := addrArgs.Pack(testToken0)
			t1, _ := addrArgs.Pack(testToken1)
			fee, _ := u24Args.Pack(big.NewInt(500))
			return []outbound.Result{
				{Success: true, ReturnData: t0},
				{Success: true, ReturnData: t1},
				{Success: true, ReturnData: fee},
			}, nil
		case 5:
			return h.poolStateResults(), nil
		}
		return nil, errors.New("unexpected call shape")
	}

	var saved *entity.UniswapV3PoolState
	h.uniswapRepo.SaveUniswapV3PoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.UniswapV3PoolState) error {
		saved = s
		return nil
	}
	if err := h.deliverReceipt(t, 100, 0, []shared.Log{h.buildSwapLog(t)}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if !staticReadDone {
		t.Error("expected static read to be triggered for unknown token addrs")
	}
	if saved == nil {
		t.Fatal("state not saved after lazy resolution")
	}
}
