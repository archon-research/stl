package morpho_indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

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

func TestMorphoBlueAddress(t *testing.T) {
	expected := "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"
	if MorphoBlueAddress.Hex() != expected {
		t.Errorf("MorphoBlueAddress = %s, want %s", MorphoBlueAddress.Hex(), expected)
	}
}

func TestMorphoBlueDeployBlock(t *testing.T) {
	tests := []struct {
		name    string
		chainID int64
		want    int64
		wantErr bool
	}{
		{"ethereum mainnet", 1, 18883124, false},
		{"base", 8453, 18925795, false},
		{"arbitrum", 42161, 226833208, false},
		{"unknown chain", 999, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MorphoBlueDeployBlock(tt.chainID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MorphoBlueDeployBlock(%d) error = %v, wantErr %v", tt.chainID, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("MorphoBlueDeployBlock(%d) = %d, want %d", tt.chainID, got, tt.want)
			}
		})
	}
}

// --- NewService validation ---

func TestNewService_ValidateDependencies(t *testing.T) {
	rc := testutil.NewMockBlockCache()
	mc := testutil.NewMockMulticaller()
	tm := &testutil.MockTxManager{}
	ur := &testutil.MockUserRepository{}
	pr := &testutil.MockProtocolRepository{}
	tr := &testutil.MockTokenRepository{}
	mRepo := &testutil.MockMorphoRepository{}
	er := &testutil.MockEventRepository{}
	rtr := &testutil.MockReceiptTokenRepository{}
	cons := &testutil.MockSQSConsumer{}

	sqsCfg := shared.SQSConsumerConfigDefaults()
	sqsCfg.ChainID = 1
	config := Config{SQSConsumerConfig: sqsCfg}

	tests := []struct {
		name             string
		consumer         outbound.SQSConsumer
		cache            outbound.BlockCache
		multicall        outbound.Multicaller
		txMgr            outbound.TxManager
		userRepo         outbound.UserRepository
		protoRepo        outbound.ProtocolRepository
		tokenRepo        outbound.TokenRepository
		morphoRepo       outbound.MorphoRepository
		eventRepo        outbound.EventRepository
		receiptTokenRepo outbound.ReceiptTokenRepository
		errContains      string
	}{
		{"nil consumer", nil, rc, mc, tm, ur, pr, tr, mRepo, er, rtr, "consumer is required"},
		{"nil cache", cons, nil, mc, tm, ur, pr, tr, mRepo, er, rtr, "cache is required"},
		{"nil multicall", cons, rc, nil, tm, ur, pr, tr, mRepo, er, rtr, "multicallClient is required"},
		{"nil txManager", cons, rc, mc, nil, ur, pr, tr, mRepo, er, rtr, "txManager is required"},
		{"nil userRepo", cons, rc, mc, tm, nil, pr, tr, mRepo, er, rtr, "userRepo is required"},
		{"nil protocolRepo", cons, rc, mc, tm, ur, nil, tr, mRepo, er, rtr, "protocolRepo is required"},
		{"nil tokenRepo", cons, rc, mc, tm, ur, pr, nil, mRepo, er, rtr, "tokenRepo is required"},
		{"nil morphoRepo", cons, rc, mc, tm, ur, pr, tr, nil, er, rtr, "morphoRepo is required"},
		{"nil eventRepo", cons, rc, mc, tm, ur, pr, tr, mRepo, nil, rtr, "eventRepo is required"},
		{"nil receiptTokenRepo", cons, rc, mc, tm, ur, pr, tr, mRepo, er, nil, "receiptTokenRepo is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(config, tt.consumer, tt.cache, tt.multicall, tt.txMgr, tt.userRepo, tt.protoRepo, tt.tokenRepo, tt.morphoRepo, tt.eventRepo, tt.receiptTokenRepo)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
			}
		})
	}

	// All deps provided: should succeed.
	svc, err := NewService(config, cons, rc, mc, tm, ur, pr, tr, mRepo, er, rtr)
	if err != nil {
		t.Fatalf("NewService with all deps: %v", err)
	}
	if svc == nil {
		t.Fatal("NewService returned nil service")
	}
}

// --- Morpho Blue events: happy path ---

func TestProcessBlockEvent_Supply(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	var savedState *entity.MorphoMarketState
	var savedPosition *entity.MorphoMarketPosition
	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoMarketState) error {
		savedState = s
		return nil
	}
	h.morphoRepo.SaveMarketPositionFn = func(_ context.Context, _ pgx.Tx, p *entity.MorphoMarketPosition) error {
		savedPosition = p
		return nil
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedState == nil {
		t.Fatal("SaveMarketState not called")
	}
	if savedState.MorphoMarketID != 42 {
		t.Errorf("MorphoMarketID = %d, want 42", savedState.MorphoMarketID)
	}
	if savedPosition == nil {
		t.Fatal("SaveMarketPosition not called")
	}
}

func TestProcessBlockEvent_PositionEvents(t *testing.T) {
	tests := []struct {
		name    string
		makeLog func(h *serviceTestHarness) shared.Log
	}{
		{"Supply", func(h *serviceTestHarness) shared.Log {
			return h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
		}},
		{"Withdraw", func(h *serviceTestHarness) shared.Log {
			return h.makeWithdrawLog(testMarketID, testCaller, testOnBehalf, testReceiver, big.NewInt(500), big.NewInt(450))
		}},
		{"Borrow", func(h *serviceTestHarness) shared.Log {
			return h.makeBorrowLog(testMarketID, testCaller, testOnBehalf, testReceiver, big.NewInt(2000), big.NewInt(1800))
		}},
		{"Repay", func(h *serviceTestHarness) shared.Log {
			return h.makeRepayLog(testMarketID, testCaller, testOnBehalf, big.NewInt(750), big.NewInt(700))
		}},
		{"SupplyCollateral", func(h *serviceTestHarness) shared.Log {
			return h.makeSupplyCollateralLog(testMarketID, testCaller, testOnBehalf, big.NewInt(3000))
		}},
		{"WithdrawCollateral", func(h *serviceTestHarness) shared.Log {
			return h.makeWithdrawCollateralLog(testMarketID, testCaller, testOnBehalf, testReceiver, big.NewInt(1500))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.setupMarketExistsInDB(testMarketID, 42)
			h.setupPositionEventMulticall()

			var stateCount, posCount int
			h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketState) error {
				stateCount++
				return nil
			}
			h.morphoRepo.SaveMarketPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketPosition) error {
				posCount++
				return nil
			}

			log := tt.makeLog(h)
			receipt := makeReceipt(testTxHash, log)

			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if stateCount != 1 {
				t.Errorf("SaveMarketState called %d times, want 1", stateCount)
			}
			if posCount != 1 {
				t.Errorf("SaveMarketPosition called %d times, want 1", posCount)
			}
		})
	}
}

func TestProcessBlockEvent_CreateMarket(t *testing.T) {
	h := newTestHarness(t)

	// getTokenPairMetadata returns 4 results, getMarketState returns 1 result.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 4:
			// getTokenPairMetadata
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("LOAN")},
				{Success: true, ReturnData: h.packUint8(18)},
				{Success: true, ReturnData: h.packString("COLL")},
				{Success: true, ReturnData: h.packUint8(8)},
			}, nil
		case 1:
			// getMarketState
			return []outbound.Result{h.defaultMarketStateResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var createdMarket *entity.MorphoMarket
	h.morphoRepo.GetOrCreateMarketFn = func(_ context.Context, _ pgx.Tx, m *entity.MorphoMarket) (int64, error) {
		createdMarket = m
		return 10, nil
	}
	var savedState *entity.MorphoMarketState
	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoMarketState) error {
		savedState = s
		return nil
	}

	log := h.makeCreateMarketLog(testMarketID, testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if createdMarket == nil {
		t.Fatal("GetOrCreateMarket not called")
	}
	if savedState == nil {
		t.Fatal("SaveMarketState not called")
	}
	if savedState.MorphoMarketID != 10 {
		t.Errorf("MorphoMarketID = %d, want 10", savedState.MorphoMarketID)
	}
}

func TestProcessBlockEvent_AccrueInterest(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 1 {
			return []outbound.Result{h.defaultMarketStateResult()}, nil
		}
		return nil, fmt.Errorf("unexpected call count: %d", len(calls))
	}

	var savedState *entity.MorphoMarketState
	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoMarketState) error {
		savedState = s
		return nil
	}

	log := h.makeAccrueInterestLog(testMarketID, big.NewInt(1000), big.NewInt(500), big.NewInt(10))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedState == nil {
		t.Fatal("SaveMarketState not called")
	}
	if savedState.PrevBorrowRate == nil || savedState.PrevBorrowRate.Int64() != 1000 {
		t.Errorf("PrevBorrowRate = %v, want 1000", savedState.PrevBorrowRate)
	}
	if savedState.InterestAccrued == nil || savedState.InterestAccrued.Int64() != 500 {
		t.Errorf("InterestAccrued = %v, want 500", savedState.InterestAccrued)
	}
	if savedState.FeeShares == nil || savedState.FeeShares.Int64() != 10 {
		t.Errorf("FeeShares = %v, want 10", savedState.FeeShares)
	}
}

func TestProcessBlockEvent_Liquidate(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult(), h.defaultPositionStateResult()}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	var posCount int
	h.morphoRepo.SaveMarketPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketPosition) error {
		posCount++
		return nil
	}

	log := h.makeLiquidateLog(testMarketID, testCaller, testBorrower,
		big.NewInt(100), big.NewInt(90), big.NewInt(200), big.NewInt(0), big.NewInt(0))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if posCount != 2 {
		t.Errorf("SaveMarketPosition called %d times, want 2 (borrower + liquidator)", posCount)
	}
}

func TestProcessBlockEvent_SetFee(t *testing.T) {
	h := newTestHarness(t)
	// SetFee only saves protocol event — no market state or position updates.
	var eventSaved bool
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		eventSaved = true
		return nil
	}

	log := h.makeSetFeeLog(testMarketID, big.NewInt(100000000000000000))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !eventSaved {
		t.Error("SaveEvent not called for SetFee")
	}
}

// --- MetaMorpho vault events ---

func TestProcessBlockEvent_VaultDeposit(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	var vaultStateSaved, vaultPosSaved bool
	h.morphoRepo.SaveVaultStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoVaultState) error {
		vaultStateSaved = true
		if s.MorphoVaultID != 7 {
			t.Errorf("MorphoVaultID = %d, want 7", s.MorphoVaultID)
		}
		return nil
	}
	h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
		vaultPosSaved = true
		return nil
	}

	log := h.makeVaultDepositLog(testVaultAddr, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !vaultStateSaved {
		t.Error("SaveVaultState not called")
	}
	if !vaultPosSaved {
		t.Error("SaveVaultPosition not called")
	}
}

// TestProcessBlockEvent_V2GovernanceEvent_AuditLogged verifies that a V2-only
// event (AddAdapter — no typed handler today) emitted by a known vault still
// produces a protocol_event audit-log row labelled with the correct event
// name. The structured-handling pieces (adapter table etc.) are deferred per
// docs/vec-198-morpho-v2-followup-plan.md, but operators must see these
// events landing.
func TestProcessBlockEvent_V2GovernanceEvent_AuditLogged(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	// Chain-verified topic for AddAdapter on sparkUSDTbc (2026-05-06).
	const addAdapterTopic = "0x8f125a24838c4c23e893904b255b5c672d43d4cb8af7e3d15841eaeabc1e68aa"
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2") // sparkUSDTbc liquidityAdapter

	log := shared.Log{
		Address: testVaultAddr.Hex(),
		Topics: []string{
			addAdapterTopic,
			common.BytesToHash(adapter.Bytes()).Hex(),
		},
		Data:            "0x",
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}

	var savedEvent *entity.ProtocolEvent
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
		savedEvent = e
		return nil
	}

	receipt := makeReceipt(testTxHash, log)
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedEvent == nil {
		t.Fatal("SaveEvent not called for V2 governance event")
	}
	if savedEvent.EventName != "AddAdapter" {
		t.Errorf("EventName = %q, want AddAdapter", savedEvent.EventName)
	}
	if !bytes.Equal(savedEvent.ContractAddress, testVaultAddr.Bytes()) {
		t.Errorf("ContractAddress = %x, want %s", savedEvent.ContractAddress, testVaultAddr.Hex())
	}
	if !json.Valid(savedEvent.EventData) {
		t.Errorf("EventData is not valid JSON: %s", savedEvent.EventData)
	}
}

func TestProcessBlockEvent_VaultWithdraw(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(50000))}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	var vaultStateSaved, vaultPosSaved bool
	h.morphoRepo.SaveVaultStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultState) error {
		vaultStateSaved = true
		return nil
	}
	h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
		vaultPosSaved = true
		return nil
	}

	log := h.makeVaultWithdrawLog(testVaultAddr, testCaller, testReceiver, testOnBehalf, big.NewInt(3000), big.NewInt(2700))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !vaultStateSaved {
		t.Error("SaveVaultState not called")
	}
	if !vaultPosSaved {
		t.Error("SaveVaultPosition not called")
	}
}

func TestProcessBlockEvent_VaultTransfer(t *testing.T) {
	from := common.HexToAddress("0x5555555555555555555555555555555555555555")
	to := common.HexToAddress("0x6666666666666666666666666666666666666666")
	zeroAddr := common.Address{}

	tests := []struct {
		name          string
		from          common.Address
		to            common.Address
		wantCalls     int
		wantPositions int
	}{
		{"from+to", from, to, 4, 2},
		{"from=zero, to=real", zeroAddr, to, 3, 1},
		{"from=real, to=zero", from, zeroAddr, 3, 1},
		{"from=vault, to=vault", testVaultAddr, testVaultAddr, 2, 0},
		{"from=zero, to=zero", zeroAddr, zeroAddr, 2, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

			var callCount int
			h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				callCount = len(calls)
				switch len(calls) {
				case 2:
					return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
				case 3:
					return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
				case 4:
					return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000)), h.defaultBalanceOfResult(big.NewInt(200000))}, nil
				default:
					return nil, fmt.Errorf("unexpected %d calls", len(calls))
				}
			}

			var posCount int
			h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
				posCount++
				return nil
			}

			log := h.makeVaultTransferLog(testVaultAddr, tt.from, tt.to, big.NewInt(5000))
			receipt := makeReceipt(testTxHash, log)

			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if callCount != tt.wantCalls {
				t.Errorf("multicall received %d calls, want %d", callCount, tt.wantCalls)
			}
			if posCount != tt.wantPositions {
				t.Errorf("SaveVaultPosition called %d times, want %d", posCount, tt.wantPositions)
			}
		})
	}
}

func TestProcessBlockEvent_VaultAccrueInterestV1(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	var savedState *entity.MorphoVaultState
	h.morphoRepo.SaveVaultStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoVaultState) error {
		savedState = s
		return nil
	}

	log := h.makeVaultAccrueInterestV1Log(testVaultAddr, big.NewInt(2000000), big.NewInt(100))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedState == nil {
		t.Fatal("SaveVaultState not called")
	}
	if savedState.FeeShares == nil || savedState.FeeShares.Int64() != 100 {
		t.Errorf("FeeShares = %v, want 100", savedState.FeeShares)
	}
	if savedState.NewTotalAssets == nil || savedState.NewTotalAssets.Int64() != 2000000 {
		t.Errorf("NewTotalAssets = %v, want 2000000", savedState.NewTotalAssets)
	}
	if savedState.PreviousTotalAssets != nil {
		t.Errorf("PreviousTotalAssets = %v, want nil (V1)", savedState.PreviousTotalAssets)
	}
	if savedState.ManagementFeeShares != nil {
		t.Errorf("ManagementFeeShares = %v, want nil (V1)", savedState.ManagementFeeShares)
	}
}

func TestProcessBlockEvent_VaultAccrueInterestV2(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	var savedState *entity.MorphoVaultState
	h.morphoRepo.SaveVaultStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoVaultState) error {
		savedState = s
		return nil
	}

	log := h.makeVaultAccrueInterestV2Log(testVaultAddr, big.NewInt(2900000), big.NewInt(3000000), big.NewInt(200), big.NewInt(150))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedState == nil {
		t.Fatal("SaveVaultState not called")
	}
	if savedState.FeeShares == nil || savedState.FeeShares.Int64() != 200 {
		t.Errorf("FeeShares = %v, want 200", savedState.FeeShares)
	}
	if savedState.NewTotalAssets == nil || savedState.NewTotalAssets.Int64() != 3000000 {
		t.Errorf("NewTotalAssets = %v, want 3000000", savedState.NewTotalAssets)
	}
	if savedState.PreviousTotalAssets == nil || savedState.PreviousTotalAssets.Int64() != 2900000 {
		t.Errorf("PreviousTotalAssets = %v, want 2900000", savedState.PreviousTotalAssets)
	}
	if savedState.ManagementFeeShares == nil || savedState.ManagementFeeShares.Int64() != 150 {
		t.Errorf("ManagementFeeShares = %v, want 150", savedState.ManagementFeeShares)
	}
}

// --- Vault discovery ---

func TestProcessBlockEvent_VaultDiscovery_Success(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// vault state + balance (after discovery, process the event)
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var vaultCreated bool
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		vaultCreated = true
		return 99, nil
	}

	// Emit a Deposit event from the unknown vault address.
	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !vaultCreated {
		t.Error("vault was not created in DB")
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("vault should be registered in vault registry")
	}
}

func TestProcessBlockEvent_VaultDiscovery_V1_1(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho V1.1 Vault", "mV1.1", 18, true), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedVault == nil {
		t.Fatal("vault not created")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV1_1 {
		t.Errorf("VaultVersion = %d, want V1.1 (%d)", savedVault.VaultVersion, entity.MorphoVaultV1_1)
	}
}

func TestProcessBlockEvent_VaultDiscovery_WrongMorphoAddress(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	wrongMorpho := common.HexToAddress("0x0000000000000000000000000000000000000001")
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			// Probe returns wrong MORPHO address (and curator/liquidityAdapter revert).
			return h.vaultProbeResults(wrongMorpho, testLoanToken), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	// Should not error (vault discovery failures are non-fatal, just marks as not-vault).
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault")
	}
}

func TestProcessBlockEvent_VaultDiscovery_AllProbeSelectorsRevert(t *testing.T) {
	// Previously named *_MorphoCallReverts. After VEC-198 a MORPHO() revert is
	// no longer sufficient on its own — the address still needs curator() and
	// liquidityAdapter() to fail to be classified as not-a-vault. (If those
	// succeed, it's a Morpho VaultV2.)
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			return h.notAVaultProbeResults(), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault when all probe selectors revert")
	}
}

func TestProcessBlockEvent_VaultDiscovery_AssetZero(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			// Probe: MORPHO succeeds but asset returns zero address.
			return h.vaultProbeResults(MorphoBlueAddress, common.Address{}), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault when asset is zero")
	}
}

// TestProcessBlockEvent_VaultDiscovery_VaultV2 covers the new probe fallback:
// MORPHO() reverts but curator() and liquidityAdapter() succeed, so the
// address is recognised as a Morpho VaultV2 (e.g. sparkUSDTbc) rather than
// silently rejected as a non-vault. See VEC-198.
func TestProcessBlockEvent_VaultDiscovery_VaultV2(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22") // sparkUSDTbc address from VEC-198
	curator := common.HexToAddress("0x0f96000000000000000000000000000000000046A3")
	liquidityAdapter := common.HexToAddress("0x7481000000000000000000000000000000007dC2")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("USDT", 6), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultV2ProbeResults(testLoanToken, curator, liquidityAdapter), nil
			}
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedVault == nil {
		t.Fatal("vault not created")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV2 {
		t.Errorf("VaultVersion = %d, want VaultV2 (%d)", savedVault.VaultVersion, entity.MorphoVaultV2)
	}
	if savedVault.Symbol != "sparkUSDTbc" {
		t.Errorf("Symbol = %q, want sparkUSDTbc", savedVault.Symbol)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("VaultV2 should be registered in vault registry")
	}
}

// TestProcessBlockEvent_VaultDiscovery_TransferOnlyDoesNotProbe verifies that
// a plain ERC20 Transfer log from an unknown address does NOT trigger
// tryDiscoverVault.
//
// Without this gate, every tx that touches a popular ERC20 (BAT, STORJ, …)
// would route into the 4-call probe path. Some legacy ERC20s terminate
// unrecognised selector calls with `INVALID` (0xfe) instead of `REVERT`,
// which consumes all available gas and pushes Multicall3's aggregate3 past
// Alchemy's 550M eth_call cap. The discovery layer would then treat that as
// a transient transport error (not ErrNotVault) and retry the SQS message
// forever. See docs/vec-198-morpho-v2-multicall-gas-cap-fix-plan.md.
func TestProcessBlockEvent_VaultDiscovery_TransferOnlyDoesNotProbe(t *testing.T) {
	h := newTestHarness(t)
	unknownAddr := common.HexToAddress("0x0D8775F648430679A709E98d2b0Cb6250d2887EF") // BAT — the original symptom

	// Fail fast if any multicall fires: the gate should short-circuit before
	// we ever reach the prober.
	probeAttempted := false
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		probeAttempted = true
		return nil, fmt.Errorf("multicall must not be called for Transfer-from-unknown")
	}

	log := h.makeVaultTransferLog(unknownAddr, testCaller, testReceiver, big.NewInt(1234))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if probeAttempted {
		t.Error("probe must not be attempted for Transfer-from-unknown")
	}
	// And the address must NOT be persisted in the negative cache — that would
	// hide a future legitimate Deposit/Withdraw discovery for the same address.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownAddr) {
		t.Error("address must not be marked as not-vault when only a Transfer was seen")
	}
	if h.svc.vaultRegistry.IsKnownVault(unknownAddr) {
		t.Error("address must not be registered as a vault from a Transfer-only receipt")
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2AccrueInterestTriggersProbe is the
// positive counterpart to *_TransferOnlyDoesNotProbe: a VaultV2 4-field
// AccrueInterest from an unknown address must trigger discovery (and succeed,
// in this case). See IsVaultActivityEvent for why this is the only triggering
// topic.
func TestProcessBlockEvent_VaultDiscovery_V2AccrueInterestTriggersProbe(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Vault", "VLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var vaultCreated bool
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		vaultCreated = true
		return 99, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !vaultCreated {
		t.Error("V2 4-field AccrueInterest from unknown address must trigger discovery")
	}
}

func TestProcessBlockEvent_VaultDiscovery_DBError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Vault", "VLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	dbErr := errors.New("db connection failed")
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 0, dbErr
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	// DB errors are transient — processBlock should fail so the event can be reprocessed.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("processBlock should fail on transient DB error so event can be reprocessed")
	}

	// Transient failure must NOT permanently mark the address as non-vault.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient DB error")
	}
}

func TestProcessBlockEvent_VaultDiscovery_RPCTransientError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// Simulate a transient RPC failure during vault probe.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return nil, fmt.Errorf("connection timeout")
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	// Transient RPC failure should fail processBlock so the event can be reprocessed.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("processBlock should fail on transient RPC error so event can be reprocessed")
	}

	// Transient RPC failure must NOT permanently mark the address as non-vault.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient RPC error")
	}
}

// TestProcessBlockEvent_VaultDiscovery_TransientErrorThenSuccess verifies that
// when the first log's vault discovery fails transiently and the second log's
// discovery succeeds, the vault is still registered in memory AND processBlock
// returns an error.
//
// VEC-188: a later success does NOT retroactively process the earlier log —
// that log's event was never saved. Returning an error forces SQS to redeliver
// so both logs are retried on the next message. On redelivery, both logs see
// the vault as already-known (registered in memory) and both are processed
// normally.
func TestProcessBlockEvent_VaultDiscovery_TransientErrorThenSuccess(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// First probe call fails (transient), second succeeds.
	probeCallCount := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				probeCallCount++
				if probeCallCount == 1 {
					return nil, fmt.Errorf("connection timeout")
				}
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 99, nil
	}

	// Two discovery-trigger logs from the same vault in one receipt.
	// First triggers discovery (fails transiently), second retries (succeeds).
	log1 := h.makeDiscoveryTriggerLog(unknownVault)
	log2 := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log1, log2)

	// VEC-188: processBlock must FAIL even though the 2nd log's discovery
	// succeeded — the 1st log's event was never persisted and must be retried
	// via SQS redelivery.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("processBlock must fail so SQS redelivers and both logs are retried (VEC-188)")
	}

	// Even though processBlock fails, the vault was registered in memory by
	// the 2nd log's successful discovery. This is correct: on SQS redelivery,
	// both logs will see the vault as already-known and process normally.
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("vault should be registered after eventual success")
	}
	// Transient RPC failure must NOT permanently mark the address as non-vault.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient RPC error")
	}
}

// TestProcessReceipt_VaultDiscoveryRace_KeepsFirstError verifies the VEC-188
// invariant that a transient failure on the first log for a vault address must
// NOT be wiped by a later success for the same address within the same receipt.
//
// Scenario: two MetaMorpho Deposit logs in the same receipt target the same
// newly-discovered vault. The first tryDiscoverVault call hits a transient
// (non-ErrNotVault) error. The second call succeeds. Under the old behavior,
// the success's delete(discoveryErrs, logAddress) wiped the error and
// processReceipt returned nil — SQS would ACK, permanently losing the first
// log's event. The fix keeps the first failure so the error propagates and SQS
// redelivers, allowing BOTH logs to be retried.
func TestProcessReceipt_VaultDiscoveryRace_KeepsFirstError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// First probe call fails transiently (simulating 429 / timeout from Alchemy).
	// Second probe call succeeds, allowing vault registration on the 2nd log.
	probeCallCount := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// vault state + balance (after discovery, process the event)
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				probeCallCount++
				if probeCallCount == 1 {
					// Transient RPC error on the FIRST log's discovery attempt.
					return nil, fmt.Errorf("connection timeout")
				}
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 99, nil
	}

	// Two discovery-trigger logs from the same vault in one receipt.
	// Log 1: triggers discovery → transient failure (event lost).
	// Log 2: retries discovery → succeeds, vault registered.
	// Per VEC-188, the first log's event was never saved, so processReceipt
	// MUST return a non-nil error to force SQS redelivery.
	log1 := h.makeDiscoveryTriggerLog(unknownVault)
	log2 := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log1, log2)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock must return error so SQS redelivers and BOTH logs are retried; " +
			"a later success for the same address does NOT retroactively save the earlier lost log")
	}
	if !strings.Contains(err.Error(), "connection timeout") {
		t.Errorf("error should surface the first log's transient failure, got: %s", err.Error())
	}

	// Transient RPC failure must NOT permanently mark the address as non-vault.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient RPC error")
	}
}

func TestProcessBlockEvent_VaultDiscovery_AlreadyKnownNotVault(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")
	h.svc.vaultRegistry.MarkNotVault(unknownVault)

	// No multicall should be made.
	var multicallCalled int32
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		atomic.AddInt32(&multicallCalled, 1)
		return nil, errors.New("should not be called")
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if atomic.LoadInt32(&multicallCalled) != 0 {
		t.Error("multicall should not be called for known non-vaults")
	}
}

// --- V1/V1.1 vault discovery via Morpho Blue caller/onBehalf path ---
//
// V1/V1.1 vaults call into Morpho Blue when allocating; their address appears
// as `caller` and/or `onBehalf` in the singleton's Supply/Withdraw/Borrow/
// Repay/SupplyCollateral/WithdrawCollateral/Liquidate events. With
// IsVaultActivityEvent narrowed to the V2 4-field AccrueInterest topic only
// (VEC-198 PR feedback), V1/V1.1 vaults can no longer be discovered via
// their own Deposit / Withdraw / V1 AccrueInterest logs.
//
// The morpho-vault-indexer backfiller already handles this via
// emitMorphoBlueCandidates, but the backfiller is recovery-only — operators
// run it when they realise something was missed, not on a schedule. The
// live indexer therefore has to cover V1/V1.1 discovery itself by mirroring
// the same Morpho Blue caller/onBehalf probe path. Otherwise a brand-new
// V1.x vault is invisible to live indexing until somebody manually triggers
// a backfill.
//
// On first discovery via this path, processBlock returns an error to force
// SQS redelivery. The redelivery lets the second pass process any
// earlier-in-receipt vault logs (Deposit / Transfer / V1 AccrueInterest)
// that were skipped while the vault was unknown. Handlers are idempotent
// (`ON CONFLICT` on protocol_event, state-snapshot keys), so reprocessing
// the Morpho Blue Supply on the second pass is safe. The V2 path doesn't
// need this retry because V2 emits its 4-field AccrueInterest first in any
// state-changing transaction — the discovery trigger always precedes
// vault-state logs in log_index order.

// TestProcessBlockEvent_VaultDiscovery_V1ViaMorphoBlueCaller verifies that
// a V1 vault is discovered when it appears as `caller` in a Morpho Blue
// Supply event. The probe identifies V1 (skimRecipient reverts on V1; V1.1
// would succeed; V2's MORPHO() reverts).
func TestProcessBlockEvent_VaultDiscovery_V1ViaMorphoBlueCaller(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// Count probe-multicall invocations. unknownVault appears as both
	// caller and onBehalf in the Supply log below; the dedup at
	// service.go's `seen` map must keep this at exactly 1. Without the
	// dedup, two probes would fire (and the test would still pass at the
	// vault-registered assertion below — wasted RPC).
	var probeCount int32
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				atomic.AddInt32(&probeCount, 1)
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("V1 Vault", "v1V", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeSupplyLog(testMarketID, unknownVault, unknownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	// Single pass: pre-walk discovers the vault before the main loop reaches
	// any vault-emitted log. No SQS redelivery needed for ordinary
	// first-activity-for-a-brand-new-vault.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("V1 vault must be registered in registry after Morpho Blue path discovery")
	}
	if savedVault == nil {
		t.Fatal("V1 vault was not persisted to DB")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV1 {
		t.Errorf("VaultVersion = %d, want V1 (%d)", savedVault.VaultVersion, entity.MorphoVaultV1)
	}
	// Dedup verification: caller == onBehalf in the Supply log, so the
	// `seen` map in discoverV1V11VaultsInReceipt must collapse the two
	// candidates into a single probe. Two probes would mean wasted RPC +
	// duplicate DB inserts (idempotent, but still wrong).
	if got := atomic.LoadInt32(&probeCount); got != 1 {
		t.Errorf("probe fired %d times for caller==onBehalf; want exactly 1 (caller/onBehalf must dedupe via seen[])", got)
	}

	// Replay: vault is now known, no further discovery, no error.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// Replay must also not re-probe — registry is consulted first.
	if got := atomic.LoadInt32(&probeCount); got != 1 {
		t.Errorf("probe fired %d times after replay; want still 1 (registry cache must short-circuit known vault)", got)
	}
}

// TestProcessBlockEvent_VaultDiscovery_V11ViaMorphoBlueOnBehalf verifies V1.1
// discovery when the vault appears as `onBehalf` (not just `caller`) — covers
// the case where the vault is the position owner but a separate router/
// integrator routed the call (their address is `caller`). Probe identifies
// V1.1 because skimRecipient succeeds.
func TestProcessBlockEvent_VaultDiscovery_V11ViaMorphoBlueOnBehalf(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	router := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				if calls[0].Target == unknownVault {
					return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
				}
				// router probe → not a vault
				return h.notAVaultProbeResults(), nil
			}
			return h.vaultDetailResults("V1.1 Vault", "v11V", 18, true), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeSupplyLog(testMarketID, router, unknownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("V1.1 vault must be registered after onBehalf-path discovery")
	}
	if savedVault == nil {
		t.Fatal("V1.1 vault was not persisted")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV1_1 {
		t.Errorf("VaultVersion = %d, want V1.1 (%d)", savedVault.VaultVersion, entity.MorphoVaultV1_1)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(router) {
		t.Error("router (caller, not a vault) must be marked known-not-vault after probe")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_EOA_MarkedNotVault
// verifies that an EOA appearing in a Morpho Blue event is probed once,
// fails the probe, and is cached as known-not-vault so subsequent events
// from the same EOA short-circuit before incurring another multicall.
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_EOA_MarkedNotVault(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	eoa := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.notAVaultProbeResults(), nil
			}
			return nil, fmt.Errorf("unexpected non-probe 4-call")
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	log := h.makeSupplyLog(testMarketID, eoa, eoa, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock should not error when probe definitively rejects an EOA: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(eoa) {
		t.Error("EOA should be marked known-not-vault after probe rejection")
	}
	// Asymmetry guard: a sign-flip that swaps vault / not-vault classification
	// would mark the EOA as a vault. Pin both sides.
	if h.svc.vaultRegistry.IsKnownVault(eoa) {
		t.Error("EOA must NOT be registered as a vault — probe definitively rejected it")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_KnownNotVault_SkipsProbe
// verifies that an address already in the known-not-vault cache short-circuits
// without firing a multicall — the cache is the only thing keeping the live
// indexer from re-probing every Morpho Blue event's user addresses.
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_KnownNotVault_SkipsProbe(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	eoa := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	h.svc.vaultRegistry.MarkNotVault(eoa)

	probeAttempted := false
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 && h.isProbeMulticall(calls) {
			probeAttempted = true
			return nil, fmt.Errorf("probe must not fire for known-not-vault address")
		}
		// 2-call market+position state for the Morpho Blue Supply.
		return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
	}

	log := h.makeSupplyLog(testMarketID, eoa, eoa, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if probeAttempted {
		t.Error("probe must short-circuit for already-known-not-vault addresses")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_TransientError_RetriesViaSQS
// verifies that a transient probe failure during Morpho Blue path discovery
// surfaces as an error so SQS redelivers the receipt — the address must NOT
// be marked known-not-vault on transient failure (that would be a permanent
// black-hole for a real V1/V1.1 vault that's just temporarily unreachable).
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_TransientError_RetriesViaSQS(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return nil, fmt.Errorf("connection timeout")
			}
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeSupplyLog(testMarketID, unknownVault, unknownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock must fail on transient probe error so SQS redelivers")
	}
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("address must NOT be marked known-not-vault on a transient error — that would black-hole a real vault that's just temporarily unreachable")
	}
	// Asymmetry guard: a swap of caching semantics (cache transient,
	// surface ErrNotVault) would only fail one of the two existing tests.
	// Assert the surfaced error is *not* an *ErrNotVault* so a future
	// reclassification has to update this test alongside the EOA test.
	var nv *ErrNotVault
	if errors.As(err, &nv) {
		t.Errorf("transient probe failure must surface as a plain error, not *ErrNotVault — wrapping it as ErrNotVault would silently mark the address as known-not-vault on the next retry: %v", err)
	}
}

// TestProcessReceipt_VaultDiscovery_MorphoBluePath_DepositPlusSupplyInOnePass
// is the contract test for the pre-walk: a typical user-deposit receipt for a
// brand-new V1.1 vault has the vault's own Deposit log AT log[0] and its
// allocation Morpho Blue Supply (vault as caller/onBehalf) at log[1]. The
// pre-walk in processReceipt runs FIRST and registers the vault from the
// Supply, so by the time the main loop reaches log[0] the vault is already
// in the registry and the Deposit is processed via the IsKnownVault branch.
// No SQS redelivery, no whole-block reprocessing.
func TestProcessReceipt_VaultDiscovery_MorphoBluePath_DepositPlusSupplyInOnePass(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			if calls[0].Target == unknownVault {
				// vault state (totalAssets + totalSupply) on Deposit handling.
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			// asset token metadata
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// vault state + balance for Deposit handling
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("V1.1 Vault", "v11V", 18, true), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 99, nil
	}

	depositLog := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	supplyLog := h.makeSupplyLog(testMarketID, unknownVault, unknownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, depositLog, supplyLog)

	var depositPositionSaved bool
	h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
		depositPositionSaved = true
		return nil
	}

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("vault must be registered by the pre-walk before the main loop reaches log[0]")
	}
	if !depositPositionSaved {
		t.Error("Deposit at log[0] was not processed in the same pass — the pre-walk should have registered the vault BEFORE the main loop reached log[0], so the Deposit hits the IsKnownVault branch in processReceipt's switch")
	}
}

// TestMorphoBlueVaultCandidates_TableDriven pins the contract that the live
// indexer and the morpho-vault-indexer backfiller share via
// MorphoBlueVaultCandidates. A future PR that, say, switches Liquidate to
// return only {Caller} or replaces the type switch with reflection on field
// names "Caller"+"OnBehalf" must fail this test rather than silently drift
// the live/backfill discovery contracts apart.
func TestMorphoBlueVaultCandidates_TableDriven(t *testing.T) {
	caller := common.HexToAddress("0x1111111111111111111111111111111111111111")
	onBehalf := common.HexToAddress("0x2222222222222222222222222222222222222222")
	receiver := common.HexToAddress("0x3333333333333333333333333333333333333333")
	borrower := common.HexToAddress("0x4444444444444444444444444444444444444444")

	tests := []struct {
		name  string
		event MorphoBlueEvent
		want  []common.Address
	}{
		{"Supply", &SupplyEvent{Caller: caller, OnBehalf: onBehalf}, []common.Address{caller, onBehalf}},
		{"Withdraw", &WithdrawEvent{Caller: caller, OnBehalf: onBehalf, Receiver: receiver}, []common.Address{caller, onBehalf}},
		{"Borrow", &BorrowEvent{Caller: caller, OnBehalf: onBehalf, Receiver: receiver}, []common.Address{caller, onBehalf}},
		{"Repay", &RepayEvent{Caller: caller, OnBehalf: onBehalf}, []common.Address{caller, onBehalf}},
		{"SupplyCollateral", &SupplyCollateralEvent{Caller: caller, OnBehalf: onBehalf}, []common.Address{caller, onBehalf}},
		{"WithdrawCollateral", &WithdrawCollateralEvent{Caller: caller, OnBehalf: onBehalf, Receiver: receiver}, []common.Address{caller, onBehalf}},
		{"Liquidate", &LiquidateEvent{Caller: caller, Borrower: borrower}, []common.Address{caller, borrower}},
		// Events without user candidates fall through to nil.
		{"CreateMarket (no candidates)", &CreateMarketEvent{}, nil},
		{"AccrueInterest (no candidates)", &AccrueInterestEvent{}, nil},
		{"SetFee (no candidates)", &SetFeeEvent{}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MorphoBlueVaultCandidates(tt.event)
			if !slices.Equal(got, tt.want) {
				t.Errorf("MorphoBlueVaultCandidates() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestProcessBlockEvent_VaultDiscovery_V1ViaMorphoBlueLiquidateBorrower
// covers the Liquidate branch of MorphoBlueVaultCandidates, which uses
// {Caller, Borrower} (not {Caller, OnBehalf} like the position events).
// MetaMorpho V1/V1.1 vaults don't borrow on Morpho Blue in practice, but
// the slot is included for symmetry — a regression that drops Liquidate
// from the switch (or swaps borrower for receiver, etc.) would be invisible
// without this test.
func TestProcessBlockEvent_VaultDiscovery_V1ViaMorphoBlueLiquidateBorrower(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")
	liquidator := common.HexToAddress("0x5555555555555555555555555555555555555555")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				// market+position state for handleLiquidateEvent. Two positions.
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// market state + borrower position + liquidator position
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult(), h.defaultPositionStateResult()}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				if calls[0].Target == unknownVault {
					return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
				}
				return h.notAVaultProbeResults(), nil
			}
			return h.vaultDetailResults("V1 Vault", "v1V", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeLiquidateLog(testMarketID, liquidator, unknownVault,
		big.NewInt(1000), big.NewInt(900), big.NewInt(1100), big.NewInt(0), big.NewInt(0))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("V1 vault as Liquidate borrower must be registered after probe")
	}
	if savedVault == nil {
		t.Fatal("V1 vault was not persisted")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV1 {
		t.Errorf("VaultVersion = %d, want V1 (%d)", savedVault.VaultVersion, entity.MorphoVaultV1)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(liquidator) {
		t.Error("liquidator (caller, not a vault) must be marked known-not-vault after probe rejection")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_KnownVault_SkipsProbe
// verifies that an already-registered vault appearing as caller/onBehalf
// short-circuits without firing the probe. The IsKnownVault check at line
// 752 of service.go guards the hot path — a regression that removes it
// would re-probe every Morpho Blue Supply for already-discovered vaults
// (silent perf regression).
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_KnownVault_SkipsProbe(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	knownVault := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	h.registerTestVault(knownVault, 77, entity.MorphoVaultV1)

	probeAttempted := false
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 && h.isProbeMulticall(calls) {
			probeAttempted = true
			return nil, fmt.Errorf("probe must not fire for already-known vault address")
		}
		return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
	}

	log := h.makeSupplyLog(testMarketID, knownVault, knownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if probeAttempted {
		t.Error("probe must short-circuit for already-known vault addresses")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_FreshUserCallerProbedExactlyOnce
// is a canary against the harness's pre-marking of testCaller / testOnBehalf
// / etc. as known-not-vault. Those pre-marks are an isolation convenience —
// they keep existing Supply/Withdraw/Borrow tests from incidentally hitting
// the V1/V1.1 probe path. But they also make those tests blind to one
// regression: a change that stops calling discoverV1V11VaultsInReceipt
// from processReceipt's Morpho Blue case would break NO existing test
// because the pre-marked addresses short-circuit before the probe anyway.
//
// This test uses a fresh, un-pre-marked EOA address as the caller and
// asserts the probe fires exactly once and resolves to known-not-vault.
// If the wiring at service.go's Morpho Blue case is ever removed, this
// test will fail with probe never fires AND eoa not cached.
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_FreshUserCallerProbedExactlyOnce(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	freshUser := common.HexToAddress("0xc0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0")

	var probeCount int32
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				atomic.AddInt32(&probeCount, 1)
				return h.notAVaultProbeResults(), nil
			}
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	// caller == onBehalf so dedup ensures exactly one probe.
	log := h.makeSupplyLog(testMarketID, freshUser, freshUser, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if got := atomic.LoadInt32(&probeCount); got != 1 {
		t.Fatalf("probe fired %d times for fresh caller; want exactly 1 — if 0, the discoverV1V11VaultsInReceipt wiring is broken; if >1, the seen-map dedup is broken", got)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(freshUser) {
		t.Error("freshUser must end up in not-vault cache after probe rejection — if missing, the cache write at service.go's ErrNotVault branch is broken")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_ZeroAddress_SkipsProbe
// verifies the zero-address / MorphoBlueAddress filter at service.go:749.
// Without it, a Borrow event with onBehalf=0x0 would route into the probe,
// resolve to *ErrNotVault, and pollute the negative-cache with the zero
// address — noise that survives across the process lifetime.
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_ZeroAddress_SkipsProbe(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	caller := common.HexToAddress("0x6666666666666666666666666666666666666666")

	probeAttempted := false
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 && h.isProbeMulticall(calls) {
			if calls[0].Target == (common.Address{}) {
				probeAttempted = true
				return nil, fmt.Errorf("probe must never fire on zero address")
			}
			// caller probe → not a vault
			return h.notAVaultProbeResults(), nil
		}
		return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
	}

	// Borrow with onBehalf == 0x0 — caller is a real address, onBehalf is zero.
	log := h.makeBorrowLog(testMarketID, caller, common.Address{}, caller, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if probeAttempted {
		t.Error("zero address must never reach the probe path")
	}
	if h.svc.vaultRegistry.IsKnownNotVault(common.Address{}) {
		t.Error("zero address must never end up in the not-vault cache")
	}
}

// --- ensureMarket paths ---

func TestProcessBlockEvent_EnsureMarket_ExistsInDB(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	var savedState *entity.MorphoMarketState
	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoMarketState) error {
		savedState = s
		return nil
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedState == nil {
		t.Fatal("SaveMarketState was not called")
	}
	if savedState.MorphoMarketID != 42 {
		t.Errorf("should reuse existing market ID 42, got %d", savedState.MorphoMarketID)
	}
}

func TestProcessBlockEvent_EnsureMarket_NotInDB(t *testing.T) {
	h := newTestHarness(t)
	// Market not in DB — ensureMarket will call getMarketParams + getTokenPairMetadata.
	h.setupMarketNotInDB()

	var marketCreated bool
	h.morphoRepo.GetOrCreateMarketFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarket) (int64, error) {
		marketCreated = true
		return 55, nil
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !marketCreated {
		t.Error("market should have been created via ensureMarket")
	}
}

func TestProcessBlockEvent_EnsureMarket_LookupError(t *testing.T) {
	h := newTestHarness(t)
	h.setupPositionEventMulticall()

	dbErr := errors.New("db timeout")
	h.morphoRepo.GetMarketByMarketIDFn = func(_ context.Context, _ int64, _ common.Hash) (*entity.MorphoMarket, error) {
		return nil, dbErr
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "db timeout") {
		t.Errorf("error should contain 'db timeout', got: %s", err.Error())
	}
}

// --- Error handling & edge cases ---

func TestProcessBlockEvent_CacheMiss_ReturnsError(t *testing.T) {
	h := newTestHarness(t)
	// Don't store anything in cache — GetReceipts returns nil, nil.
	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 99999, Version: 0,
	})
	if err == nil {
		t.Fatal("expected error for cache miss, got nil")
	}
}

func TestProcessBlockEvent_CacheConnectionError(t *testing.T) {
	h := newTestHarness(t)
	// Set an error on the mock cache to simulate a connection failure.
	h.cache.SetError(testutil.ErrCacheClosed)

	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 99999, Version: 0,
	})
	if err == nil {
		t.Fatal("expected error for cache connection failure")
	}
	if !strings.Contains(err.Error(), "fetching receipts from cache") {
		t.Errorf("error should mention cache, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_EmptyReceipt(t *testing.T) {
	h := newTestHarness(t)
	receipt := makeReceipt(testTxHash) // no logs
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("expected nil for empty receipt, got: %v", err)
	}
}

func TestProcessBlockEvent_IrrelevantLogs(t *testing.T) {
	h := newTestHarness(t)
	// Log from random address with random topic — not Morpho Blue or MetaMorpho.
	irrelevantLog := shared.Log{
		Address: "0x0000000000000000000000000000000000000001",
		Topics:  []string{"0x0000000000000000000000000000000000000000000000000000000000000001"},
		Data:    "",
	}
	receipt := makeReceipt(testTxHash, irrelevantLog)
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("expected nil for irrelevant logs, got: %v", err)
	}
}

func TestProcessReceipt_NoRelevantEvents_SkipsSpan(t *testing.T) {
	h := newTestHarness(t)

	// Wire a real tracer so we can verify no processReceipt span is created.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})

	telemetry, err := NewTelemetryWithProviders(tp, noop.NewMeterProvider(), "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}
	h.svc.telemetry = telemetry

	// Ensure no downstream work happens.
	var multicallCalled int32
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		atomic.AddInt32(&multicallCalled, 1)
		return nil, errors.New("should not be called")
	}

	// Receipt with only irrelevant logs — no Morpho Blue or MetaMorpho events.
	irrelevantLog := shared.Log{
		Address: "0x0000000000000000000000000000000000000001",
		Topics:  []string{"0x0000000000000000000000000000000000000000000000000000000000000001"},
		Data:    "",
	}
	receipt := makeReceipt(testTxHash, irrelevantLog)

	if err := h.svc.processReceipt(context.Background(), receipt, 1, 20000000, 0, time.Now()); err != nil {
		t.Fatalf("processReceipt: %v", err)
	}

	// Force flush spans.
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Errorf("force flush spans: %v", err)
	}

	// Verify no morpho.processReceipt span was created.
	spans := exporter.GetSpans()
	for _, s := range spans {
		if s.Name == "morpho.processReceipt" {
			t.Error("morpho.processReceipt span should not be created for receipts with no relevant events")
		}
	}

	if atomic.LoadInt32(&multicallCalled) != 0 {
		t.Error("multicall should not be called when receipt has no relevant events")
	}
}

func TestProcessReceipt_KnownNotVault_SkipsSpan(t *testing.T) {
	h := newTestHarness(t)

	// Wire a real tracer so we can verify no processReceipt span is created.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})

	telemetry, err := NewTelemetryWithProviders(tp, noop.NewMeterProvider(), "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}
	h.svc.telemetry = telemetry

	// Mark the address as a known non-vault.
	knownNotVault := common.HexToAddress("0x0000000000000000000000000000000000000099")
	h.svc.vaultRegistry.MarkNotVault(knownNotVault)

	// Build a receipt with a Transfer event (matches MetaMorpho ABI) from a
	// known-not-vault address. This used to create an empty span.
	transferTopic := h.svc.eventExtractor.metaMorphoABI.Events["Transfer"].ID.Hex()
	receipt := makeReceipt(testTxHash, shared.Log{
		Address: knownNotVault.Hex(),
		Topics: []string{
			transferTopic,
			"0x0000000000000000000000000000000000000000000000000000000000000001",
			"0x0000000000000000000000000000000000000000000000000000000000000002",
		},
		Data: "0x0000000000000000000000000000000000000000000000000000000000000064",
	})

	if err := h.svc.processReceipt(context.Background(), receipt, 1, 20000000, 0, time.Now()); err != nil {
		t.Fatalf("processReceipt: %v", err)
	}

	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Errorf("force flush spans: %v", err)
	}

	spans := exporter.GetSpans()
	for _, s := range spans {
		if s.Name == "morpho.processReceipt" {
			t.Error("morpho.processReceipt span should not be created for receipts from known-not-vault addresses")
		}
	}
}

func TestProcessBlockEvent_MultipleReceipts_OneError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	// First call succeeds, subsequent calls fail.
	var callCount int32
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		}
		return nil, errors.New("rpc failure")
	}

	log1 := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	log2 := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(2000), big.NewInt(1800))
	receipt1 := makeReceipt("0xaaa", log1)
	receipt2 := makeReceipt("0xbbb", log2)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt1, receipt2})
	if err == nil {
		t.Fatal("expected error when one receipt fails")
	}
	// Should contain the RPC error.
	if !strings.Contains(err.Error(), "rpc failure") {
		t.Errorf("error should contain 'rpc failure', got: %s", err.Error())
	}
}

func TestProcessBlockEvent_MulticallReturnsEmpty(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{}, nil
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for empty multicall results")
	}
	if !strings.Contains(err.Error(), "expected 2 results, got 0") {
		t.Errorf("error should mention result count mismatch, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_MulticallResultFailed(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
			{Success: true, ReturnData: h.defaultPositionStateResult().ReturnData},
		}, nil
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for failed multicall result")
	}
	if !strings.Contains(err.Error(), "call failed") {
		t.Errorf("error should mention 'call failed', got: %s", err.Error())
	}
}

func TestProcessBlockEvent_SaveMarketStateFails(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	saveErr := errors.New("save market state failed")
	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketState) error {
		return saveErr
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error when SaveMarketState fails")
	}
	if !strings.Contains(err.Error(), "save market state failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_SaveMarketPositionFails(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	posErr := errors.New("save position failed")
	h.morphoRepo.SaveMarketPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketPosition) error {
		return posErr
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error when SaveMarketPosition fails")
	}
	if !strings.Contains(err.Error(), "save position failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_GetOrCreateUserFails(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	userErr := errors.New("user creation failed")
	h.userRepo.GetOrCreateUserFn = func(_ context.Context, _ pgx.Tx, _ entity.User) (int64, error) {
		return 0, userErr
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error when GetOrCreateUser fails")
	}
	if !strings.Contains(err.Error(), "user creation failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_LogAtMorphoBlueAddress_UnknownTopic(t *testing.T) {
	h := newTestHarness(t)

	// A log at MorphoBlue address but with an unknown topic that still matches
	// a MetaMorpho event signature. This should be skipped (line 246-248).
	transferEvent := h.metaMorphoEventsABI.Events["Transfer"]
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	data, _ := transferEvent.Inputs.NonIndexed().Pack(big.NewInt(5000))

	log := shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			transferEvent.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	receipt := makeReceipt(testTxHash, log)

	// Should not error — just skip.
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("expected nil for skipped log, got: %v", err)
	}
}

// --- Start/Stop lifecycle ---

func TestStartStop(t *testing.T) {
	h := newTestHarness(t)

	// Override GetAllVaults to return a vault.
	h.morphoRepo.GetAllVaultsFn = func(_ context.Context, _ int64) (map[common.Address]*entity.MorphoVault, error) {
		return map[common.Address]*entity.MorphoVault{
			testVaultAddr: {
				ID:             1,
				ChainID:        1,
				ProtocolID:     1,
				Address:        testVaultAddr.Bytes(),
				Name:           "Test",
				Symbol:         "TST",
				AssetTokenID:   1,
				VaultVersion:   entity.MorphoVaultV1,
				CreatedAtBlock: 18000000,
			},
		}, nil
	}

	ctx := context.Background()
	if err := h.svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if h.svc.vaultRegistry.Count() != 1 {
		t.Errorf("vaultRegistry.Count() = %d, want 1", h.svc.vaultRegistry.Count())
	}

	if err := h.svc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStart_EmptyRegistry(t *testing.T) {
	h := newTestHarness(t)
	h.morphoRepo.GetAllVaultsFn = func(_ context.Context, _ int64) (map[common.Address]*entity.MorphoVault, error) {
		return nil, nil
	}

	ctx := context.Background()
	if err := h.svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if h.svc.vaultRegistry.Count() != 0 {
		t.Errorf("vaultRegistry.Count() = %d, want 0", h.svc.vaultRegistry.Count())
	}

	_ = h.svc.Stop()
}

func TestStart_RegistryLoadFailure(t *testing.T) {
	h := newTestHarness(t)
	h.morphoRepo.GetAllVaultsFn = func(_ context.Context, _ int64) (map[common.Address]*entity.MorphoVault, error) {
		return nil, errors.New("db unavailable")
	}

	err := h.svc.Start(context.Background())
	if err == nil {
		_ = h.svc.Stop()
		t.Fatal("Start should fail when vault registry cannot be loaded")
	}
	if !strings.Contains(err.Error(), "loading vault registry") {
		t.Errorf("expected 'loading vault registry' error, got: %v", err)
	}
}

func TestStop_NilCancel(t *testing.T) {
	h := newTestHarness(t)
	h.svc.cancel = nil
	if err := h.svc.Stop(); err != nil {
		t.Fatalf("Stop with nil cancel: %v", err)
	}
}

// --- Protocol event saving ---

func TestProcessBlockEvent_SavesProtocolEvent(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	var savedEvent *entity.ProtocolEvent
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
		savedEvent = e
		return nil
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedEvent == nil {
		t.Fatal("SaveEvent not called")
	}
	if savedEvent.EventName != "Supply" {
		t.Errorf("EventName = %s, want Supply", savedEvent.EventName)
	}
	if savedEvent.BlockNumber != 20000000 {
		t.Errorf("BlockNumber = %d, want 20000000", savedEvent.BlockNumber)
	}
}

// --- handleCreateMarket error paths ---

func TestProcessBlockEvent_CreateMarket_TokenPairMetadataError(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 {
			return nil, errors.New("rpc error fetching token metadata")
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeCreateMarketLog(testMarketID, testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "token pair metadata") {
		t.Errorf("error should mention token pair metadata, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_CreateMarket_MarketStateError(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 4:
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("LOAN")},
				{Success: true, ReturnData: h.packUint8(18)},
				{Success: true, ReturnData: h.packString("COLL")},
				{Success: true, ReturnData: h.packUint8(8)},
			}, nil
		case 1:
			return nil, errors.New("market state rpc error")
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	log := h.makeCreateMarketLog(testMarketID, testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "initial market state") {
		t.Errorf("error should mention initial market state, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_CreateMarket_GetOrCreateProtocolError(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 4:
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("LOAN")},
				{Success: true, ReturnData: h.packUint8(18)},
				{Success: true, ReturnData: h.packString("COLL")},
				{Success: true, ReturnData: h.packUint8(8)},
			}, nil
		case 1:
			return []outbound.Result{h.defaultMarketStateResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.protocolRepo.GetOrCreateProtocolFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ string, _ int64) (int64, error) {
		return 0, errors.New("protocol creation failed")
	}

	log := h.makeCreateMarketLog(testMarketID, testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "protocol creation failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- handleLiquidateEvent error paths ---

func TestProcessBlockEvent_Liquidate_MulticallError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc error")
	}

	log := h.makeLiquidateLog(testMarketID, testCaller, testBorrower,
		big.NewInt(100), big.NewInt(90), big.NewInt(200), big.NewInt(0), big.NewInt(0))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for liquidate multicall failure")
	}
	if !strings.Contains(err.Error(), "fetching on-chain state") {
		t.Errorf("error should mention fetching state, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_Liquidate_SaveBorrowerPositionFails(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult(), h.defaultPositionStateResult()}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	var posCount int
	h.morphoRepo.SaveMarketPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketPosition) error {
		posCount++
		if posCount == 1 {
			return errors.New("borrower position save failed")
		}
		return nil
	}

	log := h.makeLiquidateLog(testMarketID, testCaller, testBorrower,
		big.NewInt(100), big.NewInt(90), big.NewInt(200), big.NewInt(0), big.NewInt(0))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "borrower position") {
		t.Errorf("error should mention borrower position, got: %s", err.Error())
	}
}

// --- handleAccrueInterest error paths ---

func TestProcessBlockEvent_AccrueInterest_MarketStateError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("market state rpc failure")
	}

	log := h.makeAccrueInterestLog(testMarketID, big.NewInt(1000), big.NewInt(500), big.NewInt(10))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "fetching market state") {
		t.Errorf("error should mention fetching market state, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_AccrueInterest_EnsureMarketError(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 1 {
			return []outbound.Result{h.defaultMarketStateResult()}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.morphoRepo.GetMarketByMarketIDFn = func(_ context.Context, _ int64, _ common.Hash) (*entity.MorphoMarket, error) {
		return nil, errors.New("db lookup failed")
	}

	log := h.makeAccrueInterestLog(testMarketID, big.NewInt(1000), big.NewInt(500), big.NewInt(10))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "db lookup failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- handleVaultTransfer error paths ---

func TestProcessBlockEvent_VaultTransfer_MulticallError(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("vault state rpc error")
	}

	from := common.HexToAddress("0x5555555555555555555555555555555555555555")
	to := common.HexToAddress("0x6666666666666666666666666666666666666666")
	log := h.makeVaultTransferLog(testVaultAddr, from, to, big.NewInt(5000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "fetching vault state") {
		t.Errorf("error should mention vault state, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_VaultTransfer_SaveSenderPositionFails(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	from := common.HexToAddress("0x5555555555555555555555555555555555555555")
	to := common.HexToAddress("0x6666666666666666666666666666666666666666")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000)), h.defaultBalanceOfResult(big.NewInt(200000))}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
		return errors.New("position save failed")
	}

	log := h.makeVaultTransferLog(testVaultAddr, from, to, big.NewInt(5000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "sender position") {
		t.Errorf("error should mention sender position, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_VaultTransfer_SaveReceiverPositionFails(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	from := common.HexToAddress("0x5555555555555555555555555555555555555555")
	to := common.HexToAddress("0x6666666666666666666666666666666666666666")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000)), h.defaultBalanceOfResult(big.NewInt(200000))}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	var posCount int
	h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
		posCount++
		if posCount == 2 {
			return errors.New("receiver position save failed")
		}
		return nil
	}

	log := h.makeVaultTransferLog(testVaultAddr, from, to, big.NewInt(5000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "receiver position") {
		t.Errorf("error should mention receiver position, got: %s", err.Error())
	}
}

// --- handleVaultAccrueInterest error paths ---

func TestProcessBlockEvent_VaultAccrueInterest_VaultStateError(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("vault state rpc error")
	}

	log := h.makeVaultAccrueInterestV1Log(testVaultAddr, big.NewInt(2000000), big.NewInt(100))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "fetching vault state") {
		t.Errorf("error should mention vault state, got: %s", err.Error())
	}
}

// --- saveVaultEventSnapshot error paths ---

func TestProcessBlockEvent_VaultDeposit_GetStateAndBalanceError(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc error")
	}

	log := h.makeVaultDepositLog(testVaultAddr, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "fetching vault state and balance") {
		t.Errorf("error should mention vault state and balance, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_VaultDeposit_SaveVaultStateFails(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.morphoRepo.SaveVaultStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultState) error {
		return errors.New("save vault state failed")
	}

	log := h.makeVaultDepositLog(testVaultAddr, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "save vault state failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_VaultDeposit_SaveVaultPositionFails(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
		return errors.New("save vault position failed")
	}

	log := h.makeVaultDepositLog(testVaultAddr, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "save vault position failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_VaultDeposit_UserCreationFails(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.userRepo.GetOrCreateUserFn = func(_ context.Context, _ pgx.Tx, _ entity.User) (int64, error) {
		return 0, errors.New("user creation failed in vault")
	}

	log := h.makeVaultDepositLog(testVaultAddr, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "user creation failed in vault") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- saveProtocolEvent error paths ---

func TestProcessBlockEvent_SaveProtocolEvent_SaveEventError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		return errors.New("event repo save failed")
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "saving protocol event") {
		t.Errorf("error should mention saving protocol event, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_SaveProtocolEvent_GetOrCreateProtocolError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	h.protocolRepo.GetOrCreateProtocolFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ string, _ int64) (int64, error) {
		return 0, errors.New("protocol repo error")
	}

	log := h.makeSetFeeLog(testMarketID, big.NewInt(100))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "protocol repo error") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- ensureMarket error paths ---

func TestProcessBlockEvent_EnsureMarket_GetMarketParamsError(t *testing.T) {
	h := newTestHarness(t)
	h.setupPositionEventMulticall()

	// Market not in DB.
	h.morphoRepo.GetMarketByMarketIDFn = func(_ context.Context, _ int64, _ common.Hash) (*entity.MorphoMarket, error) {
		return nil, nil
	}

	// Override multicaller to fail on getMarketParams (1 call).
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			// getMarketAndPositionState
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		case 1:
			// getMarketParams
			return nil, errors.New("market params rpc error")
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "fetching market params") {
		t.Errorf("error should mention market params, got: %s", err.Error())
	}
}

// --- processMetaMorphoLog extraction error ---

func TestProcessBlockEvent_MetaMorphoLog_ExtractionError(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	// Create a log with valid MetaMorpho topic but invalid data.
	depositEvent := h.metaMorphoEventsABI.Events["Deposit"]
	log := shared.Log{
		Address: testVaultAddr.Hex(),
		Topics: []string{
			depositEvent.ID.Hex(),
			common.BytesToHash(testCaller.Bytes()).Hex(),
			common.BytesToHash(testOnBehalf.Bytes()).Hex(),
		},
		Data:            "invalid_hex_data",
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for extraction failure")
	}
	if !strings.Contains(err.Error(), "extracting MetaMorpho event") {
		t.Errorf("error should mention extraction, got: %s", err.Error())
	}
}

// --- processMorphoBlueLog extraction error ---

func TestProcessBlockEvent_MorphoBlueLog_ExtractionError(t *testing.T) {
	h := newTestHarness(t)

	// Create a log with valid Morpho Blue Supply topic but invalid data.
	supplyEvent := h.morphoBlueEventsABI.Events["Supply"]
	log := shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			supplyEvent.ID.Hex(),
			common.Hash(testMarketID).Hex(),
			common.BytesToHash(testCaller.Bytes()).Hex(),
			common.BytesToHash(testOnBehalf.Bytes()).Hex(),
		},
		Data:            "invalid_hex_data",
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for extraction failure")
	}
	if !strings.Contains(err.Error(), "extracting Morpho Blue event") {
		t.Errorf("error should mention extraction, got: %s", err.Error())
	}
}

// --- VaultTransfer saveVaultState error ---

func TestProcessBlockEvent_VaultTransfer_SaveVaultStateFails(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	from := common.HexToAddress("0x5555555555555555555555555555555555555555")
	to := common.HexToAddress("0x6666666666666666666666666666666666666666")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000)), h.defaultBalanceOfResult(big.NewInt(200000))}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.morphoRepo.SaveVaultStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultState) error {
		return errors.New("vault state save error")
	}

	log := h.makeVaultTransferLog(testVaultAddr, from, to, big.NewInt(5000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "vault state save error") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- VaultAccrueInterest saveVaultState error ---

func TestProcessBlockEvent_VaultAccrueInterest_SaveVaultStateFails(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.morphoRepo.SaveVaultStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultState) error {
		return errors.New("vault state save error")
	}

	log := h.makeVaultAccrueInterestV1Log(testVaultAddr, big.NewInt(2000000), big.NewInt(100))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "vault state save error") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- CreateMarket saveMarketState error ---

func TestProcessBlockEvent_CreateMarket_SaveMarketStateFails(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 4:
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("LOAN")},
				{Success: true, ReturnData: h.packUint8(18)},
				{Success: true, ReturnData: h.packString("COLL")},
				{Success: true, ReturnData: h.packUint8(8)},
			}, nil
		case 1:
			return []outbound.Result{h.defaultMarketStateResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketState) error {
		return errors.New("market state save failed")
	}

	log := h.makeCreateMarketLog(testMarketID, testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "market state save failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- Log index parsing error ---

func TestProcessBlockEvent_InvalidLogIndex(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	// Create a supply log with an unparseable LogIndex.
	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	log.LogIndex = "not_a_number"
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for invalid log index")
	}
	if !strings.Contains(err.Error(), "parsing log index") {
		t.Errorf("error should mention parsing log index, got: %s", err.Error())
	}
}

// --- tryDiscoverVault error paths ---

func TestProcessBlockEvent_VaultDiscovery_EventDecodeError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// Use a V2 4-field AccrueInterest topic (the only discovery trigger) with
	// empty data so the non-indexed argument unpack fails and the event-decode
	// guard inside tryDiscoverVault marks the address as not-vault.
	v2AccrueEvent := h.metaMorphoV2AccrueABI.Events["AccrueInterest"]
	log := shared.Log{
		Address: unknownVault.Hex(),
		Topics: []string{
			v2AccrueEvent.ID.Hex(),
		},
		Data:            "",
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	// Should not error (vault discovery failure is non-fatal)
	if err != nil {
		t.Fatalf("processBlock should not error on vault discovery failure: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault on event decode error")
	}
}

func TestProcessBlockEvent_VaultDiscovery_GetTokenMetadataError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			// getTokenMetadata call fails
			return nil, errors.New("token metadata rpc error")
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock should fail on transient token metadata RPC error so event can be reprocessed")
	}
	// Token metadata RPC error is transient — vault should NOT be permanently marked as non-vault
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient token metadata error")
	}
}

func TestProcessBlockEvent_VaultDiscovery_GetOrCreateTokenError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
		return 0, errors.New("token creation failed")
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock should fail on transient token creation error so event can be reprocessed")
	}
	// Token creation DB error is transient — vault should NOT be permanently marked as non-vault
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient token creation error")
	}
}

// --- handleCreateMarket token creation errors ---

func TestProcessBlockEvent_CreateMarket_LoanTokenError(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 4:
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("LOAN")},
				{Success: true, ReturnData: h.packUint8(18)},
				{Success: true, ReturnData: h.packString("COLL")},
				{Success: true, ReturnData: h.packUint8(8)},
			}, nil
		case 1:
			return []outbound.Result{h.defaultMarketStateResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, _ string, _ int, _ int64) (int64, error) {
		if addr == testLoanToken {
			return 0, errors.New("loan token creation error")
		}
		return 1, nil
	}

	log := h.makeCreateMarketLog(testMarketID, testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "loan token") {
		t.Errorf("error should mention loan token, got: %s", err.Error())
	}
}

// --- ensureMarket: getTokenPairMetadata error ---

func TestProcessBlockEvent_EnsureMarket_TokenPairMetadataError(t *testing.T) {
	h := newTestHarness(t)

	// Market not in DB.
	h.morphoRepo.GetMarketByMarketIDFn = func(_ context.Context, _ int64, _ common.Hash) (*entity.MorphoMarket, error) {
		return nil, nil
	}

	callCount := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callCount++
		switch len(calls) {
		case 2:
			// getMarketAndPositionState
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		case 1:
			// getMarketParams
			return []outbound.Result{
				{Success: true, ReturnData: h.packMarketParams(testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))},
			}, nil
		case 4:
			// getTokenPairMetadata
			return nil, errors.New("token pair metadata rpc error")
		default:
			return nil, fmt.Errorf("unexpected %d calls (call #%d)", len(calls), callCount)
		}
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "token pair metadata") {
		t.Errorf("error should mention token pair metadata, got: %s", err.Error())
	}
}

// TestProcessBlockEvent_EnsureMarket_IdleMarket exercises the full
// processReceipt → handleSupplyEvent → ensureMarket → getMarketParams →
// getTokenPairMetadata path against a Morpho Blue idle market (collateral
// token = 0x0). Pre-fix this triggered "decimals() returned no data" and the
// SQS message retried indefinitely; post-fix the pair-metadata short-circuit
// returns empty TokenMetadata for the collateral side and the market is
// persisted normally with a token row at the zero address.
//
// See docs/morpho-indexer-idle-market-fix-plan.md.
func TestProcessBlockEvent_EnsureMarket_IdleMarket(t *testing.T) {
	h := newTestHarness(t)

	// Realistic idle-market shape from mainnet:
	//   loanToken       = EURCV (0x5F78…)
	//   collateralToken = 0x0          ← idle
	//   oracle          = 0x0
	//   irm             = 0x0
	//   lltv            = 0
	loanToken := common.HexToAddress("0x5F7827FDeb7c20b443265Fc2F40845B715385Ff2")
	collateralToken := common.Address{}
	oracle := common.Address{}
	irm := common.Address{}
	lltv := big.NewInt(0)

	// Market not in DB → ensureMarket goes through the slow path.
	h.morphoRepo.GetMarketByMarketIDFn = func(_ context.Context, _ int64, _ common.Hash) (*entity.MorphoMarket, error) {
		return nil, nil
	}

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			// Either getMarketAndPositionState OR token metadata for the loan
			// side. Distinguish by target address: market+position targets
			// MorphoBlueAddress; token metadata targets the loan token.
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			if calls[0].Target == loanToken {
				return h.tokenMetadataResults("EURCV", 18), nil
			}
			return nil, fmt.Errorf("unexpected 2-call multicall to %s", calls[0].Target.Hex())
		case 1:
			// getMarketParams: idle market shape.
			return []outbound.Result{
				{Success: true, ReturnData: h.packMarketParams(loanToken, collateralToken, oracle, irm, lltv)},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected %d calls — the zero collateral side must be short-circuited, not batched", len(calls))
		}
	}

	tokenAddrs := []common.Address{}
	tokenSymbols := []string{}
	tokenDecimals := []int{}
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, sym string, dec int, _ int64) (int64, error) {
		tokenAddrs = append(tokenAddrs, addr)
		tokenSymbols = append(tokenSymbols, sym)
		tokenDecimals = append(tokenDecimals, dec)
		return int64(len(tokenAddrs)), nil // 1 for loan, 2 for collateral
	}

	var savedMarket *entity.MorphoMarket
	h.morphoRepo.GetOrCreateMarketFn = func(_ context.Context, _ pgx.Tx, m *entity.MorphoMarket) (int64, error) {
		savedMarket = m
		return 99, nil
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock for idle-market Supply event: %v", err)
	}

	if savedMarket == nil {
		t.Fatal("idle market should have been persisted via GetOrCreateMarket")
	}
	if savedMarket.LoanTokenID != 1 || savedMarket.CollateralTokenID != 2 {
		t.Errorf("market token IDs: loan=%d collateral=%d, want 1 / 2", savedMarket.LoanTokenID, savedMarket.CollateralTokenID)
	}

	// Two token rows must have been upserted: loan with real metadata, collateral with empty metadata.
	if len(tokenAddrs) != 2 {
		t.Fatalf("expected 2 GetOrCreateToken calls, got %d", len(tokenAddrs))
	}
	if tokenAddrs[0] != loanToken {
		t.Errorf("first token call addr = %s, want %s (loan token)", tokenAddrs[0].Hex(), loanToken.Hex())
	}
	if tokenSymbols[0] != "EURCV" || tokenDecimals[0] != 18 {
		t.Errorf("loan token metadata: symbol=%q decimals=%d, want EURCV / 18", tokenSymbols[0], tokenDecimals[0])
	}
	if tokenAddrs[1] != (common.Address{}) {
		t.Errorf("second token call addr = %s, want 0x0 (idle collateral)", tokenAddrs[1].Hex())
	}
	if tokenSymbols[1] != "" || tokenDecimals[1] != 0 {
		t.Errorf("idle collateral metadata: symbol=%q decimals=%d, want empty / 0", tokenSymbols[1], tokenDecimals[1])
	}
}

// --- Liquidate ensureMarket error ---

func TestProcessBlockEvent_Liquidate_EnsureMarketError(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult(), h.defaultPositionStateResult()}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.morphoRepo.GetMarketByMarketIDFn = func(_ context.Context, _ int64, _ common.Hash) (*entity.MorphoMarket, error) {
		return nil, errors.New("db lookup failed")
	}

	log := h.makeLiquidateLog(testMarketID, testCaller, testBorrower,
		big.NewInt(100), big.NewInt(90), big.NewInt(200), big.NewInt(0), big.NewInt(0))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "db lookup failed") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- Liquidate saveMarketState error ---

func TestProcessBlockEvent_Liquidate_SaveMarketStateFails(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult(), h.defaultPositionStateResult()}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketState) error {
		return errors.New("save market state failed in liquidate")
	}

	log := h.makeLiquidateLog(testMarketID, testCaller, testBorrower,
		big.NewInt(100), big.NewInt(90), big.NewInt(200), big.NewInt(0), big.NewInt(0))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "save market state failed in liquidate") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- tryDiscoverVault receipt token tests ---

func TestProcessBlockEvent_VaultDiscovery_ReceiptTokenCreated(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var capturedToken entity.ReceiptToken
	receiptTokenCalled := false
	h.receiptTokenRepo.GetOrCreateReceiptTokenFn = func(_ context.Context, _ pgx.Tx, token entity.ReceiptToken) (int64, error) {
		receiptTokenCalled = true
		capturedToken = token
		return 1, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("processBlock should succeed, got: %v", err)
	}

	if !receiptTokenCalled {
		t.Fatal("expected receiptTokenRepo.GetOrCreateReceiptToken to be called")
	}
	if capturedToken.ChainID != 1 {
		t.Errorf("receipt token ChainID = %d, want 1", capturedToken.ChainID)
	}
	if capturedToken.ReceiptTokenAddress != unknownVault {
		t.Errorf("receipt token address = %s, want %s", capturedToken.ReceiptTokenAddress.Hex(), unknownVault.Hex())
	}
	if capturedToken.Symbol != "mVLT" {
		t.Errorf("receipt token symbol = %q, want %q", capturedToken.Symbol, "mVLT")
	}
}

func TestProcessBlockEvent_VaultDiscovery_ReceiptTokenRepoError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.receiptTokenRepo.GetOrCreateReceiptTokenFn = func(_ context.Context, _ pgx.Tx, _ entity.ReceiptToken) (int64, error) {
		return 0, errors.New("receipt token db error")
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock should fail on receipt token repo error")
	}
	if !strings.Contains(err.Error(), "receipt token") {
		t.Errorf("error should mention receipt token, got: %s", err.Error())
	}
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient receipt token error")
	}
}

// TestCreateMarket_SymbolRevert_StoresEmptySymbol verifies that when a
// CreateMarket event is processed and the collateral token's symbol() call
// reverts, the collateral token is persisted with an empty symbol (which
// acts as the pending marker for the sweep). The loan token (whose symbol
// resolved) must be stored with its real symbol.
func TestCreateMarket_SymbolRevert_StoresEmptySymbol(t *testing.T) {
	const blockNumber = int64(20000001)
	h := newTestHarness(t)

	// getTokenPairMetadata: 4 calls [symbol(loan), decimals(loan), symbol(coll), decimals(coll)].
	// Loan symbol resolves; collateral symbol reverts (Success=false); both decimals OK.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 4:
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("LOAN")}, // loan symbol OK
				{Success: true, ReturnData: h.packUint8(18)},      // loan decimals OK
				{Success: false, ReturnData: nil},                 // coll symbol REVERTS
				{Success: true, ReturnData: h.packUint8(8)},       // coll decimals OK
			}, nil
		case 1:
			return []outbound.Result{h.defaultMarketStateResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateMarketFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarket) (int64, error) {
		return 10, nil
	}
	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketState) error {
		return nil
	}

	// Capture GetOrCreateToken calls to inspect what symbol was stored.
	type tokenCall struct {
		address common.Address
		symbol  string
	}
	var tokenCalls []tokenCall
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, sym string, _ int, _ int64) (int64, error) {
		tokenCalls = append(tokenCalls, tokenCall{addr, sym})
		return int64(len(tokenCalls)), nil
	}

	log := h.makeCreateMarketLog(testMarketID, testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, blockNumber, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	// Both tokens must have been persisted: loan with "LOAN", collateral with "".
	if len(tokenCalls) != 2 {
		t.Fatalf("GetOrCreateToken called %d times, want 2", len(tokenCalls))
	}
	loanCall := tokenCalls[0]
	collCall := tokenCalls[1]
	if loanCall.address != testLoanToken {
		t.Errorf("first token call addr = %s, want loan %s", loanCall.address.Hex(), testLoanToken.Hex())
	}
	if loanCall.symbol != "LOAN" {
		t.Errorf("loan token symbol = %q, want LOAN", loanCall.symbol)
	}
	if collCall.address != testCollToken {
		t.Errorf("second token call addr = %s, want coll %s", collCall.address.Hex(), testCollToken.Hex())
	}
	if collCall.symbol != "" {
		t.Errorf("collateral token symbol = %q, want empty (pending marker)", collCall.symbol)
	}
}

func TestReconcilePendingSymbols_ResolvesAtCurrentBlock(t *testing.T) {
	h := newTestHarness(t)

	pending := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		return []common.Address{pending}, nil
	}
	resolved := map[common.Address]string{}
	h.tokenRepo.ResolveTokenSymbolFn = func(_ context.Context, _ int64, address common.Address, symbol string) error {
		resolved[address] = symbol
		return nil
	}
	var sawBlock *big.Int
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		sawBlock = blockNumber
		return []outbound.Result{{Success: true, ReturnData: h.packString("stakedao-frxUsDOLA")}}, nil
	}

	// Block 25252160 is a sweep block (multiple of 10).
	h.svc.reconcilePendingSymbols(context.Background(), 1, 25252160)

	if resolved[pending] != "stakedao-frxUsDOLA" {
		t.Fatalf("resolved = %v, want symbol persisted for %s", resolved, pending.Hex())
	}
	if sawBlock == nil || sawBlock.Int64() != 25252160 {
		t.Fatalf("symbol read at block %v, want 25252160 (the block being processed)", sawBlock)
	}
}

func TestReconcilePendingSymbols_NonSweepBlockIsNoop(t *testing.T) {
	h := newTestHarness(t)
	listed := false
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		listed = true
		return nil, nil
	}
	h.svc.reconcilePendingSymbols(context.Background(), 1, 25252161) // not a multiple of 10
	if listed {
		t.Error("non-sweep block must not query missing-symbol tokens")
	}
}

func TestReconcilePendingSymbols_ListErrorDoesNotPanicOrPropagate(t *testing.T) {
	h := newTestHarness(t)
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		return nil, fmt.Errorf("db down")
	}
	// Best-effort: must not panic. It is a void method, so nothing to assert beyond no panic.
	h.svc.reconcilePendingSymbols(context.Background(), 1, 100)
}

// --- processBlockEvent / reconcilePendingSymbols integration ---

// TestProcessBlockEvent_FetchError_SkipsReconcile asserts that when
// fetchAndProcessReceipts fails (cache miss), reconcilePendingSymbols is never
// reached. Reconcile is best-effort and runs ONLY after a successful block
// fetch; a failed block must propagate its error without touching the token
// repo.
func TestProcessBlockEvent_FetchError_SkipsReconcile(t *testing.T) {
	h := newTestHarness(t)

	listCalled := false
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		listCalled = true
		return nil, nil
	}

	// Do NOT store receipts in cache: GetReceipts returns nil, nil which
	// fetchAndProcessReceipts converts to a "receipts not found" error.
	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 20, Version: 0,
	})
	if err == nil {
		t.Fatal("expected error for cache miss, got nil")
	}
	if listCalled {
		t.Error("ListTokensMissingSymbol must not be called when fetchAndProcessReceipts fails")
	}
}

// TestProcessBlockEvent_Success_RunsReconcileOnSweepBlock asserts that on a
// successful block that falls on a sweep-eligible block number (multiple of
// symbolSweepIntervalBlocks), reconcilePendingSymbols is called with the
// correct chainID. Empty receipts are used so no Morpho-specific processing
// is needed.
func TestProcessBlockEvent_Success_RunsReconcileOnSweepBlock(t *testing.T) {
	h := newTestHarness(t)

	var listCalledWithChainID int64
	listCalled := false
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, chainID int64, _ int) ([]common.Address, error) {
		listCalled = true
		listCalledWithChainID = chainID
		return nil, nil // empty list, nothing to reconcile
	}

	// Block 20 is a multiple of symbolSweepIntervalBlocks (10).
	if err := h.processBlock(t, 1, 20, 0, []shared.TransactionReceipt{}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !listCalled {
		t.Error("ListTokensMissingSymbol must be called on a sweep-eligible block after successful processing")
	}
	if listCalledWithChainID != 1 {
		t.Errorf("ListTokensMissingSymbol called with chainID %d, want 1", listCalledWithChainID)
	}
}

// TestReconcilePendingSymbols_SwallowedErrors covers the two best-effort
// error-swallow branches inside reconcilePendingSymbols. None of them must
// panic or propagate an error (the method is void).
func TestReconcilePendingSymbols_SwallowedErrors(t *testing.T) {
	addr1 := common.HexToAddress("0xAAAA000000000000000000000000000000001111")
	addr2 := common.HexToAddress("0xBBBB000000000000000000000000000000002222")

	t.Run("ResolveSymbolsAt_error_does_not_propagate", func(t *testing.T) {
		// ResolveSymbolsAt errors (multicaller returns error). reconcilePendingSymbols
		// must swallow it. ResolveTokenSymbolFn must NOT be called.
		h := newTestHarness(t)

		h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
			return []common.Address{addr1}, nil
		}
		h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			return nil, errors.New("rpc timeout")
		}
		resolveCalled := false
		h.tokenRepo.ResolveTokenSymbolFn = func(_ context.Context, _ int64, _ common.Address, _ string) error {
			resolveCalled = true
			return nil
		}

		h.svc.reconcilePendingSymbols(context.Background(), 1, 2000)

		if resolveCalled {
			t.Error("ResolveTokenSymbol must not be called when ResolveSymbolsAt errors")
		}
	})

	t.Run("ResolveTokenSymbol_error_continues_to_next_token", func(t *testing.T) {
		// ResolveTokenSymbolFn returns an error for ONE of TWO resolved tokens.
		// The other token's persist must still be attempted — one failing persist
		// must not drop the rest.
		h := newTestHarness(t)

		h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
			return []common.Address{addr1, addr2}, nil
		}
		// Both symbols resolve successfully via multicall.
		h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			results := make([]outbound.Result, len(calls))
			for i := range calls {
				results[i] = outbound.Result{Success: true, ReturnData: h.packString("SYM")}
			}
			return results, nil
		}

		// ResolveTokenSymbol fails for the first address it sees; succeeds for the second.
		var resolveAttempted []common.Address
		h.tokenRepo.ResolveTokenSymbolFn = func(_ context.Context, _ int64, address common.Address, _ string) error {
			resolveAttempted = append(resolveAttempted, address)
			if len(resolveAttempted) == 1 {
				return errors.New("persist failed")
			}
			return nil
		}

		h.svc.reconcilePendingSymbols(context.Background(), 1, 2000)

		// Both addresses must have been attempted regardless of the first failure.
		if len(resolveAttempted) != 2 {
			t.Errorf("ResolveTokenSymbol called %d times, want 2 (one failure must not skip the other)", len(resolveAttempted))
		}
	})
}

// TestVaultDiscovery_AssetSymbolRevert_StoresEmptySymbol verifies that when a
// vault is discovered via the V2 AccrueInterest path and the asset token's
// symbol() reverts while decimals() succeeds, the asset token is persisted
// with an empty symbol (the sweep picks it up later).
func TestVaultDiscovery_AssetSymbolRevert_StoresEmptySymbol(t *testing.T) {
	const blockNumber = int64(20000005)
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9898989898989898989898989898989898989898")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Asset Revert Vault", "aRV", 18, false), nil
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			// getTokenMetadata for the asset: symbol() reverts, decimals() succeeds.
			return []outbound.Result{
				{Success: false, ReturnData: nil},           // symbol() reverts
				{Success: true, ReturnData: h.packUint8(6)}, // decimals() OK
			}, nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 77, nil
	}

	// Capture GetOrCreateToken calls to assert the asset is stored with empty symbol.
	type tokenCall struct {
		address common.Address
		symbol  string
	}
	var tokenCalls []tokenCall
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, sym string, _ int, _ int64) (int64, error) {
		tokenCalls = append(tokenCalls, tokenCall{addr, sym})
		return int64(len(tokenCalls)), nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, blockNumber, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	// Find the asset token call (testLoanToken) and assert it has an empty symbol.
	var assetCall *tokenCall
	for i := range tokenCalls {
		if tokenCalls[i].address == testLoanToken {
			assetCall = &tokenCalls[i]
			break
		}
	}
	if assetCall == nil {
		t.Fatalf("GetOrCreateToken never called for asset token %s; calls: %v", testLoanToken.Hex(), tokenCalls)
	}
	if assetCall.symbol != "" {
		t.Errorf("asset token symbol = %q, want empty (pending marker for sweep)", assetCall.symbol)
	}
}

// Suppress unused import warnings.
var (
	_ = testutil.DiscardLogger
)
