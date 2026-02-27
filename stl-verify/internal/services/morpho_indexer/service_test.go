package morpho_indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

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
	cons := &testutil.MockSQSConsumer{}

	sqsCfg := shared.SQSConsumerConfigDefaults()
	sqsCfg.ChainID = 1
	config := Config{SQSConsumerConfig: sqsCfg}

	tests := []struct {
		name        string
		consumer    outbound.SQSConsumer
		cache       outbound.BlockCache
		multicall   outbound.Multicaller
		txMgr       outbound.TxManager
		userRepo    outbound.UserRepository
		protoRepo   outbound.ProtocolRepository
		tokenRepo   outbound.TokenRepository
		morphoRepo  outbound.MorphoRepository
		eventRepo   outbound.EventRepository
		errContains string
	}{
		{"nil consumer", nil, rc, mc, tm, ur, pr, tr, mRepo, er, "consumer is required"},
		{"nil cache", cons, nil, mc, tm, ur, pr, tr, mRepo, er, "cache is required"},
		{"nil multicall", cons, rc, nil, tm, ur, pr, tr, mRepo, er, "multicallClient is required"},
		{"nil txManager", cons, rc, mc, nil, ur, pr, tr, mRepo, er, "txManager is required"},
		{"nil userRepo", cons, rc, mc, tm, nil, pr, tr, mRepo, er, "userRepo is required"},
		{"nil protocolRepo", cons, rc, mc, tm, ur, nil, tr, mRepo, er, "protocolRepo is required"},
		{"nil tokenRepo", cons, rc, mc, tm, ur, pr, nil, mRepo, er, "tokenRepo is required"},
		{"nil morphoRepo", cons, rc, mc, tm, ur, pr, tr, nil, er, "morphoRepo is required"},
		{"nil eventRepo", cons, rc, mc, tm, ur, pr, tr, mRepo, nil, "eventRepo is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(config, tt.consumer, tt.cache, tt.multicall, tt.txMgr, tt.userRepo, tt.protoRepo, tt.tokenRepo, tt.morphoRepo, tt.eventRepo)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
			}
		})
	}

	// All deps provided: should succeed.
	svc, err := NewService(config, cons, rc, mc, tm, ur, pr, tr, mRepo, er)
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
				// Could be vault probe or vault state
				if calls[1].Target == unknownVault {
					// vault probe (MORPHO + asset)
					return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
				}
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// vault state + balance (after discovery, process the event)
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			// vault details (name, symbol, decimals, skimRecipient)
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
	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
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

func TestProcessBlockEvent_VaultDiscovery_V2(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				if calls[1].Target == unknownVault {
					return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
				}
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			return h.vaultDetailResults("Morpho V2 Vault", "mV2", 18, true), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedVault == nil {
		t.Fatal("vault not created")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV2 {
		t.Errorf("VaultVersion = %d, want V2 (%d)", savedVault.VaultVersion, entity.MorphoVaultV2)
	}
}

func TestProcessBlockEvent_VaultDiscovery_WrongMorphoAddress(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	wrongMorpho := common.HexToAddress("0x0000000000000000000000000000000000000001")
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			// Probe returns wrong MORPHO address.
			return h.vaultProbeResults(wrongMorpho, testLoanToken), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	// Should not error (vault discovery failures are non-fatal, just marks as not-vault).
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault")
	}
}

func TestProcessBlockEvent_VaultDiscovery_MorphoCallReverts(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			// MORPHO() call fails in the probe.
			return []outbound.Result{
				{Success: false, ReturnData: nil},
				{Success: true, ReturnData: h.packAddress(testLoanToken)},
			}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault after MORPHO() revert")
	}
}

func TestProcessBlockEvent_VaultDiscovery_AssetZero(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			// Probe: MORPHO succeeds but asset returns zero address.
			return h.vaultProbeResults(MorphoBlueAddress, common.Address{}), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault when asset is zero")
	}
}

func TestProcessBlockEvent_VaultDiscovery_DBError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			return h.vaultDetailResults("Vault", "VLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	dbErr := errors.New("db connection failed")
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 0, dbErr
	}

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
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

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
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

func TestProcessBlockEvent_VaultDiscovery_TransientErrorThenSuccess(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// First probe call fails (transient), second succeeds.
	probeCallCount := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				probeCallCount++
				if probeCallCount == 1 {
					return nil, fmt.Errorf("connection timeout")
				}
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 99, nil
	}

	// Two Deposit logs from the same vault in one receipt.
	// First triggers discovery (fails transiently), second retries (succeeds).
	log1 := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	log2 := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(3000), big.NewInt(2700))
	receipt := makeReceipt(testTxHash, log1, log2)

	// Should succeed because the second attempt discovers the vault.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock should succeed after transient error followed by success: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("vault should be registered after eventual success")
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

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if atomic.LoadInt32(&multicallCalled) != 0 {
		t.Error("multicall should not be called for known non-vaults")
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

func TestProcessBlockEvent_RedisCacheMiss(t *testing.T) {
	h := newTestHarness(t)
	// Don't store anything in Redis — should get redis.Nil.
	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 99999, Version: 0,
	})
	if err != nil {
		t.Fatalf("expected nil for cache miss, got: %v", err)
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

	// Create a log with a MetaMorpho event topic but completely invalid data that can't be decoded.
	depositEvent := h.metaMorphoEventsABI.Events["Deposit"]
	log := shared.Log{
		Address: unknownVault.Hex(),
		Topics: []string{
			depositEvent.ID.Hex(),
			// Missing required topic for sender/owner
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
			if calls[0].Target == unknownVault {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			// getTokenMetadata call fails
			return nil, errors.New("token metadata rpc error")
		case 4:
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
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
			if calls[0].Target == unknownVault {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
		return 0, errors.New("token creation failed")
	}

	log := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
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

// Suppress unused import warnings.
var (
	_ = testutil.DiscardLogger
)
