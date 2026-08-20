package morpho_indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

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

	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, _ string, _ int, _ *int64) (int64, error) {
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
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, sym string, dec int, _ *int64) (int64, error) {
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

	// Two token rows must have been written: loan with real metadata, collateral with empty metadata.
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
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, sym string, _ int, _ *int64) (int64, error) {
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
