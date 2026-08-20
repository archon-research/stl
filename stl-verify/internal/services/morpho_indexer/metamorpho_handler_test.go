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
