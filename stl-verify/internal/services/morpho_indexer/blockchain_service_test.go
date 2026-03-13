package morpho_indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func TestBigIntFromAny(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  *big.Int
	}{
		{"big.Int", big.NewInt(42), big.NewInt(42)},
		{"nil", nil, big.NewInt(0)},
		{"string", "hello", big.NewInt(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bigIntFromAny(tt.input)
			if got.Cmp(tt.want) != 0 {
				t.Errorf("bigIntFromAny() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestBigIntFromAny_IsCopy(t *testing.T) {
	original := big.NewInt(100)
	result := bigIntFromAny(original)
	result.SetInt64(999)
	if original.Int64() != 100 {
		t.Error("bigIntFromAny should return a copy, not modify the original")
	}
}

func TestIntFromAny(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  int
	}{
		{"uint8", uint8(18), 18},
		{"int", int(42), 42},
		{"int64", int64(100), 100},
		{"uint64", uint64(200), 200},
		{"string", "not a number", 0},
		{"nil", nil, 0},
		{"float64", float64(3.14), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := intFromAny(tt.input)
			if got != tt.want {
				t.Errorf("intFromAny() = %d, want %d", got, tt.want)
			}
		})
	}
}

// --- Unpack helpers ---

func TestUnpackMarketState_Valid(t *testing.T) {
	h := newTestHarness(t)
	data := h.packMarketState(big.NewInt(1000), big.NewInt(900), big.NewInt(500), big.NewInt(450), big.NewInt(1700000000), big.NewInt(0))

	ms, err := h.svc.blockchainSvc.unpackMarketState(outbound.Result{Success: true, ReturnData: data})
	if err != nil {
		t.Fatalf("unpackMarketState: %v", err)
	}
	if ms.TotalSupplyAssets.Int64() != 1000 {
		t.Errorf("TotalSupplyAssets = %s, want 1000", ms.TotalSupplyAssets)
	}
	if ms.TotalBorrowShares.Int64() != 450 {
		t.Errorf("TotalBorrowShares = %s, want 450", ms.TotalBorrowShares)
	}
}

func TestUnpackMarketState_Failed(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.svc.blockchainSvc.unpackMarketState(outbound.Result{Success: false, ReturnData: nil})
	if err == nil {
		t.Fatal("expected error for failed result")
	}
	if !strings.Contains(err.Error(), "call failed") {
		t.Errorf("error = %q, want to contain 'call failed'", err.Error())
	}
}

func TestUnpackMarketState_EmptyReturnData(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.svc.blockchainSvc.unpackMarketState(outbound.Result{Success: true, ReturnData: nil})
	if err == nil {
		t.Fatal("expected error for empty return data")
	}
}

func TestUnpackMarketState_GarbageData(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.svc.blockchainSvc.unpackMarketState(outbound.Result{Success: true, ReturnData: []byte{0x01, 0x02, 0x03}})
	if err == nil {
		t.Fatal("expected error for garbage data")
	}
}

func TestUnpackPositionState_Valid(t *testing.T) {
	h := newTestHarness(t)
	data := h.packPositionState(big.NewInt(100), big.NewInt(50), big.NewInt(200))

	ps, err := h.svc.blockchainSvc.unpackPositionState(outbound.Result{Success: true, ReturnData: data}, "test")
	if err != nil {
		t.Fatalf("unpackPositionState: %v", err)
	}
	if ps.SupplyShares.Int64() != 100 {
		t.Errorf("SupplyShares = %s, want 100", ps.SupplyShares)
	}
}

func TestUnpackPositionState_Failed(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.svc.blockchainSvc.unpackPositionState(outbound.Result{Success: false}, "borrower")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "borrower") {
		t.Errorf("error should contain label 'borrower', got: %s", err.Error())
	}
}

func TestUnpackVaultState_Valid(t *testing.T) {
	h := newTestHarness(t)
	assetsData := h.packUint256(big.NewInt(5000000))
	supplyData := h.packUint256(big.NewInt(4500000))
	addr := common.HexToAddress("0x1234")

	vs, err := h.svc.blockchainSvc.unpackVaultState(
		outbound.Result{Success: true, ReturnData: assetsData},
		outbound.Result{Success: true, ReturnData: supplyData},
		addr,
	)
	if err != nil {
		t.Fatalf("unpackVaultState: %v", err)
	}
	if vs.TotalAssets.Int64() != 5000000 {
		t.Errorf("TotalAssets = %s, want 5000000", vs.TotalAssets)
	}
	if vs.TotalSupply.Int64() != 4500000 {
		t.Errorf("TotalSupply = %s, want 4500000", vs.TotalSupply)
	}
}

func TestUnpackVaultState_TotalAssetsFailed(t *testing.T) {
	h := newTestHarness(t)
	addr := common.HexToAddress("0x1234")
	_, err := h.svc.blockchainSvc.unpackVaultState(
		outbound.Result{Success: false},
		outbound.Result{Success: true, ReturnData: h.packUint256(big.NewInt(100))},
		addr,
	)
	if err == nil {
		t.Fatal("expected error when totalAssets fails")
	}
	if !strings.Contains(err.Error(), "totalAssets()") {
		t.Errorf("error should mention totalAssets, got: %s", err.Error())
	}
}

func TestUnpackVaultState_TotalSupplyFailed(t *testing.T) {
	h := newTestHarness(t)
	addr := common.HexToAddress("0x1234")
	_, err := h.svc.blockchainSvc.unpackVaultState(
		outbound.Result{Success: true, ReturnData: h.packUint256(big.NewInt(100))},
		outbound.Result{Success: false},
		addr,
	)
	if err == nil {
		t.Fatal("expected error when totalSupply fails")
	}
	if !strings.Contains(err.Error(), "totalSupply()") {
		t.Errorf("error should mention totalSupply, got: %s", err.Error())
	}
}

func TestUnpackBalance_Valid(t *testing.T) {
	h := newTestHarness(t)
	data := h.packUint256(big.NewInt(12345))
	addr := common.HexToAddress("0x1234")

	bal, err := h.svc.blockchainSvc.unpackBalance(outbound.Result{Success: true, ReturnData: data}, "user", addr)
	if err != nil {
		t.Fatalf("unpackBalance: %v", err)
	}
	if bal.Int64() != 12345 {
		t.Errorf("balance = %s, want 12345", bal)
	}
}

func TestUnpackBalance_Failed(t *testing.T) {
	h := newTestHarness(t)
	addr := common.HexToAddress("0x1234")
	_, err := h.svc.blockchainSvc.unpackBalance(outbound.Result{Success: false}, "sender", addr)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "sender") {
		t.Errorf("error should contain label 'sender', got: %s", err.Error())
	}
}

// --- Blockchain service: metadata methods ---

func TestGetVaultMetadata_V1(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = h.vaultMetadataExecuteFn("My Vault", "MV", testLoanToken, 18, false)

	md, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err != nil {
		t.Fatalf("getVaultMetadata: %v", err)
	}

	if md.Name != "My Vault" {
		t.Errorf("Name = %s, want 'My Vault'", md.Name)
	}
	if md.Symbol != "MV" {
		t.Errorf("Symbol = %s, want 'MV'", md.Symbol)
	}
	if md.Asset != testLoanToken {
		t.Errorf("Asset = %s, want %s", md.Asset.Hex(), testLoanToken.Hex())
	}
	if md.Decimals != 18 {
		t.Errorf("Decimals = %d, want 18", md.Decimals)
	}
	if md.Version != 1 {
		t.Errorf("Version = %d, want V1 (1)", md.Version)
	}
}

func TestGetVaultMetadata_V2(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = h.vaultMetadataExecuteFn("V2 Vault", "V2", testLoanToken, 6, true)

	md, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err != nil {
		t.Fatalf("getVaultMetadata: %v", err)
	}

	if md.Version != 2 {
		t.Errorf("Version = %d, want V2 (2)", md.Version)
	}
}

func TestGetVaultMetadata_AllCallsAllowFailure(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		for i, c := range calls {
			if !c.AllowFailure {
				t.Errorf("call %d has AllowFailure=false; all calls must allow failure so Multicall3 does not revert on non-vault contracts", i)
			}
		}
		switch len(calls) {
		case 2:
			return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
		case 4:
			return h.vaultDetailResults("Vault", "V", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err != nil {
		t.Fatalf("getVaultMetadata: %v", err)
	}
}

func TestGetVaultMetadata_NonVaultContract(t *testing.T) {
	h := newTestHarness(t)
	// Simulate calling a plain ERC20 (e.g. USDC) — MORPHO() and asset() fail
	// because they don't exist on ERC20 contracts. The probe multicall detects this.
	erc20Addr := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return []outbound.Result{
				{Success: false, ReturnData: nil}, // MORPHO() reverts
				{Success: false, ReturnData: nil}, // asset() reverts
			}, nil
		}
		t.Fatal("detail multicall should not be called for non-vault contract")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), erc20Addr, 20000000)
	if err == nil {
		t.Fatal("expected error for non-vault contract")
	}
	if !strings.Contains(err.Error(), "not a MetaMorpho vault") {
		t.Errorf("error should indicate not a vault, got: %s", err.Error())
	}
}

func TestGetVaultMetadata_WrongMorphoAddress(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	wrongAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return h.vaultProbeResults(wrongAddr, testLoanToken), nil
		}
		t.Fatal("detail multicall should not be called for wrong MORPHO address")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error for wrong MORPHO address")
	}
	if !strings.Contains(err.Error(), "not a MetaMorpho vault") {
		t.Errorf("error should indicate not a vault, got: %s", err.Error())
	}
}

func TestGetVaultMetadata_MorphoReverts(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return []outbound.Result{
				{Success: false, ReturnData: nil}, // MORPHO() reverts
				{Success: true, ReturnData: h.packAddress(testLoanToken)},
			}, nil
		}
		t.Fatal("detail multicall should not be called when MORPHO() reverts")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error when MORPHO() reverts")
	}
}

func TestGetVaultMetadata_ProbeExecuteError_IsTransient(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	// Simulate a transient RPC-level failure on the probe multicall.
	// This should NOT be wrapped as errNotVault — it must remain a retryable error.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, fmt.Errorf("connection timeout")
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error when probe multicall Execute fails")
	}
	var nv *ErrNotVault
	if errors.As(err, &nv) {
		t.Error("transient RPC error should NOT be classified as ErrNotVault")
	}
}

func TestGetVaultMetadata_AssetZero(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return h.vaultProbeResults(MorphoBlueAddress, common.Address{}), nil
		}
		t.Fatal("detail multicall should not be called for zero asset")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error for zero asset address")
	}
	if !strings.Contains(err.Error(), "failed to get vault asset address") {
		t.Errorf("error should mention asset, got: %s", err.Error())
	}
}

func TestGetVaultMetadata_AssetCallFailed(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return []outbound.Result{
				{Success: true, ReturnData: h.packAddress(MorphoBlueAddress)}, // MORPHO() succeeds
				{Success: false, ReturnData: nil},                             // asset() reverts
			}, nil
		}
		t.Fatal("detail multicall should not be called when asset() fails")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error when asset() call fails")
	}
	if !strings.Contains(err.Error(), "asset() call failed") {
		t.Errorf("error should mention asset() failure, got: %s", err.Error())
	}
}

func TestGetVaultMetadata_AssetUnpackError(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return []outbound.Result{
				{Success: true, ReturnData: h.packAddress(MorphoBlueAddress)},
				{Success: true, ReturnData: []byte{0x01, 0x02}}, // garbage data that won't unpack
			}, nil
		}
		t.Fatal("detail multicall should not be called when asset() unpack fails")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error when asset() returns garbage data")
	}
	if !strings.Contains(err.Error(), "unpacking asset()") {
		t.Errorf("error should mention unpacking asset(), got: %s", err.Error())
	}
}

func TestGetTokenMetadata_CacheMiss(t *testing.T) {
	h := newTestHarness(t)
	tokenAddr := common.HexToAddress("0xAAAA")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.tokenMetadataResults("USDC", 6), nil
	}

	md, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), tokenAddr, 20000000)
	if err != nil {
		t.Fatalf("getTokenMetadata: %v", err)
	}
	if md.Symbol != "USDC" {
		t.Errorf("Symbol = %s, want USDC", md.Symbol)
	}
	if md.Decimals != 6 {
		t.Errorf("Decimals = %d, want 6", md.Decimals)
	}
}

func TestGetTokenMetadata_CacheHit(t *testing.T) {
	h := newTestHarness(t)
	tokenAddr := common.HexToAddress("0xAAAA")

	// Populate cache.
	h.svc.blockchainSvc.metadataCache[tokenAddr] = TokenMetadata{Symbol: "CACHED", Decimals: 18}

	var multicallCalled bool
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		multicallCalled = true
		return nil, errors.New("should not be called")
	}

	md, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), tokenAddr, 20000000)
	if err != nil {
		t.Fatalf("getTokenMetadata: %v", err)
	}
	if md.Symbol != "CACHED" {
		t.Errorf("Symbol = %s, want CACHED (from cache)", md.Symbol)
	}
	if multicallCalled {
		t.Error("multicall should not be called for cached token")
	}
}

func TestGetTokenMetadata_SymbolFails_DecimalsSucceeds(t *testing.T) {
	h := newTestHarness(t)
	tokenAddr := common.HexToAddress("0xBBBB")

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},            // symbol fails
			{Success: true, ReturnData: h.packUint8(18)}, // decimals succeeds
		}, nil
	}

	md, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), tokenAddr, 20000000)
	if err != nil {
		t.Fatalf("getTokenMetadata: %v", err)
	}
	if md.Symbol != "" {
		t.Errorf("Symbol = %q, want empty", md.Symbol)
	}
	if md.Decimals != 18 {
		t.Errorf("Decimals = %d, want 18", md.Decimals)
	}
}

func TestGetTokenMetadata_CachesOnlyWithSymbol(t *testing.T) {
	h := newTestHarness(t)
	tokenAddr := common.HexToAddress("0xCCCC")

	// Return empty symbol — should not be cached.
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
			{Success: true, ReturnData: h.packUint8(18)},
		}, nil
	}

	_, _ = h.svc.blockchainSvc.getTokenMetadata(context.Background(), tokenAddr, 20000000)
	if _, ok := h.svc.blockchainSvc.metadataCache[tokenAddr]; ok {
		t.Error("should not cache token with empty symbol")
	}
}

func TestGetTokenPairMetadata_BothUncached(t *testing.T) {
	h := newTestHarness(t)

	var callCount int
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callCount++
		if len(calls) == 4 {
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("TKA")},
				{Success: true, ReturnData: h.packUint8(18)},
				{Success: true, ReturnData: h.packString("TKB")},
				{Success: true, ReturnData: h.packUint8(6)},
			}, nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	tokenA := common.HexToAddress("0xAAAA")
	tokenB := common.HexToAddress("0xBBBB")

	mdA, mdB, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), tokenA, tokenB, 20000000)
	if err != nil {
		t.Fatalf("getTokenPairMetadata: %v", err)
	}
	if mdA.Symbol != "TKA" {
		t.Errorf("mdA.Symbol = %s, want TKA", mdA.Symbol)
	}
	if mdB.Decimals != 6 {
		t.Errorf("mdB.Decimals = %d, want 6", mdB.Decimals)
	}
	if callCount != 1 {
		t.Errorf("multicall called %d times, want 1 (batched)", callCount)
	}
}

func TestGetTokenPairMetadata_BothCached(t *testing.T) {
	h := newTestHarness(t)
	tokenA := common.HexToAddress("0xAAAA")
	tokenB := common.HexToAddress("0xBBBB")

	h.svc.blockchainSvc.metadataCache[tokenA] = TokenMetadata{Symbol: "A", Decimals: 18}
	h.svc.blockchainSvc.metadataCache[tokenB] = TokenMetadata{Symbol: "B", Decimals: 6}

	var multicallCalled bool
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		multicallCalled = true
		return nil, errors.New("should not be called")
	}

	mdA, mdB, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), tokenA, tokenB, 20000000)
	if err != nil {
		t.Fatalf("getTokenPairMetadata: %v", err)
	}
	if mdA.Symbol != "A" || mdB.Symbol != "B" {
		t.Error("should return cached values")
	}
	if multicallCalled {
		t.Error("should not make RPC call when both tokens cached")
	}
}

func TestGetTokenPairMetadata_OneCached(t *testing.T) {
	h := newTestHarness(t)
	tokenA := common.HexToAddress("0xAAAA")
	tokenB := common.HexToAddress("0xBBBB")

	// Only tokenA is cached.
	h.svc.blockchainSvc.metadataCache[tokenA] = TokenMetadata{Symbol: "CACHED_A", Decimals: 18}

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		// Should be a 2-call batch for just tokenB.
		if len(calls) == 2 {
			return h.tokenMetadataResults("FETCHED_B", 6), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	mdA, mdB, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), tokenA, tokenB, 20000000)
	if err != nil {
		t.Fatalf("getTokenPairMetadata: %v", err)
	}
	if mdA.Symbol != "CACHED_A" {
		t.Errorf("mdA.Symbol = %s, want CACHED_A", mdA.Symbol)
	}
	if mdB.Symbol != "FETCHED_B" {
		t.Errorf("mdB.Symbol = %s, want FETCHED_B", mdB.Symbol)
	}
}

func TestGetMarketParams_Success(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: true, ReturnData: h.packMarketParams(testLoanToken, testCollToken, testOracle, testIrm, big.NewInt(800000000000000000))},
		}, nil
	}

	params, err := h.svc.blockchainSvc.getMarketParams(context.Background(), testMarketID, 20000000)
	if err != nil {
		t.Fatalf("getMarketParams: %v", err)
	}
	if params.LoanToken != testLoanToken {
		t.Errorf("LoanToken = %s, want %s", params.LoanToken.Hex(), testLoanToken.Hex())
	}
	if params.CollateralToken != testCollToken {
		t.Errorf("CollateralToken = %s, want %s", params.CollateralToken.Hex(), testCollToken.Hex())
	}
}

func TestGetMarketParams_Failure(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
		}, nil
	}

	_, err := h.svc.blockchainSvc.getMarketParams(context.Background(), testMarketID, 20000000)
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- getMarketState ---

func TestGetMarketState_Success(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{h.defaultMarketStateResult()}, nil
	}

	ms, err := h.svc.blockchainSvc.getMarketState(context.Background(), testMarketID, 20000000)
	if err != nil {
		t.Fatalf("getMarketState: %v", err)
	}
	if ms.TotalSupplyAssets.Int64() != 1000000 {
		t.Errorf("TotalSupplyAssets = %s, want 1000000", ms.TotalSupplyAssets)
	}
}

func TestGetMarketState_MulticallError(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc failure")
	}

	_, err := h.svc.blockchainSvc.getMarketState(context.Background(), testMarketID, 20000000)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetMarketState_EmptyResults(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{}, nil
	}

	_, err := h.svc.blockchainSvc.getMarketState(context.Background(), testMarketID, 20000000)
	if err == nil {
		t.Fatal("expected error for empty results")
	}
	if !strings.Contains(err.Error(), "expected 1 result, got 0") {
		t.Errorf("error = %q, want 'expected 1 result, got 0'", err.Error())
	}
}

// --- getMarketAndPositionState ---

func TestGetMarketAndPositionState_Success(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 {
			return nil, fmt.Errorf("expected 2 calls, got %d", len(calls))
		}
		return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
	}

	ms, ps, err := h.svc.blockchainSvc.getMarketAndPositionState(context.Background(), testMarketID, testOnBehalf, 20000000)
	if err != nil {
		t.Fatalf("getMarketAndPositionState: %v", err)
	}
	if ms == nil {
		t.Fatal("market state is nil")
	}
	if ps == nil {
		t.Fatal("position state is nil")
	}
}

func TestGetMarketAndPositionState_InsufficientResults(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{h.defaultMarketStateResult()}, nil
	}

	_, _, err := h.svc.blockchainSvc.getMarketAndPositionState(context.Background(), testMarketID, testOnBehalf, 20000000)
	if err == nil {
		t.Fatal("expected error for insufficient results")
	}
}

// --- getMarketAndTwoPositionStates ---

func TestGetMarketAndTwoPositionStates_Success(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 3 {
			return nil, fmt.Errorf("expected 3 calls, got %d", len(calls))
		}
		return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult(), h.defaultPositionStateResult()}, nil
	}

	ms, psA, psB, err := h.svc.blockchainSvc.getMarketAndTwoPositionStates(context.Background(), testMarketID, testOnBehalf, testCaller, 20000000)
	if err != nil {
		t.Fatalf("getMarketAndTwoPositionStates: %v", err)
	}
	if ms == nil || psA == nil || psB == nil {
		t.Fatal("nil results")
	}
}

// --- getVaultState ---

func TestGetVaultState_Success(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 {
			return nil, fmt.Errorf("expected 2 calls, got %d", len(calls))
		}
		return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
	}

	vs, err := h.svc.blockchainSvc.getVaultState(context.Background(), testVaultAddr, 20000000)
	if err != nil {
		t.Fatalf("getVaultState: %v", err)
	}
	if vs.TotalAssets.Int64() != 5000000 {
		t.Errorf("TotalAssets = %s, want 5000000", vs.TotalAssets)
	}
}

func TestGetVaultState_InsufficientResults(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{h.defaultVaultTotalAssetsResult()}, nil
	}

	_, err := h.svc.blockchainSvc.getVaultState(context.Background(), testVaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- getVaultStateAndBalance ---

func TestGetVaultStateAndBalance_Success(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 3 {
			return nil, fmt.Errorf("expected 3 calls, got %d", len(calls))
		}
		return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(77777))}, nil
	}

	vs, bal, err := h.svc.blockchainSvc.getVaultStateAndBalance(context.Background(), testVaultAddr, testOnBehalf, 20000000)
	if err != nil {
		t.Fatalf("getVaultStateAndBalance: %v", err)
	}
	if vs == nil {
		t.Fatal("vault state is nil")
	}
	if bal.Int64() != 77777 {
		t.Errorf("balance = %s, want 77777", bal)
	}
}

// --- getVaultStateAndTwoBalances ---

func TestGetVaultStateAndTwoBalances_Success(t *testing.T) {
	h := newTestHarness(t)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 4 {
			return nil, fmt.Errorf("expected 4 calls, got %d", len(calls))
		}
		return []outbound.Result{
			h.defaultVaultTotalAssetsResult(),
			h.defaultVaultTotalSupplyResult(),
			h.defaultBalanceOfResult(big.NewInt(111)),
			h.defaultBalanceOfResult(big.NewInt(222)),
		}, nil
	}

	vs, balA, balB, err := h.svc.blockchainSvc.getVaultStateAndTwoBalances(context.Background(), testVaultAddr, testOnBehalf, testCaller, 20000000)
	if err != nil {
		t.Fatalf("getVaultStateAndTwoBalances: %v", err)
	}
	if vs == nil {
		t.Fatal("vault state is nil")
	}
	if balA.Int64() != 111 {
		t.Errorf("balA = %s, want 111", balA)
	}
	if balB.Int64() != 222 {
		t.Errorf("balB = %s, want 222", balB)
	}
}

// --- Additional unpack error tests ---

func TestUnpackPositionState_GarbageData(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.svc.blockchainSvc.unpackPositionState(outbound.Result{Success: true, ReturnData: []byte("garbage")}, "test")
	if err == nil {
		t.Fatal("expected error for garbage data")
	}
	if !strings.Contains(err.Error(), "unpacking position(test)") {
		t.Errorf("error should contain label, got: %s", err.Error())
	}
}

func TestUnpackVaultState_GarbageAssets(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.svc.blockchainSvc.unpackVaultState(
		outbound.Result{Success: true, ReturnData: []byte("garbage")},
		h.defaultVaultTotalSupplyResult(),
		testVaultAddr,
	)
	if err == nil {
		t.Fatal("expected error for garbage totalAssets data")
	}
	if !strings.Contains(err.Error(), "unpacking totalAssets()") {
		t.Errorf("error should mention totalAssets, got: %s", err.Error())
	}
}

func TestUnpackVaultState_GarbageSupply(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.svc.blockchainSvc.unpackVaultState(
		h.defaultVaultTotalAssetsResult(),
		outbound.Result{Success: true, ReturnData: []byte("garbage")},
		testVaultAddr,
	)
	if err == nil {
		t.Fatal("expected error for garbage totalSupply data")
	}
	if !strings.Contains(err.Error(), "unpacking totalSupply()") {
		t.Errorf("error should mention totalSupply, got: %s", err.Error())
	}
}

func TestUnpackBalance_GarbageData(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.svc.blockchainSvc.unpackBalance(outbound.Result{Success: true, ReturnData: []byte("garbage")}, "user", testVaultAddr)
	if err == nil {
		t.Fatal("expected error for garbage balance data")
	}
	if !strings.Contains(err.Error(), "unpacking balanceOf(user)") {
		t.Errorf("error should contain label, got: %s", err.Error())
	}
}

// --- getMarketAndTwoPositionStates error paths ---

func TestGetMarketAndTwoPositionStates_MulticallError(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc error")
	}
	_, _, _, err := h.svc.blockchainSvc.getMarketAndTwoPositionStates(context.Background(), testMarketID, testCaller, testBorrower, 20000000)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetMarketAndTwoPositionStates_InsufficientResults(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{h.defaultMarketStateResult()}, nil
	}
	_, _, _, err := h.svc.blockchainSvc.getMarketAndTwoPositionStates(context.Background(), testMarketID, testCaller, testBorrower, 20000000)
	if err == nil {
		t.Fatal("expected error for insufficient results")
	}
	if !strings.Contains(err.Error(), "expected 3 results") {
		t.Errorf("error should mention expected results, got: %s", err.Error())
	}
}

func TestGetMarketAndTwoPositionStates_MarketStateFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
			h.defaultPositionStateResult(),
			h.defaultPositionStateResult(),
		}, nil
	}
	_, _, _, err := h.svc.blockchainSvc.getMarketAndTwoPositionStates(context.Background(), testMarketID, testCaller, testBorrower, 20000000)
	if err == nil {
		t.Fatal("expected error for failed market state")
	}
}

func TestGetMarketAndTwoPositionStates_FirstPositionFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			h.defaultMarketStateResult(),
			{Success: false, ReturnData: nil},
			h.defaultPositionStateResult(),
		}, nil
	}
	_, _, _, err := h.svc.blockchainSvc.getMarketAndTwoPositionStates(context.Background(), testMarketID, testCaller, testBorrower, 20000000)
	if err == nil {
		t.Fatal("expected error for failed first position")
	}
}

func TestGetMarketAndTwoPositionStates_SecondPositionFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			h.defaultMarketStateResult(),
			h.defaultPositionStateResult(),
			{Success: false, ReturnData: nil},
		}, nil
	}
	_, _, _, err := h.svc.blockchainSvc.getMarketAndTwoPositionStates(context.Background(), testMarketID, testCaller, testBorrower, 20000000)
	if err == nil {
		t.Fatal("expected error for failed second position")
	}
}

// --- getVaultStateAndBalance error paths ---

func TestGetVaultStateAndBalance_MulticallError(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc error")
	}
	_, _, err := h.svc.blockchainSvc.getVaultStateAndBalance(context.Background(), testVaultAddr, testOnBehalf, 20000000)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetVaultStateAndBalance_InsufficientResults(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{h.defaultVaultTotalAssetsResult()}, nil
	}
	_, _, err := h.svc.blockchainSvc.getVaultStateAndBalance(context.Background(), testVaultAddr, testOnBehalf, 20000000)
	if err == nil {
		t.Fatal("expected error for insufficient results")
	}
}

func TestGetVaultStateAndBalance_VaultStateFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
			h.defaultVaultTotalSupplyResult(),
			h.defaultBalanceOfResult(big.NewInt(100)),
		}, nil
	}
	_, _, err := h.svc.blockchainSvc.getVaultStateAndBalance(context.Background(), testVaultAddr, testOnBehalf, 20000000)
	if err == nil {
		t.Fatal("expected error for failed vault state")
	}
}

func TestGetVaultStateAndBalance_BalanceFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			h.defaultVaultTotalAssetsResult(),
			h.defaultVaultTotalSupplyResult(),
			{Success: false, ReturnData: nil},
		}, nil
	}
	_, _, err := h.svc.blockchainSvc.getVaultStateAndBalance(context.Background(), testVaultAddr, testOnBehalf, 20000000)
	if err == nil {
		t.Fatal("expected error for failed balance")
	}
}

// --- getVaultStateAndTwoBalances error paths ---

func TestGetVaultStateAndTwoBalances_MulticallError(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc error")
	}
	_, _, _, err := h.svc.blockchainSvc.getVaultStateAndTwoBalances(context.Background(), testVaultAddr, testOnBehalf, testCaller, 20000000)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetVaultStateAndTwoBalances_InsufficientResults(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
	}
	_, _, _, err := h.svc.blockchainSvc.getVaultStateAndTwoBalances(context.Background(), testVaultAddr, testOnBehalf, testCaller, 20000000)
	if err == nil {
		t.Fatal("expected error for insufficient results")
	}
}

func TestGetVaultStateAndTwoBalances_VaultStateFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
			h.defaultVaultTotalSupplyResult(),
			h.defaultBalanceOfResult(big.NewInt(100)),
			h.defaultBalanceOfResult(big.NewInt(200)),
		}, nil
	}
	_, _, _, err := h.svc.blockchainSvc.getVaultStateAndTwoBalances(context.Background(), testVaultAddr, testOnBehalf, testCaller, 20000000)
	if err == nil {
		t.Fatal("expected error for failed vault state")
	}
}

func TestGetVaultStateAndTwoBalances_FirstBalanceFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			h.defaultVaultTotalAssetsResult(),
			h.defaultVaultTotalSupplyResult(),
			{Success: false, ReturnData: nil},
			h.defaultBalanceOfResult(big.NewInt(200)),
		}, nil
	}
	_, _, _, err := h.svc.blockchainSvc.getVaultStateAndTwoBalances(context.Background(), testVaultAddr, testOnBehalf, testCaller, 20000000)
	if err == nil {
		t.Fatal("expected error for failed first balance")
	}
}

func TestGetVaultStateAndTwoBalances_SecondBalanceFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			h.defaultVaultTotalAssetsResult(),
			h.defaultVaultTotalSupplyResult(),
			h.defaultBalanceOfResult(big.NewInt(100)),
			{Success: false, ReturnData: nil},
		}, nil
	}
	_, _, _, err := h.svc.blockchainSvc.getVaultStateAndTwoBalances(context.Background(), testVaultAddr, testOnBehalf, testCaller, 20000000)
	if err == nil {
		t.Fatal("expected error for failed second balance")
	}
}

// --- getMarketAndPositionState error paths ---

func TestGetMarketAndPositionState_MarketStateFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
			h.defaultPositionStateResult(),
		}, nil
	}
	_, _, err := h.svc.blockchainSvc.getMarketAndPositionState(context.Background(), testMarketID, testOnBehalf, 20000000)
	if err == nil {
		t.Fatal("expected error for failed market state")
	}
}

func TestGetMarketAndPositionState_PositionStateFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			h.defaultMarketStateResult(),
			{Success: false, ReturnData: nil},
		}, nil
	}
	_, _, err := h.svc.blockchainSvc.getMarketAndPositionState(context.Background(), testMarketID, testOnBehalf, 20000000)
	if err == nil {
		t.Fatal("expected error for failed position state")
	}
}

// --- getVaultState error paths ---

func TestGetVaultState_MulticallError(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("rpc error")
	}
	_, err := h.svc.blockchainSvc.getVaultState(context.Background(), testVaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetVaultState_InsufficientResults2(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{h.defaultVaultTotalAssetsResult()}, nil
	}
	_, err := h.svc.blockchainSvc.getVaultState(context.Background(), testVaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error for insufficient results")
	}
}

func TestGetVaultState_TotalAssetsFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
			h.defaultVaultTotalSupplyResult(),
		}, nil
	}
	_, err := h.svc.blockchainSvc.getVaultState(context.Background(), testVaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error for failed totalAssets")
	}
}

// --- getMarketState additional error ---

func TestGetMarketState_MarketCallFailed(t *testing.T) {
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: false, ReturnData: nil}}, nil
	}
	_, err := h.svc.blockchainSvc.getMarketState(context.Background(), testMarketID, 20000000)
	if err == nil {
		t.Fatal("expected error for failed market call")
	}
}
