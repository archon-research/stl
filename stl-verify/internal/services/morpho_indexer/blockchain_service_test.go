package morpho_indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
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
		if len(calls) != 4 {
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
		if h.isProbeMulticall(calls) {
			return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
		}
		return h.vaultDetailResults("Vault", "V", 18, false), nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err != nil {
		t.Fatalf("getVaultMetadata: %v", err)
	}
}

func TestGetVaultMetadata_NonVaultContract(t *testing.T) {
	h := newTestHarness(t)
	// Simulate calling a plain ERC20 (e.g. USDC) — every probe selector
	// (MORPHO, asset, curator, liquidityAdapter) reverts because none of them
	// exist on ERC20 contracts.
	erc20Addr := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			return h.notAVaultProbeResults(), nil
		}
		t.Fatal("detail multicall should not be called for non-vault contract")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), erc20Addr, 20000000)
	if err == nil {
		t.Fatal("expected error for non-vault contract")
	}
	var nv *ErrNotVault
	if !errors.As(err, &nv) {
		t.Errorf("error should be ErrNotVault, got: %T %s", err, err.Error())
	}
	if nv != nil && nv.VaultShaped {
		t.Error("plain ERC20 should not be flagged VaultShaped — every probe selector reverted")
	}
}

func TestGetVaultMetadata_WrongMorphoAddress(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	wrongAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
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
	// MORPHO returned an address — the contract is shape-confirmed, just not
	// pointing at our singleton. The WARN-on-rejection signal must fire so a
	// future foreign Morpho deployment doesn't sit invisible.
	var nv *ErrNotVault
	if !errors.As(err, &nv) {
		t.Fatalf("error should unwrap to *ErrNotVault, got: %T", err)
	}
	if !nv.VaultShaped {
		t.Error("VaultShaped must be true when MORPHO() returns an address")
	}
}

func TestGetVaultMetadata_MorphoReverts_WithoutV2Markers(t *testing.T) {
	// MORPHO() reverts and there are no V2 markers either — should be rejected
	// as a non-vault. (When MORPHO reverts but curator+liquidityAdapter
	// succeed, the address is recognised as a Morpho VaultV2 instead — that
	// path is covered by TestGetVaultMetadata_VaultV2.)
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			return []outbound.Result{
				{Success: false, ReturnData: nil},                         // MORPHO reverts
				{Success: true, ReturnData: h.packAddress(testLoanToken)}, // asset succeeds
				{Success: false, ReturnData: nil},                         // curator reverts
				{Success: false, ReturnData: nil},                         // liquidityAdapter reverts
			}, nil
		}
		t.Fatal("detail multicall should not be called when MORPHO() reverts and V2 markers also fail")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error when MORPHO() reverts and V2 markers also fail")
	}
	var nv *ErrNotVault
	if !errors.As(err, &nv) {
		t.Fatalf("error should unwrap to *ErrNotVault, got: %T", err)
	}
	if nv.VaultShaped {
		t.Error("VaultShaped must be false when MORPHO() and all V2 markers fail")
	}
}

// TestGetVaultMetadata_VaultV2 covers the new V2 probe fallback (VEC-198):
// MORPHO() reverts but curator() and liquidityAdapter() succeed, so the
// vault is recognised as a VaultV2 rather than rejected.
func TestGetVaultMetadata_VaultV2(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22") // sparkUSDTbc
	curator := common.HexToAddress("0x0f96000000000000000000000000000000000046A3")
	liquidityAdapter := common.HexToAddress("0x7481000000000000000000000000000000007dC2")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			return h.vaultV2ProbeResults(testLoanToken, curator, liquidityAdapter), nil
		}
		if len(calls) == 4 {
			// skim revert here: V2 vaults have no skimRecipient, so the
			// details phase should keep version at V2.
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 18, false), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	md, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 24481834)
	if err != nil {
		t.Fatalf("getVaultMetadata for VaultV2: %v", err)
	}
	if md.Version != entity.MorphoVaultV2 {
		t.Errorf("Version = %d, want VaultV2 (%d)", md.Version, entity.MorphoVaultV2)
	}
	if md.Symbol != "sparkUSDTbc" {
		t.Errorf("Symbol = %q, want sparkUSDTbc", md.Symbol)
	}
	if md.Asset != testLoanToken {
		t.Errorf("Asset = %s, want %s", md.Asset.Hex(), testLoanToken.Hex())
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
		if h.isProbeMulticall(calls) {
			return h.vaultProbeResults(MorphoBlueAddress, common.Address{}), nil
		}
		t.Fatal("detail multicall should not be called for zero asset")
		return nil, nil
	}

	_, err := h.svc.blockchainSvc.getVaultMetadata(context.Background(), vaultAddr, 20000000)
	if err == nil {
		t.Fatal("expected error for zero asset address")
	}
	if !strings.Contains(err.Error(), "asset() returned zero address") {
		t.Errorf("error should mention zero-address asset, got: %s", err.Error())
	}
	// MORPHO succeeded with the canonical singleton, so the address is
	// shape-confirmed even though asset() returned 0x0. The WARN-on-rejection
	// signal must fire — this is the exact "future Morpho V3-or-similar
	// exposes markers but reports asset() = 0" case the flag was added for.
	var nv *ErrNotVault
	if !errors.As(err, &nv) {
		t.Fatalf("error should unwrap to *ErrNotVault, got: %T", err)
	}
	if !nv.VaultShaped {
		t.Error("VaultShaped must be true when MORPHO succeeds and asset() returns 0x0")
	}
}

func TestGetVaultMetadata_AssetCallFailed(t *testing.T) {
	h := newTestHarness(t)
	vaultAddr := common.HexToAddress("0xABCD")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			return []outbound.Result{
				{Success: true, ReturnData: h.packAddress(MorphoBlueAddress)}, // MORPHO() succeeds
				{Success: false, ReturnData: nil},                             // asset() reverts
				{Success: false, ReturnData: nil},                             // curator reverts (MetaMorpho)
				{Success: false, ReturnData: nil},                             // liquidityAdapter reverts (MetaMorpho)
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
		if h.isProbeMulticall(calls) {
			return []outbound.Result{
				{Success: true, ReturnData: h.packAddress(MorphoBlueAddress)},
				{Success: true, ReturnData: []byte{0x01, 0x02}}, // garbage data that won't unpack
				{Success: false, ReturnData: nil},               // curator reverts (MetaMorpho)
				{Success: false, ReturnData: nil},               // liquidityAdapter reverts (MetaMorpho)
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

	// Populate cache with a resolved entry.
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

// TestGetTokenMetadata_CachesWithEmptySymbol verifies that a reverted
// symbol() does not prevent caching: the token is persisted with Symbol=""
// so future calls (within the same block-processing run) are served from
// cache without re-fetching.
// A decimals() revert still returns an error and must NOT populate the cache.
func TestGetTokenMetadata_CachesWithEmptySymbol(t *testing.T) {
	h := newTestHarness(t)
	tokenAddr := common.HexToAddress("0xCCCC")

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},
			{Success: true, ReturnData: h.packUint8(18)},
		}, nil
	}

	md, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), tokenAddr, 20000000)
	if err != nil {
		t.Fatalf("expected no error when symbol() reverts, got %v", err)
	}
	if md.Symbol != "" {
		t.Errorf("Symbol must be empty when symbol() reverted, got %q", md.Symbol)
	}
	if _, ok := h.svc.blockchainSvc.metadataCache[tokenAddr]; !ok {
		t.Error("token must be cached even when symbol() is unresolved")
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

// TestGetTokenMetadata_ErrorsWhenDecimalsSubCallFails codifies VEC-188 Finding 3.
// A reverted decimals() sub-call must surface as an error; a silent decimals=0
// would corrupt downstream unit conversions.
func TestGetTokenMetadata_ErrorsWhenDecimalsSubCallFails(t *testing.T) {
	h := newTestHarness(t)
	tokenAddr := common.HexToAddress("0xEEEE")

	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: true, ReturnData: h.packString("USDC")}, // symbol() succeeded
			{Success: false, ReturnData: nil},                 // decimals() reverted
		}, nil
	}

	_, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), tokenAddr, 20000000)
	if err == nil {
		t.Fatal("expected error when decimals() sub-call reverts; silent 0-decimals would corrupt units")
	}
	if _, ok := h.svc.blockchainSvc.metadataCache[tokenAddr]; ok {
		t.Error("metadata cache must not be populated when sub-calls revert")
	}
}

// TestGetTokenMetadata_CachesAndReturnsOnSuccess verifies the happy path plus
// the cache behavior still works after the VEC-188 Finding 3 fix.
func TestGetTokenMetadata_CachesAndReturnsOnSuccess(t *testing.T) {
	h := newTestHarness(t)
	tokenAddr := common.HexToAddress("0xFFFF")

	var callCount int
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callCount++
		return h.tokenMetadataResults("USDC", 6), nil
	}

	md, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), tokenAddr, 20000000)
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}
	if md.Symbol != "USDC" {
		t.Errorf("first call: Symbol = %q, want %q", md.Symbol, "USDC")
	}
	if md.Decimals != 6 {
		t.Errorf("first call: Decimals = %d, want 6", md.Decimals)
	}

	md2, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), tokenAddr, 20000000)
	if err != nil {
		t.Fatalf("second call: unexpected error: %v", err)
	}
	if md2.Symbol != "USDC" || md2.Decimals != 6 {
		t.Errorf("second call: got %+v, want USDC/6", md2)
	}
	if callCount != 1 {
		t.Errorf("multicaller called %d times, want 1 (second call must hit cache)", callCount)
	}
}

// TestGetTokenPairMetadata_DecimalsRevertIsFatalSymbolIsBestEffort narrows
// VEC-188 Finding 3: a reverted decimals() is still fatal (would persist bogus
// numeric metadata), but a reverted symbol() is tolerated and reconciled later.
func TestGetTokenPairMetadata_DecimalsRevertIsFatalSymbolIsBestEffort(t *testing.T) {
	tokenA := common.HexToAddress("0xA1A1")
	tokenB := common.HexToAddress("0xB2B2")

	cases := []struct {
		failIdx   int
		wantError bool
	}{
		{0, false}, // symbol(A) revert tolerated
		{1, true},  // decimals(A) revert fatal
		{2, false}, // symbol(B) revert tolerated
		{3, true},  // decimals(B) revert fatal
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("subcall_%d_fails", tc.failIdx), func(t *testing.T) {
			h := newTestHarness(t)
			h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				results := []outbound.Result{
					{Success: true, ReturnData: h.packString("TKA")},
					{Success: true, ReturnData: h.packUint8(18)},
					{Success: true, ReturnData: h.packString("TKB")},
					{Success: true, ReturnData: h.packUint8(6)},
				}
				results[tc.failIdx] = outbound.Result{Success: false, ReturnData: nil}
				return results, nil
			}
			_, _, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), tokenA, tokenB, 20000000)
			if tc.wantError && err == nil {
				t.Fatalf("expected error when decimals sub-call %d reverts", tc.failIdx)
			}
			if !tc.wantError && err != nil {
				t.Fatalf("expected no error when symbol sub-call %d reverts, got %v", tc.failIdx, err)
			}
		})
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

// --- Idle-market support ---
//
// Morpho Blue allows markets where collateralToken = 0x0 ("idle markets" used as
// liquidity buffers — vaults deposit assets without enabling borrowing). Calling
// decimals() on the zero address returns empty data, which trips the
// "decimals() returned no data" path in unpackTokenMetadataResults. The fix is
// to short-circuit the zero address in getTokenMetadata / getTokenPairMetadata
// to TokenMetadata{Symbol: "", Decimals: 0} without issuing any sub-call.
//
// See docs/morpho-indexer-idle-market-fix-plan.md.

// TestGetTokenMetadata_ZeroAddressShortCircuits is the unit-level reproducer.
// Pre-fix: this test fails because the multicall is invoked and the empty
// decimals() return triggers an error. Post-fix: the zero address is recognised
// up-front, the multicall is never issued, and an empty TokenMetadata is
// returned.
func TestGetTokenMetadata_ZeroAddressShortCircuits(t *testing.T) {
	h := newTestHarness(t)

	called := false
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		called = true
		return nil, fmt.Errorf("multicaller should not be invoked for the zero address")
	}

	md, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), common.Address{}, 20000000)
	if err != nil {
		t.Fatalf("getTokenMetadata(0x0): %v", err)
	}
	if called {
		t.Error("multicaller must not be invoked when token is the zero address")
	}
	if md.Symbol != "" {
		t.Errorf("Symbol = %q, want empty string", md.Symbol)
	}
	if md.Decimals != 0 {
		t.Errorf("Decimals = %d, want 0", md.Decimals)
	}
}

// TestGetTokenPairMetadata_ZeroCollateral covers the canonical idle-market
// shape: a real loan token on side A and the zero address on side B. The
// multicall must be issued for tokenA only, never for tokenB. Returned tokenB
// metadata must be the empty sentinel.
func TestGetTokenPairMetadata_ZeroCollateral(t *testing.T) {
	h := newTestHarness(t)

	tokenA := common.HexToAddress("0x5F7827FDeb7c20b443265Fc2F40845B715385Ff2") // EURCV — same loan token as the production idle-market repro
	tokenB := common.Address{}

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 {
			return nil, fmt.Errorf("expected 2 calls (tokenA only), got %d", len(calls))
		}
		for i, c := range calls {
			if c.Target != tokenA {
				return nil, fmt.Errorf("call %d targets %s, want %s — zero side must be short-circuited", i, c.Target.Hex(), tokenA.Hex())
			}
		}
		return h.tokenMetadataResults("EURCV", 18), nil
	}

	mdA, mdB, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), tokenA, tokenB, 20000000)
	if err != nil {
		t.Fatalf("getTokenPairMetadata(loan, 0x0): %v", err)
	}
	if mdA.Symbol != "EURCV" || mdA.Decimals != 18 {
		t.Errorf("loan side: Symbol=%q Decimals=%d, want EURCV / 18", mdA.Symbol, mdA.Decimals)
	}
	if mdB.Symbol != "" || mdB.Decimals != 0 {
		t.Errorf("collateral (zero) side: Symbol=%q Decimals=%d, want empty / 0", mdB.Symbol, mdB.Decimals)
	}
}

// TestGetTokenPairMetadata_ZeroLoan symmetric counterpart — defensive coverage
// even though Morpho Blue's createMarket does not appear to allow a zero
// loan token in production. Establishes that the short-circuit is direction-
// agnostic.
func TestGetTokenPairMetadata_ZeroLoan(t *testing.T) {
	h := newTestHarness(t)

	tokenA := common.Address{}
	tokenB := common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7") // USDT

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 {
			return nil, fmt.Errorf("expected 2 calls (tokenB only), got %d", len(calls))
		}
		for i, c := range calls {
			if c.Target != tokenB {
				return nil, fmt.Errorf("call %d targets %s, want %s — zero side must be short-circuited", i, c.Target.Hex(), tokenB.Hex())
			}
		}
		return h.tokenMetadataResults("USDT", 6), nil
	}

	mdA, mdB, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), tokenA, tokenB, 20000000)
	if err != nil {
		t.Fatalf("getTokenPairMetadata(0x0, USDT): %v", err)
	}
	if mdA.Symbol != "" || mdA.Decimals != 0 {
		t.Errorf("loan (zero) side: Symbol=%q Decimals=%d, want empty / 0", mdA.Symbol, mdA.Decimals)
	}
	if mdB.Symbol != "USDT" || mdB.Decimals != 6 {
		t.Errorf("collateral side: Symbol=%q Decimals=%d, want USDT / 6", mdB.Symbol, mdB.Decimals)
	}
}

func TestGetTokenPairMetadata_SymbolRevertTolerated(t *testing.T) {
	tokenA := common.HexToAddress("0xA1A1")
	tokenB := common.HexToAddress("0xB2B2")
	h := newTestHarness(t)

	// symbol(B) (index 2) reverts; everything else succeeds. Mirrors block 25252154.
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: true, ReturnData: h.packString("TKA")},
			{Success: true, ReturnData: h.packUint8(18)},
			{Success: false, ReturnData: nil},
			{Success: true, ReturnData: h.packUint8(6)},
		}, nil
	}

	mdA, mdB, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), tokenA, tokenB, 20000000)
	if err != nil {
		t.Fatalf("expected no error when only symbol() reverts, got %v", err)
	}
	if mdA.Symbol != "TKA" || mdA.Decimals != 18 {
		t.Errorf("token A = %+v, want Symbol=TKA Decimals=18", mdA)
	}
	if mdB.Symbol != "" || mdB.Decimals != 6 {
		t.Errorf("token B = %+v, want Symbol='' Decimals=6", mdB)
	}
}

func TestGetTokenPairMetadata_DecimalsRevertStillErrors(t *testing.T) {
	tokenA := common.HexToAddress("0xA1A1")
	tokenB := common.HexToAddress("0xB2B2")
	for _, decIdx := range []int{1, 3} {
		t.Run(fmt.Sprintf("decimals_idx_%d", decIdx), func(t *testing.T) {
			h := newTestHarness(t)
			h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				results := []outbound.Result{
					{Success: true, ReturnData: h.packString("TKA")},
					{Success: true, ReturnData: h.packUint8(18)},
					{Success: true, ReturnData: h.packString("TKB")},
					{Success: true, ReturnData: h.packUint8(6)},
				}
				results[decIdx] = outbound.Result{Success: false, ReturnData: nil}
				return results, nil
			}
			if _, _, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), tokenA, tokenB, 20000000); err == nil {
				t.Fatalf("expected error when decimals sub-call %d reverts", decIdx)
			}
		})
	}
}

func TestGetTokenMetadata_SymbolRevertTolerated(t *testing.T) {
	token := common.HexToAddress("0xC3C3")
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil},           // symbol reverts
			{Success: true, ReturnData: h.packUint8(8)}, // decimals ok
		}, nil
	}
	md, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), token, 20000000)
	if err != nil {
		t.Fatalf("expected no error when only symbol() reverts, got %v", err)
	}
	if md.Symbol != "" || md.Decimals != 8 {
		t.Errorf("md = %+v, want Symbol='' Decimals=8", md)
	}
}

// TestGetTokenMetadata_SymbolUndecodable_IsTolerated verifies that when
// symbol() SUCCEEDS but returns data that is neither a valid ABI string nor
// a bytes32 (e.g. 3 bytes of garbage), the result has Symbol="" (empty pending
// marker) with no error, while decimals is correctly decoded.
func TestGetTokenMetadata_SymbolUndecodable_IsTolerated(t *testing.T) {
	token := common.HexToAddress("0xD4D4")
	h := newTestHarness(t)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: true, ReturnData: []byte{0x01, 0x02, 0x03}}, // 3 bytes: not a valid ABI string, not bytes32
			{Success: true, ReturnData: h.packUint8(6)},
		}, nil
	}
	md, err := h.svc.blockchainSvc.getTokenMetadata(context.Background(), token, 20000000)
	if err != nil {
		t.Fatalf("expected no error for undecodable symbol, got %v", err)
	}
	if md.Symbol != "" || md.Decimals != 6 {
		t.Errorf("md = %+v, want Symbol='' Decimals=6", md)
	}
}

// --- ResolveSymbolsAt ---

func TestResolveSymbolsAt(t *testing.T) {
	resolvable := common.HexToAddress("0x1111111111111111111111111111111111111111")
	stillReverting := common.HexToAddress("0x2222222222222222222222222222222222222222")
	h := newTestHarness(t)

	// One symbol() call per address, in input order. First resolves, second reverts.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 {
			t.Fatalf("want 2 calls, got %d", len(calls))
		}
		return []outbound.Result{
			{Success: true, ReturnData: h.packString("OK")},
			{Success: false, ReturnData: nil},
		}, nil
	}

	got, err := h.svc.blockchainSvc.resolveSymbolsAt(context.Background(), []common.Address{resolvable, stillReverting}, 25252165)
	if err != nil {
		t.Fatalf("ResolveSymbolsAt: %v", err)
	}
	if len(got) != 1 || got[resolvable] != "OK" {
		t.Errorf("got %v, want only {%s: OK}", got, resolvable.Hex())
	}
	if _, ok := got[stillReverting]; ok {
		t.Error("reverting token must be absent from results (stays pending)")
	}
}

func TestResolveSymbolsAt_Empty(t *testing.T) {
	h := newTestHarness(t)
	got, err := h.svc.blockchainSvc.resolveSymbolsAt(context.Background(), nil, 100)
	if err != nil {
		t.Fatalf("ResolveSymbolsAt(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

func TestResolveSymbolsAt_UsesRequestedBlock(t *testing.T) {
	h := newTestHarness(t)
	token := common.HexToAddress("0xAABB")
	const wantBlock int64 = 25252165

	var gotBlock *big.Int
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		gotBlock = blockNumber
		return []outbound.Result{
			{Success: true, ReturnData: h.packString("SYM")},
		}, nil
	}

	_, err := h.svc.blockchainSvc.resolveSymbolsAt(context.Background(), []common.Address{token}, wantBlock)
	if err != nil {
		t.Fatalf("ResolveSymbolsAt: %v", err)
	}
	if gotBlock == nil || gotBlock.Int64() != wantBlock {
		t.Errorf("multicall block = %v, want %d", gotBlock, wantBlock)
	}
}

func TestResolveSymbolsAt_UpdatesCacheEntry(t *testing.T) {
	h := newTestHarness(t)
	addr := common.HexToAddress("0xCCDD")

	// Pre-seed cache with empty-symbol entry.
	h.svc.blockchainSvc.metadataCache[addr] = TokenMetadata{Symbol: "", Decimals: 18}

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: true, ReturnData: h.packString("OK")},
		}, nil
	}

	got, err := h.svc.blockchainSvc.resolveSymbolsAt(context.Background(), []common.Address{addr}, 25252165)
	if err != nil {
		t.Fatalf("ResolveSymbolsAt: %v", err)
	}
	if got[addr] != "OK" {
		t.Errorf("resolved symbol = %q, want OK", got[addr])
	}
	cached := h.svc.blockchainSvc.metadataCache[addr]
	if cached.Symbol != "OK" || cached.Decimals != 18 {
		t.Errorf("cache entry = %+v, want Symbol=OK Decimals=18", cached)
	}
}

func TestResolveSymbolsAt_ResultCountMismatchErrors(t *testing.T) {
	h := newTestHarness(t)
	tokens := []common.Address{
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
		common.HexToAddress("0x2222222222222222222222222222222222222222"),
	}

	// Multicall returns fewer results than calls — must surface as an error
	// rather than silently dropping a token's slot.
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true, ReturnData: h.packString("ONLY_ONE")}}, nil
	}

	if _, err := h.svc.blockchainSvc.resolveSymbolsAt(context.Background(), tokens, 100); err == nil {
		t.Fatal("expected error on result-count mismatch")
	}
}

func TestResolveSymbolsAt_UndecodableAndEmptySymbolsOmitted(t *testing.T) {
	h := newTestHarness(t)
	undecodable := common.HexToAddress("0x1111111111111111111111111111111111111111")
	emptyDecoded := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// First result is a successful call with undecodable return data; second
	// decodes to the empty string. Both must be omitted (stay pending), no error.
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: true, ReturnData: []byte{0x01, 0x02, 0x03}},
			{Success: true, ReturnData: h.packString("")},
		}, nil
	}

	got, err := h.svc.blockchainSvc.resolveSymbolsAt(context.Background(), []common.Address{undecodable, emptyDecoded}, 100)
	if err != nil {
		t.Fatalf("resolveSymbolsAt: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v, want empty map (undecodable and empty symbols stay pending)", got)
	}
}

// TestGetTokenPairMetadata_BothZero — degenerate but should still not panic
// or call the multicaller.
func TestGetTokenPairMetadata_BothZero(t *testing.T) {
	h := newTestHarness(t)

	called := false
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		called = true
		return nil, fmt.Errorf("multicaller should not be invoked when both tokens are zero")
	}

	mdA, mdB, err := h.svc.blockchainSvc.getTokenPairMetadata(context.Background(), common.Address{}, common.Address{}, 20000000)
	if err != nil {
		t.Fatalf("getTokenPairMetadata(0x0, 0x0): %v", err)
	}
	if called {
		t.Error("multicaller must not be invoked when both tokens are the zero address")
	}
	if mdA.Symbol != "" || mdA.Decimals != 0 || mdB.Symbol != "" || mdB.Decimals != 0 {
		t.Errorf("expected empty metadata on both sides, got mdA=%+v mdB=%+v", mdA, mdB)
	}
}
