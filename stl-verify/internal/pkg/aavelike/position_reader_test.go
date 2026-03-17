package aavelike

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
)

// encodeRawUserReserves builds a raw-encoded getUserReservesData result for the
// given reserves using the 4-field layout (address, scaledATokenBalance,
// usageAsCollateralEnabled, scaledVariableDebt). This is the wire format that
// decodeUserReservesRaw handles.
func encodeRawUserReserves(reserves []UserReserveData) []byte {
	const fieldsPerStruct = 4
	arrayLen := len(reserves)
	// word 0: offset to dynamic array (64 = 2 words: offset + eMode)
	// word 1: eMode category (ignored)
	// at offset: array length, then structs
	totalWords := 2 + 1 + arrayLen*fieldsPerStruct
	data := make([]byte, totalWords*wordSize)

	// word 0: offset = 64 (pointing past offset+eMode)
	copy(data[0:wordSize], common.LeftPadBytes(big.NewInt(64).Bytes(), wordSize))
	// word 1: eMode = 0
	// at offset (64): array length
	copy(data[64:64+wordSize], common.LeftPadBytes(big.NewInt(int64(arrayLen)).Bytes(), wordSize))

	for i, r := range reserves {
		base := 64 + wordSize + uint64(i)*fieldsPerStruct*wordSize
		copy(data[base:base+wordSize], common.LeftPadBytes(r.UnderlyingAsset.Bytes(), wordSize))
		copy(data[base+wordSize:base+2*wordSize], common.LeftPadBytes(r.ScaledATokenBalance.Bytes(), wordSize))
		if r.UsageAsCollateralEnabledOnUser {
			copy(data[base+2*wordSize:base+3*wordSize], common.LeftPadBytes(big.NewInt(1).Bytes(), wordSize))
		}
		copy(data[base+3*wordSize:base+4*wordSize], common.LeftPadBytes(r.ScaledVariableDebt.Bytes(), wordSize))
	}
	return data
}

// newTestBlockchainService creates a BlockchainService with real ABIs loaded
// and the given mock multicaller. Uses Aave V3 protocol version so the
// getUserReservesData ABI supports the 7-field layout. The raw fallback
// decoder handles both 4-field and 7-field layouts.
func newTestBlockchainService(t *testing.T, mock *testutil.MockMulticaller) *BlockchainService {
	t.Helper()
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	uiPoolDataProvider := common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d")
	poolAddressesProvider := common.HexToAddress("0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb")

	// Use real Aave V3 Ethereum addresses so getPoolDataProviderForBlock resolves.
	poolAddress := common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")

	svc := &BlockchainService{
		logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		metadataCache:         make(map[common.Address]TokenMetadata),
		erc20ABI:              erc20ABI,
		multicallClient:       mock,
		chainID:               1,
		poolAddress:           poolAddress,
		uiPoolDataProvider:    uiPoolDataProvider,
		poolAddressesProvider: poolAddressesProvider,
		protocolVersion:       "aave-v3",
	}

	if err := svc.loadABIs("aave-v3"); err != nil {
		t.Fatalf("loadABIs() failed: %v", err)
	}
	return svc
}

func TestGetUserReservesDataBatch_HappyPath(t *testing.T) {
	userA := common.HexToAddress("0x0000000000000000000000000000000000000001")
	userB := common.HexToAddress("0x0000000000000000000000000000000000000002")
	userC := common.HexToAddress("0x0000000000000000000000000000000000000003")
	users := []common.Address{userA, userB, userC}

	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	// userA: WETH collateral + USDC debt
	reservesA := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(1e18), UsageAsCollateralEnabledOnUser: true, ScaledVariableDebt: big.NewInt(0)},
		{UnderlyingAsset: usdc, ScaledATokenBalance: big.NewInt(0), UsageAsCollateralEnabledOnUser: false, ScaledVariableDebt: big.NewInt(1500e6)},
	}
	// userB: WETH collateral only
	reservesB := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(2e18), UsageAsCollateralEnabledOnUser: true, ScaledVariableDebt: big.NewInt(0)},
	}
	// userC: no positions (empty reserves)
	reservesC := []UserReserveData{}

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if len(calls) != 3 {
			t.Errorf("expected 3 calls, got %d", len(calls))
		}
		if blockNumber.Int64() != 100 {
			t.Errorf("expected block 100, got %d", blockNumber.Int64())
		}

		// Verify all calls target the uiPoolDataProvider
		uiPoolDataProvider := common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d")
		for i, call := range calls {
			if call.Target != uiPoolDataProvider {
				t.Errorf("call[%d] target = %s, want %s", i, call.Target.Hex(), uiPoolDataProvider.Hex())
			}
			if !call.AllowFailure {
				t.Errorf("call[%d] AllowFailure should be true", i)
			}
		}

		return []outbound.Result{
			{Success: true, ReturnData: encodeRawUserReserves(reservesA)},
			{Success: true, ReturnData: encodeRawUserReserves(reservesB)},
			{Success: true, ReturnData: encodeRawUserReserves(reservesC)},
		}, nil
	}

	svc := newTestBlockchainService(t, mock)

	resultsMap, errorsMap, err := svc.getUserReservesDataBatch(context.Background(), users, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.CallCount != 1 {
		t.Errorf("expected 1 multicall Execute, got %d", mock.CallCount)
	}

	if len(errorsMap) != 0 {
		t.Errorf("expected no errors, got %d", len(errorsMap))
	}

	// Verify userA reserves
	ra, ok := resultsMap[userA]
	if !ok {
		t.Fatal("userA not in results")
	}
	if len(ra) != 2 {
		t.Fatalf("userA: expected 2 reserves, got %d", len(ra))
	}
	if ra[0].UnderlyingAsset != weth {
		t.Errorf("userA reserve[0] asset = %s, want WETH", ra[0].UnderlyingAsset.Hex())
	}
	if ra[1].UnderlyingAsset != usdc {
		t.Errorf("userA reserve[1] asset = %s, want USDC", ra[1].UnderlyingAsset.Hex())
	}
	if ra[0].ScaledATokenBalance.Cmp(big.NewInt(1e18)) != 0 {
		t.Errorf("userA WETH balance = %s, want 1e18", ra[0].ScaledATokenBalance)
	}
	if ra[1].ScaledVariableDebt.Cmp(big.NewInt(1500e6)) != 0 {
		t.Errorf("userA USDC debt = %s, want 1500e6", ra[1].ScaledVariableDebt)
	}

	// Verify userB reserves
	rb, ok := resultsMap[userB]
	if !ok {
		t.Fatal("userB not in results")
	}
	if len(rb) != 1 {
		t.Fatalf("userB: expected 1 reserve, got %d", len(rb))
	}
	if rb[0].UnderlyingAsset != weth {
		t.Errorf("userB reserve[0] asset = %s, want WETH", rb[0].UnderlyingAsset.Hex())
	}

	// Verify userC reserves (empty)
	rc, ok := resultsMap[userC]
	if !ok {
		t.Fatal("userC not in results")
	}
	if len(rc) != 0 {
		t.Errorf("userC: expected 0 reserves, got %d", len(rc))
	}
}

func TestGetUserReservesDataBatch_SubCallFailure(t *testing.T) {
	userA := common.HexToAddress("0x0000000000000000000000000000000000000001")
	userB := common.HexToAddress("0x0000000000000000000000000000000000000002")
	users := []common.Address{userA, userB}

	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	reservesB := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(5e18), UsageAsCollateralEnabledOnUser: true, ScaledVariableDebt: big.NewInt(0)},
	}

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false, ReturnData: nil}, // userA fails
			{Success: true, ReturnData: encodeRawUserReserves(reservesB)},
		}, nil
	}

	svc := newTestBlockchainService(t, mock)

	resultsMap, errorsMap, err := svc.getUserReservesDataBatch(context.Background(), users, 100)
	if err != nil {
		t.Fatalf("unexpected top-level error: %v", err)
	}

	// userA should be in errorsMap
	if _, ok := errorsMap[userA]; !ok {
		t.Error("expected userA in errorsMap")
	}
	if _, ok := resultsMap[userA]; ok {
		t.Error("userA should not be in resultsMap")
	}

	// userB should succeed
	rb, ok := resultsMap[userB]
	if !ok {
		t.Fatal("userB not in resultsMap")
	}
	if len(rb) != 1 {
		t.Fatalf("userB: expected 1 reserve, got %d", len(rb))
	}
}

func TestGetUserReservesDataBatch_EmptyUsers(t *testing.T) {
	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("Execute should not be called for empty users")
		return nil, nil
	}

	svc := newTestBlockchainService(t, mock)

	resultsMap, errorsMap, err := svc.getUserReservesDataBatch(context.Background(), []common.Address{}, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resultsMap) != 0 {
		t.Errorf("expected empty results, got %d", len(resultsMap))
	}
	if errorsMap != nil {
		t.Errorf("expected nil errorsMap, got %v", errorsMap)
	}
}

func TestGetUserReservesDataBatch_MulticallError(t *testing.T) {
	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, fmt.Errorf("gas limit exceeded")
	}

	svc := newTestBlockchainService(t, mock)

	users := []common.Address{common.HexToAddress("0x01")}
	_, _, err := svc.getUserReservesDataBatch(context.Background(), users, 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// encodeUserReserveData ABI-encodes a getUserReserveData result (9 return values).
func encodeUserReserveData(t *testing.T, svc *BlockchainService, data ActualUserReserveData) []byte {
	t.Helper()
	outputs := svc.getUserReserveDataABI.Methods["getUserReserveData"].Outputs
	packed, err := outputs.Pack(
		data.CurrentATokenBalance,
		data.CurrentStableDebt,
		data.CurrentVariableDebt,
		data.PrincipalStableDebt,
		data.ScaledVariableDebt,
		data.StableBorrowRate,
		data.LiquidityRate,
		new(big.Int).SetUint64(data.StableRateLastUpdated),
		data.UsageAsCollateralEnabled,
	)
	if err != nil {
		t.Fatalf("failed to pack getUserReserveData: %v", err)
	}
	return packed
}

func TestBatchGetUserReserveDataMultiUser_HappyPath(t *testing.T) {
	userA := common.HexToAddress("0x0000000000000000000000000000000000000001")
	userB := common.HexToAddress("0x0000000000000000000000000000000000000002")

	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	dai := common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")

	// userA has WETH + USDC, userB has USDC + DAI
	userAssets := map[common.Address][]common.Address{
		userA: {weth, usdc},
		userB: {usdc, dai},
	}

	dataAWETH := ActualUserReserveData{
		Asset: weth, CurrentATokenBalance: big.NewInt(1e18), CurrentStableDebt: big.NewInt(0),
		CurrentVariableDebt: big.NewInt(0), PrincipalStableDebt: big.NewInt(0),
		ScaledVariableDebt: big.NewInt(0), StableBorrowRate: big.NewInt(0),
		LiquidityRate: big.NewInt(0), StableRateLastUpdated: 0, UsageAsCollateralEnabled: true,
	}
	dataAUSDC := ActualUserReserveData{
		Asset: usdc, CurrentATokenBalance: big.NewInt(0), CurrentStableDebt: big.NewInt(0),
		CurrentVariableDebt: big.NewInt(1500e6), PrincipalStableDebt: big.NewInt(0),
		ScaledVariableDebt: big.NewInt(1400e6), StableBorrowRate: big.NewInt(0),
		LiquidityRate: big.NewInt(0), StableRateLastUpdated: 0, UsageAsCollateralEnabled: false,
	}
	dataBUSDC := ActualUserReserveData{
		Asset: usdc, CurrentATokenBalance: big.NewInt(5000e6), CurrentStableDebt: big.NewInt(0),
		CurrentVariableDebt: big.NewInt(0), PrincipalStableDebt: big.NewInt(0),
		ScaledVariableDebt: big.NewInt(0), StableBorrowRate: big.NewInt(0),
		LiquidityRate: big.NewInt(0), StableRateLastUpdated: 0, UsageAsCollateralEnabled: true,
	}
	daiDebt := new(big.Int).Mul(big.NewInt(2000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	daiScaledDebt := new(big.Int).Mul(big.NewInt(1900), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	dataBDAI := ActualUserReserveData{
		Asset: dai, CurrentATokenBalance: big.NewInt(0), CurrentStableDebt: big.NewInt(0),
		CurrentVariableDebt: daiDebt, PrincipalStableDebt: big.NewInt(0),
		ScaledVariableDebt: daiScaledDebt, StableBorrowRate: big.NewInt(0),
		LiquidityRate: big.NewInt(0), StableRateLastUpdated: 0, UsageAsCollateralEnabled: false,
	}

	mock := testutil.NewMockMulticaller()

	var svc *BlockchainService

	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if len(calls) != 4 {
			t.Errorf("expected 4 calls (2 users × 2 assets), got %d", len(calls))
		}
		if blockNumber.Int64() != 20000000 {
			t.Errorf("expected block 20000000, got %d", blockNumber.Int64())
		}

		// Map results back by decoding the calldata to find which user+asset each call is for
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			if !call.AllowFailure {
				t.Errorf("call[%d] AllowFailure should be true", i)
			}

			// Decode the calldata to extract asset and user
			method, err := svc.getUserReserveDataABI.MethodById(call.CallData[:4])
			if err != nil {
				t.Fatalf("call[%d]: failed to get method: %v", i, err)
			}
			args, err := method.Inputs.Unpack(call.CallData[4:])
			if err != nil {
				t.Fatalf("call[%d]: failed to unpack: %v", i, err)
			}
			asset := args[0].(common.Address)
			user := args[1].(common.Address)

			var data ActualUserReserveData
			switch {
			case user == userA && asset == weth:
				data = dataAWETH
			case user == userA && asset == usdc:
				data = dataAUSDC
			case user == userB && asset == usdc:
				data = dataBUSDC
			case user == userB && asset == dai:
				data = dataBDAI
			default:
				t.Fatalf("unexpected call: user=%s asset=%s", user.Hex(), asset.Hex())
			}
			results[i] = outbound.Result{
				Success:    true,
				ReturnData: encodeUserReserveData(t, svc, data),
			}
		}
		return results, nil
	}

	svc = newTestBlockchainService(t, mock)

	result, err := svc.batchGetUserReserveDataMultiUser(context.Background(), userAssets, 20000000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.CallCount != 1 {
		t.Errorf("expected 1 multicall Execute, got %d", mock.CallCount)
	}

	// Verify userA
	userAData, ok := result[userA]
	if !ok {
		t.Fatal("userA not in results")
	}
	if len(userAData) != 2 {
		t.Fatalf("userA: expected 2 assets, got %d", len(userAData))
	}
	if userAData[weth].CurrentATokenBalance.Cmp(big.NewInt(1e18)) != 0 {
		t.Errorf("userA WETH balance = %s, want 1e18", userAData[weth].CurrentATokenBalance)
	}
	if userAData[usdc].CurrentVariableDebt.Cmp(big.NewInt(1500e6)) != 0 {
		t.Errorf("userA USDC debt = %s, want 1500e6", userAData[usdc].CurrentVariableDebt)
	}

	// Verify userB
	userBData, ok := result[userB]
	if !ok {
		t.Fatal("userB not in results")
	}
	if len(userBData) != 2 {
		t.Fatalf("userB: expected 2 assets, got %d", len(userBData))
	}
	if userBData[usdc].CurrentATokenBalance.Cmp(big.NewInt(5000e6)) != 0 {
		t.Errorf("userB USDC balance = %s, want 5000e6", userBData[usdc].CurrentATokenBalance)
	}
	expectedDaiDebt := new(big.Int).Mul(big.NewInt(2000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	if userBData[dai].CurrentVariableDebt.Cmp(expectedDaiDebt) != 0 {
		t.Errorf("userB DAI debt = %s, want %s", userBData[dai].CurrentVariableDebt, expectedDaiDebt)
	}
}

func TestBatchGetUserReserveDataMultiUser_SubCallFailure_SkipsFailedAsset(t *testing.T) {
	userA := common.HexToAddress("0x0000000000000000000000000000000000000001")
	userB := common.HexToAddress("0x0000000000000000000000000000000000000002")
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	userAssets := map[common.Address][]common.Address{
		userA: {weth, usdc},
		userB: {usdc},
	}

	var svc *BlockchainService

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			method, _ := svc.getUserReserveDataABI.MethodById(call.CallData[:4])
			args, _ := method.Inputs.Unpack(call.CallData[4:])
			asset := args[0].(common.Address)
			user := args[1].(common.Address)

			if user == userA && asset == weth {
				// userA's WETH call fails
				results[i] = outbound.Result{Success: false}
			} else {
				// All other calls succeed
				data := ActualUserReserveData{
					Asset: asset, CurrentATokenBalance: big.NewInt(100e6), CurrentStableDebt: big.NewInt(0),
					CurrentVariableDebt: big.NewInt(0), PrincipalStableDebt: big.NewInt(0),
					ScaledVariableDebt: big.NewInt(0), StableBorrowRate: big.NewInt(0),
					LiquidityRate: big.NewInt(0), StableRateLastUpdated: 0, UsageAsCollateralEnabled: true,
				}
				results[i] = outbound.Result{Success: true, ReturnData: encodeUserReserveData(t, svc, data)}
			}
		}
		return results, nil
	}

	svc = newTestBlockchainService(t, mock)

	result, err := svc.batchGetUserReserveDataMultiUser(context.Background(), userAssets, 20000000)
	if err != nil {
		t.Fatalf("expected no error (failed sub-call should be skipped), got: %v", err)
	}

	// userA should have USDC but not WETH (WETH sub-call failed)
	userAData, ok := result[userA]
	if !ok {
		t.Fatal("userA not in results")
	}
	if len(userAData) != 1 {
		t.Fatalf("userA: expected 1 asset (USDC only, WETH skipped), got %d", len(userAData))
	}
	if _, hasUSDC := userAData[usdc]; !hasUSDC {
		t.Error("userA should have USDC data")
	}
	if _, hasWETH := userAData[weth]; hasWETH {
		t.Error("userA should NOT have WETH data (sub-call failed)")
	}

	// userB should be fully intact
	userBData, ok := result[userB]
	if !ok {
		t.Fatal("userB not in results")
	}
	if len(userBData) != 1 {
		t.Fatalf("userB: expected 1 asset, got %d", len(userBData))
	}
}

func TestBatchGetUserReserveDataMultiUser_EmptyInput(t *testing.T) {
	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("Execute should not be called for empty input")
		return nil, nil
	}

	svc := newTestBlockchainService(t, mock)

	result, err := svc.batchGetUserReserveDataMultiUser(context.Background(), map[common.Address][]common.Address{}, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty results, got %d", len(result))
	}
}

func TestBatchGetUserReserveDataMultiUser_MulticallError(t *testing.T) {
	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, fmt.Errorf("gas limit exceeded")
	}

	svc := newTestBlockchainService(t, mock)

	userAssets := map[common.Address][]common.Address{
		common.HexToAddress("0x01"): {common.HexToAddress("0x02")},
	}
	_, err := svc.batchGetUserReserveDataMultiUser(context.Background(), userAssets, 20000000)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetBatchUserPositionData(t *testing.T) {
	userA := common.HexToAddress("0x0000000000000000000000000000000000000001")
	userB := common.HexToAddress("0x0000000000000000000000000000000000000002")
	userC := common.HexToAddress("0x0000000000000000000000000000000000000003")

	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	// userA: WETH collateral + USDC debt
	reservesA := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(1e18), UsageAsCollateralEnabledOnUser: true, ScaledVariableDebt: big.NewInt(0)},
		{UnderlyingAsset: usdc, ScaledATokenBalance: big.NewInt(0), UsageAsCollateralEnabledOnUser: false, ScaledVariableDebt: big.NewInt(1500e6)},
	}
	// userB: WETH collateral only
	reservesB := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(2e18), UsageAsCollateralEnabledOnUser: true, ScaledVariableDebt: big.NewInt(0)},
	}
	// userC: no positions
	reservesC := []UserReserveData{}

	var svc *BlockchainService
	callNumber := 0

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callNumber++
		switch callNumber {
		case 1:
			// Multicall 1: getUserReservesDataBatch (3 users)
			if len(calls) != 3 {
				t.Errorf("multicall 1: expected 3 calls, got %d", len(calls))
			}
			return []outbound.Result{
				{Success: true, ReturnData: encodeRawUserReserves(reservesA)},
				{Success: true, ReturnData: encodeRawUserReserves(reservesB)},
				{Success: true, ReturnData: encodeRawUserReserves(reservesC)},
			}, nil

		case 2:
			// Multicall 2: batchGetUserReserveDataMultiUser
			// (metadata is pre-cached, so no metadata multicall)
			// userA needs WETH+USDC, userB needs WETH = 3 calls
			if len(calls) != 3 {
				t.Errorf("multicall 2: expected 3 calls, got %d", len(calls))
			}
			results := make([]outbound.Result, len(calls))
			for i, call := range calls {
				method, _ := svc.getUserReserveDataABI.MethodById(call.CallData[:4])
				args, _ := method.Inputs.Unpack(call.CallData[4:])
				asset := args[0].(common.Address)
				user := args[1].(common.Address)

				var data ActualUserReserveData
				switch {
				case user == userA && asset == weth:
					data = ActualUserReserveData{
						Asset: weth, CurrentATokenBalance: big.NewInt(1050e15),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(0),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(0),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: true,
					}
				case user == userA && asset == usdc:
					data = ActualUserReserveData{
						Asset: usdc, CurrentATokenBalance: big.NewInt(0),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(1500e6),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(1400e6),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: false,
					}
				case user == userB && asset == weth:
					data = ActualUserReserveData{
						Asset: weth, CurrentATokenBalance: big.NewInt(2100e15),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(0),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(0),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: true,
					}
				default:
					t.Fatalf("unexpected reserve data call: user=%s asset=%s", user.Hex(), asset.Hex())
				}
				results[i] = outbound.Result{Success: true, ReturnData: encodeUserReserveData(t, svc, data)}
			}
			return results, nil

		default:
			t.Fatalf("unexpected multicall #%d", callNumber)
			return nil, nil
		}
	}

	svc = newTestBlockchainService(t, mock)

	// Pre-seed metadata cache so the metadata multicall result doesn't matter.
	svc.metadataCache[weth] = TokenMetadata{Symbol: "WETH", Decimals: 18, Name: "Wrapped Ether"}
	svc.metadataCache[usdc] = TokenMetadata{Symbol: "USDC", Decimals: 6, Name: "USD Coin"}

	// Build a PositionReader that wraps our test BlockchainService.
	reader := &PositionReader{
		logger:             svc.logger,
		blockchainServices: make(map[blockchain.ProtocolKey]*BlockchainService),
	}
	// Pre-register the service so GetOrCreateBlockchainService doesn't hit the registry.
	poolAddress := common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")
	reader.blockchainServices[blockchain.ProtocolKey{ChainID: 1, PoolAddress: poolAddress}] = svc

	results, err := reader.GetBatchUserPositionData(
		context.Background(),
		[]common.Address{userA, userB, userC},
		poolAddress, 1, 20000000,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify userA: 1 collateral (WETH) + 1 debt (USDC)
	ra := results[userA]
	if ra.Err != nil {
		t.Fatalf("userA error: %v", ra.Err)
	}
	if len(ra.Collaterals) != 1 {
		t.Fatalf("userA: expected 1 collateral, got %d", len(ra.Collaterals))
	}
	if ra.Collaterals[0].Symbol != "WETH" {
		t.Errorf("userA collateral symbol = %s, want WETH", ra.Collaterals[0].Symbol)
	}
	if ra.Collaterals[0].ActualBalance.Cmp(big.NewInt(1050e15)) != 0 {
		t.Errorf("userA WETH balance = %s, want 1050e15", ra.Collaterals[0].ActualBalance)
	}
	if len(ra.Debts) != 1 {
		t.Fatalf("userA: expected 1 debt, got %d", len(ra.Debts))
	}
	if ra.Debts[0].Symbol != "USDC" {
		t.Errorf("userA debt symbol = %s, want USDC", ra.Debts[0].Symbol)
	}
	if ra.Debts[0].CurrentDebt.Cmp(big.NewInt(1500e6)) != 0 {
		t.Errorf("userA USDC debt = %s, want 1500e6", ra.Debts[0].CurrentDebt)
	}

	// Verify userB: 1 collateral (WETH), no debt
	rb := results[userB]
	if rb.Err != nil {
		t.Fatalf("userB error: %v", rb.Err)
	}
	if len(rb.Collaterals) != 1 {
		t.Fatalf("userB: expected 1 collateral, got %d", len(rb.Collaterals))
	}
	if rb.Collaterals[0].ActualBalance.Cmp(big.NewInt(2100e15)) != 0 {
		t.Errorf("userB WETH balance = %s, want 2100e15", rb.Collaterals[0].ActualBalance)
	}
	if len(rb.Debts) != 0 {
		t.Errorf("userB: expected 0 debts, got %d", len(rb.Debts))
	}

	// Verify userC: no positions
	rc := results[userC]
	if rc.Err != nil {
		t.Fatalf("userC error: %v", rc.Err)
	}
	if len(rc.Collaterals) != 0 {
		t.Errorf("userC: expected 0 collaterals, got %d", len(rc.Collaterals))
	}
	if len(rc.Debts) != 0 {
		t.Errorf("userC: expected 0 debts, got %d", len(rc.Debts))
	}

	// Should have used exactly 2 multicalls (metadata was cached)
	if callNumber != 2 {
		t.Errorf("expected 2 multicall executions, got %d", callNumber)
	}
}

// TestGetBatchUserPositionData_StaleUIPoolDataProvider verifies that debt is
// detected even when the UIPoolDataProvider returns ScaledVariableDebt=0 for
// all reserves (the Aave V3 ETH bug after protocol upgrades). The fix queries
// the PoolDataProvider for ALL reserves and derives debt from actual
// CurrentVariableDebt values.
func TestGetBatchUserPositionData_StaleUIPoolDataProvider(t *testing.T) {
	userA := common.HexToAddress("0x0000000000000000000000000000000000000001")

	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	usdt := common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")

	// Simulate stale UIPoolDataProvider: user has WETH collateral and
	// USDC+USDT debt, but ScaledVariableDebt is 0 for ALL reserves.
	reservesA := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(5e18), UsageAsCollateralEnabledOnUser: true, ScaledVariableDebt: big.NewInt(0)},
		{UnderlyingAsset: usdc, ScaledATokenBalance: big.NewInt(0), UsageAsCollateralEnabledOnUser: false, ScaledVariableDebt: big.NewInt(0)}, // stale: should be non-zero
		{UnderlyingAsset: usdt, ScaledATokenBalance: big.NewInt(0), UsageAsCollateralEnabledOnUser: false, ScaledVariableDebt: big.NewInt(0)}, // stale: should be non-zero
	}

	var svc *BlockchainService
	callNumber := 0

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callNumber++
		switch callNumber {
		case 1:
			// Multicall 1: getUserReservesDataBatch (1 user)
			return []outbound.Result{
				{Success: true, ReturnData: encodeRawUserReserves(reservesA)},
			}, nil

		case 2:
			// Multicall 2: token metadata for ALL 3 reserves (WETH cached, USDC+USDT need fetch)
			// 3 calls per token × 2 uncached tokens = 6 calls
			results := make([]outbound.Result, len(calls))
			for i, call := range calls {
				switch {
				case call.Target == usdc || call.Target == usdt:
					symbol := "USDC"
					decimals := 6
					if call.Target == usdt {
						symbol = "USDT"
					}
					method, _ := svc.erc20ABI.MethodById(call.CallData[:4])
					switch method.Name {
					case "decimals":
						packed, _ := method.Outputs.Pack(uint8(decimals))
						results[i] = outbound.Result{Success: true, ReturnData: packed}
					case "symbol":
						packed, _ := method.Outputs.Pack(symbol)
						results[i] = outbound.Result{Success: true, ReturnData: packed}
					case "name":
						packed, _ := method.Outputs.Pack(symbol + " Token")
						results[i] = outbound.Result{Success: true, ReturnData: packed}
					}
				default:
					t.Fatalf("unexpected metadata call for %s", call.Target.Hex())
				}
			}
			return results, nil

		case 3:
			// Multicall 3: getUserReserveData for ALL 3 reserves (the fix!)
			// Before the fix, only WETH would be queried (ScaledATokenBalance > 0).
			// After the fix, ALL reserves are queried.
			if len(calls) != 3 {
				t.Errorf("multicall 3: expected 3 calls (all reserves), got %d", len(calls))
			}
			results := make([]outbound.Result, len(calls))
			for i, call := range calls {
				method, _ := svc.getUserReserveDataABI.MethodById(call.CallData[:4])
				args, _ := method.Inputs.Unpack(call.CallData[4:])
				asset := args[0].(common.Address)

				var data ActualUserReserveData
				switch asset {
				case weth:
					data = ActualUserReserveData{
						Asset: weth, CurrentATokenBalance: big.NewInt(5100e15),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(0),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(0),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: true,
					}
				case usdc:
					data = ActualUserReserveData{
						Asset: usdc, CurrentATokenBalance: big.NewInt(0),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(75000e6),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(0),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: false,
					}
				case usdt:
					data = ActualUserReserveData{
						Asset: usdt, CurrentATokenBalance: big.NewInt(0),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(108000e6),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(0),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: false,
					}
				default:
					t.Fatalf("unexpected reserve data call: asset=%s", asset.Hex())
				}
				results[i] = outbound.Result{Success: true, ReturnData: encodeUserReserveData(t, svc, data)}
			}
			return results, nil

		default:
			t.Fatalf("unexpected multicall #%d", callNumber)
			return nil, nil
		}
	}

	svc = newTestBlockchainService(t, mock)
	svc.metadataCache[weth] = TokenMetadata{Symbol: "WETH", Decimals: 18, Name: "Wrapped Ether"}

	reader := &PositionReader{
		logger:             svc.logger,
		blockchainServices: make(map[blockchain.ProtocolKey]*BlockchainService),
	}
	poolAddress := common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")
	reader.blockchainServices[blockchain.ProtocolKey{ChainID: 1, PoolAddress: poolAddress}] = svc

	results, err := reader.GetBatchUserPositionData(
		context.Background(),
		[]common.Address{userA},
		poolAddress, 1, 20000000,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ra := results[userA]
	if ra.Err != nil {
		t.Fatalf("userA error: %v", ra.Err)
	}

	// Must detect collateral despite stale UIPoolDataProvider
	if len(ra.Collaterals) != 1 {
		t.Fatalf("expected 1 collateral, got %d", len(ra.Collaterals))
	}
	if ra.Collaterals[0].Symbol != "WETH" {
		t.Errorf("collateral symbol = %s, want WETH", ra.Collaterals[0].Symbol)
	}

	// Must detect debt despite ScaledVariableDebt=0 from UIPoolDataProvider
	if len(ra.Debts) != 2 {
		t.Fatalf("expected 2 debts (USDC+USDT), got %d — stale UIPoolDataProvider bug not fixed", len(ra.Debts))
	}
	debtSymbols := map[string]bool{}
	for _, d := range ra.Debts {
		debtSymbols[d.Symbol] = true
	}
	if !debtSymbols["USDC"] {
		t.Error("expected USDC debt, not found")
	}
	if !debtSymbols["USDT"] {
		t.Error("expected USDT debt, not found")
	}
}

func TestGetBatchUserPositionData_EmptyUsers(t *testing.T) {
	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("Execute should not be called for empty users")
		return nil, nil
	}

	svc := newTestBlockchainService(t, mock)
	reader := &PositionReader{
		logger:             svc.logger,
		blockchainServices: make(map[blockchain.ProtocolKey]*BlockchainService),
	}
	poolAddress := common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")
	reader.blockchainServices[blockchain.ProtocolKey{ChainID: 1, PoolAddress: poolAddress}] = svc

	results, err := reader.GetBatchUserPositionData(context.Background(), []common.Address{}, poolAddress, 1, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results, got %d", len(results))
	}
}

// TestGetBatchUserPositionData_SingleUserReserveDataFailure verifies that when
// getUserReserveData fails for one user's asset, other users still get their
// full position data. The failed asset is simply omitted.
func TestGetBatchUserPositionData_SingleUserReserveDataFailure(t *testing.T) {
	userA := common.HexToAddress("0x0000000000000000000000000000000000000001")
	userB := common.HexToAddress("0x0000000000000000000000000000000000000002")

	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	// Both users have WETH + USDC reserves.
	reservesAB := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(1e18), UsageAsCollateralEnabledOnUser: true, ScaledVariableDebt: big.NewInt(0)},
		{UnderlyingAsset: usdc, ScaledATokenBalance: big.NewInt(0), UsageAsCollateralEnabledOnUser: false, ScaledVariableDebt: big.NewInt(1500e6)},
	}

	var svc *BlockchainService
	callNumber := 0

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callNumber++
		switch callNumber {
		case 1:
			// Multicall 1: getUserReservesDataBatch (both users succeed)
			return []outbound.Result{
				{Success: true, ReturnData: encodeRawUserReserves(reservesAB)},
				{Success: true, ReturnData: encodeRawUserReserves(reservesAB)},
			}, nil

		case 2:
			// Multicall 2: getUserReserveData for all user x asset pairs.
			// Fail userA's WETH call, succeed for everything else.
			results := make([]outbound.Result, len(calls))
			for i, call := range calls {
				method, _ := svc.getUserReserveDataABI.MethodById(call.CallData[:4])
				args, _ := method.Inputs.Unpack(call.CallData[4:])
				asset := args[0].(common.Address)
				user := args[1].(common.Address)

				if user == userA && asset == weth {
					// Simulate RPC failure for this specific sub-call
					results[i] = outbound.Result{Success: false}
					continue
				}

				var data ActualUserReserveData
				switch {
				case user == userA && asset == usdc:
					data = ActualUserReserveData{
						Asset: usdc, CurrentATokenBalance: big.NewInt(0),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(1500e6),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(0),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: false,
					}
				case user == userB && asset == weth:
					data = ActualUserReserveData{
						Asset: weth, CurrentATokenBalance: big.NewInt(2e18),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(0),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(0),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: true,
					}
				case user == userB && asset == usdc:
					data = ActualUserReserveData{
						Asset: usdc, CurrentATokenBalance: big.NewInt(0),
						CurrentStableDebt: big.NewInt(0), CurrentVariableDebt: big.NewInt(500e6),
						PrincipalStableDebt: big.NewInt(0), ScaledVariableDebt: big.NewInt(0),
						StableBorrowRate: big.NewInt(0), LiquidityRate: big.NewInt(0),
						UsageAsCollateralEnabled: false,
					}
				default:
					t.Fatalf("unexpected call: user=%s asset=%s", user.Hex(), asset.Hex())
				}
				results[i] = outbound.Result{Success: true, ReturnData: encodeUserReserveData(t, svc, data)}
			}
			return results, nil

		default:
			t.Fatalf("unexpected multicall #%d", callNumber)
			return nil, nil
		}
	}

	svc = newTestBlockchainService(t, mock)
	svc.metadataCache[weth] = TokenMetadata{Symbol: "WETH", Decimals: 18, Name: "Wrapped Ether"}
	svc.metadataCache[usdc] = TokenMetadata{Symbol: "USDC", Decimals: 6, Name: "USD Coin"}

	reader := &PositionReader{
		logger:             svc.logger,
		blockchainServices: make(map[blockchain.ProtocolKey]*BlockchainService),
	}
	poolAddress := common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")
	reader.blockchainServices[blockchain.ProtocolKey{ChainID: 1, PoolAddress: poolAddress}] = svc

	results, err := reader.GetBatchUserPositionData(
		context.Background(),
		[]common.Address{userA, userB},
		poolAddress, 1, 20000000,
	)
	if err != nil {
		t.Fatalf("batch should succeed despite single sub-call failure: %v", err)
	}

	// userA: WETH data was lost (sub-call failed), but USDC debt should still be present.
	ra := results[userA]
	if ra.Err != nil {
		t.Fatalf("userA should not have a top-level error: %v", ra.Err)
	}
	// userA had WETH collateral but the sub-call failed, so no collateral returned.
	if len(ra.Collaterals) != 0 {
		t.Errorf("userA: expected 0 collaterals (WETH sub-call failed), got %d", len(ra.Collaterals))
	}
	// userA's USDC debt should still be present.
	if len(ra.Debts) != 1 {
		t.Fatalf("userA: expected 1 debt (USDC), got %d", len(ra.Debts))
	}
	if ra.Debts[0].Symbol != "USDC" {
		t.Errorf("userA debt symbol = %s, want USDC", ra.Debts[0].Symbol)
	}

	// userB: fully intact — 1 collateral (WETH) + 1 debt (USDC).
	rb := results[userB]
	if rb.Err != nil {
		t.Fatalf("userB error: %v", rb.Err)
	}
	if len(rb.Collaterals) != 1 {
		t.Fatalf("userB: expected 1 collateral, got %d", len(rb.Collaterals))
	}
	if rb.Collaterals[0].Symbol != "WETH" {
		t.Errorf("userB collateral symbol = %s, want WETH", rb.Collaterals[0].Symbol)
	}
	if len(rb.Debts) != 1 {
		t.Fatalf("userB: expected 1 debt, got %d", len(rb.Debts))
	}
	if rb.Debts[0].Symbol != "USDC" {
		t.Errorf("userB debt symbol = %s, want USDC", rb.Debts[0].Symbol)
	}
}

func TestBuildCollateralData_MissingMetadata(t *testing.T) {
	asset := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	actualDataMap := map[common.Address]ActualUserReserveData{
		asset: {CurrentATokenBalance: big.NewInt(1e18), UsageAsCollateralEnabled: true},
	}

	_, err := buildCollateralData([]common.Address{asset}, map[common.Address]TokenMetadata{}, actualDataMap)
	if err == nil {
		t.Fatal("expected error for missing metadata, got nil")
	}
}

func TestBuildCollateralData_ZeroDecimals(t *testing.T) {
	asset := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	metadataMap := map[common.Address]TokenMetadata{
		asset: {Symbol: "WETH", Decimals: 0, Name: "Wrapped Ether"},
	}
	actualDataMap := map[common.Address]ActualUserReserveData{
		asset: {CurrentATokenBalance: big.NewInt(1e18), UsageAsCollateralEnabled: true},
	}

	_, err := buildCollateralData([]common.Address{asset}, metadataMap, actualDataMap)
	if err == nil {
		t.Fatal("expected error for zero decimals, got nil")
	}
}

func TestBuildCollateralData_MissingActualData_SkipsAsset(t *testing.T) {
	asset := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	metadataMap := map[common.Address]TokenMetadata{
		asset: {Symbol: "WETH", Decimals: 18, Name: "Wrapped Ether"},
	}

	// Missing actual data (transient RPC failure) should be skipped, not error.
	collaterals, err := buildCollateralData([]common.Address{asset}, metadataMap, map[common.Address]ActualUserReserveData{})
	if err != nil {
		t.Fatalf("expected no error for missing actual data, got: %v", err)
	}
	if len(collaterals) != 0 {
		t.Fatalf("expected 0 collaterals when actual data is missing, got %d", len(collaterals))
	}
}

func TestBuildDebtData_MissingMetadata(t *testing.T) {
	asset := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	actualDataMap := map[common.Address]ActualUserReserveData{
		asset: {CurrentVariableDebt: big.NewInt(1000e6)},
	}

	_, err := buildDebtData([]common.Address{asset}, map[common.Address]TokenMetadata{}, actualDataMap)
	if err == nil {
		t.Fatal("expected error for missing metadata, got nil")
	}
}

func TestBuildDebtData_MissingActualData_SkipsAsset(t *testing.T) {
	asset := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	metadataMap := map[common.Address]TokenMetadata{
		asset: {Symbol: "USDC", Decimals: 6, Name: "USD Coin"},
	}

	// Missing actual data (transient RPC failure) should be skipped, not error.
	debts, err := buildDebtData([]common.Address{asset}, metadataMap, map[common.Address]ActualUserReserveData{})
	if err != nil {
		t.Fatalf("expected no error for missing actual data, got: %v", err)
	}
	if len(debts) != 0 {
		t.Fatalf("expected 0 debts when actual data is missing, got %d", len(debts))
	}
}
