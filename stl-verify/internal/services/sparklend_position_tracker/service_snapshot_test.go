package sparklend_position_tracker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"
)

// ----- helpers -----

// newPositionTestService builds a Service wired with all mock dependencies and
// a pre-seeded blockchainService for the AaveV3 mainnet pool so that no real
// network calls are required.
//
// The returned mockPositionRepository captures every SaveBorrower / collateral
// call for later assertions.
func newPositionTestService(
	t *testing.T,
	ethURL string, // URL of the mock eth RPC server
	mc *testutil.MockMulticaller,
) (*Service, *testutil.MockPositionRepository) {
	t.Helper()

	ctx := context.Background()
	client, err := ethclient.DialContext(ctx, ethURL)
	if err != nil {
		t.Fatalf("dial mock eth: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}

	posRepo := &testutil.MockPositionRepository{}

	svc := &Service{
		ethClient:          client,
		multicallClient:    mc,
		erc20ABI:           erc20ABI,
		blockchainServices: make(map[blockchain.ProtocolKey]*blockchainService),
		userRepo:           &testutil.MockUserRepository{},
		protocolRepo:       &testutil.MockProtocolRepository{},
		tokenRepo:          &testutil.MockTokenRepository{},
		positionRepo:       posRepo,
		eventRepo:          &testutil.MockEventRepository{},
		txManager:          &testutil.MockTxManager{},
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	// Pre-seed a blockchainService for AaveV3 Ethereum so that
	// getOrCreateBlockchainService doesn't try to hit any network.
	key := blockchain.ProtocolKey{
		ChainID:     1,
		PoolAddress: common.HexToAddress(aaveV3EthPool),
	}
	bsvc := &blockchainService{
		chainID:               1,
		ethClient:             client,
		multicallClient:       mc,
		erc20ABI:              erc20ABI,
		uiPoolDataProvider:    common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d"),
		poolAddress:           common.HexToAddress(aaveV3EthPool),
		poolAddressesProvider: common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"),
		protocolVersion:       blockchain.ProtocolVersionAaveV3,
		metadataCache:         make(map[common.Address]TokenMetadata),
		logger:                svc.logger.With("component", "blockchain-service"),
	}
	if err := bsvc.loadABIs(blockchain.ProtocolVersionAaveV3); err != nil {
		t.Fatalf("load ABIs: %v", err)
	}
	svc.blockchainServices[key] = bsvc

	return svc, posRepo
}

// startMockEthServer starts an httptest server that handles eth_call by
// returning the provided hex-encoded result bytes for every call.
func startMockEthServer(t *testing.T, hexResult string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"result":  hexResult,
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("mock eth server encode: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// buildUserReservesDataResponse encodes a getUserReservesData response in the
// SparkLend 4-field format:
//   - [address underlyingAsset, uint256 scaledATokenBalance,
//     bool usageAsCollateralEnabledOnUser, uint256 scaledVariableDebt]
//
// The raw encoding follows the ABI layout used by decodeUserReservesRaw.
func buildUserReservesDataResponse(t *testing.T, reserves []UserReserveData) string {
	t.Helper()

	n := len(reserves)
	// Layout: [offset (32)] [eMode uint8 (32)] [length (32)] [n * 4 * 32]
	data := make([]byte, 32*2+32+n*4*32)

	// word 0: offset to array = 64 (0x40)
	data[31] = 64

	// word 1: eMode = 0 (already zero)

	// word 2 (at offset 64): array length
	arrayLenBytes := common.LeftPadBytes(big.NewInt(int64(n)).Bytes(), 32)
	copy(data[64:96], arrayLenBytes)

	for i, r := range reserves {
		base := 96 + i*4*32
		// asset (padded to 32 bytes)
		copy(data[base:base+32], common.LeftPadBytes(r.UnderlyingAsset.Bytes(), 32))
		// scaledATokenBalance (default 0 if nil)
		if r.ScaledATokenBalance != nil {
			copy(data[base+32:base+64], common.LeftPadBytes(r.ScaledATokenBalance.Bytes(), 32))
		}
		// usageAsCollateralEnabledOnUser (bool as uint256)
		if r.UsageAsCollateralEnabledOnUser {
			data[base+95] = 1
		}
		// scaledVariableDebt (default 0 if nil)
		if r.ScaledVariableDebt != nil {
			copy(data[base+96:base+128], common.LeftPadBytes(r.ScaledVariableDebt.Bytes(), 32))
		}
	}

	return "0x" + hex.EncodeToString(data)
}

// buildUserReservesDataResponse7Field encodes a getUserReservesData response
// using the 7-field legacy AaveV3 layout which includes stable debt fields:
//
//	[0] address  underlyingAsset
//	[1] uint256  scaledATokenBalance
//	[2] bool     usageAsCollateralEnabledOnUser
//	[3] uint256  scaledVariableDebt
//	[4] uint256  stableBorrowRate
//	[5] uint256  principalStableDebt
//	[6] uint256  stableBorrowLastUpdateTimestamp
func buildUserReservesDataResponse7Field(t *testing.T, reserves []UserReserveData) string {
	t.Helper()

	n := len(reserves)
	// Layout: [offset (32)] [eMode uint8 (32)] [length (32)] [n * 7 * 32]
	data := make([]byte, 32*2+32+n*7*32)

	// word 0: offset to array = 64 (0x40)
	data[31] = 64

	// word 2 (at offset 64): array length
	copy(data[64:96], common.LeftPadBytes(big.NewInt(int64(n)).Bytes(), 32))

	for i, r := range reserves {
		base := 96 + i*7*32
		copy(data[base:base+32], common.LeftPadBytes(r.UnderlyingAsset.Bytes(), 32))
		if r.ScaledATokenBalance != nil {
			copy(data[base+32:base+64], common.LeftPadBytes(r.ScaledATokenBalance.Bytes(), 32))
		}
		if r.UsageAsCollateralEnabledOnUser {
			data[base+95] = 1
		}
		if r.ScaledVariableDebt != nil {
			copy(data[base+96:base+128], common.LeftPadBytes(r.ScaledVariableDebt.Bytes(), 32))
		}
		// stableBorrowRate (word 4) — zero
		if r.PrincipalStableDebt != nil {
			copy(data[base+5*32:base+6*32], common.LeftPadBytes(r.PrincipalStableDebt.Bytes(), 32))
		}
		// stableBorrowLastUpdateTimestamp (word 6) — zero
	}

	return "0x" + hex.EncodeToString(data)
}

// buildGetUserReserveDataResult packs a single getUserReserveData ABI response.
func buildGetUserReserveDataResult(
	t *testing.T,
	aTokenBalance, stableDebt, variableDebt *big.Int,
	collateralEnabled bool,
) []byte {
	t.Helper()

	userReserveDataABI, err := abis.GetPoolDataProviderUserReserveDataABI()
	if err != nil {
		t.Fatalf("load getUserReserveData ABI: %v", err)
	}

	var lastUpdated uint64 = 0
	packed, err := userReserveDataABI.Methods["getUserReserveData"].Outputs.Pack(
		aTokenBalance,                       // currentATokenBalance
		stableDebt,                          // currentStableDebt
		variableDebt,                        // currentVariableDebt
		big.NewInt(0),                       // principalStableDebt
		big.NewInt(0),                       // scaledVariableDebt
		big.NewInt(0),                       // stableBorrowRate
		big.NewInt(0),                       // liquidityRate
		new(big.Int).SetUint64(lastUpdated), // stableRateLastUpdated (uint40 → *big.Int)
		collateralEnabled,                   // usageAsCollateralEnabled
	)
	if err != nil {
		t.Fatalf("pack getUserReserveData: %v", err)
	}
	return packed
}

// buildERC20MetadataResult packs the result of decimals(), symbol(), and name()
// into three separate []byte slices in the order used by batchGetTokenMetadata.
func buildERC20MetadataResult(t *testing.T, decimals uint8, symbol, name string) ([]byte, []byte, []byte) {
	t.Helper()
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}

	decPacked, err := erc20ABI.Methods["decimals"].Outputs.Pack(decimals)
	if err != nil {
		t.Fatalf("pack decimals: %v", err)
	}
	symPacked, err := erc20ABI.Methods["symbol"].Outputs.Pack(symbol)
	if err != nil {
		t.Fatalf("pack symbol: %v", err)
	}
	namePacked, err := erc20ABI.Methods["name"].Outputs.Pack(name)
	if err != nil {
		t.Fatalf("pack name: %v", err)
	}
	return decPacked, symPacked, namePacked
}

const (
	aaveV3EthPool = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
	// Block with a valid PoolDataProvider for AaveV3 Ethereum.
	testBlockNumber = int64(21000000)
	testChainID     = int64(1)
)

// ----- borrower / debt snapshot tests -----

// TestSavePositionSnapshot_BorrowAmountIsProviderDebt verifies that when a
// Borrow event is processed the borrower row uses the provider-snapped
// CurrentVariableDebt, NOT the event's Amount field.
func TestSavePositionSnapshot_BorrowAmountIsProviderDebt(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	// Provider says user has 1 ETH variable debt on WETH.
	providerDebt := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1 ETH
	eventDelta := big.NewInt(500000000000000000)                          // 0.5 ETH (event amount)

	// Borrow event for WETH: scaled supply = 0, scaled debt > 0
	reserves := []UserReserveData{
		{
			UnderlyingAsset:                weth,
			ScaledATokenBalance:            big.NewInt(0),
			UsageAsCollateralEnabledOnUser: false,
			ScaledVariableDebt:             providerDebt, // scaled ≈ actual for simplicity
			PrincipalStableDebt:            big.NewInt(0),
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	reserveDataPacked := buildGetUserReserveDataResult(t,
		big.NewInt(0), // aTokenBalance = 0 (no supply)
		big.NewInt(0), // stableDebt
		providerDebt,  // variableDebt = 1 ETH
		false,         // collateralEnabled
	)

	callCount := 0
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callCount++
		switch callCount {
		case 1: // batchGetTokenMetadata (3 calls: decimals, symbol, name)
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 2: // batchGetUserReserveData (1 call per asset)
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected Execute call %d", callCount)
		}
	}

	ethHex := buildUserReservesDataResponse(t, reserves)
	srv := startMockEthServer(t, ethHex)
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	// savePositionSnapshot calls batchGetTokenMetadata once for the event
	// reserve, then extractUserPositionSnapshots calls it again for all active
	// assets. Use len(calls) to distinguish the two multicall batches so the
	// mock remains independent of call order.
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3: // token metadata (decimals, symbol, name for 1 token)
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1: // getUserReserveData
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call with %d args", len(calls))
		}
	}

	eventData := &PositionEventData{
		EventType: EventBorrow,
		TxHash:    "0xabc",
		User:      user,
		Reserve:   weth,
		Amount:    eventDelta, // event delta — should NOT be used as the debt amount
	}

	err := svc.savePositionSnapshot(
		context.Background(),
		eventData,
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, 0,
	)
	if err != nil {
		t.Fatalf("savePositionSnapshot: %v", err)
	}

	// Debt row should store the provider-snapped amount (1 ETH), not the event delta (0.5 ETH).
	if len(posRepo.SavedBorrowers) != 1 {
		t.Fatalf("expected 1 borrower row, got %d", len(posRepo.SavedBorrowers))
	}
	got := posRepo.SavedBorrowers[0]
	if got.Amount != "1" {
		t.Errorf("borrower Amount = %q, want %q (provider snapshot)", got.Amount, "1")
	}
	if got.Change != "0" {
		t.Errorf("borrower Change = %q, want %q", got.Change, "0")
	}
}

// TestSavePositionSnapshot_BorrowChangeIsZero verifies the change column is
// explicitly zero for every snapshot row, never the event amount.
func TestSavePositionSnapshot_BorrowChangeIsZero(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	providerDebt := big.NewInt(2000000000000000000) // 2 ETH

	reserves := []UserReserveData{
		{
			UnderlyingAsset:     weth,
			ScaledATokenBalance: big.NewInt(0),
			ScaledVariableDebt:  providerDebt,
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	reserveDataPacked := buildGetUserReserveDataResult(t, big.NewInt(0), big.NewInt(0), providerDebt, false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventBorrow,
		TxHash:    "0xbbb",
		User:      user,
		Reserve:   weth,
		Amount:    big.NewInt(1000000000000000000), // event delta
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)
	if err != nil {
		t.Fatalf("savePositionSnapshot: %v", err)
	}

	for _, row := range posRepo.SavedBorrowers {
		if row.Change != "0" {
			t.Errorf("borrower.Change = %q, want %q", row.Change, "0")
		}
	}
	for _, row := range posRepo.SavedCollaterals {
		if row.Change != "0" {
			t.Errorf("collateral.Change = %q, want %q", row.Change, "0")
		}
	}
}

// TestSavePositionSnapshot_SuppliedBalanceIsProviderSnapshot verifies that the
// collateral row Amount comes from CurrentATokenBalance, not the event Amount.
func TestSavePositionSnapshot_SuppliedBalanceIsProviderSnapshot(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	providerSupply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1 ETH
	eventAmount := big.NewInt(500000000000000000)                           // 0.5 ETH

	reserves := []UserReserveData{
		{
			UnderlyingAsset:                weth,
			ScaledATokenBalance:            providerSupply,
			UsageAsCollateralEnabledOnUser: true,
			ScaledVariableDebt:             big.NewInt(0),
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	// Supply = 1 ETH, no debt
	reserveDataPacked := buildGetUserReserveDataResult(t, providerSupply, big.NewInt(0), big.NewInt(0), true)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventSupply,
		TxHash:    "0xccc",
		User:      user,
		Reserve:   weth,
		Amount:    eventAmount, // event delta — should NOT appear in the collateral row
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)
	if err != nil {
		t.Fatalf("savePositionSnapshot: %v", err)
	}

	if len(posRepo.SavedCollaterals) != 1 {
		t.Fatalf("expected 1 collateral row, got %d", len(posRepo.SavedCollaterals))
	}
	got := posRepo.SavedCollaterals[0]
	if got.Amount != "1" {
		t.Errorf("collateral.Amount = %q, want %q (provider snapshot)", got.Amount, "1")
	}
	if got.Change != "0" {
		t.Errorf("collateral.Change = %q, want %q", got.Change, "0")
	}
}

// TestSavePositionSnapshot_NonCollateralEnabledAssetPersisted verifies that a
// supplied-but-not-collateral-enabled asset still produces a collateral row
// with collateral_enabled = false.
func TestSavePositionSnapshot_NonCollateralEnabledAssetPersisted(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	supply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1 ETH

	reserves := []UserReserveData{
		{
			UnderlyingAsset:                weth,
			ScaledATokenBalance:            supply,
			UsageAsCollateralEnabledOnUser: false, // explicitly NOT enabled
			ScaledVariableDebt:             big.NewInt(0),
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	// collateralEnabled = false in the provider response
	reserveDataPacked := buildGetUserReserveDataResult(t, supply, big.NewInt(0), big.NewInt(0), false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventSupply,
		TxHash:    "0xddd",
		User:      user,
		Reserve:   weth,
		Amount:    supply,
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)
	if err != nil {
		t.Fatalf("savePositionSnapshot: %v", err)
	}

	// Non-collateral-enabled asset must still produce a row.
	if len(posRepo.SavedCollaterals) != 1 {
		t.Fatalf("expected 1 collateral row (non-enabled asset), got %d", len(posRepo.SavedCollaterals))
	}
	if posRepo.SavedCollaterals[0].CollateralEnabled {
		t.Error("collateral.CollateralEnabled = true, want false")
	}
}

// TestSavePositionSnapshot_ZeroDebtRepayPersistsZeroRow verifies that when the
// provider reports zero debt after a full repayment, a borrower row with
// Amount="0" is still written so downstream consumers know the debt is closed.
func TestSavePositionSnapshot_ZeroDebtRepayPersistsZeroRow(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	reserves := []UserReserveData{
		{
			UnderlyingAsset:     weth,
			ScaledATokenBalance: big.NewInt(0),
			ScaledVariableDebt:  big.NewInt(0),
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	// Zero debt and zero supply after repayment
	reserveDataPacked := buildGetUserReserveDataResult(t, big.NewInt(0), big.NewInt(0), big.NewInt(0), false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	// getUserReservesData returns the asset with zero balances — extractUserPositionSnapshots
	// should not include it since both supply and debt are zero.
	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventRepay,
		TxHash:    "0xeee",
		User:      user,
		Reserve:   weth,
		Amount:    new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)
	if err != nil {
		t.Fatalf("savePositionSnapshot: %v", err)
	}

	// For Repay events, a zero-debt row must be written so downstream knows debt is closed.
	if len(posRepo.SavedBorrowers) != 1 {
		t.Fatalf("expected 1 borrower row for zero-debt Repay, got %d", len(posRepo.SavedBorrowers))
	}
	got := posRepo.SavedBorrowers[0]
	if got.Amount != "0" {
		t.Errorf("borrower.Amount = %q, want %q", got.Amount, "0")
	}
	if got.Change != "0" {
		t.Errorf("borrower.Change = %q, want %q", got.Change, "0")
	}
	if got.EventType != string(EventRepay) {
		t.Errorf("borrower.EventType = %q, want %q", got.EventType, string(EventRepay))
	}
	// Zero supply + zero debt should produce no collateral rows.
	if len(posRepo.SavedCollaterals) != 0 {
		t.Errorf("expected 0 collateral rows for zero supply, got %d", len(posRepo.SavedCollaterals))
	}
}

// TestSavePositionSnapshot_ZeroSupplyNotPersisted verifies that assets with
// zero supplied balance produce no collateral row.
func TestSavePositionSnapshot_ZeroSupplyNotPersisted(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	providerDebt := big.NewInt(500000000000000000) // 0.5 ETH

	reserves := []UserReserveData{
		{
			UnderlyingAsset:     weth,
			ScaledATokenBalance: big.NewInt(0), // no supply
			ScaledVariableDebt:  providerDebt,
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	reserveDataPacked := buildGetUserReserveDataResult(t, big.NewInt(0), big.NewInt(0), providerDebt, false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventBorrow,
		TxHash:    "0xfff",
		User:      user,
		Reserve:   weth,
		Amount:    providerDebt,
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)
	if err != nil {
		t.Fatalf("savePositionSnapshot: %v", err)
	}

	// Debt row must exist (there IS debt), but no collateral row (supply = 0).
	if len(posRepo.SavedBorrowers) != 1 {
		t.Errorf("expected 1 borrower row, got %d", len(posRepo.SavedBorrowers))
	}
	if len(posRepo.SavedCollaterals) != 0 {
		t.Errorf("expected 0 collateral rows for zero supply, got %d", len(posRepo.SavedCollaterals))
	}
}

// TestSavePositionSnapshot_MultipleTokensPersistIndependently verifies that when
// a user has positions across multiple assets, all of them are captured.
func TestSavePositionSnapshot_MultipleTokensPersistIndependently(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	wethSupply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1 ETH
	usdcDebt := big.NewInt(1000000000)                                  // 1000 USDC (6 decimals)

	reserves := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: wethSupply, UsageAsCollateralEnabledOnUser: true},
		{UnderlyingAsset: usdc, ScaledVariableDebt: usdcDebt},
	}

	wethDecPacked, wethSymPacked, wethNamePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	usdcDecPacked, usdcSymPacked, usdcNamePacked := buildERC20MetadataResult(t, 6, "USDC", "USD Coin")
	wethReserveData := buildGetUserReserveDataResult(t, wethSupply, big.NewInt(0), big.NewInt(0), true)
	usdcReserveData := buildGetUserReserveDataResult(t, big.NewInt(0), big.NewInt(0), usdcDebt, false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3: // metadata for 1 token (event reserve)
			return []outbound.Result{
				{Success: true, ReturnData: usdcDecPacked},
				{Success: true, ReturnData: usdcSymPacked},
				{Success: true, ReturnData: usdcNamePacked},
			}, nil
		case 6: // metadata for 2 tokens (WETH + USDC) — 3 calls each
			return []outbound.Result{
				{Success: true, ReturnData: wethDecPacked},
				{Success: true, ReturnData: wethSymPacked},
				{Success: true, ReturnData: wethNamePacked},
				{Success: true, ReturnData: usdcDecPacked},
				{Success: true, ReturnData: usdcSymPacked},
				{Success: true, ReturnData: usdcNamePacked},
			}, nil
		case 2: // getUserReserveData for 2 assets
			return []outbound.Result{
				{Success: true, ReturnData: wethReserveData},
				{Success: true, ReturnData: usdcReserveData},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected Execute call with %d calls", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventBorrow,
		TxHash:    "0xaaa111",
		User:      user,
		Reserve:   usdc,
		Amount:    usdcDebt,
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)
	if err != nil {
		t.Fatalf("savePositionSnapshot: %v", err)
	}

	// Exactly 1 debt row (USDC) and 1 collateral row (WETH).
	if len(posRepo.SavedBorrowers) != 1 {
		t.Errorf("expected 1 borrower row, got %d", len(posRepo.SavedBorrowers))
	}
	if len(posRepo.SavedCollaterals) != 1 {
		t.Errorf("expected 1 collateral row (WETH supply), got %d", len(posRepo.SavedCollaterals))
	}
}

// TestSnapshotUserPosition_LiquidationPersistsDebtRows verifies that
// snapshotUserPosition (used for liquidation events) now writes debt rows as
// well as collateral rows.
func TestSnapshotUserPosition_LiquidationPersistsDebtRows(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	wethSupply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	usdcDebt := big.NewInt(500000000) // 500 USDC

	reserves := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: wethSupply, UsageAsCollateralEnabledOnUser: true},
		{UnderlyingAsset: usdc, ScaledVariableDebt: usdcDebt},
	}

	wethDecPacked, wethSymPacked, wethNamePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	usdcDecPacked, usdcSymPacked, usdcNamePacked := buildERC20MetadataResult(t, 6, "USDC", "USD Coin")
	wethReserveData := buildGetUserReserveDataResult(t, wethSupply, big.NewInt(0), big.NewInt(0), true)
	usdcReserveData := buildGetUserReserveDataResult(t, big.NewInt(0), big.NewInt(0), usdcDebt, false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 6: // metadata for 2 assets
			return []outbound.Result{
				{Success: true, ReturnData: wethDecPacked},
				{Success: true, ReturnData: wethSymPacked},
				{Success: true, ReturnData: wethNamePacked},
				{Success: true, ReturnData: usdcDecPacked},
				{Success: true, ReturnData: usdcSymPacked},
				{Success: true, ReturnData: usdcNamePacked},
			}, nil
		case 2: // getUserReserveData for 2 assets
			return []outbound.Result{
				{Success: true, ReturnData: wethReserveData},
				{Success: true, ReturnData: usdcReserveData},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected Execute call with %d calls", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.snapshotUserPosition(
		context.Background(),
		nil, // nil tx is handled by MockTxManager + mock repos
		user,
		"LiquidationCall",
		common.FromHex("0xdeadbeef"),
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, 0,
	)
	if err != nil {
		t.Fatalf("snapshotUserPosition: %v", err)
	}

	// Should have 1 debt row (USDC) and 1 collateral row (WETH).
	if len(posRepo.SavedBorrowers) != 1 {
		t.Errorf("expected 1 borrower (debt) row from liquidation snapshot, got %d", len(posRepo.SavedBorrowers))
	}
	if len(posRepo.SavedCollaterals) != 1 {
		t.Errorf("expected 1 collateral row from liquidation snapshot, got %d", len(posRepo.SavedCollaterals))
	}
	if len(posRepo.SavedBorrowers) > 0 && posRepo.SavedBorrowers[0].Change != "0" {
		t.Errorf("liquidation borrower.Change = %q, want %q", posRepo.SavedBorrowers[0].Change, "0")
	}
}

// TestSaveCollateralToggleEvent_NonEnabledBalancePersisted verifies that when
// a ReserveUsedAsCollateralDisabled event fires, the actual supply balance is
// stored even though collateral_enabled = false (the user still holds the asset).
func TestSaveCollateralToggleEvent_NonEnabledBalancePersisted(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	supply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1 ETH still held

	// getUserReservesData shows the user still holds supply (collateral_enabled = false
	// after the disable event)
	reserves := []UserReserveData{
		{
			UnderlyingAsset:                weth,
			ScaledATokenBalance:            supply,
			UsageAsCollateralEnabledOnUser: false,
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	reserveDataPacked := buildGetUserReserveDataResult(t, supply, big.NewInt(0), big.NewInt(0), false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	eventData := &PositionEventData{
		EventType:         EventReserveUsedAsCollateralDisabled,
		TxHash:            "0xabc999",
		User:              user,
		Reserve:           weth,
		CollateralEnabled: false,
	}
	err := svc.saveCollateralToggleEvent(
		context.Background(),
		eventData,
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, 0,
	)
	if err != nil {
		t.Fatalf("saveCollateralToggleEvent: %v", err)
	}

	if len(posRepo.SavedCollaterals) != 1 {
		t.Fatalf("expected 1 collateral row, got %d", len(posRepo.SavedCollaterals))
	}
	got := posRepo.SavedCollaterals[0]
	// Balance should be the actual supply (1 ETH), not zero.
	if got.Amount != "1" {
		t.Errorf("collateral.Amount = %q, want %q", got.Amount, "1")
	}
	if got.CollateralEnabled {
		t.Error("collateral.CollateralEnabled = true, want false for disabled event")
	}
	if got.Change != "0" {
		t.Errorf("collateral.Change = %q, want %q", got.Change, "0")
	}
}

// TestSaveCollateralToggleEvent_FullSnapshot_MultipleReserves verifies that a
// collateral toggle event produces a full snapshot: collateral rows for all
// reserves with supply AND debt rows for reserves with non-zero debt.
func TestSaveCollateralToggleEvent_FullSnapshot_MultipleReserves(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	wethSupply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1 ETH
	usdcDebt := big.NewInt(1000000000)                                  // 1000 USDC (6 decimals)

	reserves := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: wethSupply, UsageAsCollateralEnabledOnUser: true},
		{UnderlyingAsset: usdc, ScaledVariableDebt: usdcDebt},
	}

	wethDecPacked, wethSymPacked, wethNamePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	usdcDecPacked, usdcSymPacked, usdcNamePacked := buildERC20MetadataResult(t, 6, "USDC", "USD Coin")
	wethReserveData := buildGetUserReserveDataResult(t, wethSupply, big.NewInt(0), big.NewInt(0), true)
	usdcReserveData := buildGetUserReserveDataResult(t, big.NewInt(0), big.NewInt(0), usdcDebt, false)

	// Metadata results keyed by token address for order-independent lookup.
	// batchGetTokenMetadata targets individual token contracts, so we match on call target.
	metadataByToken := map[common.Address]struct{ dec, sym, name []byte }{
		weth: {wethDecPacked, wethSymPacked, wethNamePacked},
		usdc: {usdcDecPacked, usdcSymPacked, usdcNamePacked},
	}

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 6: // metadata for 2 tokens — 3 calls each (decimals, symbol, name)
			results := make([]outbound.Result, 6)
			for i := 0; i < len(calls); i += 3 {
				token := calls[i].Target
				md, ok := metadataByToken[token]
				if !ok {
					return nil, fmt.Errorf("unexpected token %s", token.Hex())
				}
				results[i] = outbound.Result{Success: true, ReturnData: md.dec}
				results[i+1] = outbound.Result{Success: true, ReturnData: md.sym}
				results[i+2] = outbound.Result{Success: true, ReturnData: md.name}
			}
			return results, nil
		case 2: // getUserReserveData for 2 assets (targets PoolDataProvider, order matches reserves)
			return []outbound.Result{
				{Success: true, ReturnData: wethReserveData},
				{Success: true, ReturnData: usdcReserveData},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected Execute call with %d calls", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.saveCollateralToggleEvent(context.Background(), &PositionEventData{
		EventType:         EventReserveUsedAsCollateralEnabled,
		TxHash:            "0xtoggle1",
		User:              user,
		Reserve:           weth,
		CollateralEnabled: true,
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)
	if err != nil {
		t.Fatalf("saveCollateralToggleEvent: %v", err)
	}

	// 1 collateral row (WETH with supply) and 1 debt row (USDC with debt).
	if len(posRepo.SavedCollaterals) != 1 {
		t.Fatalf("expected 1 collateral row, got %d", len(posRepo.SavedCollaterals))
	}
	if posRepo.SavedCollaterals[0].Amount != "1" {
		t.Errorf("collateral.Amount = %q, want %q", posRepo.SavedCollaterals[0].Amount, "1")
	}
	if !posRepo.SavedCollaterals[0].CollateralEnabled {
		t.Error("collateral.CollateralEnabled = false, want true for enabled event")
	}

	if len(posRepo.SavedBorrowers) != 1 {
		t.Fatalf("expected 1 borrower (debt) row, got %d", len(posRepo.SavedBorrowers))
	}
	if posRepo.SavedBorrowers[0].Amount != "1000" {
		t.Errorf("borrower.Amount = %q, want %q", posRepo.SavedBorrowers[0].Amount, "1000")
	}
	if posRepo.SavedBorrowers[0].EventType != string(EventReserveUsedAsCollateralEnabled) {
		t.Errorf("borrower.EventType = %q, want %q", posRepo.SavedBorrowers[0].EventType, string(EventReserveUsedAsCollateralEnabled))
	}
}

// TestSaveCollateralToggleEvent_DebtOnEventReserve verifies that when the event
// reserve itself has debt, both a collateral row and a debt row are written for it.
func TestSaveCollateralToggleEvent_DebtOnEventReserve(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	supply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1 ETH
	debt := new(big.Int).Mul(big.NewInt(5), big.NewInt(1e17))       // 0.5 ETH

	reserves := []UserReserveData{
		{
			UnderlyingAsset:                weth,
			ScaledATokenBalance:            supply,
			UsageAsCollateralEnabledOnUser: false,
			ScaledVariableDebt:             debt,
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	reserveDataPacked := buildGetUserReserveDataResult(t, supply, big.NewInt(0), debt, false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.saveCollateralToggleEvent(context.Background(), &PositionEventData{
		EventType:         EventReserveUsedAsCollateralDisabled,
		TxHash:            "0xtoggle2",
		User:              user,
		Reserve:           weth,
		CollateralEnabled: false,
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)
	if err != nil {
		t.Fatalf("saveCollateralToggleEvent: %v", err)
	}

	// Both a collateral row (supply) and a debt row should be written for WETH.
	if len(posRepo.SavedCollaterals) != 1 {
		t.Fatalf("expected 1 collateral row, got %d", len(posRepo.SavedCollaterals))
	}
	if posRepo.SavedCollaterals[0].Amount != "1" {
		t.Errorf("collateral.Amount = %q, want %q", posRepo.SavedCollaterals[0].Amount, "1")
	}
	if posRepo.SavedCollaterals[0].CollateralEnabled {
		t.Error("collateral.CollateralEnabled = true, want false")
	}

	if len(posRepo.SavedBorrowers) != 1 {
		t.Fatalf("expected 1 borrower row, got %d", len(posRepo.SavedBorrowers))
	}
	// 0.5 ETH = 500000000000000000 / 10^18 = 0.5
	if posRepo.SavedBorrowers[0].Amount != "0.500000000000000000" {
		t.Errorf("borrower.Amount = %q, want %q", posRepo.SavedBorrowers[0].Amount, "0.500000000000000000")
	}
}

// with debt but no supply are also included in the snapshot.
func TestExtractUserPositionSnapshots_IncludesDebtOnlyAssets(t *testing.T) {
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	usdcDebt := big.NewInt(1000000000) // 1000 USDC

	// Only debt, no supply
	reserves := []UserReserveData{
		{
			UnderlyingAsset:     usdc,
			ScaledATokenBalance: big.NewInt(0),
			ScaledVariableDebt:  usdcDebt,
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 6, "USDC", "USD Coin")
	reserveDataPacked := buildGetUserReserveDataResult(t, big.NewInt(0), big.NewInt(0), usdcDebt, false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, _ := newPositionTestService(t, srv.URL, mc)

	snapshots, err := svc.extractUserPositionSnapshots(
		context.Background(),
		user,
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, "0xtest",
	)
	if err != nil {
		t.Fatalf("extractUserPositionSnapshots: %v", err)
	}

	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot for debt-only asset, got %d", len(snapshots))
	}
	snap := snapshots[0]
	if snap.DebtBalance.Cmp(usdcDebt) != 0 {
		t.Errorf("DebtBalance = %v, want %v", snap.DebtBalance, usdcDebt)
	}
	if snap.SuppliedBalance.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("SuppliedBalance = %v, want 0", snap.SuppliedBalance)
	}
}

// TestExtractUserPositionSnapshots_EmptyWhenNoPositions verifies that an empty
// slice is returned when the user has no reserves.
func TestExtractUserPositionSnapshots_EmptyWhenNoPositions(t *testing.T) {
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	// Empty getUserReservesData response
	emptyHex := buildUserReservesDataResponse(t, []UserReserveData{})

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, fmt.Errorf("Execute should not be called for empty reserves")
	}

	srv := startMockEthServer(t, emptyHex)
	svc, _ := newPositionTestService(t, srv.URL, mc)

	snapshots, err := svc.extractUserPositionSnapshots(
		context.Background(),
		user,
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, "0xtest",
	)
	if err != nil {
		t.Fatalf("extractUserPositionSnapshots: %v", err)
	}
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots, got %d", len(snapshots))
	}
}

// TestExtractUserPositionSnapshots_ProviderUnavailable verifies graceful
// handling when batchGetUserReserveData fails (e.g. PoolDataProvider contract
// doesn't exist at the historical block). The error should be surfaced so
// callers can decide whether to continue or abort.
func TestExtractUserPositionSnapshots_ProviderUnavailable(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	reserves := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(1e9)},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 3 {
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		}
		// Simulate multicall failure for getUserReserveData
		return nil, fmt.Errorf("provider unavailable at this block")
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, _ := newPositionTestService(t, srv.URL, mc)

	_, err := svc.extractUserPositionSnapshots(
		context.Background(),
		user,
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, "0xtest",
	)
	// The error should be propagated so callers can log/skip.
	if err == nil {
		t.Error("expected error when provider is unavailable, got nil")
	}
	if !strings.Contains(err.Error(), "provider unavailable") {
		t.Errorf("error = %q, want to contain 'provider unavailable'", err.Error())
	}
}

// TestSavePositionSnapshot_SnapshotExtractionFailureReturnsError verifies that
// savePositionSnapshot propagates extractUserPositionSnapshots errors rather
// than silently continuing with empty snapshots.
func TestSavePositionSnapshot_SnapshotExtractionFailureReturnsError(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	// The mock eth server returns a valid getUserReservesData response with a
	// single active asset so extractUserPositionSnapshots proceeds past
	// getUserReservesData and into batchGetUserReserveData.
	reserves := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(1e18)},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		// 3 calls = batchGetTokenMetadata (succeeds for both the initial event-reserve
		// lookup in savePositionSnapshot and the per-asset lookup inside
		// extractUserPositionSnapshots).
		// 1 call  = batchGetUserReserveData (fails to simulate provider unavailable).
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return nil, fmt.Errorf("provider contract unavailable")
		default:
			return nil, fmt.Errorf("unexpected call with %d calls", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventBorrow,
		TxHash:    "0xfail",
		User:      user,
		Reserve:   weth,
		Amount:    big.NewInt(1e18),
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)

	// Must return an error — must not silently drop the borrower row.
	if err == nil {
		t.Fatal("expected error when snapshot extraction fails, got nil")
	}
	if !strings.Contains(err.Error(), "provider contract unavailable") {
		t.Errorf("error = %q, want to contain provider contract unavailable", err.Error())
	}

	// No partial data must have been written.
	if len(posRepo.SavedBorrowers) != 0 {
		t.Errorf("expected 0 borrower rows on failure, got %d", len(posRepo.SavedBorrowers))
	}
	if len(posRepo.SavedCollaterals) != 0 {
		t.Errorf("expected 0 collateral rows on failure, got %d", len(posRepo.SavedCollaterals))
	}
}

// TestSaveCollateralToggleEvent_SnapshotExtractionFailureReturnsError verifies
// that saveCollateralToggleEvent propagates extractUserPositionSnapshots errors
// rather than persisting a fabricated zero balance.
func TestSaveCollateralToggleEvent_SnapshotExtractionFailureReturnsError(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	reserves := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: big.NewInt(1e18)},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		// 3 calls = batchGetTokenMetadata (succeeds).
		// 1 call  = batchGetUserReserveData (fails to simulate provider unavailable).
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return nil, fmt.Errorf("provider contract unavailable")
		default:
			return nil, fmt.Errorf("unexpected call with %d calls", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	err := svc.saveCollateralToggleEvent(context.Background(), &PositionEventData{
		EventType:         EventReserveUsedAsCollateralDisabled,
		TxHash:            "0xfail2",
		User:              user,
		Reserve:           weth,
		CollateralEnabled: false,
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)

	// Must return an error — must not persist a fabricated zero balance.
	if err == nil {
		t.Fatal("expected error when snapshot extraction fails, got nil")
	}
	if !strings.Contains(err.Error(), "provider contract unavailable") {
		t.Errorf("error = %q, want to contain provider contract unavailable", err.Error())
	}

	if len(posRepo.SavedCollaterals) != 0 {
		t.Errorf("expected 0 collateral rows on failure, got %d", len(posRepo.SavedCollaterals))
	}
}

// TestSnapshotUserPosition_SaveBorrowerFailureRollsBack verifies that a
// SaveBorrower failure causes snapshotUserPosition to return an error, so the
// enclosing transaction rolls back and collateral rows are not committed.
func TestSnapshotUserPosition_SaveBorrowerFailureRollsBack(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	wethSupply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	usdcDebt := big.NewInt(500000000) // 500 USDC

	reserves := []UserReserveData{
		{UnderlyingAsset: weth, ScaledATokenBalance: wethSupply, UsageAsCollateralEnabledOnUser: true},
		{UnderlyingAsset: usdc, ScaledVariableDebt: usdcDebt},
	}

	wethDecPacked, wethSymPacked, wethNamePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	usdcDecPacked, usdcSymPacked, usdcNamePacked := buildERC20MetadataResult(t, 6, "USDC", "USD Coin")
	wethReserveData := buildGetUserReserveDataResult(t, wethSupply, big.NewInt(0), big.NewInt(0), true)
	usdcReserveData := buildGetUserReserveDataResult(t, big.NewInt(0), big.NewInt(0), usdcDebt, false)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 6:
			return []outbound.Result{
				{Success: true, ReturnData: wethDecPacked},
				{Success: true, ReturnData: wethSymPacked},
				{Success: true, ReturnData: wethNamePacked},
				{Success: true, ReturnData: usdcDecPacked},
				{Success: true, ReturnData: usdcSymPacked},
				{Success: true, ReturnData: usdcNamePacked},
			}, nil
		case 2:
			return []outbound.Result{
				{Success: true, ReturnData: wethReserveData},
				{Success: true, ReturnData: usdcReserveData},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call with %d calls", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	// Inject a SaveBorrower failure.
	posRepo.SaveBorrowerFn = func(_ context.Context, _ pgx.Tx, _, _, _ int64, _ int64, _ int, _, _, _ string, _ []byte) error {
		return fmt.Errorf("db write failure")
	}

	err := svc.snapshotUserPosition(
		context.Background(),
		nil,
		user,
		"LiquidationCall",
		common.FromHex("0xdeadbeef"),
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, 0,
	)

	// snapshotUserPosition must surface the SaveBorrower error so the caller's
	// transaction can roll back, leaving no partial snapshot.
	if err == nil {
		t.Fatal("expected error from SaveBorrower failure, got nil")
	}
	if !strings.Contains(err.Error(), "db write failure") {
		t.Errorf("error = %q, want to contain db write failure", err.Error())
	}

	// No collateral rows must have been committed (transaction would be rolled back).
	if len(posRepo.SavedCollaterals) != 0 {
		t.Errorf("expected 0 collateral rows when borrower write fails, got %d", len(posRepo.SavedCollaterals))
	}
}

// TestExtractUserPositionSnapshots_StableDebtOnlyAsset verifies that an asset
// with only stable debt (and no variable debt or supply) is discovered and
// included in the snapshot, with DebtBalance equal to the stable debt amount.
// This test uses the 7-field getUserReservesData layout (legacy AaveV3) which
// carries PrincipalStableDebt in the discovery response.
func TestExtractUserPositionSnapshots_StableDebtOnlyAsset(t *testing.T) {
	dai := common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	stableDebt := big.NewInt(1000000000000000000) // 1 DAI

	// getUserReservesData returns the asset with PrincipalStableDebt > 0 only
	// (7-field layout so the decoder picks up the stable debt field).
	reserves := []UserReserveData{
		{
			UnderlyingAsset:     dai,
			ScaledATokenBalance: big.NewInt(0),
			ScaledVariableDebt:  big.NewInt(0),
			PrincipalStableDebt: stableDebt,
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "DAI", "Dai Stablecoin")
	// Provider reports: no supply, no variable debt, currentStableDebt = 1 DAI.
	reserveDataPacked := buildGetUserReserveDataResult(t,
		big.NewInt(0), // currentATokenBalance
		stableDebt,    // currentStableDebt
		big.NewInt(0), // currentVariableDebt
		false,
	)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse7Field(t, reserves))
	svc, _ := newPositionTestService(t, srv.URL, mc)

	snapshots, err := svc.extractUserPositionSnapshots(
		context.Background(),
		user,
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, "0xtest",
	)
	if err != nil {
		t.Fatalf("extractUserPositionSnapshots: %v", err)
	}

	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot for stable-debt-only asset, got %d", len(snapshots))
	}
	snap := snapshots[0]
	if snap.DebtBalance.Cmp(stableDebt) != 0 {
		t.Errorf("DebtBalance = %v, want %v (stableDebt)", snap.DebtBalance, stableDebt)
	}
	if snap.SuppliedBalance.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("SuppliedBalance = %v, want 0", snap.SuppliedBalance)
	}
}

// TestExtractUserPositionSnapshots_UserReservesDataFails verifies that
// extractUserPositionSnapshots propagates a getUserReservesData failure as an
// error rather than silently returning an empty snapshot (which would cause
// callers to write no rows without knowing why).
func TestExtractUserPositionSnapshots_UserReservesDataFails(t *testing.T) {
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	// Start a server that returns an RPC execution error for every eth_call,
	// simulating a reverted getUserReservesData call.
	revertingSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"error": map[string]any{
				"code":    -32000,
				"message": "execution reverted: contract not deployed at block",
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("mock eth server encode: %v", err)
		}
	}))
	t.Cleanup(revertingSrv.Close)

	mc := testutil.NewMockMulticaller()
	svc, _ := newPositionTestService(t, revertingSrv.URL, mc)

	_, err := svc.extractUserPositionSnapshots(
		context.Background(),
		user,
		common.HexToAddress(aaveV3EthPool),
		testChainID, testBlockNumber, "0xtest",
	)
	// Must surface the error — must not silently return an empty snapshot.
	if err == nil {
		t.Fatal("expected error when getUserReservesData fails, got nil")
	}
}

// TestSavePositionSnapshot_CollateralTokenFailureReturnsError verifies that a
// GetOrCreateToken failure while building collateral rows in savePositionSnapshot
// returns an error rather than silently dropping the row.
func TestSavePositionSnapshot_CollateralTokenFailureReturnsError(t *testing.T) {
	weth := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	supply := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1 ETH

	reserves := []UserReserveData{
		{
			UnderlyingAsset:                weth,
			ScaledATokenBalance:            supply,
			UsageAsCollateralEnabledOnUser: true,
		},
	}

	decPacked, symPacked, namePacked := buildERC20MetadataResult(t, 18, "WETH", "Wrapped Ether")
	reserveDataPacked := buildGetUserReserveDataResult(t, supply, big.NewInt(0), big.NewInt(0), true)

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 3:
			return []outbound.Result{
				{Success: true, ReturnData: decPacked},
				{Success: true, ReturnData: symPacked},
				{Success: true, ReturnData: namePacked},
			}, nil
		case 1:
			return []outbound.Result{
				{Success: true, ReturnData: reserveDataPacked},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected call len=%d", len(calls))
		}
	}

	srv := startMockEthServer(t, buildUserReservesDataResponse(t, reserves))
	svc, posRepo := newPositionTestService(t, srv.URL, mc)

	// Inject a GetOrCreateToken failure for the collateral token lookup.
	svc.tokenRepo = &testutil.MockTokenRepository{
		GetOrCreateTokenFn: func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
			return 0, fmt.Errorf("token registry unavailable")
		},
	}

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventSupply,
		TxHash:    "0xtok",
		User:      user,
		Reserve:   weth,
		Amount:    supply,
	}, common.HexToAddress(aaveV3EthPool), testChainID, testBlockNumber, 0)

	// Must return an error — must not silently drop the collateral row.
	if err == nil {
		t.Fatal("expected error when GetOrCreateToken fails, got nil")
	}
	if !strings.Contains(err.Error(), "token registry unavailable") {
		t.Errorf("error = %q, want to contain 'token registry unavailable'", err.Error())
	}

	// No partial data must have been written.
	if len(posRepo.SavedCollaterals) != 0 {
		t.Errorf("expected 0 collateral rows on failure, got %d", len(posRepo.SavedCollaterals))
	}
}
