package shared

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Spec coverage summary:
// - Current tracker semantics #1: snapshot path uses GetOrCreateUser, not EnsureUser
//   -> TestPositionSnapshotter_SnapshotUser_PreservesCollateralOnlyTrackerSemantics
// - Current tracker semantics #2: snapshot path persists collateral snapshots via SaveBorrowerCollaterals
//   -> TestPositionSnapshotter_SnapshotUser_PreservesCollateralOnlyTrackerSemantics
// - Current tracker semantics #3: snapshot path does NOT persist debt rows in snapshotUserPosition
//   -> TestPositionSnapshotter_SnapshotUser_PreservesCollateralOnlyTrackerSemantics
// - Current tracker semantics #4: extract failure during snapshotting warns-and-continues with no error
//   -> TestPositionSnapshotter_SnapshotUser_ExtractFailure_ReturnsNoErrorAndWritesNoRows
// - Desired coverage: no-position user => no rows written, no error
//   -> TestPositionSnapshotter_SnapshotUser_NoPositionUser_WritesNoRowsAndReturnsNoError

// Assumptions:
// - This slice is a pure extraction only. The shared snapshotter must preserve the
//   existing tracker's observable snapshot behavior exactly, even where the
//   broader transfer-user-discovery plan later describes different behavior.
// - "No rows written" is asserted via persisted borrower/collateral rows, without
//   over-constraining whether internal no-op repository methods are invoked.

func TestPositionSnapshotter_SnapshotUser_PreservesCollateralOnlyTrackerSemantics(t *testing.T) {
	// Spec: preserve current snapshot semantics during extraction.
	const (
		chainID      = int64(1)
		blockNumber  = int64(24033627)
		blockVersion = 2
	)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdcAddress := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	txHash := []byte{0xde, 0xad, 0xbe, 0xef}

	oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	debtUSDC := new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
	zero := big.NewInt(0)

	ethClient, cleanup := newEthClientReturningUserReservesForSnapshotterTests(t, []sparkLendUserReserve{{
		UnderlyingAsset:                wethAddress,
		ScaledATokenBalance:            oneETH,
		UsageAsCollateralEnabledOnUser: true,
		ScaledVariableDebt:             zero,
	}, {
		UnderlyingAsset:                usdcAddress,
		ScaledATokenBalance:            zero,
		UsageAsCollateralEnabledOnUser: false,
		ScaledVariableDebt:             debtUSDC,
	}})
	defer cleanup()

	erc20ABI := mustERC20ABIForSnapshotterTests(t)
	userReserveDataABI := mustUserReserveDataABIForSnapshotterTests(t)

	multicaller := testutil.NewMockMulticaller()
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			methodID := hex.EncodeToString(call.CallData[:4])
			switch {
			case call.Target == wethAddress || call.Target == usdcAddress:
				results[i] = outbound.Result{Success: true, ReturnData: packERC20MetadataResponseForSnapshotterTests(t, erc20ABI, call.Target, methodID)}
			case methodID == methodIDHexForSnapshotterTests(userReserveDataABI, "getUserReserveData"):
				asset := unpackUserReserveAssetForSnapshotterTests(t, userReserveDataABI, call.CallData)
				results[i] = outbound.Result{Success: true, ReturnData: packUserReserveDataResponseForSnapshotterTests(t, userReserveDataABI, asset)}
			default:
				t.Fatalf("unexpected multicall target %s method %s", call.Target.Hex(), methodID)
			}
		}
		return results, nil
	}

	userRepo := &snapshotterTestUserRepository{}
	protocolRepo := &snapshotterTestProtocolRepository{protocolID: 22}
	tokenRepo := &snapshotterTestTokenRepository{tokenIDs: map[common.Address]int64{
		wethAddress: 101,
		usdcAddress: 202,
	}}
	positionRepo := &snapshotterTestPositionRepository{}

	snapshotter := newPositionSnapshotterForTest(
		t,
		ethClient,
		multicaller,
		userRepo,
		protocolRepo,
		tokenRepo,
		positionRepo,
	)

	err := snapshotter.SnapshotUser(
		context.Background(),
		chainID,
		protocolAddress,
		userAddress,
		blockNumber,
		blockVersion,
		"Transfer",
		txHash,
	)
	if err != nil {
		t.Fatalf("SnapshotUser() failed: %v", err)
	}

	if userRepo.getOrCreateCalls != 1 {
		t.Fatalf("GetOrCreateUser call count = %d, want 1", userRepo.getOrCreateCalls)
	}
	if userRepo.ensureCalls != 0 {
		t.Fatalf("EnsureUser call count = %d, want 0", userRepo.ensureCalls)
	}

	if len(positionRepo.savedCollaterals) != 1 {
		t.Fatalf("saved collateral row count = %d, want 1", len(positionRepo.savedCollaterals))
	}
	collateral := positionRepo.savedCollaterals[0]
	if collateral.UserID != 11 {
		t.Errorf("collateral userID = %d, want 11", collateral.UserID)
	}
	if collateral.ProtocolID != 22 {
		t.Errorf("collateral protocolID = %d, want 22", collateral.ProtocolID)
	}
	if collateral.TokenID != 101 {
		t.Errorf("collateral tokenID = %d, want 101", collateral.TokenID)
	}
	if collateral.Amount != "1" {
		t.Errorf("collateral amount = %q, want %q", collateral.Amount, "1")
	}
	if collateral.EventType != "Transfer" {
		t.Errorf("collateral eventType = %q, want %q", collateral.EventType, "Transfer")
	}
	if !collateral.CollateralEnabled {
		t.Error("collateral row should preserve collateralEnabled=true")
	}

	if positionRepo.saveBorrowerCollateralsCalls != 1 {
		t.Fatalf("SaveBorrowerCollaterals call count = %d, want 1", positionRepo.saveBorrowerCollateralsCalls)
	}
	if positionRepo.saveBorrowerCalls != 0 {
		t.Fatalf("SaveBorrower call count = %d, want 0", positionRepo.saveBorrowerCalls)
	}
}

func TestPositionSnapshotter_SnapshotUser_NoPositionUser_WritesNoRowsAndReturnsNoError(t *testing.T) {
	// Spec: no-position user should produce no persisted rows and no error.
	const (
		chainID      = int64(1)
		blockNumber  = int64(24033627)
		blockVersion = 0
	)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	ethClient, cleanup := newEthClientReturningUserReservesForSnapshotterTests(t, nil)
	defer cleanup()

	positionRepo := &snapshotterTestPositionRepository{}
	multicaller := testutil.NewMockMulticaller()
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		t.Fatalf("unexpected multicall execution for zero-position snapshot")
		return nil, nil
	}

	snapshotter := newPositionSnapshotterForTest(
		t,
		ethClient,
		multicaller,
		&snapshotterTestUserRepository{},
		&snapshotterTestProtocolRepository{protocolID: 22},
		&snapshotterTestTokenRepository{tokenIDs: map[common.Address]int64{}},
		positionRepo,
	)

	err := snapshotter.SnapshotUser(
		context.Background(),
		chainID,
		protocolAddress,
		userAddress,
		blockNumber,
		blockVersion,
		"Transfer",
		[]byte{0xaa},
	)
	if err != nil {
		t.Fatalf("SnapshotUser() returned unexpected error: %v", err)
	}

	if len(positionRepo.savedCollaterals) != 0 {
		t.Fatalf("saved collateral row count = %d, want 0", len(positionRepo.savedCollaterals))
	}
	if positionRepo.saveBorrowerCalls != 0 {
		t.Fatalf("SaveBorrower call count = %d, want 0", positionRepo.saveBorrowerCalls)
	}
}

func TestPositionSnapshotter_SnapshotUser_ExtractFailure_ReturnsNoErrorAndWritesNoRows(t *testing.T) {
	// Spec: preserve current tracker warn-and-continue semantics on extract failure.
	const (
		chainID      = int64(1)
		blockNumber  = int64(24033627)
		blockVersion = 0
	)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	ethClient, cleanup := newEthClientReturningRPCErrorForSnapshotterTests(t, "eth_call failed")
	defer cleanup()

	positionRepo := &snapshotterTestPositionRepository{}
	snapshotter := newPositionSnapshotterForTest(
		t,
		ethClient,
		testutil.NewMockMulticaller(),
		&snapshotterTestUserRepository{},
		&snapshotterTestProtocolRepository{protocolID: 22},
		&snapshotterTestTokenRepository{tokenIDs: map[common.Address]int64{}},
		positionRepo,
	)

	err := snapshotter.SnapshotUser(
		context.Background(),
		chainID,
		protocolAddress,
		userAddress,
		blockNumber,
		blockVersion,
		"Transfer",
		[]byte{0xbb},
	)
	if err != nil {
		t.Fatalf("SnapshotUser() returned unexpected error: %v", err)
	}

	if len(positionRepo.savedCollaterals) != 0 {
		t.Fatalf("saved collateral row count = %d, want 0", len(positionRepo.savedCollaterals))
	}
	if positionRepo.saveBorrowerCalls != 0 {
		t.Fatalf("SaveBorrower call count = %d, want 0", positionRepo.saveBorrowerCalls)
	}
}

type snapshotterTestUserRepository struct {
	getOrCreateCalls int
	ensureCalls      int
	userID           int64
	getOrCreateErr   error
	ensureErr        error
	users            []entity.User
}

func (m *snapshotterTestUserRepository) GetOrCreateUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error) {
	m.getOrCreateCalls++
	m.users = append(m.users, user)
	if m.getOrCreateErr != nil {
		return 0, m.getOrCreateErr
	}
	if m.userID == 0 {
		m.userID = 11
	}
	return m.userID, nil
}

func (m *snapshotterTestUserRepository) EnsureUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, bool, error) {
	m.ensureCalls++
	if m.ensureErr != nil {
		return 0, false, m.ensureErr
	}
	return 0, false, errors.New("unexpected EnsureUser call in extracted tracker snapshot path")
}

func (m *snapshotterTestUserRepository) UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error {
	return nil
}

type snapshotterTestProtocolRepository struct {
	protocolID int64
	callCount  int
}

func (m *snapshotterTestProtocolRepository) GetOrCreateProtocol(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, name string, protocolType string, createdAtBlock int64) (int64, error) {
	m.callCount++
	if m.protocolID == 0 {
		m.protocolID = 22
	}
	return m.protocolID, nil
}

func (m *snapshotterTestProtocolRepository) UpsertReserveData(ctx context.Context, tx pgx.Tx, data []*entity.SparkLendReserveData) error {
	return nil
}

type snapshotterTestTokenRepository struct {
	tokenIDs map[common.Address]int64
	lookups  []common.Address
}

func (m *snapshotterTestTokenRepository) GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error) {
	m.lookups = append(m.lookups, address)
	if tokenID, ok := m.tokenIDs[address]; ok {
		return tokenID, nil
	}
	return 0, fmt.Errorf("unexpected token lookup for %s", address.Hex())
}

type snapshotterTestPositionRepository struct {
	saveBorrowerCalls            int
	saveBorrowerCollateralsCalls int
	savedCollaterals             []outbound.CollateralRecord
	savedBorrowers               []savedBorrowerRow
}

type savedBorrowerRow struct {
	UserID     int64
	ProtocolID int64
	TokenID    int64
	Amount     string
	Change     string
	EventType  string
	TxHash     []byte
}

func (m *snapshotterTestPositionRepository) SaveBorrower(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte) error {
	m.saveBorrowerCalls++
	m.savedBorrowers = append(m.savedBorrowers, savedBorrowerRow{
		UserID:     userID,
		ProtocolID: protocolID,
		TokenID:    tokenID,
		Amount:     amount,
		Change:     change,
		EventType:  eventType,
		TxHash:     append([]byte(nil), txHash...),
	})
	return nil
}

func (m *snapshotterTestPositionRepository) SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte, collateralEnabled bool) error {
	return errors.New("unexpected SaveBorrowerCollateral call; snapshot path should batch collateral rows")
}

func (m *snapshotterTestPositionRepository) SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, records []outbound.CollateralRecord) error {
	m.saveBorrowerCollateralsCalls++
	m.savedCollaterals = append(m.savedCollaterals, records...)
	return nil
}

type sparkLendUserReserve struct {
	UnderlyingAsset                common.Address
	ScaledATokenBalance            *big.Int
	UsageAsCollateralEnabledOnUser bool
	ScaledVariableDebt             *big.Int
}

func newPositionSnapshotterForTest(
	t *testing.T,
	ethClient *ethclient.Client,
	multicaller outbound.Multicaller,
	userRepo outbound.UserRepository,
	protocolRepo outbound.ProtocolRepository,
	tokenRepo outbound.TokenRepository,
	positionRepo outbound.PositionRepository,
) *PositionSnapshotter {
	t.Helper()

	return &PositionSnapshotter{
		ethClient:          ethClient,
		txManager:          &testutil.MockTxManager{},
		userRepo:           userRepo,
		protocolRepo:       protocolRepo,
		tokenRepo:          tokenRepo,
		positionRepo:       positionRepo,
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		blockchainServices: make(map[blockchain.ProtocolKey]*blockchainService),
		multicallClient:    multicaller,
		erc20ABI:           mustERC20ABIForSnapshotterTests(t),
	}
}

func newEthClientReturningUserReservesForSnapshotterTests(t *testing.T, reserves []sparkLendUserReserve) (*ethclient.Client, func()) {
	t.Helper()

	response := packSparkLendUserReservesForSnapshotterTests(t, reserves)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req rpcutil.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}

		switch req.Method {
		case "eth_call":
			resultHex := "0x" + hex.EncodeToString(response)
			resultJSON, err := json.Marshal(resultHex)
			if err != nil {
				t.Fatalf("marshal eth_call result: %v", err)
			}
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
		default:
			testutil.WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
		}
	}))

	client, err := ethclient.Dial(server.URL)
	if err != nil {
		server.Close()
		t.Fatalf("ethclient.Dial() failed: %v", err)
	}

	return client, func() {
		client.Close()
		server.Close()
	}
}

func newEthClientReturningRPCErrorForSnapshotterTests(t *testing.T, message string) (*ethclient.Client, func()) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req rpcutil.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}

		testutil.WriteRPCError(w, req.ID, -32000, message)
	}))

	client, err := ethclient.Dial(server.URL)
	if err != nil {
		server.Close()
		t.Fatalf("ethclient.Dial() failed: %v", err)
	}

	return client, func() {
		client.Close()
		server.Close()
	}
}

func packSparkLendUserReservesForSnapshotterTests(t *testing.T, reserves []sparkLendUserReserve) []byte {
	t.Helper()

	sparklendABI, err := abis.GetSparklendUserReservesDataABI()
	if err != nil {
		t.Fatalf("load SparkLend ABI: %v", err)
	}

	data, err := sparklendABI.Methods["getUserReservesData"].Outputs.Pack(reserves, uint8(0))
	if err != nil {
		t.Fatalf("pack getUserReservesData: %v", err)
	}

	return data
}

func packERC20MetadataResponseForSnapshotterTests(t *testing.T, erc20ABI *abi.ABI, token common.Address, methodID string) []byte {
	t.Helper()

	switch token {
	case common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"):
		switch methodID {
		case methodIDHexForSnapshotterTests(erc20ABI, "decimals"):
			return mustPackOutputForSnapshotterTests(t, erc20ABI, "decimals", uint8(18))
		case methodIDHexForSnapshotterTests(erc20ABI, "symbol"):
			return mustPackOutputForSnapshotterTests(t, erc20ABI, "symbol", "WETH")
		case methodIDHexForSnapshotterTests(erc20ABI, "name"):
			return mustPackOutputForSnapshotterTests(t, erc20ABI, "name", "Wrapped Ether")
		}
	case common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"):
		switch methodID {
		case methodIDHexForSnapshotterTests(erc20ABI, "decimals"):
			return mustPackOutputForSnapshotterTests(t, erc20ABI, "decimals", uint8(6))
		case methodIDHexForSnapshotterTests(erc20ABI, "symbol"):
			return mustPackOutputForSnapshotterTests(t, erc20ABI, "symbol", "USDC")
		case methodIDHexForSnapshotterTests(erc20ABI, "name"):
			return mustPackOutputForSnapshotterTests(t, erc20ABI, "name", "USD Coin")
		}
	}

	t.Fatalf("unexpected metadata request for token %s method %s", token.Hex(), methodID)
	return nil
}

func packUserReserveDataResponseForSnapshotterTests(t *testing.T, userReserveDataABI *abi.ABI, asset common.Address) []byte {
	t.Helper()

	zero := big.NewInt(0)
	stableRateLastUpdated := big.NewInt(0)

	switch asset {
	case common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"):
		oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
		return mustPackOutputForSnapshotterTests(t, userReserveDataABI, "getUserReserveData",
			oneETH,
			zero,
			zero,
			zero,
			zero,
			zero,
			zero,
			stableRateLastUpdated,
			true,
		)
	case common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"):
		debtUSDC := new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
		return mustPackOutputForSnapshotterTests(t, userReserveDataABI, "getUserReserveData",
			zero,
			zero,
			debtUSDC,
			zero,
			zero,
			zero,
			zero,
			stableRateLastUpdated,
			false,
		)
	default:
		t.Fatalf("unexpected asset in getUserReserveData response: %s", asset.Hex())
		return nil
	}
}

func unpackUserReserveAssetForSnapshotterTests(t *testing.T, userReserveDataABI *abi.ABI, callData []byte) common.Address {
	t.Helper()

	args, err := userReserveDataABI.Methods["getUserReserveData"].Inputs.Unpack(callData[4:])
	if err != nil {
		t.Fatalf("unpack getUserReserveData call: %v", err)
	}
	asset, ok := args[0].(common.Address)
	if !ok {
		t.Fatalf("unexpected asset type %T", args[0])
	}
	return asset
}

func mustPackOutputForSnapshotterTests(t *testing.T, parsedABI *abi.ABI, method string, values ...any) []byte {
	t.Helper()

	data, err := parsedABI.Methods[method].Outputs.Pack(values...)
	if err != nil {
		t.Fatalf("pack %s output: %v", method, err)
	}
	return data
}

func mustERC20ABIForSnapshotterTests(t *testing.T) *abi.ABI {
	t.Helper()

	parsedABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	return parsedABI
}

func mustUserReserveDataABIForSnapshotterTests(t *testing.T) *abi.ABI {
	t.Helper()

	parsedABI, err := abis.GetPoolDataProviderUserReserveDataABI()
	if err != nil {
		t.Fatalf("load user reserve data ABI: %v", err)
	}
	return parsedABI
}

func methodIDHexForSnapshotterTests(parsedABI *abi.ABI, method string) string {
	return hex.EncodeToString(parsedABI.Methods[method].ID)
}
