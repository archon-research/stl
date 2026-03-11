package sparklend_position_tracker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
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

type mockPositionRepository struct {
	saveBorrowerCalls []saveBorrowerCall

	SaveBorrowerFn            func(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte) error
	SaveBorrowerCollateralFn  func(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, eventType string, txHash []byte, collateralEnabled bool) error
	SaveBorrowerCollateralsFn func(ctx context.Context, tx pgx.Tx, records []outbound.CollateralRecord) error
}

type saveBorrowerCall struct {
	Amount    string
	Change    string
	EventType string
}

func (m *mockPositionRepository) SaveBorrower(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, change, eventType string, txHash []byte) error {
	m.saveBorrowerCalls = append(m.saveBorrowerCalls, saveBorrowerCall{
		Amount:    amount,
		Change:    change,
		EventType: eventType,
	})
	if m.SaveBorrowerFn != nil {
		return m.SaveBorrowerFn(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, amount, change, eventType, txHash)
	}
	return nil
}

func (m *mockPositionRepository) SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, userID, protocolID, tokenID, blockNumber int64, blockVersion int, amount, eventType string, txHash []byte, collateralEnabled bool) error {
	if m.SaveBorrowerCollateralFn != nil {
		return m.SaveBorrowerCollateralFn(ctx, tx, userID, protocolID, tokenID, blockNumber, blockVersion, amount, eventType, txHash, collateralEnabled)
	}
	return nil
}

func (m *mockPositionRepository) SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, records []outbound.CollateralRecord) error {
	if m.SaveBorrowerCollateralsFn != nil {
		return m.SaveBorrowerCollateralsFn(ctx, tx, records)
	}
	return nil
}

func (m *mockPositionRepository) UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error {
	return nil
}

func (m *mockPositionRepository) UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error {
	return nil
}

type sparkLendUserReserve struct {
	UnderlyingAsset                common.Address
	ScaledATokenBalance            *big.Int
	UsageAsCollateralEnabledOnUser bool
	ScaledVariableDebt             *big.Int
}

func TestExtractUserPositionData_ReturnsDebtAndCollateral(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdcAddress := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	debtUSDC := new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
	zero := big.NewInt(0)

	ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{
		{
			UnderlyingAsset:                wethAddress,
			ScaledATokenBalance:            oneETH,
			UsageAsCollateralEnabledOnUser: true,
			ScaledVariableDebt:             zero,
		},
		{
			UnderlyingAsset:                usdcAddress,
			ScaledATokenBalance:            zero,
			UsageAsCollateralEnabledOnUser: false,
			ScaledVariableDebt:             debtUSDC,
		},
	})
	defer cleanup()

	erc20ABI := mustERC20ABI(t)
	userReserveDataABI := mustUserReserveDataABI(t)

	multicaller := testutil.NewMockMulticaller()
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			methodID := hex.EncodeToString(call.CallData[:4])
			switch {
			case call.Target == wethAddress || call.Target == usdcAddress:
				returnData := packERC20MetadataResponse(t, erc20ABI, call.Target, methodID)
				results[i] = outbound.Result{Success: true, ReturnData: returnData}
			case methodID == methodIDHex(userReserveDataABI, "getUserReserveData"):
				asset := unpackUserReserveAsset(t, userReserveDataABI, call.CallData)
				returnData := packUserReserveDataResponse(t, userReserveDataABI, asset)
				results[i] = outbound.Result{Success: true, ReturnData: returnData}
			default:
				t.Fatalf("unexpected multicall target %s method %s", call.Target.Hex(), methodID)
			}
		}
		return results, nil
	}

	svc := newServiceWithCachedBlockchainService(t, ethClient, multicaller, chainID, protocolAddress)

	collaterals, debts, err := svc.extractUserPositionData(context.Background(), userAddress, protocolAddress, chainID, blockNumber, "0xabc")
	if err != nil {
		t.Fatalf("extractUserPositionData() failed: %v", err)
	}

	if len(collaterals) != 1 {
		t.Fatalf("expected 1 collateral, got %d", len(collaterals))
	}
	if collaterals[0].Asset != wethAddress {
		t.Errorf("collateral asset = %s, want %s", collaterals[0].Asset.Hex(), wethAddress.Hex())
	}
	if collaterals[0].ActualBalance.Cmp(oneETH) != 0 {
		t.Errorf("collateral balance = %s, want %s", collaterals[0].ActualBalance.String(), oneETH.String())
	}
	if !collaterals[0].CollateralEnabled {
		t.Error("expected collateral to be enabled")
	}

	if len(debts) != 1 {
		t.Fatalf("expected 1 debt position, got %d", len(debts))
	}
	if debts[0].Asset != usdcAddress {
		t.Errorf("debt asset = %s, want %s", debts[0].Asset.Hex(), usdcAddress.Hex())
	}
	if debts[0].CurrentDebt.Cmp(debtUSDC) != 0 {
		t.Errorf("debt amount = %s, want %s", debts[0].CurrentDebt.String(), debtUSDC.String())
	}
}

func TestSaveBorrowerRecord_BorrowUsesCurrentDebtNotEventDelta(t *testing.T) {
	const blockNumber = int64(20000000)
	const decimals = 18

	eventDelta := new(big.Int).Mul(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	currentDebt := new(big.Int).Mul(big.NewInt(3), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	reserveAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	userAddr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	positionRepo := &mockPositionRepository{}
	svc := newBorrowerRecordTestService(positionRepo)

	err := svc.txManager.WithTransaction(context.Background(), func(tx pgx.Tx) error {
		return svc.saveBorrowerRecord(context.Background(), tx, &PositionEventData{
			EventType: EventBorrow,
			TxHash:    "0xabc",
			User:      userAddr,
			Reserve:   reserveAddr,
			Amount:    eventDelta,
		}, TokenMetadata{Symbol: "WETH", Decimals: decimals, Name: "Wrapped Ether"}, []DebtData{{
			Asset:       reserveAddr,
			Decimals:    decimals,
			Symbol:      "WETH",
			Name:        "Wrapped Ether",
			CurrentDebt: currentDebt,
		}}, 1, 1, 1, blockNumber, 0)
	})
	if err != nil {
		t.Fatalf("saveBorrowerRecord() failed: %v", err)
	}

	if len(positionRepo.saveBorrowerCalls) != 1 {
		t.Fatalf("expected 1 SaveBorrower call, got %d", len(positionRepo.saveBorrowerCalls))
	}

	call := positionRepo.saveBorrowerCalls[0]
	if call.Amount != "3" {
		t.Errorf("SaveBorrower amount = %q, want %q", call.Amount, "3")
	}
	if call.Change != "1" {
		t.Errorf("SaveBorrower change = %q, want %q", call.Change, "1")
	}
}

func TestSaveBorrowerRecord_RepayUsesCurrentDebtNotEventDelta(t *testing.T) {
	const blockNumber = int64(20000001)
	const decimals = 6

	repayDelta := new(big.Int).Mul(big.NewInt(500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
	currentDebt := new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))

	reserveAddr := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	userAddr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	positionRepo := &mockPositionRepository{}
	svc := newBorrowerRecordTestService(positionRepo)

	err := svc.txManager.WithTransaction(context.Background(), func(tx pgx.Tx) error {
		return svc.saveBorrowerRecord(context.Background(), tx, &PositionEventData{
			EventType: EventRepay,
			TxHash:    "0xdef",
			User:      userAddr,
			Reserve:   reserveAddr,
			Amount:    repayDelta,
		}, TokenMetadata{Symbol: "USDC", Decimals: decimals, Name: "USD Coin"}, []DebtData{{
			Asset:       reserveAddr,
			Decimals:    decimals,
			Symbol:      "USDC",
			Name:        "USD Coin",
			CurrentDebt: currentDebt,
		}}, 1, 1, 1, blockNumber, 0)
	})
	if err != nil {
		t.Fatalf("saveBorrowerRecord() failed: %v", err)
	}

	if len(positionRepo.saveBorrowerCalls) != 1 {
		t.Fatalf("expected 1 SaveBorrower call, got %d", len(positionRepo.saveBorrowerCalls))
	}

	call := positionRepo.saveBorrowerCalls[0]
	if call.Amount != "1500" {
		t.Errorf("SaveBorrower amount = %q, want %q", call.Amount, "1500")
	}
	if call.Change != "500" {
		t.Errorf("SaveBorrower change = %q, want %q", call.Change, "500")
	}
}

func TestSaveBorrowerRecord_RepayToZeroUsesZeroAmountWhenDebtMissing(t *testing.T) {
	const decimals = 18

	repayDelta := new(big.Int).Mul(big.NewInt(2), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	reserveAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	userAddr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	positionRepo := &mockPositionRepository{}
	svc := newBorrowerRecordTestService(positionRepo)

	err := svc.txManager.WithTransaction(context.Background(), func(tx pgx.Tx) error {
		return svc.saveBorrowerRecord(context.Background(), tx, &PositionEventData{
			EventType: EventRepay,
			TxHash:    "0xfallback",
			User:      userAddr,
			Reserve:   reserveAddr,
			Amount:    repayDelta,
		}, TokenMetadata{Symbol: "WETH", Decimals: decimals, Name: "Wrapped Ether"}, nil, 1, 1, 1, 20000000, 0)
	})
	if err != nil {
		t.Fatalf("saveBorrowerRecord() failed: %v", err)
	}

	if len(positionRepo.saveBorrowerCalls) != 1 {
		t.Fatalf("expected 1 SaveBorrower call, got %d", len(positionRepo.saveBorrowerCalls))
	}

	call := positionRepo.saveBorrowerCalls[0]
	if call.Amount != "0" {
		t.Errorf("fallback amount = %q, want %q", call.Amount, "0")
	}
	if call.Change != "2" {
		t.Errorf("fallback change = %q, want %q", call.Change, "2")
	}
}

func TestSaveBorrowerRecord_BorrowReturnsErrorWhenDebtMissing(t *testing.T) {
	const decimals = 18

	borrowDelta := new(big.Int).Mul(big.NewInt(2), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	reserveAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	userAddr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	positionRepo := &mockPositionRepository{}
	svc := newBorrowerRecordTestService(positionRepo)

	err := svc.txManager.WithTransaction(context.Background(), func(tx pgx.Tx) error {
		return svc.saveBorrowerRecord(context.Background(), tx, &PositionEventData{
			EventType: EventBorrow,
			TxHash:    "0xmissing",
			User:      userAddr,
			Reserve:   reserveAddr,
			Amount:    borrowDelta,
		}, TokenMetadata{Symbol: "WETH", Decimals: decimals, Name: "Wrapped Ether"}, nil, 1, 1, 1, 20000000, 0)
	})
	if err == nil {
		t.Fatal("expected error when borrow debt data is missing, got nil")
	}
	if len(positionRepo.saveBorrowerCalls) != 0 {
		t.Fatalf("expected 0 SaveBorrower calls, got %d", len(positionRepo.saveBorrowerCalls))
	}
}

func TestSavePositionSnapshot_BorrowAndRepayFailWhenPositionExtractionFails(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	zero := big.NewInt(0)

	for _, eventType := range []entity.EventType{EventBorrow, EventRepay} {
		t.Run(string(eventType), func(t *testing.T) {
			ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{{
				UnderlyingAsset:                wethAddress,
				ScaledATokenBalance:            zero,
				UsageAsCollateralEnabledOnUser: false,
				ScaledVariableDebt:             oneETH,
			}})
			defer cleanup()

			erc20ABI := mustERC20ABI(t)
			multicaller := testutil.NewMockMulticaller()
			executeCount := 0
			multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
				executeCount++
				switch executeCount {
				case 1:
					results := make([]outbound.Result, len(calls))
					for i, call := range calls {
						methodID := hex.EncodeToString(call.CallData[:4])
						results[i] = outbound.Result{Success: true, ReturnData: packERC20MetadataResponse(t, erc20ABI, call.Target, methodID)}
					}
					return results, nil
				case 2:
					return nil, errors.New("boom")
				default:
					t.Fatalf("unexpected multicall execution count: %d", executeCount)
					return nil, nil
				}
			}

			positionRepo := &mockPositionRepository{}
			svc := newPositionSnapshotTestService(t, ethClient, multicaller, positionRepo, chainID, protocolAddress)

			err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
				EventType: eventType,
				TxHash:    "0xdeadbeef",
				User:      userAddress,
				Reserve:   wethAddress,
				Amount:    oneETH,
			}, protocolAddress, chainID, blockNumber, 0)
			if err == nil {
				t.Fatal("expected savePositionSnapshot to fail, got nil")
			}
			if len(positionRepo.saveBorrowerCalls) != 0 {
				t.Fatalf("expected 0 SaveBorrower calls, got %d", len(positionRepo.saveBorrowerCalls))
			}
		})
	}
}

func newBorrowerRecordTestService(positionRepo *mockPositionRepository) *Service {
	return &Service{
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		positionRepo: positionRepo,
		userRepo:     &testutil.MockUserRepository{},
		protocolRepo: &testutil.MockProtocolRepository{},
		tokenRepo:    &testutil.MockTokenRepository{},
		txManager:    &testutil.MockTxManager{},
	}
}

func newPositionSnapshotTestService(t *testing.T, ethClient *ethclient.Client, multicaller outbound.Multicaller, positionRepo *mockPositionRepository, chainID int64, protocolAddress common.Address) *Service {
	t.Helper()

	svc := newServiceWithCachedBlockchainService(t, ethClient, multicaller, chainID, protocolAddress)
	svc.positionRepo = positionRepo
	svc.userRepo = &testutil.MockUserRepository{}
	svc.protocolRepo = &testutil.MockProtocolRepository{}
	svc.tokenRepo = &testutil.MockTokenRepository{}
	svc.txManager = &testutil.MockTxManager{}

	return svc
}

func newServiceWithCachedBlockchainService(t *testing.T, ethClient *ethclient.Client, multicaller outbound.Multicaller, chainID int64, protocolAddress common.Address) *Service {
	t.Helper()

	erc20ABI := mustERC20ABI(t)
	svc := &Service{
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		ethClient:          ethClient,
		multicallClient:    multicaller,
		erc20ABI:           erc20ABI,
		blockchainServices: make(map[blockchain.ProtocolKey]*blockchainService),
	}

	config, ok := blockchain.GetProtocolConfig(chainID, protocolAddress)
	if !ok {
		t.Fatalf("protocol config not found for %s", protocolAddress.Hex())
	}

	blockchainSvc, err := newBlockchainService(
		chainID,
		ethClient,
		multicaller,
		erc20ABI,
		config.UIPoolDataProvider.Address,
		config.PoolAddress.Address,
		config.PoolAddressesProvider.Address,
		config.ProtocolVersion,
		svc.logger,
	)
	if err != nil {
		t.Fatalf("newBlockchainService() failed: %v", err)
	}

	svc.blockchainServices[blockchain.ProtocolKey{ChainID: chainID, PoolAddress: protocolAddress}] = blockchainSvc
	return svc
}

func newEthClientReturningUserReserves(t *testing.T, reserves []sparkLendUserReserve) (*ethclient.Client, func()) {
	t.Helper()

	response := packSparkLendUserReserves(t, reserves)
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

	cleanup := func() {
		client.Close()
		server.Close()
	}

	return client, cleanup
}

func packSparkLendUserReserves(t *testing.T, reserves []sparkLendUserReserve) []byte {
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

func packERC20MetadataResponse(t *testing.T, erc20ABI *abi.ABI, token common.Address, methodID string) []byte {
	t.Helper()

	switch token {
	case common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"):
		switch methodID {
		case methodIDHex(erc20ABI, "decimals"):
			return mustPackOutput(t, erc20ABI, "decimals", uint8(18))
		case methodIDHex(erc20ABI, "symbol"):
			return mustPackOutput(t, erc20ABI, "symbol", "WETH")
		case methodIDHex(erc20ABI, "name"):
			return mustPackOutput(t, erc20ABI, "name", "Wrapped Ether")
		}
	case common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"):
		switch methodID {
		case methodIDHex(erc20ABI, "decimals"):
			return mustPackOutput(t, erc20ABI, "decimals", uint8(6))
		case methodIDHex(erc20ABI, "symbol"):
			return mustPackOutput(t, erc20ABI, "symbol", "USDC")
		case methodIDHex(erc20ABI, "name"):
			return mustPackOutput(t, erc20ABI, "name", "USD Coin")
		}
	}

	t.Fatalf("unexpected metadata request for token %s method %s", token.Hex(), methodID)
	return nil
}

func packUserReserveDataResponse(t *testing.T, userReserveDataABI *abi.ABI, asset common.Address) []byte {
	t.Helper()

	zero := big.NewInt(0)
	stableRateLastUpdated := big.NewInt(0)

	switch asset {
	case common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"):
		oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
		return mustPackOutput(t, userReserveDataABI, "getUserReserveData",
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
		return mustPackOutput(t, userReserveDataABI, "getUserReserveData",
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

func unpackUserReserveAsset(t *testing.T, userReserveDataABI *abi.ABI, callData []byte) common.Address {
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

func mustPackOutput(t *testing.T, parsedABI *abi.ABI, method string, values ...any) []byte {
	t.Helper()

	data, err := parsedABI.Methods[method].Outputs.Pack(values...)
	if err != nil {
		t.Fatalf("pack %s output: %v", method, err)
	}
	return data
}

func mustERC20ABI(t *testing.T) *abi.ABI {
	t.Helper()

	parsedABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	return parsedABI
}

func mustUserReserveDataABI(t *testing.T) *abi.ABI {
	t.Helper()

	parsedABI, err := abis.GetPoolDataProviderUserReserveDataABI()
	if err != nil {
		t.Fatalf("load user reserve data ABI: %v", err)
	}
	return parsedABI
}

func methodIDHex(parsedABI *abi.ABI, method string) string {
	return hex.EncodeToString(parsedABI.Methods[method].ID)
}
