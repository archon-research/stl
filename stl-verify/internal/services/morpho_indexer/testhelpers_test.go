package morpho_indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/testutils"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// serviceTestHarness provides a fully wired Service with mock dependencies.
type serviceTestHarness struct {
	t            testing.TB
	svc          *Service
	multicaller  *testutil.MockMulticaller
	txManager    *testutil.MockTxManager
	userRepo     *testutil.MockUserRepository
	protocolRepo *testutil.MockProtocolRepository
	tokenRepo    *testutil.MockTokenRepository
	morphoRepo   *mockMorphoRepo
	eventRepo    *testutil.MockEventRepository
	consumer     *mockSQSConsumer
	cache        *mockBlockCache

	// ABIs for building multicall return data.
	morphoBlueReadABI *abi.ABI
	metaMorphoReadABI *abi.ABI
	erc20ABI          *abi.ABI

	// ABIs for building event logs.
	morphoBlueEventsABI   *abi.ABI
	metaMorphoEventsABI   *abi.ABI
	metaMorphoV2AccrueABI *abi.ABI
}

func newTestHarness(t *testing.T) *serviceTestHarness {
	t.Helper()

	cache := newMockBlockCache()

	multicaller := testutil.NewMockMulticaller()
	txManager := &testutil.MockTxManager{}
	userRepo := &testutil.MockUserRepository{}
	protocolRepo := &testutil.MockProtocolRepository{}
	tokenRepo := &testutil.MockTokenRepository{}
	morphoRepo := &mockMorphoRepo{}
	eventRepo := &testutil.MockEventRepository{}
	consumer := &mockSQSConsumer{}

	// Set up sequential token IDs for GetOrCreateToken.
	var tokenCounter int64
	tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
		tokenCounter++
		return tokenCounter, nil
	}

	config := Config{
		SQSConsumerConfig: shared.SQSConsumerConfigDefaults(),
		ChainID:           1,
	}

	svc, err := NewService(config, consumer, cache, multicaller, txManager, userRepo, protocolRepo, tokenRepo, morphoRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())

	// Load ABIs for packing return data.
	morphoBlueReadABI, err := abis.GetMorphoBlueReadABI()
	if err != nil {
		t.Fatalf("GetMorphoBlueReadABI: %v", err)
	}
	metaMorphoReadABI, err := abis.GetMetaMorphoReadABI()
	if err != nil {
		t.Fatalf("GetMetaMorphoReadABI: %v", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}
	morphoBlueEventsABI, err := abis.GetMorphoBlueEventsABI()
	if err != nil {
		t.Fatalf("GetMorphoBlueEventsABI: %v", err)
	}
	metaMorphoEventsABI, err := abis.GetMetaMorphoEventsABI()
	if err != nil {
		t.Fatalf("GetMetaMorphoEventsABI: %v", err)
	}
	v2AccrueABI, err := abis.GetMetaMorphoV2AccrueInterestABI()
	if err != nil {
		t.Fatalf("GetMetaMorphoV2AccrueInterestABI: %v", err)
	}

	return &serviceTestHarness{
		t:            t,
		svc:          svc,
		multicaller:  multicaller,
		txManager:    txManager,
		userRepo:     userRepo,
		protocolRepo: protocolRepo,
		tokenRepo:    tokenRepo,
		morphoRepo:   morphoRepo,
		eventRepo:    eventRepo,
		consumer:     consumer,
		cache:        cache,

		morphoBlueReadABI:     morphoBlueReadABI,
		metaMorphoReadABI:     metaMorphoReadABI,
		erc20ABI:              erc20ABI,
		morphoBlueEventsABI:   morphoBlueEventsABI,
		metaMorphoEventsABI:   metaMorphoEventsABI,
		metaMorphoV2AccrueABI: v2AccrueABI,
	}
}

// --- ABI packing helpers for multicall return data ---

func (h *serviceTestHarness) packMarketState(tsa, tss, tba, tbs, lastUpdate, fee *big.Int) []byte {
	data, err := h.morphoBlueReadABI.Methods["market"].Outputs.Pack(tsa, tss, tba, tbs, lastUpdate, fee)
	if err != nil {
		panic(fmt.Sprintf("packMarketState: %v", err))
	}
	return data
}

func (h *serviceTestHarness) packPositionState(supplyShares, borrowShares, collateral *big.Int) []byte {
	data, err := h.morphoBlueReadABI.Methods["position"].Outputs.Pack(supplyShares, borrowShares, collateral)
	if err != nil {
		panic(fmt.Sprintf("packPositionState: %v", err))
	}
	return data
}

func (h *serviceTestHarness) packMarketParams(loan, coll, oracle, irm common.Address, lltv *big.Int) []byte {
	data, err := h.morphoBlueReadABI.Methods["idToMarketParams"].Outputs.Pack(loan, coll, oracle, irm, lltv)
	if err != nil {
		panic(fmt.Sprintf("packMarketParams: %v", err))
	}
	return data
}

func (h *serviceTestHarness) packUint256(v *big.Int) []byte {
	data, err := abi.Arguments{{Type: mustABIType("uint256")}}.Pack(v)
	if err != nil {
		panic(fmt.Sprintf("packUint256: %v", err))
	}
	return data
}

func (h *serviceTestHarness) packAddress(addr common.Address) []byte {
	data, err := abi.Arguments{{Type: mustABIType("address")}}.Pack(addr)
	if err != nil {
		panic(fmt.Sprintf("packAddress: %v", err))
	}
	return data
}

func (h *serviceTestHarness) packString(s string) []byte {
	data, err := abi.Arguments{{Type: mustABIType("string")}}.Pack(s)
	if err != nil {
		panic(fmt.Sprintf("packString: %v", err))
	}
	return data
}

func (h *serviceTestHarness) packUint8(v uint8) []byte {
	data, err := abi.Arguments{{Type: mustABIType("uint8")}}.Pack(v)
	if err != nil {
		panic(fmt.Sprintf("packUint8: %v", err))
	}
	return data
}

func mustABIType(s string) abi.Type {
	t, err := abi.NewType(s, "", nil)
	if err != nil {
		panic(fmt.Sprintf("mustABIType(%q): %v", s, err))
	}
	return t
}

// --- Default multicall return data ---

func defaultMarketStateData() (*big.Int, *big.Int, *big.Int, *big.Int, *big.Int, *big.Int) {
	return big.NewInt(1000000), big.NewInt(900000), big.NewInt(500000), big.NewInt(450000), big.NewInt(1700000000), big.NewInt(0)
}

func defaultPositionStateData() (*big.Int, *big.Int, *big.Int) {
	return big.NewInt(100000), big.NewInt(50000), big.NewInt(200000)
}

// defaultMarketStateResult returns an outbound.Result with ABI-encoded default market state.
func (h *serviceTestHarness) defaultMarketStateResult() outbound.Result {
	tsa, tss, tba, tbs, lu, fee := defaultMarketStateData()
	return outbound.Result{Success: true, ReturnData: h.packMarketState(tsa, tss, tba, tbs, lu, fee)}
}

// defaultPositionStateResult returns an outbound.Result with ABI-encoded default position state.
func (h *serviceTestHarness) defaultPositionStateResult() outbound.Result {
	ss, bs, coll := defaultPositionStateData()
	return outbound.Result{Success: true, ReturnData: h.packPositionState(ss, bs, coll)}
}

func (h *serviceTestHarness) defaultVaultTotalAssetsResult() outbound.Result {
	return outbound.Result{Success: true, ReturnData: h.packUint256(big.NewInt(5000000))}
}

func (h *serviceTestHarness) defaultVaultTotalSupplyResult() outbound.Result {
	return outbound.Result{Success: true, ReturnData: h.packUint256(big.NewInt(4500000))}
}

func (h *serviceTestHarness) defaultBalanceOfResult(balance *big.Int) outbound.Result {
	return outbound.Result{Success: true, ReturnData: h.packUint256(balance)}
}

// tokenMetadataResults returns 2 results: symbol + decimals for a token.
func (h *serviceTestHarness) tokenMetadataResults(symbol string, decimals uint8) []outbound.Result {
	return []outbound.Result{
		{Success: true, ReturnData: h.packString(symbol)},
		{Success: true, ReturnData: h.packUint8(decimals)},
	}
}

// vaultMetadataResults returns 6 results for getVaultMetadata:
// name, symbol, asset, decimals, MORPHO, skimRecipient.
func (h *serviceTestHarness) vaultMetadataResults(name, symbol string, asset common.Address, decimals uint8, isV2 bool) []outbound.Result {
	skimResult := outbound.Result{Success: false, ReturnData: nil}
	if isV2 {
		skimResult = outbound.Result{Success: true, ReturnData: h.packAddress(common.HexToAddress("0x1"))}
	}
	return []outbound.Result{
		{Success: true, ReturnData: h.packString(name)},
		{Success: true, ReturnData: h.packString(symbol)},
		{Success: true, ReturnData: h.packAddress(asset)},
		{Success: true, ReturnData: h.packUint8(decimals)},
		{Success: true, ReturnData: h.packAddress(MorphoBlueAddress)},
		skimResult,
	}
}

// --- Event log construction helpers ---

var (
	testMarketID  = common.HexToHash("0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc")
	testCaller    = common.HexToAddress("0x1111111111111111111111111111111111111111")
	testOnBehalf  = common.HexToAddress("0x2222222222222222222222222222222222222222")
	testReceiver  = common.HexToAddress("0x3333333333333333333333333333333333333333")
	testBorrower  = common.HexToAddress("0x4444444444444444444444444444444444444444")
	testLoanToken = common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	testCollToken = common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	testOracle    = common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")
	testIrm       = common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddd")
	testVaultAddr = common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	testTxHash    = "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
)

// makeSupplyLog creates a Supply event log.
// Supply: indexed(id, caller, onBehalf), non-indexed(assets, shares)
func (h *serviceTestHarness) makeSupplyLog(marketID [32]byte, caller, onBehalf common.Address, assets, shares *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["Supply"]
	data, err := event.Inputs.NonIndexed().Pack(assets, shares)
	if err != nil {
		panic(fmt.Sprintf("makeSupplyLog: %v", err))
	}
	return shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(marketID[:]).Hex(),
			common.BytesToHash(caller.Bytes()).Hex(),
			common.BytesToHash(onBehalf.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeWithdrawLog creates a Withdraw event log.
// Withdraw: indexed(id, onBehalf, receiver), non-indexed(caller, assets, shares)
func (h *serviceTestHarness) makeWithdrawLog(marketID [32]byte, caller, onBehalf, receiver common.Address, assets, shares *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["Withdraw"]
	data, err := event.Inputs.NonIndexed().Pack(caller, assets, shares)
	if err != nil {
		panic(fmt.Sprintf("makeWithdrawLog: %v", err))
	}
	return shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(marketID[:]).Hex(),
			common.BytesToHash(onBehalf.Bytes()).Hex(),
			common.BytesToHash(receiver.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeBorrowLog creates a Borrow event log.
// Borrow: indexed(id, onBehalf, receiver), non-indexed(caller, assets, shares)
func (h *serviceTestHarness) makeBorrowLog(marketID [32]byte, caller, onBehalf, receiver common.Address, assets, shares *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["Borrow"]
	data, err := event.Inputs.NonIndexed().Pack(caller, assets, shares)
	if err != nil {
		panic(fmt.Sprintf("makeBorrowLog: %v", err))
	}
	return shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(marketID[:]).Hex(),
			common.BytesToHash(onBehalf.Bytes()).Hex(),
			common.BytesToHash(receiver.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeRepayLog creates a Repay event log.
// Repay: indexed(id, caller, onBehalf), non-indexed(assets, shares)
func (h *serviceTestHarness) makeRepayLog(marketID [32]byte, caller, onBehalf common.Address, assets, shares *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["Repay"]
	data, err := event.Inputs.NonIndexed().Pack(assets, shares)
	if err != nil {
		panic(fmt.Sprintf("makeRepayLog: %v", err))
	}
	return shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(marketID[:]).Hex(),
			common.BytesToHash(caller.Bytes()).Hex(),
			common.BytesToHash(onBehalf.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeSupplyCollateralLog creates a SupplyCollateral event log.
// SupplyCollateral: indexed(id, caller, onBehalf), non-indexed(assets)
func (h *serviceTestHarness) makeSupplyCollateralLog(marketID [32]byte, caller, onBehalf common.Address, assets *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["SupplyCollateral"]
	data, err := event.Inputs.NonIndexed().Pack(assets)
	if err != nil {
		panic(fmt.Sprintf("makeSupplyCollateralLog: %v", err))
	}
	return shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(marketID[:]).Hex(),
			common.BytesToHash(caller.Bytes()).Hex(),
			common.BytesToHash(onBehalf.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeWithdrawCollateralLog creates a WithdrawCollateral event log.
// WithdrawCollateral: indexed(id, onBehalf, receiver), non-indexed(caller, assets)
func (h *serviceTestHarness) makeWithdrawCollateralLog(marketID [32]byte, caller, onBehalf, receiver common.Address, assets *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["WithdrawCollateral"]
	data, err := event.Inputs.NonIndexed().Pack(caller, assets)
	if err != nil {
		panic(fmt.Sprintf("makeWithdrawCollateralLog: %v", err))
	}
	return shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(marketID[:]).Hex(),
			common.BytesToHash(onBehalf.Bytes()).Hex(),
			common.BytesToHash(receiver.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeCreateMarketLog creates a CreateMarket event log.
func (h *serviceTestHarness) makeCreateMarketLog(marketID [32]byte, loan, coll, oracle, irm common.Address, lltv *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["CreateMarket"]
	// CreateMarket has id (indexed) + marketParams (non-indexed tuple)
	mp := struct {
		LoanToken       common.Address
		CollateralToken common.Address
		Oracle          common.Address
		Irm             common.Address
		Lltv            *big.Int
	}{loan, coll, oracle, irm, lltv}
	data, err := event.Inputs.NonIndexed().Pack(mp)
	if err != nil {
		panic(fmt.Sprintf("makeCreateMarketLog: %v", err))
	}
	return shared.Log{
		Address:         MorphoBlueAddress.Hex(),
		Topics:          []string{event.ID.Hex(), common.BytesToHash(marketID[:]).Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeAccrueInterestLog creates an AccrueInterest event log (Morpho Blue).
func (h *serviceTestHarness) makeAccrueInterestLog(marketID [32]byte, prevBorrowRate, interest, feeShares *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["AccrueInterest"]
	data, err := event.Inputs.NonIndexed().Pack(prevBorrowRate, interest, feeShares)
	if err != nil {
		panic(fmt.Sprintf("makeAccrueInterestLog: %v", err))
	}
	return shared.Log{
		Address:         MorphoBlueAddress.Hex(),
		Topics:          []string{event.ID.Hex(), common.BytesToHash(marketID[:]).Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeLiquidateLog creates a Liquidate event log.
// Liquidate: indexed(id, caller, borrower), non-indexed(repaidAssets, repaidShares, seizedAssets, badDebtAssets, badDebtShares)
func (h *serviceTestHarness) makeLiquidateLog(marketID [32]byte, caller, borrower common.Address, repaidAssets, repaidShares, seizedAssets, badDebtAssets, badDebtShares *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["Liquidate"]
	data, err := event.Inputs.NonIndexed().Pack(repaidAssets, repaidShares, seizedAssets, badDebtAssets, badDebtShares)
	if err != nil {
		panic(fmt.Sprintf("makeLiquidateLog: %v", err))
	}
	return shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(marketID[:]).Hex(),
			common.BytesToHash(caller.Bytes()).Hex(),
			common.BytesToHash(borrower.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeSetFeeLog creates a SetFee event log.
func (h *serviceTestHarness) makeSetFeeLog(marketID [32]byte, newFee *big.Int) shared.Log {
	event := h.morphoBlueEventsABI.Events["SetFee"]
	data, err := event.Inputs.NonIndexed().Pack(newFee)
	if err != nil {
		panic(fmt.Sprintf("makeSetFeeLog: %v", err))
	}
	return shared.Log{
		Address:         MorphoBlueAddress.Hex(),
		Topics:          []string{event.ID.Hex(), common.BytesToHash(marketID[:]).Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeVaultDepositLog creates a MetaMorpho Deposit event log.
func (h *serviceTestHarness) makeVaultDepositLog(vaultAddr common.Address, sender, owner common.Address, assets, shares *big.Int) shared.Log {
	event := h.metaMorphoEventsABI.Events["Deposit"]
	data, err := event.Inputs.NonIndexed().Pack(assets, shares)
	if err != nil {
		panic(fmt.Sprintf("makeVaultDepositLog: %v", err))
	}
	return shared.Log{
		Address: vaultAddr.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(sender.Bytes()).Hex(),
			common.BytesToHash(owner.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeVaultWithdrawLog creates a MetaMorpho Withdraw event log.
func (h *serviceTestHarness) makeVaultWithdrawLog(vaultAddr common.Address, sender, receiver, owner common.Address, assets, shares *big.Int) shared.Log {
	event := h.metaMorphoEventsABI.Events["Withdraw"]
	data, err := event.Inputs.NonIndexed().Pack(assets, shares)
	if err != nil {
		panic(fmt.Sprintf("makeVaultWithdrawLog: %v", err))
	}
	return shared.Log{
		Address: vaultAddr.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(sender.Bytes()).Hex(),
			common.BytesToHash(receiver.Bytes()).Hex(),
			common.BytesToHash(owner.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeVaultTransferLog creates a MetaMorpho Transfer event log.
func (h *serviceTestHarness) makeVaultTransferLog(vaultAddr, from, to common.Address, value *big.Int) shared.Log {
	event := h.metaMorphoEventsABI.Events["Transfer"]
	data, err := event.Inputs.NonIndexed().Pack(value)
	if err != nil {
		panic(fmt.Sprintf("makeVaultTransferLog: %v", err))
	}
	return shared.Log{
		Address: vaultAddr.Hex(),
		Topics: []string{
			event.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeVaultAccrueInterestV1Log creates a MetaMorpho V1 AccrueInterest event log.
func (h *serviceTestHarness) makeVaultAccrueInterestV1Log(vaultAddr common.Address, newTotalAssets, feeShares *big.Int) shared.Log {
	event := h.metaMorphoEventsABI.Events["AccrueInterest"]
	data, err := event.Inputs.NonIndexed().Pack(newTotalAssets, feeShares)
	if err != nil {
		panic(fmt.Sprintf("makeVaultAccrueInterestV1Log: %v", err))
	}
	return shared.Log{
		Address:         vaultAddr.Hex(),
		Topics:          []string{event.ID.Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// makeVaultAccrueInterestV2Log creates a MetaMorpho V2 AccrueInterest event log.
func (h *serviceTestHarness) makeVaultAccrueInterestV2Log(vaultAddr common.Address, previousTotalAssets, newTotalAssets, performanceFeeShares, managementFeeShares *big.Int) shared.Log {
	event := h.metaMorphoV2AccrueABI.Events["AccrueInterest"]
	data, err := event.Inputs.NonIndexed().Pack(previousTotalAssets, newTotalAssets, performanceFeeShares, managementFeeShares)
	if err != nil {
		panic(fmt.Sprintf("makeVaultAccrueInterestV2Log: %v", err))
	}
	return shared.Log{
		Address:         vaultAddr.Hex(),
		Topics:          []string{event.ID.Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
}

// --- Receipt + Redis helpers ---

func makeReceipt(txHash string, logs ...shared.Log) shared.TransactionReceipt {
	return shared.TransactionReceipt{
		TransactionHash: txHash,
		Logs:            logs,
	}
}

// storeReceipts stores receipt data in the mock block cache.
func (h *serviceTestHarness) storeReceipts(t *testing.T, chainID, blockNumber int64, version int, receipts []shared.TransactionReceipt) {
	t.Helper()
	data, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	h.cache.SetReceipts(chainID, blockNumber, version, data)
}

// processBlock is a convenience method that stores receipts and calls processBlockEvent.
func (h *serviceTestHarness) processBlock(t *testing.T, chainID, blockNumber int64, version int, receipts []shared.TransactionReceipt) error {
	t.Helper()
	h.storeReceipts(t, chainID, blockNumber, version, receipts)
	return h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     version,
	})
}

// registerTestVault pre-registers a vault in the vault registry for testing MetaMorpho events.
func (h *serviceTestHarness) registerTestVault(vaultAddr common.Address, vaultID int64, version entity.MorphoVaultVersion) {
	vault := &entity.MorphoVault{
		ID:             vaultID,
		ProtocolID:     1,
		Address:        vaultAddr.Bytes(),
		Name:           "Test Vault",
		Symbol:         "tVAULT",
		AssetTokenID:   1,
		VaultVersion:   version,
		CreatedAtBlock: 18000000,
	}
	h.svc.vaultRegistry.RegisterVault(vaultAddr, vault)
}

// setupPositionEventMulticall sets up the multicaller to return market+position state
// for position events (Supply, Withdraw, Borrow, Repay, SupplyCollateral, WithdrawCollateral).
func (h *serviceTestHarness) setupPositionEventMulticall() {
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		}
		return nil, fmt.Errorf("unexpected call count: %d", len(calls))
	}
}

// setupMarketExistsInDB configures morphoRepo to return a market with the given ID.
func (h *serviceTestHarness) setupMarketExistsInDB(marketID [32]byte, dbID int64) {
	h.morphoRepo.GetMarketByMarketIDFn = func(_ context.Context, id []byte) (*entity.MorphoMarket, error) {
		if common.BytesToHash(id) == common.BytesToHash(marketID[:]) {
			return &entity.MorphoMarket{ID: dbID, MarketID: id}, nil
		}
		return nil, nil
	}
}

// setupMarketNotInDB configures the multicaller with getMarketParams + getTokenPairMetadata responses
// for the ensureMarket flow when the market doesn't exist in DB yet.
func (h *serviceTestHarness) setupMarketNotInDB() {
	// Override multicaller to handle both position calls and market params/token metadata calls.
	origFn := h.multicaller.ExecuteFn
	h.multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 1:
			// getMarketParams or getMarketState
			return []outbound.Result{
				{Success: true, ReturnData: h.packMarketParams(testLoanToken, testCollToken, testOracle, testIrm, testutils.BigFromStr(h.t, "800000000000000000"))},
			}, nil
		case 2:
			// Could be market+position or vault totalAssets+totalSupply or token metadata
			// Check if first call targets MorphoBlue
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			// Token metadata (symbol + decimals)
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("TKN")},
				{Success: true, ReturnData: h.packUint8(18)},
			}, nil
		case 3:
			// market + 2 positions (Liquidate) OR vault state + balance
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult(), h.defaultPositionStateResult()}, nil
			}
			// vault state + balance
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			// getTokenPairMetadata (4 calls: symbolA, decimalsA, symbolB, decimalsB)
			// OR vault state + 2 balances
			if calls[0].Target != MorphoBlueAddress && calls[0].Target != testLoanToken && calls[0].Target != testCollToken {
				// vault state + 2 balances
				return []outbound.Result{
					h.defaultVaultTotalAssetsResult(),
					h.defaultVaultTotalSupplyResult(),
					h.defaultBalanceOfResult(big.NewInt(100000)),
					h.defaultBalanceOfResult(big.NewInt(200000)),
				}, nil
			}
			return []outbound.Result{
				{Success: true, ReturnData: h.packString("LOAN")},
				{Success: true, ReturnData: h.packUint8(18)},
				{Success: true, ReturnData: h.packString("COLL")},
				{Success: true, ReturnData: h.packUint8(18)},
			}, nil
		case 6:
			// getVaultMetadata
			return h.vaultMetadataResults("Test Vault", "tVLT", testLoanToken, 18, false), nil
		default:
			if origFn != nil {
				return origFn(ctx, calls, blockNumber)
			}
			return nil, fmt.Errorf("unexpected call count: %d", len(calls))
		}
	}
}
