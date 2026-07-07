package fluid_vault_indexer

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// fakeChain is a Multicaller that routes each sub-call by its 4-byte selector:
// getAllVaultsAddresses, getVaultEntireData (per-vault fixture), and ERC-20
// symbol()/decimals(). It lets a service test exercise the full
// receipt → resolver-read → persist path without a real RPC.
type fakeChain struct {
	// t.Fatalf must only be called from the goroutine that runs the test, so
	// Execute (which calls it on packing errors) must only run on the test
	// goroutine. Drive processBlockEvent/ReconcileVaults directly; never feed
	// messages through Start's consumer goroutine, which would call Execute — and
	// thus t.Fatalf — off-goroutine.
	t *testing.T

	resolverABI *abi.ABI
	erc20ABI    *abi.ABI

	allVaults   []common.Address
	vaultData   map[common.Address][]byte // vault -> raw getVaultEntireData blob
	tokenSymbol map[common.Address]string
	tokenDec    map[common.Address]uint8

	// executeErr fails every Execute call. executeErrAfterGetAll fails only
	// Execute batches that are not the getAllVaultsAddresses enumeration (i.e.
	// the getVaultEntireData read), so a test can drive the enumerate-succeeds /
	// read-fails path.
	executeErr            error
	executeErrAfterGetAll error

	getVaultEntireDataSel [4]byte
	getAllSel             [4]byte
	symbolSel             [4]byte
	decimalsSel           [4]byte
}

func newFakeChain(t *testing.T) *fakeChain {
	t.Helper()
	rABI := mustResolverABI(t)
	eABI := mustERC20ABI(t)
	fc := &fakeChain{
		t:           t,
		resolverABI: rABI,
		erc20ABI:    eABI,
		vaultData:   map[common.Address][]byte{},
		tokenSymbol: map[common.Address]string{},
		tokenDec:    map[common.Address]uint8{},
	}
	copy(fc.getVaultEntireDataSel[:], rABI.Methods["getVaultEntireData"].ID)
	copy(fc.getAllSel[:], rABI.Methods["getAllVaultsAddresses"].ID)
	copy(fc.symbolSel[:], eABI.Methods["symbol"].ID)
	copy(fc.decimalsSel[:], eABI.Methods["decimals"].ID)
	return fc
}

func (f *fakeChain) Address() common.Address { return common.Address{} }

func (f *fakeChain) ExecuteAtHash(ctx context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
	return f.Execute(ctx, calls, nil)
}

func (f *fakeChain) Execute(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	if f.executeErr != nil {
		return nil, f.executeErr
	}
	if f.executeErrAfterGetAll != nil && !isGetAllBatch(calls, f.getAllSel) {
		return nil, f.executeErrAfterGetAll
	}
	out := make([]outbound.Result, len(calls))
	for i, c := range calls {
		if len(c.CallData) < 4 {
			out[i] = outbound.Result{Success: false}
			continue
		}
		var sel [4]byte
		copy(sel[:], c.CallData[:4])
		switch sel {
		case f.getAllSel:
			packed, err := f.resolverABI.Methods["getAllVaultsAddresses"].Outputs.Pack(f.allVaults)
			if err != nil {
				f.t.Fatalf("packing getAllVaultsAddresses outputs: %v", err)
			}
			out[i] = outbound.Result{Success: true, ReturnData: packed}
		case f.getVaultEntireDataSel:
			vault := common.BytesToAddress(c.CallData[len(c.CallData)-20:])
			blob, ok := f.vaultData[vault]
			out[i] = outbound.Result{Success: ok, ReturnData: blob}
		case f.symbolSel:
			sym := f.tokenSymbol[c.Target]
			packed, err := f.erc20ABI.Methods["symbol"].Outputs.Pack(sym)
			if err != nil {
				f.t.Fatalf("packing symbol outputs: %v", err)
			}
			out[i] = outbound.Result{Success: true, ReturnData: packed}
		case f.decimalsSel:
			dec := f.tokenDec[c.Target]
			packed, err := f.erc20ABI.Methods["decimals"].Outputs.Pack(dec)
			if err != nil {
				f.t.Fatalf("packing decimals outputs: %v", err)
			}
			out[i] = outbound.Result{Success: true, ReturnData: packed}
		default:
			out[i] = outbound.Result{Success: false}
		}
	}
	return out, nil
}

// isGetAllBatch reports whether calls is the single-call getAllVaultsAddresses
// enumeration, so executeErrAfterGetAll can target only the later
// getVaultEntireData read.
func isGetAllBatch(calls []outbound.Call, getAllSel [4]byte) bool {
	if len(calls) != 1 || len(calls[0].CallData) < 4 {
		return false
	}
	var sel [4]byte
	copy(sel[:], calls[0].CallData[:4])
	return sel == getAllSel
}

type serviceFixture struct {
	svc       *Service
	chain     *fakeChain
	repo      *stubFluidRepo
	tokenRepo *stubTokenRepo
	cache     *stubCache
	txm       *stubTxManager
	querier   *stubBlockQuerier
}

func newServiceForTest(t *testing.T) *serviceFixture {
	t.Helper()
	chain := newFakeChain(t)
	repo := newStubFluidRepo()
	tokenRepo := newStubTokenRepo()
	cache := &stubCache{receipts: map[int64]json.RawMessage{}}
	txm := &stubTxManager{}
	querier := &stubBlockQuerier{head: 19_000_000}

	svc, err := NewService(
		Config{SQSConsumerConfig: shared.SQSConsumerConfig{ChainID: 1, Logger: testLogger()}},
		stubConsumer{}, cache, querier, chain, txm, repo, tokenRepo, &stubProtocolRepo{},
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return &serviceFixture{svc: svc, chain: chain, repo: repo, tokenRepo: tokenRepo, cache: cache, txm: txm, querier: querier}
}

// logOperateTopic returns the topic0 the service treats as a position-change
// trigger (a known vault's LogOperate).
func logOperateTopic(t *testing.T) common.Hash {
	t.Helper()
	eventsABI, err := abis.GetFluidVaultEventsABI()
	if err != nil {
		t.Fatalf("loading events ABI: %v", err)
	}
	return eventsABI.Events["LogOperate"].ID
}

// receiptsWithLog builds a one-receipt block whose single log has the given
// emitter address and topics.
func receiptsWithLog(t *testing.T, address common.Address, topics ...common.Hash) json.RawMessage {
	t.Helper()
	hexTopics := make([]string, len(topics))
	for i, tp := range topics {
		hexTopics[i] = tp.Hex()
	}
	receipts := []shared.TransactionReceipt{{
		TransactionHash: "0xdead",
		Logs: []shared.Log{{
			Address: address.Hex(),
			Topics:  hexTopics,
		}},
	}}
	raw, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshalling receipts: %v", err)
	}
	return raw
}

func blockEvent(block int64) outbound.BlockEvent {
	return outbound.BlockEvent{ChainID: 1, BlockNumber: block, Version: 0, BlockTimestamp: 1_700_000_000}
}

const (
	susdsVaultAddr = "0x75305a6a8977E998573076FA3293A235E23C32Ad"
	smartVaultAddr = "0x57fed7c9b3c763999c519264931790cBcA331417"
)

// deps bundles NewService's interface arguments so each nil-dependency case can
// null one field and leave the rest valid.
type deps struct {
	consumer     outbound.SQSConsumer
	cache        outbound.BlockCacheReader
	querier      entity.BlockQuerier
	multicaller  outbound.Multicaller
	txManager    outbound.TxManager
	vaultRepo    outbound.FluidVaultRepository
	tokenRepo    outbound.TokenRepository
	protocolRepo outbound.ProtocolRepository
}

func validDeps(t *testing.T) deps {
	t.Helper()
	return deps{
		consumer: stubConsumer{}, cache: &stubCache{}, querier: stubBlockQuerier{},
		multicaller: newFakeChain(t), txManager: &stubTxManager{}, vaultRepo: newStubFluidRepo(),
		tokenRepo: newStubTokenRepo(), protocolRepo: &stubProtocolRepo{},
	}
}

func TestNewService_MissingDependency(t *testing.T) {
	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfig{ChainID: 1, Logger: testLogger()}}
	tests := []struct {
		name string
		null func(*deps)
	}{
		{"nil consumer", func(d *deps) { d.consumer = nil }},
		{"nil cache", func(d *deps) { d.cache = nil }},
		{"nil block querier", func(d *deps) { d.querier = nil }},
		{"nil multicaller", func(d *deps) { d.multicaller = nil }},
		{"nil tx manager", func(d *deps) { d.txManager = nil }},
		{"nil vault repo", func(d *deps) { d.vaultRepo = nil }},
		{"nil token repo", func(d *deps) { d.tokenRepo = nil }},
		{"nil protocol repo", func(d *deps) { d.protocolRepo = nil }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := validDeps(t)
			tc.null(&d)
			_, err := NewService(cfg, d.consumer, d.cache, d.querier, d.multicaller, d.txManager, d.vaultRepo, d.tokenRepo, d.protocolRepo)
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestNewService_DefaultsDebtTokenToSUSDS(t *testing.T) {
	f := newServiceForTest(t)
	if f.svc.config.TargetDebtToken != SUSDSAddress {
		t.Errorf("default debt token = %s, want sUSDS", f.svc.config.TargetDebtToken)
	}
}

// TestProcessBlockEvent_KnownVaultLogWritesSnapshot: a LogOperate-style log from
// a registered vault triggers an end-of-block resolver read and one state row.
func TestProcessBlockEvent_KnownVaultLogWritesSnapshot(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)

	// Pre-register the vault (as if loaded from DB).
	f.svc.registry.RegisterVault(&entity.FluidVault{
		ID: 5, ChainID: 1, ProtocolID: 42, Address: vault.Bytes(),
		VaultType: "10000", CollateralTokenID: 1, DebtTokenID: 2, CreatedAtBlock: 1,
	})
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")

	f.cache.receipts[10] = receiptsWithLog(t, vault, logOperateTopic(t))

	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	states := f.repo.savedStates()
	if len(states) != 1 {
		t.Fatalf("got %d states, want 1", len(states))
	}
	got := states[0]
	if got.FluidVaultID != 5 {
		t.Errorf("vaultID = %d, want 5", got.FluidVaultID)
	}
	if got.TotalCollateral.String() != "11328444893030209" {
		t.Errorf("totalCollateral = %s", got.TotalCollateral)
	}
	if got.TotalDebt.String() != "8021962986715460141" {
		t.Errorf("totalDebt = %s", got.TotalDebt)
	}
	if got.BlockNumber != 10 {
		t.Errorf("blockNumber = %d, want 10", got.BlockNumber)
	}
}

// TestProcessBlockEvent_IrrelevantLogNoWrite: a log from an unknown,
// non-factory address writes nothing.
func TestProcessBlockEvent_IrrelevantLogNoWrite(t *testing.T) {
	f := newServiceForTest(t)
	f.cache.receipts[10] = receiptsWithLog(t,
		common.HexToAddress("0x9999999999999999999999999999999999999999"),
		common.HexToHash("0xabc"))

	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if len(f.repo.savedStates()) != 0 {
		t.Errorf("expected no state rows, got %d", len(f.repo.savedStates()))
	}
}

// TestProcessBlockEvent_KnownVaultNonTriggerLogIgnored: a non-position log (e.g.
// an ERC-20 Transfer) from a known vault must NOT trigger a snapshot — only
// LogOperate / LogLiquidate do.
func TestProcessBlockEvent_KnownVaultNonTriggerLogIgnored(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	f.svc.registry.RegisterVault(&entity.FluidVault{
		ID: 5, ChainID: 1, ProtocolID: 42, Address: vault.Bytes(),
		VaultType: "10000", CollateralTokenID: 1, DebtTokenID: 2, CreatedAtBlock: 1,
	})
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")
	f.cache.receipts[10] = receiptsWithLog(t, vault, common.HexToHash("0xdeadbeef"))

	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if len(f.repo.savedStates()) != 0 {
		t.Errorf("non-trigger log should write no state, got %d rows", len(f.repo.savedStates()))
	}
}

// TestProcessBlockEvent_DiscoversInScopeVaultViaFactory: a VaultDeployed log from
// the factory registers a new in-scope sUSDS vault and persists it.
func TestProcessBlockEvent_DiscoversInScopeVaultViaFactory(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")
	// ETH collateral is the sentinel (no RPC); sUSDS debt metadata via fakeChain.
	f.chain.tokenSymbol[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = "sUSDS"
	f.chain.tokenDec[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = 18

	f.cache.receipts[10] = receiptsWithLog(t, FluidVaultFactoryAddress,
		f.svc.deployedTopic, common.BytesToHash(vault.Bytes()))

	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if !f.svc.registry.IsKnownVault(vault) {
		t.Errorf("expected vault registered after factory discovery")
	}
	if len(f.repo.upserted) != 1 {
		t.Errorf("expected 1 RecordVaults call, got %d", len(f.repo.upserted))
	}
}

// TestProcessBlockEvent_SkipsSmartVaultViaFactory: a smart/DEX vault is cached
// as not-vault and never persisted.
func TestProcessBlockEvent_SkipsSmartVaultViaFactory(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(smartVaultAddr)
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_smart.hex")

	f.cache.receipts[10] = receiptsWithLog(t, FluidVaultFactoryAddress,
		f.svc.deployedTopic, common.BytesToHash(vault.Bytes()))

	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if f.svc.registry.IsKnownVault(vault) {
		t.Errorf("smart vault must not be registered")
	}
	if !f.svc.registry.IsKnownNotVault(vault) {
		t.Errorf("smart vault should be cached as not-vault")
	}
	if len(f.repo.upserted) != 0 {
		t.Errorf("expected no upsert for smart vault")
	}
}

// TestProcessBlockEvent_SkipsNonTargetDebtViaFactory: a plain vault whose debt is
// not the targeted token (the smoke set has GHO/USDC vaults) is skipped.
func TestProcessBlockEvent_SkipsNonTargetDebtViaFactory(t *testing.T) {
	f := newServiceForTest(t)
	// Re-target to a token the fixture vault does NOT use, so the sUSDS fixture
	// becomes out of scope — exercises the debt-token filter generically.
	f.svc.config.TargetDebtToken = common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F") // DAI
	vault := common.HexToAddress(susdsVaultAddr)
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")

	f.cache.receipts[10] = receiptsWithLog(t, FluidVaultFactoryAddress,
		f.svc.deployedTopic, common.BytesToHash(vault.Bytes()))

	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if f.svc.registry.IsKnownVault(vault) {
		t.Errorf("non-target-debt vault must not be registered")
	}
	if !f.svc.registry.IsKnownNotVault(vault) {
		t.Errorf("non-target-debt vault should be cached as not-vault")
	}
}

// TestProcessBlockEvent_DiscoverUnservableVaultLeftUnknown: a VaultDeployed log
// for a vault the resolver cannot yet serve (no vaultData entry -> best-effort
// nil) is left unknown — nothing registered or upserted, and crucially NOT
// cached as not-vault, so a later startup reconcile retries it. This contrasts
// with the out-of-scope smart/non-target cases, which DO MarkNotVault.
func TestProcessBlockEvent_DiscoverUnservableVaultLeftUnknown(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	// No vaultData entry -> fakeChain returns Success:false -> best-effort nil.
	f.cache.receipts[10] = receiptsWithLog(t, FluidVaultFactoryAddress,
		f.svc.deployedTopic, common.BytesToHash(vault.Bytes()))

	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if f.svc.registry.IsKnownVault(vault) {
		t.Errorf("unservable vault must not be registered")
	}
	if f.svc.registry.IsKnownNotVault(vault) {
		t.Errorf("unservable vault must not be cached as not-vault (must be retried later)")
	}
	if len(f.repo.upserted) != 0 {
		t.Errorf("expected no upsert for unservable vault, got %d", len(f.repo.upserted))
	}
}

// TestProcessBlockEvent_DiscoverExecuteError: a transport-level Execute error
// while reading a newly-deployed vault propagates out of processBlockEvent.
func TestProcessBlockEvent_DiscoverExecuteError(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	f.chain.executeErr = errTest
	f.cache.receipts[10] = receiptsWithLog(t, FluidVaultFactoryAddress,
		f.svc.deployedTopic, common.BytesToHash(vault.Bytes()))

	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err == nil {
		t.Fatal("expected error when reading deployed vault fails at the transport level")
	}
}

func TestProcessBlockEvent_ReceiptsMissing(t *testing.T) {
	f := newServiceForTest(t)
	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err == nil {
		t.Fatal("expected error when receipts missing from cache")
	}
}

// packVaultEntireData builds a synthetic getVaultEntireData return blob with
// chosen collateral/debt/rate values, so tests can exercise paths the captured
// fixtures don't (e.g. a negative vault rate). Field order mirrors the verified
// resolver tuple.
func packVaultEntireData(t *testing.T, vault, collateral, debt common.Address, smartCol, smartDebt bool, totalColl, totalDebt, supplyRate, borrowRate *big.Int) []byte {
	t.Helper()
	rABI := mustResolverABI(t)
	z := common.Address{}
	zh := [32]byte{}
	type tk struct{ Token0, Token1 common.Address }
	supplyTok := tk{collateral, z}
	borrowTok := tk{debt, z}
	if smartCol {
		supplyTok = tk{collateral, collateral}
	}
	if smartDebt {
		borrowTok = tk{debt, collateral}
	}

	ved := struct {
		Vault             common.Address
		IsSmartCol        bool
		IsSmartDebt       bool
		ConstantVariables struct {
			Liquidity, Factory, OperateImplementation, AdminImplementation, SecondaryImplementation, Deployer, Supply, Borrow common.Address
			SupplyToken, BorrowToken                                                                                          tk
			VaultId, VaultType                                                                                                *big.Int
			SupplyExchangePriceSlot, BorrowExchangePriceSlot, UserSupplySlot, UserBorrowSlot                                  [32]byte
		}
		Configs struct {
			SupplyRateMagnifier, BorrowRateMagnifier, CollateralFactor, LiquidationThreshold, LiquidationMaxLimit, WithdrawalGap, LiquidationPenalty, BorrowFee uint16
			Oracle                                                                                                                                              common.Address
			OraclePriceOperate, OraclePriceLiquidate                                                                                                            *big.Int
			Rebalancer                                                                                                                                          common.Address
			LastUpdateTimestamp                                                                                                                                 *big.Int
		}
		ExchangePricesAndRates struct {
			LastStoredLiquiditySupplyExchangePrice, LastStoredLiquidityBorrowExchangePrice, LastStoredVaultSupplyExchangePrice, LastStoredVaultBorrowExchangePrice *big.Int
			LiquiditySupplyExchangePrice, LiquidityBorrowExchangePrice, VaultSupplyExchangePrice, VaultBorrowExchangePrice                                         *big.Int
			SupplyRateLiquidity, BorrowRateLiquidity, SupplyRateVault, BorrowRateVault, RewardsOrFeeRateSupply, RewardsOrFeeRateBorrow                             *big.Int
		}
		TotalSupplyAndBorrow struct {
			TotalSupplyVault, TotalBorrowVault, TotalSupplyLiquidityOrDex, TotalBorrowLiquidityOrDex, AbsorbedSupply, AbsorbedBorrow *big.Int
		}
		LimitsAndAvailability struct {
			WithdrawLimit, WithdrawableUntilLimit, Withdrawable, BorrowLimit, BorrowableUntilLimit, Borrowable, BorrowLimitUtilization, MinimumBorrowing *big.Int
		}
		VaultState struct {
			TotalPositions, TopTick, CurrentBranch, TotalBranch, TotalBorrow, TotalSupply *big.Int
			CurrentBranchState                                                            struct {
				Status, MinimaTick, DebtFactor, Partials, DebtLiquidity, BaseBranchId, BaseBranchMinima *big.Int
			}
		}
		LiquidityUserSupplyData struct {
			ModeWithInterest                                                                                                                                                       bool
			Supply, WithdrawalLimit, LastUpdateTimestamp, ExpandPercent, ExpandDuration, BaseWithdrawalLimit, WithdrawableUntilLimit, Withdrawable, DecayEndTimestamp, DecayAmount *big.Int
		}
		LiquidityUserBorrowData struct {
			ModeWithInterest                                                                                                                                                   bool
			Borrow, BorrowLimit, LastUpdateTimestamp, ExpandPercent, ExpandDuration, BaseBorrowLimit, MaxBorrowLimit, BorrowableUntilLimit, Borrowable, BorrowLimitUtilization *big.Int
		}
	}{}

	zero := big.NewInt(0)
	ved.Vault = vault
	ved.IsSmartCol = smartCol
	ved.IsSmartDebt = smartDebt
	ved.ConstantVariables.SupplyToken = supplyTok
	ved.ConstantVariables.BorrowToken = borrowTok
	ved.ConstantVariables.VaultId = zero
	ved.ConstantVariables.VaultType = big.NewInt(10000)
	ved.ConstantVariables.SupplyExchangePriceSlot = zh
	ved.ConstantVariables.BorrowExchangePriceSlot = zh
	ved.ConstantVariables.UserSupplySlot = zh
	ved.ConstantVariables.UserBorrowSlot = zh
	ved.Configs.OraclePriceOperate, ved.Configs.OraclePriceLiquidate, ved.Configs.LastUpdateTimestamp = zero, zero, zero
	e := &ved.ExchangePricesAndRates
	e.LastStoredLiquiditySupplyExchangePrice, e.LastStoredLiquidityBorrowExchangePrice = zero, zero
	e.LastStoredVaultSupplyExchangePrice, e.LastStoredVaultBorrowExchangePrice = zero, zero
	e.LiquiditySupplyExchangePrice, e.LiquidityBorrowExchangePrice = zero, zero
	e.VaultSupplyExchangePrice, e.VaultBorrowExchangePrice = big.NewInt(1), big.NewInt(1)
	e.SupplyRateLiquidity, e.BorrowRateLiquidity = zero, zero
	e.SupplyRateVault, e.BorrowRateVault = supplyRate, borrowRate
	e.RewardsOrFeeRateSupply, e.RewardsOrFeeRateBorrow = zero, zero
	tsb := &ved.TotalSupplyAndBorrow
	tsb.TotalSupplyVault, tsb.TotalBorrowVault = totalColl, totalDebt
	tsb.TotalSupplyLiquidityOrDex, tsb.TotalBorrowLiquidityOrDex, tsb.AbsorbedSupply, tsb.AbsorbedBorrow = zero, zero, zero, zero
	lim := &ved.LimitsAndAvailability
	lim.WithdrawLimit, lim.WithdrawableUntilLimit, lim.Withdrawable, lim.BorrowLimit = zero, zero, zero, zero
	lim.BorrowableUntilLimit, lim.Borrowable, lim.BorrowLimitUtilization, lim.MinimumBorrowing = zero, zero, zero, zero
	vs := &ved.VaultState
	vs.TotalPositions, vs.TopTick, vs.CurrentBranch, vs.TotalBranch, vs.TotalBorrow, vs.TotalSupply = zero, zero, zero, zero, zero, zero
	cb := &vs.CurrentBranchState
	cb.Status, cb.MinimaTick, cb.DebtFactor, cb.Partials, cb.DebtLiquidity, cb.BaseBranchId, cb.BaseBranchMinima = zero, zero, zero, zero, zero, zero, zero
	us := &ved.LiquidityUserSupplyData
	us.Supply, us.WithdrawalLimit, us.LastUpdateTimestamp, us.ExpandPercent, us.ExpandDuration = zero, zero, zero, zero, zero
	us.BaseWithdrawalLimit, us.WithdrawableUntilLimit, us.Withdrawable, us.DecayEndTimestamp, us.DecayAmount = zero, zero, zero, zero, zero
	ub := &ved.LiquidityUserBorrowData
	ub.Borrow, ub.BorrowLimit, ub.LastUpdateTimestamp, ub.ExpandPercent, ub.ExpandDuration = zero, zero, zero, zero, zero
	ub.BaseBorrowLimit, ub.MaxBorrowLimit, ub.BorrowableUntilLimit, ub.Borrowable, ub.BorrowLimitUtilization = zero, zero, zero, zero, zero

	packed, err := rABI.Methods["getVaultEntireData"].Outputs.Pack(ved)
	if err != nil {
		t.Fatalf("packing synthetic VaultEntireData: %v", err)
	}
	return packed
}

// TestProcessBlockEvent_NegativeRatePreserved: a vault whose resolver supply rate
// is negative (int256) writes a snapshot storing the signed value verbatim — a
// negative rate is real data, not a capture error, so it must not be nulled.
func TestProcessBlockEvent_NegativeRatePreserved(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	f.svc.registry.RegisterVault(&entity.FluidVault{
		ID: 9, ChainID: 1, ProtocolID: 42, Address: vault.Bytes(),
		VaultType: "10000", CollateralTokenID: 1, DebtTokenID: 2, CreatedAtBlock: 1,
	})
	f.chain.vaultData[vault] = packVaultEntireData(t, vault, ethSentinel, SUSDSAddress, false, false,
		big.NewInt(100), big.NewInt(50), big.NewInt(-5), big.NewInt(7))

	f.cache.receipts[10] = receiptsWithLog(t, vault, logOperateTopic(t))
	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	states := f.repo.savedStates()
	if len(states) != 1 {
		t.Fatalf("got %d states, want 1", len(states))
	}
	if states[0].SupplyRate == nil || states[0].SupplyRate.Cmp(big.NewInt(-5)) != 0 {
		t.Errorf("negative supplyRate should be preserved as -5, got %v", states[0].SupplyRate)
	}
	if states[0].BorrowRate == nil || states[0].BorrowRate.Cmp(big.NewInt(7)) != 0 {
		t.Errorf("positive borrowRate should be preserved as 7, got %v", states[0].BorrowRate)
	}
}

func TestStop_WithoutStart(t *testing.T) {
	f := newServiceForTest(t)
	if err := f.svc.Stop(); err != nil {
		t.Fatalf("Stop without Start should be a no-op, got: %v", err)
	}
}

func TestStartStop_Lifecycle(t *testing.T) {
	f := newServiceForTest(t)
	if err := f.svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := f.svc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStart_RegistryLoadError(t *testing.T) {
	f := newServiceForTest(t)
	f.repo.getAllErr = errTest
	if err := f.svc.Start(context.Background()); err == nil {
		t.Fatal("expected error when registry load fails")
	}
}

func TestStart_BlockQuerierError(t *testing.T) {
	f := newServiceForTest(t)
	f.querier.err = errTest
	if err := f.svc.Start(context.Background()); err == nil {
		t.Fatal("expected error when block querier fails")
	}
}

// TestStart_ReconcilesPreexistingVaults: Start must pick up a vault that already
// exists on-chain (via getAllVaultsAddresses) even with no VaultDeployed event.
func TestStart_ReconcilesPreexistingVaults(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	f.chain.allVaults = []common.Address{vault}
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")
	f.chain.tokenSymbol[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = "sUSDS"
	f.chain.tokenDec[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = 18

	if err := f.svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = f.svc.Stop() })
	if !f.svc.registry.IsKnownVault(vault) {
		t.Errorf("expected pre-existing vault registered by startup reconcile")
	}
}

func TestSnapshotVaults_SaveError(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	f.svc.registry.RegisterVault(&entity.FluidVault{
		ID: 9, ChainID: 1, ProtocolID: 42, Address: vault.Bytes(),
		VaultType: "10000", CollateralTokenID: 1, DebtTokenID: 2, CreatedAtBlock: 1,
	})
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")
	f.repo.saveErr = errTest
	f.cache.receipts[10] = receiptsWithLog(t, vault, logOperateTopic(t))
	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err == nil {
		t.Fatal("expected error when SaveVaultStates fails")
	}
}

func TestReconcileVaults_RegistersInScope(t *testing.T) {
	f := newServiceForTest(t)
	susds := common.HexToAddress(susdsVaultAddr)
	smart := common.HexToAddress(smartVaultAddr)
	f.chain.allVaults = []common.Address{susds, smart}
	f.chain.vaultData[susds] = readFixture(t, "vault_entire_data_single_susds.hex")
	f.chain.vaultData[smart] = readFixture(t, "vault_entire_data_smart.hex")
	f.chain.tokenSymbol[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = "sUSDS"
	f.chain.tokenDec[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = 18

	if err := f.svc.ReconcileVaults(context.Background(), 100); err != nil {
		t.Fatalf("ReconcileVaults: %v", err)
	}
	if !f.svc.registry.IsKnownVault(susds) {
		t.Errorf("expected sUSDS vault registered")
	}
	if f.svc.registry.IsKnownVault(smart) {
		t.Errorf("smart vault must not be registered")
	}
}

// TestReconcileVaults_UnservableVaultSkippedAndRetried: an unknown vault the
// resolver cannot serve (no vaultData entry -> best-effort nil) is left unknown
// — neither registered nor cached as not-vault — while a servable sibling is
// registered. A later reconcile, once the resolver can serve it, registers it,
// pinning the "retried later" contract for the nil best-effort branch.
func TestReconcileVaults_UnservableVaultSkippedAndRetried(t *testing.T) {
	f := newServiceForTest(t)
	servable := common.HexToAddress(susdsVaultAddr)
	unservable := common.HexToAddress(smartVaultAddr)
	f.chain.allVaults = []common.Address{servable, unservable}
	f.chain.vaultData[servable] = readFixture(t, "vault_entire_data_single_susds.hex")
	// No vaultData entry for unservable -> fakeChain returns Success:false ->
	// best-effort nil entry.
	f.chain.tokenSymbol[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = "sUSDS"
	f.chain.tokenDec[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = 18

	if err := f.svc.ReconcileVaults(context.Background(), 100); err != nil {
		t.Fatalf("ReconcileVaults: %v", err)
	}
	if !f.svc.registry.IsKnownVault(servable) {
		t.Errorf("servable vault should be registered")
	}
	if f.svc.registry.IsKnownVault(unservable) {
		t.Errorf("unservable vault must not be registered")
	}
	if f.svc.registry.IsKnownNotVault(unservable) {
		t.Errorf("unservable vault must not be cached as not-vault (gap may be transient)")
	}

	// Resolver can now serve the previously-unservable vault; a second reconcile
	// must pick it up. The blob decodes to the unservable address itself (an
	// in-scope sUSDS plain vault) so it registers under that address.
	f.chain.vaultData[unservable] = packVaultEntireData(t, unservable, ethSentinel, SUSDSAddress, false, false,
		big.NewInt(1), big.NewInt(1), big.NewInt(0), big.NewInt(0))
	if err := f.svc.ReconcileVaults(context.Background(), 101); err != nil {
		t.Fatalf("second ReconcileVaults: %v", err)
	}
	if !f.svc.registry.IsKnownVault(unservable) {
		t.Errorf("previously-unservable vault should be registered on retry")
	}
}

// TestReconcileVaults_ClassifyRegisterError: a repo upsert error while
// registering a reconciled vault fails ReconcileVaults with wrapped context.
func TestReconcileVaults_ClassifyRegisterError(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	f.chain.allVaults = []common.Address{vault}
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")
	f.chain.tokenSymbol[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = "sUSDS"
	f.chain.tokenDec[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = 18
	f.repo.upsertErr = errTest

	err := f.svc.ReconcileVaults(context.Background(), 100)
	if err == nil {
		t.Fatal("expected error when a reconciled vault fails to persist")
	}
	if !strings.Contains(err.Error(), "registering vault") {
		t.Errorf("error %q should wrap %q", err.Error(), "registering vault")
	}
}

func TestReconcileVaults_SkipsAlreadyKnown(t *testing.T) {
	f := newServiceForTest(t)
	known := common.HexToAddress(susdsVaultAddr)
	f.svc.registry.RegisterVault(&entity.FluidVault{
		ID: 1, ChainID: 1, ProtocolID: 42, Address: known.Bytes(),
		VaultType: "10000", CollateralTokenID: 1, DebtTokenID: 2, CreatedAtBlock: 1,
	})
	f.chain.allVaults = []common.Address{known}
	// No vaultData entry for known — if the reconcile tried to read it, the call
	// would fail. It must skip the already-known address entirely.
	if err := f.svc.ReconcileVaults(context.Background(), 100); err != nil {
		t.Fatalf("ReconcileVaults: %v", err)
	}
	if len(f.repo.upserted) != 0 {
		t.Errorf("known vault should not be re-upserted")
	}
}

func TestReconcileVaults_EmptyEnumerationNoOp(t *testing.T) {
	f := newServiceForTest(t)
	f.chain.allVaults = nil // getAllVaultsAddresses returns empty -> no error, no work
	if err := f.svc.ReconcileVaults(context.Background(), 100); err != nil {
		t.Fatalf("empty enumerate should be a no-op, got: %v", err)
	}
}

func TestReconcileVaults_EnumerateError(t *testing.T) {
	f := newServiceForTest(t)
	f.chain.executeErr = errTest
	err := f.svc.ReconcileVaults(context.Background(), 100)
	if err == nil {
		t.Fatal("expected error when vault enumeration fails")
	}
	if !strings.Contains(err.Error(), "enumerating vaults") {
		t.Errorf("error %q should wrap %q", err.Error(), "enumerating vaults")
	}
}

// TestReconcileVaults_ReadDataError: the enumeration succeeds, but the
// getVaultEntireData multicall for the unknown vaults fails wholesale (an Execute
// error, not a per-vault revert), which must propagate wrapped.
func TestReconcileVaults_ReadDataError(t *testing.T) {
	f := newServiceForTest(t)
	f.chain.allVaults = []common.Address{common.HexToAddress(susdsVaultAddr)}
	f.chain.executeErrAfterGetAll = errTest
	err := f.svc.ReconcileVaults(context.Background(), 100)
	if err == nil {
		t.Fatal("expected error when reading vault data fails")
	}
	if !strings.Contains(err.Error(), "reading vault data for reconcile") {
		t.Errorf("error %q should wrap %q", err.Error(), "reading vault data for reconcile")
	}
}

func TestFetchReceipts_CacheError(t *testing.T) {
	f := newServiceForTest(t)
	f.cache.err = errTest
	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err == nil {
		t.Fatal("expected error when cache read fails")
	}
}

func TestFetchReceipts_MalformedJSON(t *testing.T) {
	f := newServiceForTest(t)
	f.cache.receipts[10] = json.RawMessage(`{not valid`)
	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err == nil {
		t.Fatal("expected error unmarshalling malformed receipts")
	}
}

func TestDiscoverDeployedVault_MissingVaultTopic(t *testing.T) {
	f := newServiceForTest(t)
	// VaultDeployed topic0 present but no indexed vault topic.
	f.cache.receipts[10] = receiptsWithLog(t, FluidVaultFactoryAddress, f.svc.deployedTopic)
	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err == nil {
		t.Fatal("expected error for VaultDeployed log missing vault topic")
	}
}

func TestClassifyAndRegister_TokenMetadataError(t *testing.T) {
	f := newServiceForTest(t)
	vault := common.HexToAddress(susdsVaultAddr)
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")
	f.tokenRepo.err = errTest // GetOrCreateToken fails inside the tx
	f.chain.tokenSymbol[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = "sUSDS"
	f.chain.tokenDec[common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")] = 18
	f.cache.receipts[10] = receiptsWithLog(t, FluidVaultFactoryAddress,
		f.svc.deployedTopic, common.BytesToHash(vault.Bytes()))
	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err == nil {
		t.Fatal("expected error when token persistence fails")
	}
}

func TestProcessBlockEvent_LogWithNoTopicsIgnored(t *testing.T) {
	f := newServiceForTest(t)
	receipts := []shared.TransactionReceipt{{Logs: []shared.Log{{Address: FluidVaultFactoryAddress.Hex()}}}}
	raw, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshalling receipts: %v", err)
	}
	f.cache.receipts[10] = raw
	if err := f.svc.processBlockEvent(context.Background(), blockEvent(10)); err != nil {
		t.Fatalf("a topic-less log must be ignored, got: %v", err)
	}
}
