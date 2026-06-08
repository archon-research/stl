package curve_dex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// -----------------------------------------------------------------------------
// Additional helpers used across the new tests.
// -----------------------------------------------------------------------------

// poolStateResultsNG returns mock results matching readPoolState's call layout
// for a 2-coin NG pool (balances×2 + virtual_price + A + fee + last_price(0) +
// price_oracle(0) + no-arg last_price() + no-arg price_oracle() + totalSupply +
// get_dy×2). NG pools have NCoins=2 ⇒ one indexed last_price/price_oracle pair
// plus the factory-v2 no-arg fallback pair. Here the indexed calls succeed, so
// the fallback values must be ignored.
func (h *harness) poolStateResultsNG() []outbound.Result {
	return []outbound.Result{
		{Success: true, ReturnData: h.packUint256(big.NewInt(100))},      // balances(0)
		{Success: true, ReturnData: h.packUint256(big.NewInt(200))},      // balances(1)
		{Success: true, ReturnData: h.packUint256(big.NewInt(1e9))},      // get_virtual_price
		{Success: true, ReturnData: h.packUint256(big.NewInt(100))},      // A
		{Success: true, ReturnData: h.packUint256(big.NewInt(4e6))},      // fee
		{Success: true, ReturnData: h.packUint256(big.NewInt(2e18))},     // last_price(0)
		{Success: true, ReturnData: h.packUint256(big.NewInt(1_999e15))}, // price_oracle(0)
		{Success: false}, // last_price() — no-arg variant absent on true NG
		{Success: false}, // price_oracle() — no-arg variant absent on true NG
		{Success: true, ReturnData: h.packUint256(big.NewInt(300))}, // totalSupply
		{Success: true, ReturnData: h.packUint256(big.NewInt(99))},  // get_dy(0,1)
		{Success: true, ReturnData: h.packUint256(big.NewInt(101))}, // get_dy(1,0)
	}
}

// packAddress encodes a single address as 32-byte ABI data.
func (h *harness) packAddress(a common.Address) []byte {
	t, _ := abi.NewType("address", "", nil)
	args := abi.Arguments{{Type: t}}
	out, err := args.Pack(a)
	if err != nil {
		h.t.Fatalf("packAddress: %v", err)
	}
	return out
}

// makeGauge builds a gauge entity bound to the given pool ID and address.
func makeGauge(poolID int64, addr common.Address) *entity.CurveGauge {
	return &entity.CurveGauge{
		ID:          11,
		CurvePoolID: poolID,
		ChainID:     1,
		Address:     addr,
		Enabled:     true,
	}
}

// makeNGSwapLog builds an NG TokenExchange or TokenExchangeUnderlying log.
func (h *harness) makeNGSwapLog(eventName string, poolAddr common.Address, buyer common.Address, soldID, boughtID int64, sold, bought *big.Int) shared.Log {
	ev := h.ngEventsABI.Events[eventName]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(soldID), sold, big.NewInt(boughtID), bought)
	if err != nil {
		h.t.Fatalf("packing %s data: %v", eventName, err)
	}
	return shared.Log{
		Address: poolAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(buyer.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x2",
	}
}

// makeAddLiquidityLog builds a V1 AddLiquidity log.
func (h *harness) makeAddLiquidityLog(poolAddr, provider common.Address, amounts, fees []*big.Int, invariant, totalSupply *big.Int) shared.Log {
	ev := h.v1EventsABI.Events["AddLiquidity"]
	data, err := ev.Inputs.NonIndexed().Pack(amounts, fees, invariant, totalSupply)
	if err != nil {
		h.t.Fatalf("packing AddLiquidity data: %v", err)
	}
	return shared.Log{
		Address: poolAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x3",
	}
}

// makeRemoveLiquidityLog builds a V1 RemoveLiquidity log (no invariant).
func (h *harness) makeRemoveLiquidityLog(poolAddr, provider common.Address, amounts, fees []*big.Int, totalSupply *big.Int) shared.Log {
	ev := h.v1EventsABI.Events["RemoveLiquidity"]
	data, err := ev.Inputs.NonIndexed().Pack(amounts, fees, totalSupply)
	if err != nil {
		h.t.Fatalf("packing RemoveLiquidity data: %v", err)
	}
	return shared.Log{
		Address: poolAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x4",
	}
}

// makeRemoveLiquidityImbalanceLog builds a V1 RemoveLiquidityImbalance log.
func (h *harness) makeRemoveLiquidityImbalanceLog(poolAddr, provider common.Address, amounts, fees []*big.Int, invariant, totalSupply *big.Int) shared.Log {
	ev := h.v1EventsABI.Events["RemoveLiquidityImbalance"]
	data, err := ev.Inputs.NonIndexed().Pack(amounts, fees, invariant, totalSupply)
	if err != nil {
		h.t.Fatalf("packing RemoveLiquidityImbalance data: %v", err)
	}
	return shared.Log{
		Address: poolAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x5",
	}
}

// makeRemoveLiquidityOneV1Log builds a V1 RemoveLiquidityOne log (3 fields).
func (h *harness) makeRemoveLiquidityOneV1Log(poolAddr, provider common.Address, tokenAmount, coinAmount *big.Int) shared.Log {
	ev := h.v1EventsABI.Events["RemoveLiquidityOne"]
	data, err := ev.Inputs.NonIndexed().Pack(tokenAmount, coinAmount)
	if err != nil {
		h.t.Fatalf("packing V1 RemoveLiquidityOne data: %v", err)
	}
	return shared.Log{
		Address: poolAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x6",
	}
}

// makeRemoveLiquidityOneNGLog builds an NG RemoveLiquidityOne log (5 fields).
func (h *harness) makeRemoveLiquidityOneNGLog(poolAddr, provider common.Address, tokenAmount, coinAmount, coinIndex, tokenSupply *big.Int) shared.Log {
	ev := h.ngEventsABI.Events["RemoveLiquidityOne"]
	data, err := ev.Inputs.NonIndexed().Pack(tokenAmount, coinAmount, coinIndex, tokenSupply)
	if err != nil {
		h.t.Fatalf("packing NG RemoveLiquidityOne data: %v", err)
	}
	return shared.Log{
		Address: poolAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x7",
	}
}

// makeNewFeeLog builds a V1 NewFee(fee, admin_fee) log.
func (h *harness) makeNewFeeLog(poolAddr common.Address, fee, adminFee *big.Int) shared.Log {
	ev := h.v1EventsABI.Events["NewFee"]
	data, err := ev.Inputs.NonIndexed().Pack(fee, adminFee)
	if err != nil {
		h.t.Fatalf("packing NewFee data: %v", err)
	}
	return shared.Log{
		Address:         poolAddr.Hex(),
		Topics:          []string{ev.ID.Hex()},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x8",
	}
}

// makeApplyNewFeeLog builds an NG ApplyNewFee(fee, offpeg_fee_multiplier) log.
func (h *harness) makeApplyNewFeeLog(poolAddr common.Address, fee, offpegMultiplier *big.Int) shared.Log {
	ev := h.ngEventsABI.Events["ApplyNewFee"]
	data, err := ev.Inputs.NonIndexed().Pack(fee, offpegMultiplier)
	if err != nil {
		h.t.Fatalf("packing ApplyNewFee data: %v", err)
	}
	return shared.Log{
		Address:         poolAddr.Hex(),
		Topics:          []string{ev.ID.Hex()},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x9",
	}
}

// makeRampALog builds a RampA(old_A, new_A, initial_time, future_time) log.
func (h *harness) makeRampALog(poolAddr common.Address, oldA, newA, initialTime, futureTime *big.Int) shared.Log {
	ev := h.v1EventsABI.Events["RampA"]
	data, err := ev.Inputs.NonIndexed().Pack(oldA, newA, initialTime, futureTime)
	if err != nil {
		h.t.Fatalf("packing RampA data: %v", err)
	}
	return shared.Log{
		Address:         poolAddr.Hex(),
		Topics:          []string{ev.ID.Hex()},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0xa",
	}
}

// makeStopRampALog builds a StopRampA(A, t) log.
func (h *harness) makeStopRampALog(poolAddr common.Address, a, t *big.Int) shared.Log {
	ev := h.v1EventsABI.Events["StopRampA"]
	data, err := ev.Inputs.NonIndexed().Pack(a, t)
	if err != nil {
		h.t.Fatalf("packing StopRampA data: %v", err)
	}
	return shared.Log{
		Address:         poolAddr.Hex(),
		Topics:          []string{ev.ID.Hex()},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0xb",
	}
}

// makeGaugeDepositLog builds a gauge Deposit(provider, value) log.
func (h *harness) makeGaugeDepositLog(gaugeAddr, provider common.Address, value *big.Int) shared.Log {
	ev := h.gaugeABI.Events["Deposit"]
	data, err := ev.Inputs.NonIndexed().Pack(value)
	if err != nil {
		h.t.Fatalf("packing Deposit data: %v", err)
	}
	return shared.Log{
		Address: gaugeAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0xc",
	}
}

// makeGaugeWithdrawLog builds a gauge Withdraw(provider, value) log.
func (h *harness) makeGaugeWithdrawLog(gaugeAddr, provider common.Address, value *big.Int) shared.Log {
	ev := h.gaugeABI.Events["Withdraw"]
	data, err := ev.Inputs.NonIndexed().Pack(value)
	if err != nil {
		h.t.Fatalf("packing Withdraw data: %v", err)
	}
	return shared.Log{
		Address: gaugeAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0xd",
	}
}

// makeUpdateLiquidityLimitLog builds a gauge UpdateLiquidityLimit log.
func (h *harness) makeUpdateLiquidityLimitLog(gaugeAddr, user common.Address, origBal, origSup, workBal, workSup *big.Int) shared.Log {
	ev := h.gaugeABI.Events["UpdateLiquidityLimit"]
	data, err := ev.Inputs.NonIndexed().Pack(user, origBal, origSup, workBal, workSup)
	if err != nil {
		h.t.Fatalf("packing UpdateLiquidityLimit data: %v", err)
	}
	return shared.Log{
		Address:         gaugeAddr.Hex(),
		Topics:          []string{ev.ID.Hex()},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0xe",
	}
}

// makeKilledLog builds a controller-emitted Killed(addr) log (addr indexed).
func (h *harness) makeKilledLog(gaugeAddr common.Address) shared.Log {
	ev := h.gcABI.Events["Killed"]
	return shared.Log{
		Address: CurveGaugeControllerAddress.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(gaugeAddr.Bytes()).Hex(),
		},
		Data:            "0x",
		TransactionHash: testTxHash,
		LogIndex:        "0xf",
	}
}

// runProcess pushes the receipt into the cache and invokes processBlockEvent.
func (h *harness) runProcess(t *testing.T, blockNumber int64, blockVersion int, receipt shared.TransactionReceipt) error {
	t.Helper()
	body, err := json.Marshal([]shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	h.cache.SetReceipts(1, blockNumber, blockVersion, body)
	return h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        blockVersion,
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	})
}

// -----------------------------------------------------------------------------
// A. Event-decoder coverage
// -----------------------------------------------------------------------------

func TestProcessReceipt_NGTokenExchange_WritesNGStateWithOracles(t *testing.T) {
	h := newHarness(t)
	pool := makePoolNG()
	h.svc.registry.addPool(pool)

	var savedSwap *entity.CurvePoolSwap
	var savedState *entity.CurvePoolState
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolSwap) error {
		savedSwap = s
		return nil
	}
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolState) error {
		savedState = s
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsNG(), nil
	}

	log := h.makeNGSwapLog("TokenExchange", pool.Address, testUser, 0, 1, big.NewInt(70), big.NewInt(69))
	if err := h.runProcess(t, 100, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if savedSwap == nil {
		t.Fatal("NG swap row not persisted")
	}
	if savedSwap.TokensSold.Int64() != 70 {
		t.Errorf("tokens sold = %s, want 70", savedSwap.TokensSold)
	}
	if savedState == nil {
		t.Fatal("NG pool state row not persisted")
	}
	if len(savedState.LastPrice) != 1 || savedState.LastPrice[0] == nil {
		t.Errorf("LastPrice = %v, want non-nil 1-element slice", savedState.LastPrice)
	}
	if len(savedState.PriceOracle) != 1 || savedState.PriceOracle[0] == nil {
		t.Errorf("PriceOracle = %v, want non-nil 1-element slice", savedState.PriceOracle)
	}
}

// Factory-v2-era 2-coin NG pools (e.g. stETH-ng 0x21E27a5E…843a) expose
// last_price()/price_oracle() WITHOUT the index argument — the indexed
// last_price(uint256) selector deterministically reverts there. Verified live
// at block 25229189 in the 2026-06-02 E2E run. The worker must fall back to
// the no-arg variant so the EMA oracle for the pool is captured instead of
// silently storing NULL forever.
func TestProcessReceipt_NGTokenExchange_NoArgOracleFallback(t *testing.T) {
	h := newHarness(t)
	pool := makePoolNG()
	h.svc.registry.addPool(pool)

	var savedState *entity.CurvePoolState
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolState) error {
		savedState = s
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		// 12-call layout for a 2-coin NG pool: balances×2, vp, A, fee,
		// last_price(0), price_oracle(0), last_price(), price_oracle(),
		// totalSupply, get_dy×2. Indexed oracle calls revert (factory-v2
		// pool); the no-arg variants succeed.
		return []outbound.Result{
			{Success: true, ReturnData: h.packUint256(big.NewInt(100))}, // balances(0)
			{Success: true, ReturnData: h.packUint256(big.NewInt(200))}, // balances(1)
			{Success: true, ReturnData: h.packUint256(big.NewInt(1e9))}, // get_virtual_price
			{Success: true, ReturnData: h.packUint256(big.NewInt(100))}, // A
			{Success: true, ReturnData: h.packUint256(big.NewInt(4e6))}, // fee
			{Success: false}, // last_price(0) — reverts
			{Success: false}, // price_oracle(0) — reverts
			{Success: true, ReturnData: h.packUint256(big.NewInt(999847548204443202))}, // last_price()
			{Success: true, ReturnData: h.packUint256(big.NewInt(999844490224437801))}, // price_oracle()
			{Success: true, ReturnData: h.packUint256(big.NewInt(300))},                // totalSupply
			{Success: true, ReturnData: h.packUint256(big.NewInt(99))},                 // get_dy(0,1)
			{Success: true, ReturnData: h.packUint256(big.NewInt(101))},                // get_dy(1,0)
		}, nil
	}

	log := h.makeNGSwapLog("TokenExchange", pool.Address, testUser, 0, 1, big.NewInt(70), big.NewInt(69))
	if err := h.runProcess(t, 100, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if savedState == nil {
		t.Fatal("NG pool state row not persisted")
	}
	if len(savedState.LastPrice) != 1 || savedState.LastPrice[0] == nil ||
		savedState.LastPrice[0].Int64() != 999847548204443202 {
		t.Errorf("LastPrice = %v, want [999847548204443202] from no-arg fallback", savedState.LastPrice)
	}
	if len(savedState.PriceOracle) != 1 || savedState.PriceOracle[0] == nil ||
		savedState.PriceOracle[0].Int64() != 999844490224437801 {
		t.Errorf("PriceOracle = %v, want [999844490224437801] from no-arg fallback", savedState.PriceOracle)
	}
}

func TestProcessReceipt_NGTokenExchangeUnderlying_WritesSwap(t *testing.T) {
	h := newHarness(t)
	pool := makePoolNG()
	h.svc.registry.addPool(pool)

	var savedSwap *entity.CurvePoolSwap
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolSwap) error {
		savedSwap = s
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsNG(), nil
	}

	log := h.makeNGSwapLog("TokenExchangeUnderlying", pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	if err := h.runProcess(t, 101, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if savedSwap == nil {
		t.Fatal("NG underlying swap row not persisted")
	}
	if savedSwap.TokensSold.Int64() != 50 || savedSwap.TokensBought.Int64() != 49 {
		t.Errorf("swap amounts = (%s,%s), want (50,49)", savedSwap.TokensSold, savedSwap.TokensBought)
	}
	// S1/N-CURVE1: underlying vs wrapped variants must be distinguishable in
	// curve_pool_swap. Without the is_underlying column, the (sold_id, bought_id)
	// indices are ambiguous between the wrapped and underlying coin sets.
	if !savedSwap.IsUnderlying {
		t.Error("IsUnderlying = false for TokenExchangeUnderlying event, want true")
	}
}

// Wrapped TokenExchange must NOT set IsUnderlying.
func TestProcessReceipt_NGTokenExchange_IsNotUnderlying(t *testing.T) {
	h := newHarness(t)
	pool := makePoolNG()
	h.svc.registry.addPool(pool)
	var savedSwap *entity.CurvePoolSwap
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolSwap) error {
		savedSwap = s
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsNG(), nil
	}
	log := h.makeNGSwapLog("TokenExchange", pool.Address, testUser, 0, 1, big.NewInt(100), big.NewInt(99))
	if err := h.runProcess(t, 105, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if savedSwap == nil {
		t.Fatal("swap row not persisted")
	}
	if savedSwap.IsUnderlying {
		t.Error("IsUnderlying = true for wrapped TokenExchange, want false")
	}
}

func TestProcessReceipt_AddLiquidity_V1_WritesLiquidityEvent(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolLiquidityEvent
	h.curveRepo.SaveCurvePoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolLiquidityEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeAddLiquidityLog(pool.Address, testUser,
		[]*big.Int{big.NewInt(10), big.NewInt(20)},
		[]*big.Int{big.NewInt(1), big.NewInt(2)},
		big.NewInt(12345),
		big.NewInt(67890),
	)
	if err := h.runProcess(t, 200, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("AddLiquidity row not persisted")
	}
	if saved.EventKind != entity.CurveLiquidityEventAddLiquidity {
		t.Errorf("EventKind = %q, want %q", saved.EventKind, entity.CurveLiquidityEventAddLiquidity)
	}
	if saved.InvariantD == nil || saved.InvariantD.Int64() != 12345 {
		t.Errorf("InvariantD = %v, want 12345", saved.InvariantD)
	}
	if saved.TokenSupply == nil || saved.TokenSupply.Int64() != 67890 {
		t.Errorf("TokenSupply = %v, want 67890", saved.TokenSupply)
	}
	if len(saved.TokenAmounts) != 2 || saved.TokenAmounts[1].Int64() != 20 {
		t.Errorf("TokenAmounts = %v, want [10,20]", saved.TokenAmounts)
	}
	if len(saved.Fees) != 2 || saved.Fees[0].Int64() != 1 {
		t.Errorf("Fees = %v, want [1,2]", saved.Fees)
	}
}

func TestProcessReceipt_RemoveLiquidity_V1_NoInvariant(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolLiquidityEvent
	h.curveRepo.SaveCurvePoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolLiquidityEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeRemoveLiquidityLog(pool.Address, testUser,
		[]*big.Int{big.NewInt(5), big.NewInt(15)},
		[]*big.Int{big.NewInt(0), big.NewInt(0)},
		big.NewInt(1000),
	)
	if err := h.runProcess(t, 201, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("RemoveLiquidity row not persisted")
	}
	if saved.EventKind != entity.CurveLiquidityEventRemoveLiquidity {
		t.Errorf("EventKind = %q, want %q", saved.EventKind, entity.CurveLiquidityEventRemoveLiquidity)
	}
	if saved.InvariantD != nil {
		t.Errorf("InvariantD = %v, want nil (V1 RemoveLiquidity has no invariant)", saved.InvariantD)
	}
	if saved.TokenSupply == nil || saved.TokenSupply.Int64() != 1000 {
		t.Errorf("TokenSupply = %v, want 1000", saved.TokenSupply)
	}
}

func TestProcessReceipt_RemoveLiquidityOne_V1_NoCoinIndex(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolLiquidityEvent
	h.curveRepo.SaveCurvePoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolLiquidityEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeRemoveLiquidityOneV1Log(pool.Address, testUser, big.NewInt(50), big.NewInt(48))
	if err := h.runProcess(t, 202, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("V1 RemoveLiquidityOne row not persisted")
	}
	if saved.EventKind != entity.CurveLiquidityEventRemoveLiquidityOne {
		t.Errorf("EventKind = %q, want %q", saved.EventKind, entity.CurveLiquidityEventRemoveLiquidityOne)
	}
	if saved.TokenAmount == nil || saved.TokenAmount.Int64() != 50 {
		t.Errorf("TokenAmount = %v, want 50", saved.TokenAmount)
	}
	if saved.CoinAmount == nil || saved.CoinAmount.Int64() != 48 {
		t.Errorf("CoinAmount = %v, want 48", saved.CoinAmount)
	}
	if saved.CoinIndex != nil {
		t.Errorf("CoinIndex = %v, want nil for V1", saved.CoinIndex)
	}
}

func TestProcessReceipt_RemoveLiquidityOne_NG_HasCoinIndex(t *testing.T) {
	h := newHarness(t)
	pool := makePoolNG()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolLiquidityEvent
	h.curveRepo.SaveCurvePoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolLiquidityEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsNG(), nil
	}

	log := h.makeRemoveLiquidityOneNGLog(pool.Address, testUser, big.NewInt(60), big.NewInt(58), big.NewInt(1), big.NewInt(99))
	if err := h.runProcess(t, 203, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("NG RemoveLiquidityOne row not persisted")
	}
	if saved.CoinIndex == nil || *saved.CoinIndex != 1 {
		t.Errorf("CoinIndex = %v, want 1", saved.CoinIndex)
	}
	if saved.TokenSupply == nil || saved.TokenSupply.Int64() != 99 {
		t.Errorf("TokenSupply = %v, want 99", saved.TokenSupply)
	}
}

func TestProcessReceipt_RemoveLiquidityImbalance_V1_AllFields(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolLiquidityEvent
	h.curveRepo.SaveCurvePoolLiquidityEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolLiquidityEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeRemoveLiquidityImbalanceLog(pool.Address, testUser,
		[]*big.Int{big.NewInt(7), big.NewInt(3)},
		[]*big.Int{big.NewInt(0), big.NewInt(0)},
		big.NewInt(555),
		big.NewInt(888),
	)
	if err := h.runProcess(t, 204, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("RemoveLiquidityImbalance row not persisted")
	}
	if saved.EventKind != entity.CurveLiquidityEventRemoveLiquidityImbalance {
		t.Errorf("EventKind = %q, want %q", saved.EventKind, entity.CurveLiquidityEventRemoveLiquidityImbalance)
	}
	if saved.InvariantD == nil || saved.InvariantD.Int64() != 555 {
		t.Errorf("InvariantD = %v, want 555", saved.InvariantD)
	}
	if saved.TokenSupply == nil || saved.TokenSupply.Int64() != 888 {
		t.Errorf("TokenSupply = %v, want 888", saved.TokenSupply)
	}
}

func TestProcessReceipt_NewFee_V1_WritesParameterEvent(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolParameterEvent
	h.curveRepo.SaveCurvePoolParameterEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolParameterEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeNewFeeLog(pool.Address, big.NewInt(4_000_000), big.NewInt(5_000_000_000))
	if err := h.runProcess(t, 205, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("NewFee row not persisted")
	}
	if saved.EventKind != entity.CurveParameterEventNewFee {
		t.Errorf("EventKind = %q, want %q (whitelist per migration CHECK)", saved.EventKind, entity.CurveParameterEventNewFee)
	}
	if saved.NewFee == nil || saved.NewFee.Int64() != 4_000_000 {
		t.Errorf("NewFee = %v, want 4_000_000", saved.NewFee)
	}
	if saved.NewAdminFee == nil || saved.NewAdminFee.Int64() != 5_000_000_000 {
		t.Errorf("NewAdminFee = %v, want 5e9", saved.NewAdminFee)
	}
}

func TestProcessReceipt_ApplyNewFee_NG_NormalisedToNewFeeWithExtra(t *testing.T) {
	h := newHarness(t)
	pool := makePoolNG()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolParameterEvent
	h.curveRepo.SaveCurvePoolParameterEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolParameterEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsNG(), nil
	}

	log := h.makeApplyNewFeeLog(pool.Address, big.NewInt(2_000_000), big.NewInt(20_000_000_000))
	if err := h.runProcess(t, 206, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("ApplyNewFee row not persisted")
	}
	// Worker normalises NG ApplyNewFee to event_kind = "NewFee" so it satisfies
	// the CHECK constraint on curve_pool_parameter_event.event_kind.
	if saved.EventKind != entity.CurveParameterEventNewFee {
		t.Errorf("EventKind = %q, want %q (NG ApplyNewFee → NewFee in DB)", saved.EventKind, entity.CurveParameterEventNewFee)
	}
	if saved.NewFee == nil || saved.NewFee.Int64() != 2_000_000 {
		t.Errorf("NewFee = %v, want 2_000_000", saved.NewFee)
	}
	if len(saved.Extra) == 0 {
		t.Fatalf("Extra JSONB missing; want offpeg_fee_multiplier")
	}
	if !strings.Contains(string(saved.Extra), "offpeg_fee_multiplier") {
		t.Errorf("Extra = %s, want contains offpeg_fee_multiplier", saved.Extra)
	}
}

func TestProcessReceipt_RampA_WritesParameterEvent(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolParameterEvent
	h.curveRepo.SaveCurvePoolParameterEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolParameterEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeRampALog(pool.Address, big.NewInt(100), big.NewInt(200), big.NewInt(1_700_000_000), big.NewInt(1_700_086_400))
	if err := h.runProcess(t, 207, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("RampA row not persisted")
	}
	if saved.EventKind != entity.CurveParameterEventRampA {
		t.Errorf("EventKind = %q, want %q", saved.EventKind, entity.CurveParameterEventRampA)
	}
	if saved.OldA == nil || saved.OldA.Int64() != 100 {
		t.Errorf("OldA = %v, want 100", saved.OldA)
	}
	if saved.NewA == nil || saved.NewA.Int64() != 200 {
		t.Errorf("NewA = %v, want 200", saved.NewA)
	}
	if saved.InitialTime == nil || saved.FutureTime == nil {
		t.Errorf("InitialTime/FutureTime must be non-nil for RampA, got %v / %v", saved.InitialTime, saved.FutureTime)
	}
}

// S5: StopRampA must NOT overload the RampA columns. The on-chain event
// emits (A, t) — neither maps to old_a/new_a/initial_time/future_time
// semantically. We park them in `extra` JSONB so a consumer reading the
// RampA-shaped typed columns gets clean NULLs.
func TestProcessReceipt_StopRampA_ParkedInExtra(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var saved *entity.CurvePoolParameterEvent
	h.curveRepo.SaveCurvePoolParameterEventFn = func(_ context.Context, _ pgx.Tx, e *entity.CurvePoolParameterEvent) error {
		saved = e
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeStopRampALog(pool.Address, big.NewInt(150), big.NewInt(1_700_000_500))
	if err := h.runProcess(t, 208, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("StopRampA row not persisted")
	}
	if saved.EventKind != entity.CurveParameterEventStopRampA {
		t.Errorf("EventKind = %q, want %q", saved.EventKind, entity.CurveParameterEventStopRampA)
	}
	// Typed RampA columns must stay NULL for StopRampA — the on-chain event
	// doesn't supply old_a / future_time, and conflating "A at stop" with
	// "new_a from a RampA started later" would corrupt downstream queries.
	if saved.OldA != nil {
		t.Errorf("OldA = %v, want nil for StopRampA", saved.OldA)
	}
	if saved.NewA != nil {
		t.Errorf("NewA = %v, want nil for StopRampA (parked in Extra instead)", saved.NewA)
	}
	if saved.InitialTime != nil {
		t.Errorf("InitialTime = %v, want nil for StopRampA (parked in Extra instead)", saved.InitialTime)
	}
	if saved.FutureTime != nil {
		t.Errorf("FutureTime = %v, want nil for StopRampA", saved.FutureTime)
	}
	// The actual on-chain (A, t) lives in Extra so consumers can still
	// reconstruct the post-stop state.
	if len(saved.Extra) == 0 {
		t.Fatal("Extra JSONB must carry the StopRampA payload (stop_a, stopped_at_time)")
	}
	var extra map[string]string
	if err := json.Unmarshal(saved.Extra, &extra); err != nil {
		t.Fatalf("unmarshalling Extra: %v", err)
	}
	if extra["stop_a"] != "150" {
		t.Errorf("Extra.stop_a = %q, want %q", extra["stop_a"], "150")
	}
	if extra["stopped_at_time"] != "1700000500" {
		t.Errorf("Extra.stopped_at_time = %q, want %q", extra["stopped_at_time"], "1700000500")
	}
}

// gaugeWithStateMulticaller returns a mock Execute that handles BOTH a
// readGaugeState call (5 results) and a readGaugeRewards call (token+data),
// dispatching by len(calls). reward_count=0 ⇒ readGaugeRewards is never called.
func gaugeWithStateMulticaller(h *harness) func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error) {
	return func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		// readGaugeState issues exactly 5 view calls.
		if len(calls) == 5 {
			return h.gaugeStateResults(0), nil
		}
		return nil, fmt.Errorf("unexpected multicall layout: %d calls", len(calls))
	}
}

func TestProcessReceipt_GaugeDeposit_WritesGaugeState(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	var saved *entity.CurveGaugeState
	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurveGaugeState) error {
		saved = s
		return nil
	}
	h.multicaller.ExecuteFn = gaugeWithStateMulticaller(h)

	log := h.makeGaugeDepositLog(gauge.Address, testUser, big.NewInt(7777))
	if err := h.runProcess(t, 300, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("gauge state row not persisted")
	}
	if saved.CurveGaugeID != gauge.ID {
		t.Errorf("CurveGaugeID = %d, want %d", saved.CurveGaugeID, gauge.ID)
	}
	if saved.InflationRate == nil || saved.WorkingSupply == nil || saved.TotalSupply == nil {
		t.Errorf("gauge state must carry inflation/working/total, got %+v", saved)
	}
}

func TestProcessReceipt_GaugeWithdraw_WritesGaugeState(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	called := false
	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGaugeState) error {
		called = true
		return nil
	}
	h.multicaller.ExecuteFn = gaugeWithStateMulticaller(h)

	log := h.makeGaugeWithdrawLog(gauge.Address, testUser, big.NewInt(123))
	if err := h.runProcess(t, 301, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if !called {
		t.Error("gauge state save was not invoked for Withdraw")
	}
}

func TestProcessReceipt_UpdateLiquidityLimit_WritesGaugeState(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	called := false
	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGaugeState) error {
		called = true
		return nil
	}
	h.multicaller.ExecuteFn = gaugeWithStateMulticaller(h)

	log := h.makeUpdateLiquidityLimitLog(gauge.Address, testUser, big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4))
	if err := h.runProcess(t, 302, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if !called {
		t.Error("gauge state save was not invoked for UpdateLiquidityLimit")
	}
}

// V1 gauges (e.g. 3pool's 0xbFcF63294aD7105dEa65aA58F8AE5BE2D9d0952A, deployed
// 2020) predate is_killed() and reward_count() — both revert on-chain. The
// worker must degrade gracefully (IsKilled=nil, RewardCount=nil) instead of
// failing the block: the revert is deterministic, so erroring would poison the
// FIFO queue with infinite redeliveries. Found in the live E2E run 2026-06-02.
func TestProcessReceipt_V1GaugeWithoutIsKilled_DegradesGracefully(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	var saved *entity.CurveGaugeState
	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurveGaugeState) error {
		saved = s
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 5 { // readGaugeState layout
			return []outbound.Result{
				{Success: true, ReturnData: h.packUint256(big.NewInt(1e15))}, // inflation_rate
				{Success: true, ReturnData: h.packUint256(big.NewInt(500))},  // working_supply
				{Success: true, ReturnData: h.packUint256(big.NewInt(300))},  // totalSupply
				{Success: false}, // is_killed — V1 gauge: reverts
				{Success: false}, // reward_count — V1 gauge: reverts
			}, nil
		}
		return nil, fmt.Errorf("unexpected multicall layout: %d calls", len(calls))
	}

	log := h.makeGaugeDepositLog(gauge.Address, testUser, big.NewInt(42))
	if err := h.runProcess(t, 310, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent must not fail on a V1 gauge without is_killed/reward_count: %v", err)
	}
	if saved == nil {
		t.Fatal("gauge state row not persisted for V1 gauge")
	}
	if saved.IsKilled != nil {
		t.Errorf("IsKilled = %v, want nil for V1 gauge (method unsupported)", *saved.IsKilled)
	}
	if saved.RewardCount != nil {
		t.Errorf("RewardCount = %v, want nil for V1 gauge (method unsupported)", *saved.RewardCount)
	}
	if saved.InflationRate == nil || saved.WorkingSupply == nil || saved.TotalSupply == nil {
		t.Errorf("core V1 fields must still be captured, got %+v", saved)
	}
}

// PR #345: the registry is_killed flag must self-heal from the on-chain
// is_killed() read on an ordinary gauge event, even when no discrete
// Kill/Unkill controller event is seen — closing the "gauge-emitted kill is
// dropped" gap. A gauge Deposit whose on-chain is_killed()=true must flip the
// flag via SetCurveGaugeKilled.
func TestProcessReceipt_GaugeStateReconcilesIsKilled(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr) // makeGauge sets IsKilled=false baseline
	h.svc.registry.addGauge(gauge)

	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGaugeState) error { return nil }
	var killedTo *bool
	var killedAddr common.Address
	h.curveRepo.SetCurveGaugeKilledFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, isKilled bool) error {
		v := isKilled
		killedTo = &v
		killedAddr = addr
		return nil
	}
	// readGaugeState layout (5 calls) with is_killed=true at slot 3.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 5 {
			return []outbound.Result{
				{Success: true, ReturnData: h.packUint256(big.NewInt(1e15))},
				{Success: true, ReturnData: h.packUint256(big.NewInt(500))},
				{Success: true, ReturnData: h.packUint256(big.NewInt(300))},
				{Success: true, ReturnData: h.packBool(true)}, // is_killed = true
				{Success: true, ReturnData: h.packUint256(big.NewInt(0))},
			}, nil
		}
		return nil, fmt.Errorf("unexpected multicall layout: %d calls", len(calls))
	}

	log := h.makeGaugeDepositLog(gauge.Address, testUser, big.NewInt(1))
	if err := h.runProcess(t, 320, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if killedTo == nil || !*killedTo {
		t.Fatal("SetCurveGaugeKilled(true) not called — is_killed did not self-heal from the on-chain read")
	}
	if killedAddr != gauge.Address {
		t.Errorf("SetCurveGaugeKilled addr = %s, want %s", killedAddr.Hex(), gauge.Address.Hex())
	}
}

// An active gauge (is_killed=false, matching the baseline) must NOT write the
// flag on every event — the reconcile is gated on the loaded baseline.
func TestProcessReceipt_GaugeStateActive_NoKilledWrite(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGaugeState) error { return nil }
	called := false
	h.curveRepo.SetCurveGaugeKilledFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ bool) error {
		called = true
		return nil
	}
	h.multicaller.ExecuteFn = gaugeWithStateMulticaller(h) // is_killed=false

	log := h.makeGaugeDepositLog(gauge.Address, testUser, big.NewInt(1))
	if err := h.runProcess(t, 321, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if called {
		t.Error("SetCurveGaugeKilled should not be called for an active gauge matching the baseline")
	}
}

func TestProcessReceipt_GaugeControllerKilled_FlipsFlag(t *testing.T) {
	h := newHarness(t)
	gauge := makeGauge(1, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	var killed bool
	h.curveRepo.SetCurveGaugeKilledFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, isKilled bool) error {
		if addr == testGaugeAddr && isKilled {
			killed = true
		}
		return nil
	}

	log := h.makeKilledLog(testGaugeAddr)
	if err := h.runProcess(t, 303, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if !killed {
		t.Error("SetCurveGaugeKilled was not called for Killed event")
	}
}

// -----------------------------------------------------------------------------
// B. Critical SQS-worker edge cases
// -----------------------------------------------------------------------------

func TestProcessBlockEvent_IdempotentRedelivery_SameKeys(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var swaps []*entity.CurvePoolSwap
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolSwap) error {
		swaps = append(swaps, s)
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	for range 2 {
		if err := h.runProcess(t, 400, 0, receipt); err != nil {
			t.Fatalf("processBlockEvent: %v", err)
		}
	}
	if len(swaps) != 2 {
		t.Fatalf("expected 2 swap writes (mock doesn't dedup), got %d", len(swaps))
	}
	// The worker must pass IDENTICAL natural-key inputs both times so the
	// DB-layer dedup (trigger) can collapse them.
	if swaps[0].BlockNumber != swaps[1].BlockNumber ||
		swaps[0].BlockVersion != swaps[1].BlockVersion ||
		swaps[0].LogIndex != swaps[1].LogIndex ||
		swaps[0].TxHash != swaps[1].TxHash {
		t.Errorf("natural-key fields drifted between redeliveries: %+v vs %+v", swaps[0], swaps[1])
	}
}

func TestProcessBlockEvent_ReorgBumpsBlockVersion(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	var swaps []*entity.CurvePoolSwap
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolSwap) error {
		swaps = append(swaps, s)
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	receipt := shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}

	if err := h.runProcess(t, 500, 0, receipt); err != nil {
		t.Fatalf("processBlockEvent v0: %v", err)
	}
	if err := h.runProcess(t, 500, 1, receipt); err != nil {
		t.Fatalf("processBlockEvent v1: %v", err)
	}
	if len(swaps) != 2 {
		t.Fatalf("expected 2 swap writes across versions, got %d", len(swaps))
	}
	if swaps[0].BlockVersion == swaps[1].BlockVersion {
		t.Errorf("expected distinct block_versions, got %d and %d", swaps[0].BlockVersion, swaps[1].BlockVersion)
	}
}

func TestProcessBlockEvent_TxManagerError_Propagates(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	sentinel := errors.New("tx-boom")
	h.txManager.WithTransactionFn = func(_ context.Context, _ func(pgx.Tx) error) error {
		return sentinel
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	err := h.runProcess(t, 600, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected tx-manager error to propagate (SQS must NOT ack)")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel error, got %v", err)
	}
}

func TestProcessBlockEvent_MulticallError_Propagates(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	sentinel := errors.New("rpc-down")
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, sentinel
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	err := h.runProcess(t, 601, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected multicall error to propagate")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel error, got %v", err)
	}
}

func TestProcessBlockEvent_BlockWithNoRelevantEvents_NoWrites(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	swapCalled := false
	stateCalled := false
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolSwap) error {
		swapCalled = true
		return nil
	}
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolState) error {
		stateCalled = true
		return nil
	}
	eventCalled := false
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		eventCalled = true
		return nil
	}

	// Log from a random address with a random topic. Not in registry, not
	// LP-token transfer, not gauge, not gauge controller.
	stranger := common.HexToAddress("0xDeaDDEadDEADdEAdDeadDeADdEAddEaDDeAdDeAD")
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address:         stranger.Hex(),
			Topics:          []string{common.HexToHash("0xabcdef00").Hex()},
			Data:            "0x",
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}
	if err := h.runProcess(t, 700, 0, receipt); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if swapCalled || stateCalled || eventCalled {
		t.Errorf("unrelated log triggered writes (swap=%v state=%v event=%v)", swapCalled, stateCalled, eventCalled)
	}
}

func TestProcessReceipt_LPTransferMint_OnlyToSide(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	from := common.Address{} // zero ⇒ mint
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	ev := h.lpEventsABI.Events["Transfer"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(42))
	if err != nil {
		t.Fatalf("pack transfer: %v", err)
	}
	log := shared.Log{
		Address: testLPTokenAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	// On mint we expect ONE balanceOf call (only the To side).
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 1 {
			return nil, fmt.Errorf("mint should issue 1 balanceOf call, got %d", len(calls))
		}
		return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(42))}}, nil
	}

	var positions []*entity.CurveUserLpPosition
	h.curveRepo.SaveCurveUserLpPositionFn = func(_ context.Context, _ pgx.Tx, p *entity.CurveUserLpPosition) error {
		positions = append(positions, p)
		return nil
	}

	if err := h.runProcess(t, 800, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("mint should produce exactly 1 LP position row, got %d", len(positions))
	}
	if positions[0].UserAddress != to {
		t.Errorf("mint position user = %s, want %s", positions[0].UserAddress.Hex(), to.Hex())
	}
	if positions[0].Delta.Sign() <= 0 {
		t.Errorf("mint delta should be positive, got %s", positions[0].Delta)
	}
}

func TestProcessReceipt_LPTransferBurn_OnlyFromSide(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.Address{} // zero ⇒ burn
	ev := h.lpEventsABI.Events["Transfer"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(13))
	if err != nil {
		t.Fatalf("pack transfer: %v", err)
	}
	log := shared.Log{
		Address: testLPTokenAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 1 {
			return nil, fmt.Errorf("burn should issue 1 balanceOf call, got %d", len(calls))
		}
		return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(0))}}, nil
	}

	var positions []*entity.CurveUserLpPosition
	h.curveRepo.SaveCurveUserLpPositionFn = func(_ context.Context, _ pgx.Tx, p *entity.CurveUserLpPosition) error {
		positions = append(positions, p)
		return nil
	}

	if err := h.runProcess(t, 801, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("burn should produce exactly 1 LP position row, got %d", len(positions))
	}
	if positions[0].UserAddress != from {
		t.Errorf("burn position user = %s, want %s", positions[0].UserAddress.Hex(), from.Hex())
	}
	if positions[0].Delta.Sign() >= 0 {
		t.Errorf("burn delta should be negative, got %s", positions[0].Delta)
	}
}

// PR #345: a self-transfer (from == to) must collapse to ONE net-zero LP
// position row, not two rows with the same primary key (the second of which
// ON CONFLICT would silently drop, leaving a misleading non-zero delta).
func TestProcessReceipt_LPTransferSelf_CollapsesToOneNetZeroRow(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	user := common.HexToAddress("0x3333333333333333333333333333333333333333")
	ev := h.lpEventsABI.Events["Transfer"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(55))
	if err != nil {
		t.Fatalf("pack transfer: %v", err)
	}
	log := shared.Log{
		Address: testLPTokenAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(user.Bytes()).Hex(),
			common.BytesToHash(user.Bytes()).Hex(), // from == to
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	// Self-transfer must read the holder's balance exactly once.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 1 {
			return nil, fmt.Errorf("self-transfer should issue 1 balanceOf call, got %d", len(calls))
		}
		return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(999))}}, nil
	}

	var positions []*entity.CurveUserLpPosition
	h.curveRepo.SaveCurveUserLpPositionFn = func(_ context.Context, _ pgx.Tx, p *entity.CurveUserLpPosition) error {
		positions = append(positions, p)
		return nil
	}

	if err := h.runProcess(t, 802, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("self-transfer should produce exactly 1 LP position row, got %d", len(positions))
	}
	if positions[0].UserAddress != user {
		t.Errorf("position user = %s, want %s", positions[0].UserAddress.Hex(), user.Hex())
	}
	if positions[0].Delta.Sign() != 0 {
		t.Errorf("self-transfer delta must be 0 (net-zero), got %s", positions[0].Delta)
	}
	if positions[0].LpBalance == nil || positions[0].LpBalance.Int64() != 999 {
		t.Errorf("self-transfer balance = %v, want 999", positions[0].LpBalance)
	}
}

func TestProcessReceipt_PoolStateDedupAcrossLogsInOneReceipt(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	swapCount := 0
	stateCount := 0
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolSwap) error {
		swapCount++
		return nil
	}
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolState) error {
		stateCount++
		return nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	mk := func(logIdx string) shared.Log {
		ev := h.v1EventsABI.Events["TokenExchange"]
		data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(0), big.NewInt(10), big.NewInt(1), big.NewInt(9))
		if err != nil {
			t.Fatalf("pack: %v", err)
		}
		return shared.Log{
			Address: pool.Address.Hex(),
			Topics: []string{
				ev.ID.Hex(),
				common.BytesToHash(testUser.Bytes()).Hex(),
			},
			Data:            "0x" + common.Bytes2Hex(data),
			TransactionHash: testTxHash,
			LogIndex:        logIdx,
		}
	}
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs:            []shared.Log{mk("0x1"), mk("0x2"), mk("0x3")},
	}
	if err := h.runProcess(t, 900, 0, receipt); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if swapCount != 3 {
		t.Errorf("swap saves = %d, want 3 (one per log)", swapCount)
	}
	if stateCount != 1 {
		t.Errorf("pool state saves = %d, want 1 (dedup per receipt)", stateCount)
	}
}

func TestProcessReceipt_NewGauge_UntrackedPool_OnlyAuditRow(t *testing.T) {
	h := newHarness(t)
	// Note: NO pool registered with the LP token we'll return.

	newGaugeAddr := common.HexToAddress("0xb000000000000000000000000000000000000002")
	ev := h.gcABI.Events["NewGauge"]
	data, err := ev.Inputs.NonIndexed().Pack(newGaugeAddr, big.NewInt(0), big.NewInt(0))
	if err != nil {
		t.Fatalf("pack NewGauge: %v", err)
	}
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address:         CurveGaugeControllerAddress.Hex(),
			Topics:          []string{ev.ID.Hex()},
			Data:            "0x" + common.Bytes2Hex(data),
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}

	// Multicall sequence: gauge_types (= 0 → stableswap, OK) then lp_token
	// (returns a stranger LP not in our registry).
	stranger := common.HexToAddress("0xc000000000000000000000000000000000000003")
	int128T, _ := abi.NewType("int128", "", nil)
	gtypeOut, _ := (abi.Arguments{{Type: int128T}}).Pack(big.NewInt(0))
	calls := 0
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		calls++
		switch calls {
		case 1:
			return []outbound.Result{{Success: true, ReturnData: gtypeOut}}, nil
		case 2:
			return []outbound.Result{{Success: true, ReturnData: h.packAddress(stranger)}}, nil
		default:
			return nil, fmt.Errorf("unexpected multicall call #%d", calls)
		}
	}

	upsertCalled := false
	h.curveRepo.UpsertCurveGaugeFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGauge) (int64, error) {
		upsertCalled = true
		return 0, nil
	}
	auditCalled := false
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		auditCalled = true
		return nil
	}

	if err := h.runProcess(t, 1000, 0, receipt); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if upsertCalled {
		t.Error("UpsertCurveGauge should NOT be called for untracked pool")
	}
	if !auditCalled {
		t.Error("protocol_event audit row should still be persisted")
	}
}

// TestProcessReceipt_PoolEvent_WithAttachedGauge_WritesBothStates exercises the
// handlePoolLog branch where pool.gauge != nil — the worker reads pool state
// AND gauge state in the same handler and writes both in one transaction.
func TestProcessReceipt_PoolEvent_WithAttachedGauge_WritesBothStates(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	var poolState *entity.CurvePoolState
	var gaugeState *entity.CurveGaugeState
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolState) error {
		poolState = s
		return nil
	}
	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurveGaugeState) error {
		gaugeState = s
		return nil
	}

	// First Execute call → pool state (8 calls for V1 2-coin);
	// second Execute call → gauge state (5 calls, reward_count=0).
	callIdx := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callIdx++
		switch len(calls) {
		case 8:
			return h.poolStateResultsV1(), nil
		case 5:
			return h.gaugeStateResults(0), nil
		default:
			return nil, fmt.Errorf("unexpected call layout #%d: %d calls", callIdx, len(calls))
		}
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	if err := h.runProcess(t, 1100, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if poolState == nil {
		t.Fatal("pool state row not persisted")
	}
	if gaugeState == nil {
		t.Fatal("gauge state row not persisted")
	}
	if gaugeState.CurveGaugeID != gauge.ID {
		t.Errorf("gauge state CurveGaugeID = %d, want %d", gaugeState.CurveGaugeID, gauge.ID)
	}
}

// TestProcessReceipt_GaugeDeposit_WithRewards_ReadsRewardData exercises the
// readGaugeRewards path: reward_count=2 ⇒ two follow-up multicalls
// (reward_tokens then reward_data).
func TestProcessReceipt_GaugeDeposit_WithRewards_ReadsRewardData(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	reward1 := common.HexToAddress("0xD533a949740bb3306d119CC777fa900bA034cd52") // CRV
	reward2 := common.HexToAddress("0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32") // LDO

	// Encode reward_data tuple (token, distributor, period_finish, rate, last_update, integral).
	tAddr, _ := abi.NewType("address", "", nil)
	tU256, _ := abi.NewType("uint256", "", nil)
	rdArgs := abi.Arguments{
		{Type: tAddr}, {Type: tAddr}, {Type: tU256}, {Type: tU256}, {Type: tU256}, {Type: tU256},
	}
	encodeRewardData := func(token common.Address, periodFinish, rate int64) []byte {
		out, err := rdArgs.Pack(token, common.Address{}, big.NewInt(periodFinish), big.NewInt(rate), big.NewInt(0), big.NewInt(0))
		if err != nil {
			t.Fatalf("packing reward_data: %v", err)
		}
		return out
	}

	callIdx := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callIdx++
		switch callIdx {
		case 1:
			// readGaugeState — 5 results with reward_count=2.
			return h.gaugeStateResults(2), nil
		case 2:
			// readGaugeRewards step 1: reward_tokens(0), reward_tokens(1).
			return []outbound.Result{
				{Success: true, ReturnData: h.packAddress(reward1)},
				{Success: true, ReturnData: h.packAddress(reward2)},
			}, nil
		case 3:
			// readGaugeRewards step 2: reward_data(reward1), reward_data(reward2).
			return []outbound.Result{
				{Success: true, ReturnData: encodeRewardData(reward1, 1_700_086_400, 100)},
				{Success: true, ReturnData: encodeRewardData(reward2, 1_700_086_400, 50)},
			}, nil
		case 4, 5:
			// readERC20Metadata for each first-seen reward token: symbol() + decimals().
			strT, _ := abi.NewType("string", "", nil)
			uint8T, _ := abi.NewType("uint8", "", nil)
			symOut, _ := (abi.Arguments{{Type: strT}}).Pack("RW")
			decOut, _ := (abi.Arguments{{Type: uint8T}}).Pack(uint8(18))
			return []outbound.Result{
				{Success: true, ReturnData: symOut},
				{Success: true, ReturnData: decOut},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected extra Execute call #%d", callIdx)
		}
	}

	var saved *entity.CurveGaugeState
	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurveGaugeState) error {
		saved = s
		return nil
	}
	// Register the on-the-fly token registrations (CRV + LDO seed).
	tokenCalls := 0
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
		tokenCalls++
		return int64(tokenCalls), nil
	}

	log := h.makeGaugeDepositLog(gauge.Address, testUser, big.NewInt(9999))
	if err := h.runProcess(t, 1200, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if saved == nil {
		t.Fatal("gauge state row not persisted")
	}
	if len(saved.RewardTokens) != 2 {
		t.Errorf("RewardTokens len = %d, want 2", len(saved.RewardTokens))
	}
	if len(saved.RewardRates) != 2 || saved.RewardRates[0] == nil || saved.RewardRates[0].Int64() != 100 {
		t.Errorf("RewardRates = %v, want [100, 50]", saved.RewardRates)
	}
	if len(saved.RewardPeriodFinish) != 2 || saved.RewardPeriodFinish[0].IsZero() {
		t.Errorf("RewardPeriodFinish should carry timestamps, got %v", saved.RewardPeriodFinish)
	}
	if tokenCalls != 2 {
		t.Errorf("expected 2 GetOrCreateToken calls (one per reward), got %d", tokenCalls)
	}
}

func newHarnessWithTelemetry(t *testing.T, tel *dextelemetry.Telemetry) *harness {
	t.Helper()
	h := newHarness(t)
	h.svc.telemetry = tel
	return h
}

// N7-4: every invocation of processBlockEvent must emit exactly one
// curve.blocks.processed datapoint, labelled status=success on the happy
// path and status=error on the error path. Without this metric the alert
// rule curve_blocks_processed_total in alerts/vector-indexers.yaml cannot
// fire and a stalled worker has no pager.
func TestProcessBlockEvent_EmitsBlockProcessedMetric(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := dextelemetry.NewTelemetry("curve", 1)
	if err != nil {
		t.Fatalf("dextelemetry.NewTelemetry: %v", err)
	}
	h := newHarnessWithTelemetry(t, tel)

	// Happy path: a receipt with no Curve-relevant logs is a valid no-op,
	// so processBlockEvent returns nil and the metric records status=success.
	if err := h.runProcess(t, 100, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: nil}); err != nil {
		t.Fatalf("happy path processBlockEvent: %v", err)
	}

	// Error path: receipts missing from cache => processBlockEvent errors,
	// status=error must be recorded.
	errEvent := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    9999,
		Version:        0,
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}
	if err := h.svc.processBlockEvent(context.Background(), errEvent); err == nil {
		t.Fatal("expected processBlockEvent to fail when receipts not in cache")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	successCount, errorCount := readBlockCountersByStatus(t, &rm, "curve.blocks.processed")
	if successCount != 1 {
		t.Errorf("curve.blocks.processed{status=success} = %d, want 1", successCount)
	}
	if errorCount != 1 {
		t.Errorf("curve.blocks.processed{status=error} = %d, want 1", errorCount)
	}
}

func readBlockCountersByStatus(t *testing.T, rm *metricdata.ResourceMetrics, name string) (success, errCount int64) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s: unexpected metric type %T", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				status, _ := dp.Attributes.Value("status")
				switch status.AsString() {
				case "success":
					success += dp.Value
				case "error":
					errCount += dp.Value
				}
			}
			return success, errCount
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0, 0
}

// B3: the in-process registeredTokens cache must NOT be updated until the
// surrounding tx commits. Pre-fix, a tx rollback (e.g. SaveCurveGaugeState
// fails after GetOrCreateToken succeeded) left the cache thinking the token
// row existed when it didn't, permanently skipping the metadata read on
// subsequent gauge events for that token.
//
// The test forces a SaveCurveGaugeState failure after a successful
// GetOrCreateToken call and then asserts the cache stays empty so the SQS
// retry will re-register the token.
func TestSaveGaugeState_TxRollback_DoesNotPoisonTokenCache(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	reward := common.HexToAddress("0xD533a949740bb3306d119CC777fa900bA034cd52") // CRV

	tAddr, _ := abi.NewType("address", "", nil)
	tU256, _ := abi.NewType("uint256", "", nil)
	rdArgs := abi.Arguments{{Type: tAddr}, {Type: tAddr}, {Type: tU256}, {Type: tU256}, {Type: tU256}, {Type: tU256}}
	encodeRewardData := func() []byte {
		out, _ := rdArgs.Pack(reward, common.Address{}, big.NewInt(1_700_000_000), big.NewInt(7), big.NewInt(0), big.NewInt(0))
		return out
	}

	// 1: gauge state with reward_count=1
	// 2: reward_tokens(0) → reward
	// 3: reward_data(reward)
	// 4: readERC20Metadata(reward): symbol + decimals
	callIdx := 0
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callIdx++
		switch callIdx {
		case 1:
			return h.gaugeStateResults(1), nil
		case 2:
			return []outbound.Result{{Success: true, ReturnData: h.packAddress(reward)}}, nil
		case 3:
			return []outbound.Result{{Success: true, ReturnData: encodeRewardData()}}, nil
		case 4:
			strT, _ := abi.NewType("string", "", nil)
			uint8T, _ := abi.NewType("uint8", "", nil)
			symOut, _ := (abi.Arguments{{Type: strT}}).Pack("CRV")
			decOut, _ := (abi.Arguments{{Type: uint8T}}).Pack(uint8(18))
			return []outbound.Result{
				{Success: true, ReturnData: symOut},
				{Success: true, ReturnData: decOut},
			}, nil
		}
		return nil, fmt.Errorf("unexpected multicall #%d", callIdx)
	}

	tokenCalls := 0
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
		tokenCalls++
		return 1, nil
	}
	// Force SaveCurveGaugeState to fail — this rolls back the tx and undoes
	// the GetOrCreateToken INSERT.
	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGaugeState) error {
		return fmt.Errorf("simulated downstream commit failure")
	}

	log := h.makeGaugeDepositLog(gauge.Address, testUser, big.NewInt(1))
	err := h.runProcess(t, 1200, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected error (SaveCurveGaugeState forced to fail) so SQS doesn't ack")
	}

	// Critical assertion: the in-process cache must be empty so the retry
	// will fetch ERC20 metadata + call GetOrCreateToken again.
	h.svc.registeredTokensMu.Lock()
	_, seen := h.svc.registeredTokens[reward]
	cacheSize := len(h.svc.registeredTokens)
	h.svc.registeredTokensMu.Unlock()
	if seen {
		t.Errorf("registeredTokens cache contains %s after the tx rolled back — retry will skip re-registration", reward.Hex())
	}
	if cacheSize != 0 {
		t.Errorf("registeredTokens cache has %d entries after rolled-back tx; want 0", cacheSize)
	}
}

// TestProcessReceipt_PoolEvent_WithAttachedGauge_WithRewards exercises the
// handlePoolLog branch where pool.gauge != nil AND reward_count > 0, so the
// worker chains pool state → gauge state → reward_tokens → reward_data.
func TestProcessReceipt_PoolEvent_WithAttachedGauge_WithRewards(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	reward := common.HexToAddress("0xD533a949740bb3306d119CC777fa900bA034cd52") // CRV

	tAddr, _ := abi.NewType("address", "", nil)
	tU256, _ := abi.NewType("uint256", "", nil)
	rdArgs := abi.Arguments{
		{Type: tAddr}, {Type: tAddr}, {Type: tU256}, {Type: tU256}, {Type: tU256}, {Type: tU256},
	}
	encodeRewardData := func(token common.Address) []byte {
		out, err := rdArgs.Pack(token, common.Address{}, big.NewInt(1_700_000_000), big.NewInt(7), big.NewInt(0), big.NewInt(0))
		if err != nil {
			t.Fatalf("packing reward_data: %v", err)
		}
		return out
	}

	callIdx := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callIdx++
		switch {
		case len(calls) == 8: // pool state V1
			return h.poolStateResultsV1(), nil
		case len(calls) == 5: // gauge state w/ reward_count=1
			return h.gaugeStateResults(1), nil
		case callIdx == 3: // reward_tokens(0)
			return []outbound.Result{{Success: true, ReturnData: h.packAddress(reward)}}, nil
		case callIdx == 4: // reward_data(reward)
			return []outbound.Result{{Success: true, ReturnData: encodeRewardData(reward)}}, nil
		case callIdx == 5: // readERC20Metadata(reward): symbol() + decimals()
			strT, _ := abi.NewType("string", "", nil)
			uint8T, _ := abi.NewType("uint8", "", nil)
			symOut, _ := (abi.Arguments{{Type: strT}}).Pack("CRV")
			decOut, _ := (abi.Arguments{{Type: uint8T}}).Pack(uint8(18))
			return []outbound.Result{
				{Success: true, ReturnData: symOut},
				{Success: true, ReturnData: decOut},
			}, nil
		}
		return nil, fmt.Errorf("unexpected multicall #%d: %d calls", callIdx, len(calls))
	}

	var poolState *entity.CurvePoolState
	var gaugeState *entity.CurveGaugeState
	h.curveRepo.SaveCurvePoolStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurvePoolState) error {
		poolState = s
		return nil
	}
	h.curveRepo.SaveCurveGaugeStateFn = func(_ context.Context, _ pgx.Tx, s *entity.CurveGaugeState) error {
		gaugeState = s
		return nil
	}
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ int64) (int64, error) {
		return 1, nil
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	if err := h.runProcess(t, 1250, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if poolState == nil || gaugeState == nil {
		t.Fatalf("pool+gauge state must both persist (pool=%v gauge=%v)", poolState != nil, gaugeState != nil)
	}
	if len(gaugeState.RewardTokens) != 1 {
		t.Errorf("RewardTokens len = %d, want 1", len(gaugeState.RewardTokens))
	}
}

// TestProcessReceipt_LPTransfer_NGPool_ResolvedFromPoolAddress verifies the
// effectiveLPToken path where the pool has no separate LP-token contract
// (LPTokenAddress == nil): Transfer events are emitted by the pool address
// itself, so the registry must resolve via poolByLPToken(pool.Address).
func TestProcessReceipt_LPTransfer_NGPool_ResolvedFromPoolAddress(t *testing.T) {
	h := newHarness(t)
	pool := makePoolNG() // LPTokenAddress is nil
	h.svc.registry.addPool(pool)

	from := common.HexToAddress("0x3333333333333333333333333333333333333333")
	to := common.HexToAddress("0x4444444444444444444444444444444444444444")
	ev := h.lpEventsABI.Events["Transfer"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(77))
	if err != nil {
		t.Fatalf("pack transfer: %v", err)
	}
	log := shared.Log{
		Address: pool.Address.Hex(), // NG pool emits Transfer from its own address
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}

	var balanceTarget common.Address
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) > 0 {
			balanceTarget = calls[0].Target
		}
		out := make([]outbound.Result, len(calls))
		for i := range calls {
			out[i] = outbound.Result{Success: true, ReturnData: h.packUint256(big.NewInt(int64(100 + i)))}
		}
		return out, nil
	}

	var positions []*entity.CurveUserLpPosition
	h.curveRepo.SaveCurveUserLpPositionFn = func(_ context.Context, _ pgx.Tx, p *entity.CurveUserLpPosition) error {
		positions = append(positions, p)
		return nil
	}

	if err := h.runProcess(t, 1300, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if len(positions) != 2 {
		t.Fatalf("expected 2 LP position rows for NG transfer, got %d", len(positions))
	}
	if balanceTarget != pool.Address {
		t.Errorf("balanceOf target = %s, want pool.Address %s (NG: pool IS the LP)", balanceTarget.Hex(), pool.Address.Hex())
	}
}

// TestProcessReceipt_LPBalancesRevert_PropagatesError verifies that a reverting
// balanceOf on the LP-token contract surfaces as an error to the SQS handler.
func TestProcessReceipt_LPBalancesRevert_PropagatesError(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	from := common.HexToAddress("0x5555555555555555555555555555555555555555")
	to := common.HexToAddress("0x6666666666666666666666666666666666666666")
	ev := h.lpEventsABI.Events["Transfer"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(11))
	if err != nil {
		t.Fatalf("pack transfer: %v", err)
	}
	log := shared.Log{
		Address: testLPTokenAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	// All balanceOf sub-calls revert ⇒ readLPBalances returns wrapped error.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		out := make([]outbound.Result, len(calls))
		for i := range out {
			out[i] = outbound.Result{Success: false}
		}
		return out, nil
	}

	saveCalled := false
	h.curveRepo.SaveCurveUserLpPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveUserLpPosition) error {
		saveCalled = true
		return nil
	}
	err = h.runProcess(t, 1301, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected error when balanceOf reverts on healthy LP token")
	}
	if !strings.Contains(err.Error(), "balanceOf") && !strings.Contains(err.Error(), "balances") {
		t.Errorf("error should mention LP balance read, got: %v", err)
	}
	if saveCalled {
		t.Error("position save should NOT happen when LP balance read fails")
	}
}

// TestProcessReceipt_GaugeStateRevert_PropagatesError verifies that a reverting
// gauge multicall surfaces as an error to the SQS handler.
func TestProcessReceipt_GaugeStateRevert_PropagatesError(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	gauge := makeGauge(pool.ID, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		// Every gauge sub-call reverts.
		out := make([]outbound.Result, len(calls))
		for i := range out {
			out[i] = outbound.Result{Success: false}
		}
		return out, nil
	}

	log := h.makeGaugeDepositLog(gauge.Address, testUser, big.NewInt(42))
	err := h.runProcess(t, 1302, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected error when gauge view methods all revert")
	}
	if !strings.Contains(err.Error(), "gauge") && !strings.Contains(err.Error(), "inflation_rate") {
		t.Errorf("error should mention gauge read, got: %v", err)
	}
}

// TestProcessReceipt_LPPositionSave_Error_Propagates verifies that a failure in
// SaveCurveUserLpPosition surfaces back through processBlockEvent.
func TestProcessReceipt_LPPositionSave_Error_Propagates(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	ev := h.lpEventsABI.Events["Transfer"]
	data, err := ev.Inputs.NonIndexed().Pack(big.NewInt(7))
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	log := shared.Log{
		Address: testLPTokenAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		out := make([]outbound.Result, len(calls))
		for i := range calls {
			out[i] = outbound.Result{Success: true, ReturnData: h.packUint256(big.NewInt(1))}
		}
		return out, nil
	}
	sentinel := errors.New("position-write-fail")
	h.curveRepo.SaveCurveUserLpPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveUserLpPosition) error {
		return sentinel
	}

	err = h.runProcess(t, 1600, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected position-save error to propagate")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel error, got %v", err)
	}
}

// TestProcessReceipt_GaugeKill_RepoError_Propagates covers handleGaugeControllerLog
// where SetCurveGaugeKilled returns an error inside the transaction.
func TestProcessReceipt_GaugeKill_RepoError_Propagates(t *testing.T) {
	h := newHarness(t)
	gauge := makeGauge(1, testGaugeAddr)
	h.svc.registry.addGauge(gauge)

	sentinel := errors.New("kill-write-fail")
	h.curveRepo.SetCurveGaugeKilledFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ bool) error {
		return sentinel
	}

	ev := h.gcABI.Events["KillGauge"]
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address: CurveGaugeControllerAddress.Hex(),
			Topics: []string{
				ev.ID.Hex(),
				common.BytesToHash(testGaugeAddr.Bytes()).Hex(),
			},
			Data:            "0x",
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}
	err := h.runProcess(t, 1601, 0, receipt)
	if err == nil {
		t.Fatal("expected kill-write error to propagate")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel error, got %v", err)
	}
}

// TestExtractor_LogIndexParse_Errors covers parseLogIndex's error branch and the
// downstream wrap from extractPoolEvent.
func TestExtractor_PoolEvent_BadLogIndex(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	log.LogIndex = "not-a-hex"
	err := h.runProcess(t, 1400, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected error for invalid log index")
	}
}

// TestProcessReceipt_SavePoolSwap_Error_Propagates covers the error branch in
// handlePoolLog where savePoolEventProjection (the repo write) fails.
// TestHelpers_NegateAndBigIntCopy directly exercises the small math helpers
// used by handleLPTokenTransfer.
func TestHelpers_NegateAndBigIntCopy(t *testing.T) {
	if got := negate(nil); got == nil || got.Sign() != 0 {
		t.Errorf("negate(nil) = %v, want 0", got)
	}
	if got := negate(big.NewInt(7)); got.Sign() >= 0 {
		t.Errorf("negate(7) = %v, want -7", got)
	}
	if got := bigIntCopy(nil); got != nil {
		t.Errorf("bigIntCopy(nil) = %v, want nil", got)
	}
	src := big.NewInt(42)
	cp := bigIntCopy(src)
	if cp.Int64() != 42 {
		t.Errorf("bigIntCopy(42) = %v, want 42", cp)
	}
	cp.Add(cp, big.NewInt(1))
	if src.Int64() != 42 {
		t.Errorf("bigIntCopy did not return a fresh value (src mutated to %v)", src)
	}
}

// TestHelpers_BigIntToTimePtr covers the nil / zero / positive branches.
func TestHelpers_BigIntToTimePtr(t *testing.T) {
	if got := bigIntToTimePtr(nil); got != nil {
		t.Errorf("bigIntToTimePtr(nil) = %v, want nil", got)
	}
	if got := bigIntToTimePtr(big.NewInt(0)); got != nil {
		t.Errorf("bigIntToTimePtr(0) = %v, want nil (sign==0 means unset)", got)
	}
	got := bigIntToTimePtr(big.NewInt(1_700_000_000))
	if got == nil || got.Unix() != 1_700_000_000 {
		t.Errorf("bigIntToTimePtr(1.7e9) = %v, want 2023-11-14 UTC", got)
	}
}

// TestHelpers_IsZeroAddress covers both branches.
func TestHelpers_IsZeroAddress(t *testing.T) {
	if !isZeroAddress(common.Address{}) {
		t.Error("zero address must report zero")
	}
	if isZeroAddress(common.HexToAddress("0x0000000000000000000000000000000000000001")) {
		t.Error("non-zero address must report non-zero")
	}
}

func TestProcessReceipt_SavePoolSwap_Error_Propagates(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	sentinel := errors.New("swap-write-fail")
	h.curveRepo.SaveCurvePoolSwapFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurvePoolSwap) error {
		return sentinel
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	err := h.runProcess(t, 1500, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected swap-write error to propagate")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel error, got %v", err)
	}
}

// TestProcessReceipt_SaveProtocolEvent_Error_Propagates covers the error branch
// where the audit-row write inside the tx fails.
func TestProcessReceipt_SaveProtocolEvent_Error_Propagates(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	sentinel := errors.New("audit-fail")
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		return sentinel
	}
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return h.poolStateResultsV1(), nil
	}

	log := h.makeTokenExchangeLog(pool.Address, testUser, 0, 1, big.NewInt(50), big.NewInt(49))
	err := h.runProcess(t, 1501, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected audit-write error to propagate")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel error, got %v", err)
	}
}

// TestProcessReceipt_BadLPTransferData_DoesNotWrite verifies a Transfer log on
// a registered LP-token address but with malformed data is reported as an
// error and produces no write.
func TestProcessReceipt_BadLPTransferData_Errors(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	ev := h.lpEventsABI.Events["Transfer"]
	log := shared.Log{
		Address: testLPTokenAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            "0xdead", // not enough bytes for uint256 value
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	called := false
	h.curveRepo.SaveCurveUserLpPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveUserLpPosition) error {
		called = true
		return nil
	}
	err := h.runProcess(t, 1502, 0, shared.TransactionReceipt{TransactionHash: testTxHash, Logs: []shared.Log{log}})
	if err == nil {
		t.Fatal("expected error decoding bad Transfer payload")
	}
	if called {
		t.Error("LP position should not be written when decode fails")
	}
}

// A stableswap gauge (gauge_types == 0) whose lp_token() reverts is UNEXPECTED
// state — propagate the error so SQS retries.
func TestProcessReceipt_NewGauge_StableswapButLpTokenReverts_PropagatesError(t *testing.T) {
	h := newHarness(t)
	pool := makePoolV1()
	h.svc.registry.addPool(pool)

	newGaugeAddr := common.HexToAddress("0xb000000000000000000000000000000000000004")
	ev := h.gcABI.Events["NewGauge"]
	data, err := ev.Inputs.NonIndexed().Pack(newGaugeAddr, big.NewInt(0), big.NewInt(0))
	if err != nil {
		t.Fatalf("pack NewGauge: %v", err)
	}
	receipt := shared.TransactionReceipt{
		TransactionHash: testTxHash,
		Logs: []shared.Log{{
			Address:         CurveGaugeControllerAddress.Hex(),
			Topics:          []string{ev.ID.Hex()},
			Data:            "0x" + common.Bytes2Hex(data),
			TransactionHash: testTxHash,
			LogIndex:        "0x0",
		}},
	}

	// gauge_types succeeds and returns 0 (stableswap), then lp_token reverts.
	int128T, _ := abi.NewType("int128", "", nil)
	gtypeOut, _ := (abi.Arguments{{Type: int128T}}).Pack(big.NewInt(0))
	calls := 0
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		calls++
		switch calls {
		case 1:
			return []outbound.Result{{Success: true, ReturnData: gtypeOut}}, nil
		case 2:
			return []outbound.Result{{Success: false}}, nil
		default:
			return nil, fmt.Errorf("unexpected multicall #%d", calls)
		}
	}

	upsertCalled := false
	h.curveRepo.UpsertCurveGaugeFn = func(_ context.Context, _ pgx.Tx, _ *entity.CurveGauge) (int64, error) {
		upsertCalled = true
		return 0, nil
	}

	err = h.runProcess(t, 1001, 0, receipt)
	if err == nil {
		t.Fatal("expected error when lp_token reverts on a stableswap-typed gauge, got nil")
	}
	if upsertCalled {
		t.Error("UpsertCurveGauge must NOT be called when lp_token() reverts")
	}
}
