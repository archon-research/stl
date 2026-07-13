package curveindexer

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// buildCryptoswapReceiptWithTokenExchange constructs a TransactionReceipt containing
// a single cryptoswap TokenExchange log. The indexed buyer goes in Topics[1]; the
// six non-indexed fields (sold_id, tokens_sold, bought_id, tokens_bought, fee,
// packed_price_scale) are ABI-packed into Data.
func buildCryptoswapReceiptWithTokenExchange(
	t *testing.T,
	a *abi.ABI,
	logAddr common.Address,
	buyer common.Address,
	soldID int64,
	tokensSold *big.Int,
	boughtID int64,
	tokensBought *big.Int,
	fee *big.Int,
	packedPriceScale *big.Int,
	logIndex uint,
) shared.TransactionReceipt {
	t.Helper()

	ev, ok := a.Events["TokenExchange"]
	if !ok {
		t.Fatal("TokenExchange event not in ABI")
	}

	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}

	// All non-indexed fields are uint256 in the cryptoswap ABI.
	packed, err := nonIndexed.Pack(
		big.NewInt(soldID),
		tokensSold,
		big.NewInt(boughtID),
		tokensBought,
		fee,
		packedPriceScale,
	)
	if err != nil {
		t.Fatalf("packing cryptoswap TokenExchange data: %v", err)
	}

	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")

	log := shared.Log{
		Address: logAddr.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(buyer.Bytes()).Hex(),
		},
		Data:            "0x" + hex.EncodeToString(packed),
		TransactionHash: txHash.Hex(),
		LogIndex:        hexUintStr(logIndex),
	}

	return shared.TransactionReceipt{
		Logs:            []shared.Log{log},
		TransactionHash: txHash.Hex(),
	}
}

func TestCryptoswapHandler_DecodeTokenExchange(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := RegisteredPool{
		ID:      10,
		Address: common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:    KindCryptoswap,
		NCoins:  3,
	}
	buyer := common.HexToAddress("0xabc")
	receipt := buildCryptoswapReceiptWithTokenExchange(
		t, a, pool.Address, buyer,
		1, big.NewInt(2000),
		0, big.NewInt(1999),
		big.NewInt(500000),
		big.NewInt(1234567890),
		5,
	)

	got, err := h.DecodeEvents(receipt, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Swaps) != 1 {
		t.Fatalf("swaps = %d, want 1", len(got.Swaps))
	}
	s := got.Swaps[0]
	if s.SoldID != 1 {
		t.Errorf("SoldID = %d, want 1", s.SoldID)
	}
	if s.BoughtID != 0 {
		t.Errorf("BoughtID = %d, want 0", s.BoughtID)
	}
	if s.TokensSold.Cmp(big.NewInt(2000)) != 0 {
		t.Errorf("TokensSold = %s, want 2000", s.TokensSold)
	}
	if s.TokensBought.Cmp(big.NewInt(1999)) != 0 {
		t.Errorf("TokensBought = %s, want 1999", s.TokensBought)
	}
	if s.LogIndex != 5 {
		t.Errorf("LogIndex = %d, want 5", s.LogIndex)
	}
	if s.Buyer != buyer {
		t.Errorf("Buyer = %s, want %s", s.Buyer, buyer)
	}
	// Cryptoswap TokenExchange MUST carry fee (non-nil).
	if s.Fee == nil {
		t.Error("Fee must be non-nil for cryptoswap")
	} else if s.Fee.Cmp(big.NewInt(500000)) != 0 {
		t.Errorf("Fee = %s, want 500000", s.Fee)
	}
	// Capture-net must also include this event.
	if len(got.Captured) == 0 {
		t.Error("captured events should be non-empty")
	}
}

func TestCryptoswapHandler_EmptyReceipt(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := RegisteredPool{
		ID:      10,
		Address: common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:    KindCryptoswap,
		NCoins:  3,
	}
	got, err := h.DecodeEvents(shared.TransactionReceipt{}, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got.Swaps) != 0 || len(got.Liquidity) != 0 || len(got.Captured) != 0 {
		t.Errorf("expected empty result for empty receipt, got %+v", got)
	}
}

func TestCryptoswapHandler_IgnoresForeignAddress(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := RegisteredPool{
		ID:      10,
		Address: common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:    KindCryptoswap,
		NCoins:  3,
	}
	receipt := buildCryptoswapReceiptWithTokenExchange(
		t, a, common.HexToAddress("0xdead"), common.HexToAddress("0xabc"),
		1, big.NewInt(1), 0, big.NewInt(1),
		big.NewInt(1), big.NewInt(1), 0,
	)
	got, err := h.DecodeEvents(receipt, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Swaps) != 0 {
		t.Fatalf("expected foreign-address log ignored, got %d swaps", len(got.Swaps))
	}
	if len(got.Captured) != 0 {
		t.Errorf("expected no captured events for foreign address, got %d", len(got.Captured))
	}
}

func TestCryptoswapHandler_UnknownTopicCaptured(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := RegisteredPool{
		ID:      10,
		Address: common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:    KindCryptoswap,
		NCoins:  3,
	}

	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	unknownTopic := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	log := shared.Log{
		Address:         pool.Address.Hex(),
		Topics:          []string{unknownTopic.Hex()},
		Data:            "0x",
		TransactionHash: txHash.Hex(),
		LogIndex:        "0x0",
	}

	receipt := shared.TransactionReceipt{
		Logs:            []shared.Log{log},
		TransactionHash: txHash.Hex(),
	}

	got, err := h.DecodeEvents(receipt, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got.Swaps) != 0 {
		t.Errorf("expected no swaps, got %d", len(got.Swaps))
	}
	if len(got.Liquidity) != 0 {
		t.Errorf("expected no liquidity events, got %d", len(got.Liquidity))
	}
	if len(got.Captured) != 1 {
		t.Errorf("expected 1 captured event, got %d", len(got.Captured))
	}
}

func TestCryptoswapHandler_CorruptKnownEventErrors(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := RegisteredPool{
		ID:      10,
		Address: common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:    KindCryptoswap,
		NCoins:  3,
	}

	ev, ok := a.Events["TokenExchange"]
	if !ok {
		t.Fatal("TokenExchange event not in ABI")
	}

	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	buyer := common.HexToAddress("0xabc")

	// TokenExchange topic but truncated/garbage data.
	log := shared.Log{
		Address: pool.Address.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(buyer.Bytes()).Hex(),
		},
		Data:            "0xdead", // Too short to unpack 6 uint256 fields
		TransactionHash: txHash.Hex(),
		LogIndex:        "0x0",
	}

	receipt := shared.TransactionReceipt{
		Logs:            []shared.Log{log},
		TransactionHash: txHash.Hex(),
	}

	got, err := h.DecodeEvents(receipt, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Errorf("expected error for corrupt TokenExchange data, got success: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// SnapshotState tests
// ---------------------------------------------------------------------------

// cryptoswapResults builds canned multicall results for a 3-coin cryptoswap pool.
// Order must match buildSnapshotCalls for NCoins=3:
//
//	0: balances(0)
//	1: balances(1)
//	2: balances(2)
//	3: get_virtual_price()
//	4: totalSupply()
//	5: A()
//	6: gamma()
//	7: fee()
//	8: get_dy(0,1,dx0)
//	9: get_dy(0,2,dx0)
//	10: get_dy(1,0,dx1)
//	11: get_dy(1,2,dx1)
//	12: get_dy(2,0,dx2)
//	13: get_dy(2,1,dx2)
//	14: price_scale(0)
//	15: price_scale(1)
//	16: price_oracle(0)
//	17: price_oracle(1)
//	18: last_prices(0)
//	19: last_prices(1)
//	20: D()
//	21: xcp_profit()
//
// Extended reads (all AllowFailure=true). admin_balances() is NOT issued for
// cryptoswap pools (gated: Tricrypto-NG has no admin_balances getter), so the
// extended reads start at lp_price:
//
//	22: lp_price()
//	23: xcp_profit_a()
//	24: last_prices_timestamp()
//	25-30: get_dx for the 6 ordered pairs
//	31: calc_token_amount()
//	32-34: calc_withdraw_one_coin(0..2)
//	35-45: config getters (initial_A_gamma, future_A_gamma, initial_A_gamma_time,
//	       future_A_gamma_time, mid_fee, out_fee, fee_gamma, allowed_extra_profit,
//	       adjustment_step, ma_time, ADMIN_FEE)
func cryptoswapResults(_ *testing.T, _ *abi.ABI) []outbound.Result {
	pack := func(v int64) outbound.Result {
		return outbound.Result{Success: true, ReturnData: packUint256(big.NewInt(v))}
	}
	return []outbound.Result{
		pack(1000000000000000000), // 0 balances(0)
		pack(1000000000000000000), // 1 balances(1)
		pack(1000000000000000000), // 2 balances(2)
		pack(1001000000000000000), // 3 get_virtual_price
		pack(3000000000000000000), // 4 totalSupply
		pack(270000),              // 5 A
		pack(11809167828997),      // 6 gamma
		pack(5000000),             // 7 fee
		pack(999000000000000000),  // 8 get_dy(0,1)
		pack(998000000000000000),  // 9 get_dy(0,2)
		pack(999500000000000000),  // 10 get_dy(1,0)
		pack(998500000000000000),  // 11 get_dy(1,2)
		pack(999800000000000000),  // 12 get_dy(2,0)
		pack(999300000000000000),  // 13 get_dy(2,1)
		pack(1500000000000000000), // 14 price_scale(0)
		pack(1600000000000000000), // 15 price_scale(1)
		pack(1510000000000000000), // 16 price_oracle(0)
		pack(1610000000000000000), // 17 price_oracle(1)
		pack(1505000000000000000), // 18 last_prices(0)
		pack(1605000000000000000), // 19 last_prices(1)
		pack(3003000000000000000), // 20 D
		pack(1000100000000000000), // 21 xcp_profit
		pack(1407313225375571094), // 22 lp_price
		pack(1068150721095887980), // 23 xcp_profit_a
		pack(1782820487),          // 24 last_prices_timestamp
		pack(591050417),           // 25 get_dx(0,1)
		pack(592000000),           // 26 get_dx(0,2)
		pack(593000000),           // 27 get_dx(1,0)
		pack(594000000),           // 28 get_dx(1,2)
		pack(595000000),           // 29 get_dx(2,0)
		pack(596000000),           // 30 get_dx(2,1)
		pack(42853623908762784),   // 31 calc_token_amount
		pack(1405602713),          // 32 calc_withdraw_one_coin(0)
		pack(1406000000),          // 33 calc_withdraw_one_coin(1)
		pack(1407000000),          // 34 calc_withdraw_one_coin(2)
		pack(581076037942835227),  // 35 initial_A_gamma
		pack(581076037942835227),  // 36 future_A_gamma
		pack(0),                   // 37 initial_A_gamma_time
		pack(0),                   // 38 future_A_gamma_time
		pack(3000000),             // 39 mid_fee
		pack(30000000),            // 40 out_fee
		pack(500000000000000),     // 41 fee_gamma
		pack(2000000000000),       // 42 allowed_extra_profit
		pack(490000000000000),     // 43 adjustment_step
		pack(600),                 // 44 ma_time
		pack(5000000000),          // 45 ADMIN_FEE
	}
}

// cryptoswap state-read result indices used by tests.
const (
	cryptoswapDIdx           = 20
	cryptoswapConfigFirstIdx = 35 // initial_A_gamma
)

// cryptoswapResultsDRevert builds canned results where D() reverts.
func cryptoswapResultsDRevert(t *testing.T, a *abi.ABI) []outbound.Result {
	t.Helper()
	base := cryptoswapResults(t, a)
	result := make([]outbound.Result, len(base))
	copy(result, base)
	result[cryptoswapDIdx] = outbound.Result{Success: false, ReturnData: nil}
	return result
}

func TestCryptoswapHandler_Snapshot(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := RegisteredPool{
		ID:           10,
		Address:      common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:         KindCryptoswap,
		NCoins:       3,
		CoinDecimals: []int{18, 18, 6},
	}
	mc := curveMC(cryptoswapResults(t, a))
	ss, err := h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if ss.Cryptoswap == nil {
		t.Fatal("want cryptoswap snapshot, Cryptoswap must be non-nil")
	}
	if ss.Stableswap != nil {
		t.Fatal("Stableswap must be nil for cryptoswap handler")
	}
	if len(ss.Cryptoswap.Balances) != 3 {
		t.Fatalf("balances len = %d, want 3", len(ss.Cryptoswap.Balances))
	}
	// n-1 price array entries for n=3 means 2 entries each.
	if len(ss.Cryptoswap.PriceScale) != 2 {
		t.Fatalf("PriceScale len = %d, want 2", len(ss.Cryptoswap.PriceScale))
	}
	if len(ss.Cryptoswap.PriceOracle) != 2 {
		t.Fatalf("PriceOracle len = %d, want 2", len(ss.Cryptoswap.PriceOracle))
	}
	if len(ss.Cryptoswap.LastPrices) != 2 {
		t.Fatalf("LastPrices len = %d, want 2", len(ss.Cryptoswap.LastPrices))
	}
	// n*(n-1) = 6 ordered pairs for n=3.
	if len(ss.Cryptoswap.SpotDy) != 6 {
		t.Fatalf("SpotDy len = %d, want 6", len(ss.Cryptoswap.SpotDy))
	}
	if ss.Cryptoswap.Gamma == nil {
		t.Error("Gamma must be non-nil")
	}
	// D and xcp_profit were successful so must be non-nil.
	if ss.Cryptoswap.D == nil {
		t.Error("D must be non-nil when result succeeded")
	}
	if ss.Cryptoswap.XcpProfit == nil {
		t.Error("XcpProfit must be non-nil when result succeeded")
	}
	if ss.BlockNumber != 200 {
		t.Errorf("BlockNumber = %d, want 200", ss.BlockNumber)
	}

	// admin_balances is not issued for cryptoswap pools (gated: Tricrypto-NG has
	// no admin_balances getter), so the column is NULL by structural design.
	if ss.Cryptoswap.AdminBalances != nil {
		t.Errorf("AdminBalances must be nil (call gated out), got %v", ss.Cryptoswap.AdminBalances)
	}
	if ss.Cryptoswap.LpPrice == nil {
		t.Error("LpPrice must be non-nil")
	}
	if ss.Cryptoswap.XcpProfitA == nil {
		t.Error("XcpProfitA must be non-nil")
	}
	if ss.Cryptoswap.LastPricesTimestamp == nil {
		t.Error("LastPricesTimestamp must be non-nil")
	} else if *ss.Cryptoswap.LastPricesTimestamp != 1782820487 {
		t.Errorf("LastPricesTimestamp = %d, want 1782820487", *ss.Cryptoswap.LastPricesTimestamp)
	}
	if len(ss.Cryptoswap.GetDx) != 6 {
		t.Errorf("GetDx len = %d, want 6", len(ss.Cryptoswap.GetDx))
	}
	if ss.Cryptoswap.CalcTokenAmount == nil {
		t.Error("CalcTokenAmount must be non-nil")
	}
	if len(ss.Cryptoswap.CalcWithdrawOneCoin) != 3 {
		t.Errorf("CalcWithdrawOneCoin len = %d, want 3", len(ss.Cryptoswap.CalcWithdrawOneCoin))
	}

	// Config is built from the (all-successful) config getters. Assert every
	// field so an off-by-one in the dst<->cryptoswapConfigGetters index mapping
	// (decodeCryptoswapConfigReads) is caught, not just a representative subset.
	if ss.CryptoswapConfig == nil {
		t.Fatal("CryptoswapConfig must be non-nil when config getters succeed")
	}
	cfg := ss.CryptoswapConfig
	bigEq := func(name string, got *big.Int, want int64) {
		t.Helper()
		if got == nil || got.Cmp(big.NewInt(want)) != 0 {
			t.Errorf("%s = %v, want %d", name, got, want)
		}
	}
	bigEq("InitialAGamma", cfg.InitialAGamma, 581076037942835227)
	bigEq("FutureAGamma", cfg.FutureAGamma, 581076037942835227)
	bigEq("MidFee", cfg.MidFee, 3000000)
	bigEq("OutFee", cfg.OutFee, 30000000)
	bigEq("FeeGamma", cfg.FeeGamma, 500000000000000)
	bigEq("AllowedExtraProfit", cfg.AllowedExtraProfit, 2000000000000)
	bigEq("AdjustmentStep", cfg.AdjustmentStep, 490000000000000)
	bigEq("MaTime", cfg.MaTime, 600)
	// admin_fee comes from the ADMIN_FEE constant.
	bigEq("AdminFee", cfg.AdminFee, 5000000000)
	if cfg.InitialAGammaTime != 0 {
		t.Errorf("InitialAGammaTime = %d, want 0", cfg.InitialAGammaTime)
	}
	if cfg.FutureAGammaTime != 0 {
		t.Errorf("FutureAGammaTime = %d, want 0", cfg.FutureAGammaTime)
	}
}

// TestCryptoswapHandler_SnapshotDRevertErrors verifies that a reverted D() read
// (issued with AllowFailure=true) bubbles up as an error and stops the block
// rather than being swallowed into a nil D field.
func TestCryptoswapHandler_SnapshotDRevertErrors(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := cryptoswapPool()
	mc := curveMC(cryptoswapResultsDRevert(t, a))
	_, err = h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err == nil {
		t.Error("reverted D() read must error, got nil")
	}
}

// cryptoswapTotalSupplyCallIndex is the index of the totalSupply call for a
// 3-coin cryptoswap pool: balances(0,1,2), get_virtual_price, totalSupply -> index 4.
const cryptoswapTotalSupplyIdx = 4

// TestCryptoswapHandler_SnapshotTotalSupplyTargetsLpToken verifies that when
// LpTokenAddress is set, the totalSupply call targets the LP token, not the pool.
// All other calls must still target the pool address.
func TestCryptoswapHandler_SnapshotTotalSupplyTargetsLpToken(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)

	poolAddr := common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46")
	lpAddr := common.HexToAddress("0x06325440D014e39736583c165C2963BA99fAf14E")
	pool := RegisteredPool{
		ID:             10,
		Address:        poolAddr,
		Kind:           KindCryptoswap,
		NCoins:         3,
		CoinDecimals:   []int{18, 18, 6},
		LpTokenAddress: &lpAddr,
	}

	mc := curveMC(cryptoswapResults(t, a))
	_, err = h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	captured := snapshotCalls(t, mc)
	if len(captured) <= cryptoswapTotalSupplyIdx {
		t.Fatalf("captured %d calls, want at least %d", len(captured), cryptoswapTotalSupplyIdx+1)
	}

	tsCall := captured[cryptoswapTotalSupplyIdx]
	if tsCall.Target != lpAddr {
		t.Errorf("totalSupply call Target = %s, want LP token %s", tsCall.Target, lpAddr)
	}

	// All other calls must target the pool, not the LP token.
	for i, c := range captured {
		if i == cryptoswapTotalSupplyIdx {
			continue
		}
		if c.Target != poolAddr {
			t.Errorf("call[%d].Target = %s, want pool %s", i, c.Target, poolAddr)
		}
	}
}

// TestCryptoswapHandler_SnapshotTotalSupplyTargetsPoolWhenNoLpToken verifies that
// when LpTokenAddress is nil, totalSupply targets the pool address.
func TestCryptoswapHandler_SnapshotTotalSupplyTargetsPoolWhenNoLpToken(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)

	poolAddr := common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46")
	pool := RegisteredPool{
		ID:             10,
		Address:        poolAddr,
		Kind:           KindCryptoswap,
		NCoins:         3,
		CoinDecimals:   []int{18, 18, 6},
		LpTokenAddress: nil,
	}

	mc := curveMC(cryptoswapResults(t, a))
	_, err = h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	captured := snapshotCalls(t, mc)
	if len(captured) <= cryptoswapTotalSupplyIdx {
		t.Fatalf("captured %d calls, want at least %d", len(captured), cryptoswapTotalSupplyIdx+1)
	}

	tsCall := captured[cryptoswapTotalSupplyIdx]
	if tsCall.Target != poolAddr {
		t.Errorf("totalSupply call Target = %s, want pool %s", tsCall.Target, poolAddr)
	}
}

func TestCryptoswapHandler_SnapshotRevertErrors(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := RegisteredPool{
		ID:           10,
		Address:      common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:         KindCryptoswap,
		NCoins:       3,
		CoinDecimals: []int{18, 18, 6},
	}

	// Required call (balances(0)) reverts -> must error.
	baseResults := cryptoswapResults(t, a)
	revertResults := make([]outbound.Result, len(baseResults))
	copy(revertResults, baseResults)
	revertResults[0] = outbound.Result{Success: false, ReturnData: nil}

	mc := curveMC(revertResults)
	_, err = h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err == nil {
		t.Errorf("snapshot with required call revert should error, got nil")
	}
}

// cryptoswapPool is the standard 3-coin fixture pool used across these tests.
func cryptoswapPool() RegisteredPool {
	return RegisteredPool{
		ID:           10,
		Address:      common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:         KindCryptoswap,
		NCoins:       3,
		CoinDecimals: []int{18, 18, 6},
	}
}

// TestCryptoswapHandler_SnapshotAdminBalancesNotIssued verifies admin_balances is
// not issued for cryptoswap pools (gated: Tricrypto-NG has no admin_balances
// getter) and that the column is consequently NULL by structural design.
func TestCryptoswapHandler_SnapshotAdminBalancesNotIssued(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := cryptoswapPool()

	calls, err := h.buildSnapshotCalls(pool)
	if err != nil {
		t.Fatalf("building snapshot calls: %v", err)
	}
	adminBalancesData, err := a.Pack("admin_balances", big.NewInt(0))
	if err != nil {
		t.Fatalf("packing admin_balances: %v", err)
	}
	for i, c := range calls {
		if string(c.CallData) == string(adminBalancesData) {
			t.Errorf("call[%d] is admin_balances; it must not be issued for cryptoswap pools", i)
		}
	}

	mc := curveMC(cryptoswapResults(t, a))
	ss, err := h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if ss.Cryptoswap.AdminBalances != nil {
		t.Errorf("AdminBalances must be nil (call gated out), got %v", ss.Cryptoswap.AdminBalances)
	}
}

// TestCryptoswapHandler_SnapshotGetDxRevertErrors verifies that a single reverted
// per-coin get_dx element bubbles up as an error rather than collapsing the whole
// array to nil (the all-or-nothing nil collapse was itself a swallow).
func TestCryptoswapHandler_SnapshotGetDxRevertErrors(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := cryptoswapPool()

	results := cryptoswapResults(t, a)
	// get_dx(0,1) lives at idx 25 (first get_dx entry).
	const firstGetDxIdx = 25
	results[firstGetDxIdx] = outbound.Result{Success: false, ReturnData: nil}

	_, err = h.SnapshotState(context.Background(), curveMC(results), pool, 200, 0, time.Unix(2, 0).UTC())
	if err == nil {
		t.Error("reverted get_dx element must error, got nil")
	}
}

// TestCryptoswapHandler_SnapshotConfigGetterRevertErrors verifies that a reverted
// required config getter bubbles up as an error and stops the block rather than
// silently dropping the config row.
func TestCryptoswapHandler_SnapshotConfigGetterRevertErrors(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := cryptoswapPool()

	results := cryptoswapResults(t, a)
	results[cryptoswapConfigFirstIdx] = outbound.Result{Success: false, ReturnData: nil}

	_, err = h.SnapshotState(context.Background(), curveMC(results), pool, 200, 0, time.Unix(2, 0).UTC())
	if err == nil {
		t.Error("reverted required config getter must error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Cryptoswap parameter-event and LP-event decode
// ---------------------------------------------------------------------------

func TestCryptoswapHandler_DecodeParameterEvents(t *testing.T) {
	pool := cryptoswapPool()

	cases := []struct {
		name       string
		eventName  string
		indexed    []common.Hash
		nonIndexed []any
		wantEvent  string
		wantParams map[string]string
	}{
		{
			name:       "RampAgamma",
			eventName:  "RampAgamma",
			nonIndexed: []any{big.NewInt(270000), big.NewInt(540000), big.NewInt(11809167828997), big.NewInt(23618335657994), big.NewInt(1731805535), big.NewInt(1732495784)},
			wantEvent:  "ramp_a_gamma",
			wantParams: map[string]string{
				"initial_a": "270000", "future_a": "540000",
				"initial_gamma": "11809167828997", "future_gamma": "23618335657994",
				"initial_time": "1731805535", "future_time": "1732495784",
			},
		},
		{
			name:       "NewParameters",
			eventName:  "NewParameters",
			nonIndexed: []any{big.NewInt(3000000), big.NewInt(30000000), big.NewInt(500000000000000), big.NewInt(2000000000000), big.NewInt(490000000000000), big.NewInt(600)},
			wantEvent:  "new_parameters",
			wantParams: map[string]string{
				"mid_fee": "3000000", "out_fee": "30000000", "fee_gamma": "500000000000000",
				"allowed_extra_profit": "2000000000000", "adjustment_step": "490000000000000", "ma_time": "600",
			},
		},
		{
			name:       "CommitNewParameters",
			eventName:  "CommitNewParameters",
			indexed:    []common.Hash{uintTopic(1732500000)},
			nonIndexed: []any{big.NewInt(3000000), big.NewInt(30000000), big.NewInt(500000000000000), big.NewInt(2000000000000), big.NewInt(490000000000000), big.NewInt(600)},
			wantEvent:  "commit_new_parameters",
			wantParams: map[string]string{
				"deadline": "1732500000",
				"mid_fee":  "3000000", "out_fee": "30000000", "fee_gamma": "500000000000000",
				"allowed_extra_profit": "2000000000000", "adjustment_step": "490000000000000", "ma_time": "600",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := abis.CurveCryptoswapABI()
			if err != nil {
				t.Fatalf("loading ABI: %v", err)
			}
			h := NewCryptoswapHandler(a)
			log := buildEventLog(t, a, tc.eventName, pool.Address, tc.indexed, tc.nonIndexed...)
			got, err := h.DecodeEvents(buildReceiptFromLog(log), pool, 1, 100, 0, time.Unix(1, 0).UTC())
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(got.ParameterEvents) != 1 {
				t.Fatalf("parameter events = %d, want 1", len(got.ParameterEvents))
			}
			pe := got.ParameterEvents[0]
			if pe.EventName != tc.wantEvent {
				t.Errorf("event_name = %q, want %q", pe.EventName, tc.wantEvent)
			}
			gotParams := decodeParamsMap(t, pe.Params)
			if len(gotParams) != len(tc.wantParams) {
				t.Errorf("params keys = %v, want %v", gotParams, tc.wantParams)
			}
			for k, want := range tc.wantParams {
				if gotParams[k] != want {
					t.Errorf("params[%q] = %q, want %q", k, gotParams[k], want)
				}
			}
			// Parameter events must also reach the capture net.
			if len(got.Captured) != 1 {
				t.Errorf("captured = %d, want 1", len(got.Captured))
			}
		})
	}
}

func TestCryptoswapHandler_DecodeClaimAdminFee(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := cryptoswapPool()
	admin := common.HexToAddress("0x000000000000000000000000000000000000dEaD")

	log := buildEventLog(t, a, "ClaimAdminFee", pool.Address,
		[]common.Hash{addrTopic(admin)}, big.NewInt(123456789))
	got, err := h.DecodeEvents(buildReceiptFromLog(log), pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.ParameterEvents) != 1 {
		t.Fatalf("parameter events = %d, want 1", len(got.ParameterEvents))
	}
	pe := got.ParameterEvents[0]
	if pe.EventName != "claim_admin_fee" {
		t.Errorf("event_name = %q, want claim_admin_fee", pe.EventName)
	}
	params := decodeParamsMap(t, pe.Params)
	if common.HexToAddress(params["admin"]) != admin {
		t.Errorf("params[admin] = %q, want %s", params["admin"], admin.Hex())
	}
	if params["tokens"] != "123456789" {
		t.Errorf("params[tokens] = %q, want 123456789", params["tokens"])
	}
}

func TestCryptoswapHandler_DecodeLpTransfer(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := cryptoswapPool() // pool == LP token for cryptoswap
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")

	log := buildEventLog(t, a, "Transfer", pool.Address,
		[]common.Hash{addrTopic(from), addrTopic(to)}, big.NewInt(123456))
	got, err := h.DecodeEvents(buildReceiptFromLog(log), pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.LpTokenEvents) != 1 {
		t.Fatalf("lp token events = %d, want 1", len(got.LpTokenEvents))
	}
	le := got.LpTokenEvents[0]
	if le.EventName != "transfer" {
		t.Errorf("event_name = %q, want transfer", le.EventName)
	}
	if le.From != from {
		t.Errorf("From = %s, want %s", le.From, from)
	}
	if le.To != to {
		t.Errorf("To = %s, want %s", le.To, to)
	}
	if le.Value.Cmp(big.NewInt(123456)) != 0 {
		t.Errorf("Value = %s, want 123456", le.Value)
	}
}

func TestCryptoswapHandler_DecodeLpApproval(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)
	pool := cryptoswapPool()
	owner := common.HexToAddress("0x3333333333333333333333333333333333333333")
	spender := common.HexToAddress("0x4444444444444444444444444444444444444444")

	log := buildEventLog(t, a, "Approval", pool.Address,
		[]common.Hash{addrTopic(owner), addrTopic(spender)}, big.NewInt(777))
	got, err := h.DecodeEvents(buildReceiptFromLog(log), pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.LpTokenEvents) != 1 {
		t.Fatalf("lp token events = %d, want 1", len(got.LpTokenEvents))
	}
	le := got.LpTokenEvents[0]
	if le.EventName != "approval" {
		t.Errorf("event_name = %q, want approval", le.EventName)
	}
	// Approval: owner -> From, spender -> To.
	if le.From != owner {
		t.Errorf("From = %s, want owner %s", le.From, owner)
	}
	if le.To != spender {
		t.Errorf("To = %s, want spender %s", le.To, spender)
	}
	if le.Value.Cmp(big.NewInt(777)) != 0 {
		t.Errorf("Value = %s, want 777", le.Value)
	}
}
