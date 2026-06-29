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
func cryptoswapResults(_ *testing.T, _ *abi.ABI) []outbound.Result {
	pack := func(v int64) outbound.Result {
		return outbound.Result{Success: true, ReturnData: packUint256(big.NewInt(v))}
	}
	return []outbound.Result{
		pack(1000000000000000000), // balances(0)
		pack(1000000000000000000), // balances(1)
		pack(1000000000000000000), // balances(2)
		pack(1001000000000000000), // get_virtual_price
		pack(3000000000000000000), // totalSupply
		pack(270000),              // A
		pack(11809167828997),      // gamma
		pack(5000000),             // fee
		pack(999000000000000000),  // get_dy(0,1)
		pack(998000000000000000),  // get_dy(0,2)
		pack(999500000000000000),  // get_dy(1,0)
		pack(998500000000000000),  // get_dy(1,2)
		pack(999800000000000000),  // get_dy(2,0)
		pack(999300000000000000),  // get_dy(2,1)
		pack(1500000000000000000), // price_scale(0)
		pack(1600000000000000000), // price_scale(1)
		pack(1510000000000000000), // price_oracle(0)
		pack(1610000000000000000), // price_oracle(1)
		pack(1505000000000000000), // last_prices(0)
		pack(1605000000000000000), // last_prices(1)
		pack(3003000000000000000), // D
		pack(1000100000000000000), // xcp_profit
	}
}

// cryptoswapResultsDNil builds canned results where D() and xcp_profit() revert.
func cryptoswapResultsDNil(t *testing.T, a *abi.ABI) []outbound.Result {
	t.Helper()
	base := cryptoswapResults(t, a)
	result := make([]outbound.Result, len(base))
	copy(result, base)
	// D() and xcp_profit() revert -> AllowFailure=true means nil, not error.
	result[20] = outbound.Result{Success: false, ReturnData: nil}
	result[21] = outbound.Result{Success: false, ReturnData: nil}
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
	mc := &fakeMulticaller{results: cryptoswapResults(t, a)}
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
}

func TestCryptoswapHandler_SnapshotDNilOnRevert(t *testing.T) {
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
	mc := &fakeMulticaller{results: cryptoswapResultsDNil(t, a)}
	ss, err := h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("D/xcp_profit revert must not error, got: %v", err)
	}
	if ss.Cryptoswap == nil {
		t.Fatal("Cryptoswap must be non-nil")
	}
	if ss.Cryptoswap.D != nil {
		t.Errorf("D must be nil when result reverted, got %s", ss.Cryptoswap.D)
	}
	if ss.Cryptoswap.XcpProfit != nil {
		t.Errorf("XcpProfit must be nil when result reverted, got %s", ss.Cryptoswap.XcpProfit)
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

	mc := &capturingMulticaller{results: cryptoswapResults(t, a)}
	_, err = h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if len(mc.captured) <= cryptoswapTotalSupplyIdx {
		t.Fatalf("captured %d calls, want at least %d", len(mc.captured), cryptoswapTotalSupplyIdx+1)
	}

	tsCall := mc.captured[cryptoswapTotalSupplyIdx]
	if tsCall.Target != lpAddr {
		t.Errorf("totalSupply call Target = %s, want LP token %s", tsCall.Target, lpAddr)
	}

	// All other calls must target the pool, not the LP token.
	for i, c := range mc.captured {
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

	mc := &capturingMulticaller{results: cryptoswapResults(t, a)}
	_, err = h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if len(mc.captured) <= cryptoswapTotalSupplyIdx {
		t.Fatalf("captured %d calls, want at least %d", len(mc.captured), cryptoswapTotalSupplyIdx+1)
	}

	tsCall := mc.captured[cryptoswapTotalSupplyIdx]
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

	mc := &fakeMulticaller{results: revertResults}
	_, err = h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err == nil {
		t.Errorf("snapshot with required call revert should error, got nil")
	}
}
