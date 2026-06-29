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

// buildReceiptWithTokenExchange constructs a shared.TransactionReceipt containing
// a single TokenExchange log. The indexed buyer goes in Topics[1]; the four
// non-indexed fields (sold_id, tokens_sold, bought_id, tokens_bought) are
// ABI-packed into Data.
func buildReceiptWithTokenExchange(
	t *testing.T,
	a *abi.ABI,
	logAddr common.Address,
	buyer common.Address,
	soldID int64,
	tokensSold *big.Int,
	boughtID int64,
	tokensBought *big.Int,
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

	// sold_id and bought_id are int128 in the ABI; Go's abi package represents
	// int128 as *big.Int.
	packed, err := nonIndexed.Pack(
		big.NewInt(soldID),
		tokensSold,
		big.NewInt(boughtID),
		tokensBought,
	)
	if err != nil {
		t.Fatalf("packing TokenExchange data: %v", err)
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

// hexUintStr formats a uint as a 0x-prefixed hex string, matching the JSON-RPC
// log field convention used in shared.Log.LogIndex.
func hexUintStr(n uint) string {
	if n == 0 {
		return "0x0"
	}
	return "0x" + hex.EncodeToString(big.NewInt(int64(n)).Bytes())
}

func TestStableswapHandler_DecodeTokenExchange(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := RegisteredPool{
		ID:      1,
		Address: common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:    KindStableswapPreNG,
		NCoins:  2,
	}
	buyer := common.HexToAddress("0xabc")
	receipt := buildReceiptWithTokenExchange(
		t, a, pool.Address, buyer,
		1, big.NewInt(1000),
		0, big.NewInt(999),
		3,
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
	if s.TokensSold.Cmp(big.NewInt(1000)) != 0 {
		t.Errorf("TokensSold = %s, want 1000", s.TokensSold)
	}
	if s.TokensBought.Cmp(big.NewInt(999)) != 0 {
		t.Errorf("TokensBought = %s, want 999", s.TokensBought)
	}
	if s.LogIndex != 3 {
		t.Errorf("LogIndex = %d, want 3", s.LogIndex)
	}
	if s.Buyer != buyer {
		t.Errorf("Buyer = %s, want %s", s.Buyer, buyer)
	}
	if s.Fee != nil {
		t.Errorf("Fee should be nil for stableswap, got %s", s.Fee)
	}
	// Capture net must also include this event.
	if len(got.Captured) == 0 {
		t.Error("captured events should be non-empty")
	}
}

func TestStableswapHandler_IgnoresForeignAddress(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := RegisteredPool{
		ID:      1,
		Address: common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:    KindStableswapPreNG,
		NCoins:  2,
	}
	receipt := buildReceiptWithTokenExchange(
		t, a, common.HexToAddress("0xdead"), common.HexToAddress("0xabc"),
		1, big.NewInt(1), 0, big.NewInt(1), 0,
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

func TestStableswapHandler_Handles(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	cases := []struct {
		kind PoolKind
		want bool
	}{
		{KindStableswapPreNG, true},
		{KindStableswapNG, true},
		{KindCryptoswap, false},
	}
	for _, tc := range cases {
		got := h.Handles(tc.kind)
		if got != tc.want {
			t.Errorf("Handles(%q) = %v, want %v", tc.kind, got, tc.want)
		}
	}
}

func TestStableswapHandler_EmptyReceipt(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := RegisteredPool{
		ID:      1,
		Address: common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:    KindStableswapPreNG,
		NCoins:  2,
	}
	got, err := h.DecodeEvents(shared.TransactionReceipt{}, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got.Swaps) != 0 || len(got.Liquidity) != 0 || len(got.Captured) != 0 {
		t.Errorf("expected empty result for empty receipt, got %+v", got)
	}
}

func TestStableswapHandler_UnknownTopicCaptured(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := RegisteredPool{
		ID:      1,
		Address: common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:    KindStableswapPreNG,
		NCoins:  2,
	}

	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	unknownTopic := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

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

func TestStableswapHandler_CorruptKnownEventErrors(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := RegisteredPool{
		ID:      1,
		Address: common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:    KindStableswapPreNG,
		NCoins:  2,
	}

	ev, ok := a.Events["TokenExchange"]
	if !ok {
		t.Fatal("TokenExchange event not in ABI")
	}

	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	buyer := common.HexToAddress("0xabc")

	// Create a log with TokenExchange topic but truncated/garbage data.
	log := shared.Log{
		Address: pool.Address.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(buyer.Bytes()).Hex(),
		},
		Data:            "0xdead", // Too short to unpack
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

// fakeMulticaller is a test double for outbound.Multicaller that returns
// pre-built results in order.
type fakeMulticaller struct {
	results []outbound.Result
}

func (f *fakeMulticaller) Execute(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	return f.results, nil
}

func (f *fakeMulticaller) Address() common.Address {
	return common.Address{}
}

// packUint256 ABI-encodes a uint256 value as a 32-byte big-endian word.
func packUint256(v *big.Int) []byte {
	return common.LeftPadBytes(v.Bytes(), 32)
}

// stableswapPreNGResults builds canned multicall results for a 2-coin pre-NG pool.
// Order must match buildSnapshotCalls for NCoins=2, Kind=plain_pre_ng:
//
//	0: balances(0)
//	1: balances(1)
//	2: get_virtual_price()
//	3: totalSupply()
//	4: A()
//	5: fee()
//	6: get_dy(0,1,dx0)
//	7: get_dy(1,0,dx1)
func stableswapPreNGResults(_ *testing.T, _ *abi.ABI) []outbound.Result {
	pack := func(v int64) outbound.Result {
		return outbound.Result{Success: true, ReturnData: packUint256(big.NewInt(v))}
	}
	return []outbound.Result{
		pack(1000000000000000000), // balances(0)
		pack(1000000000000000000), // balances(1)
		pack(1001000000000000000), // get_virtual_price
		pack(2000000000000000000), // totalSupply
		pack(900),                 // A
		pack(4000000),             // fee
		pack(999000000000000000),  // get_dy(0,1)
		pack(998000000000000000),  // get_dy(1,0)
	}
}

// stableswapNGResults builds canned results for a 2-coin NG pool.
// Same as pre-NG plus price_oracle and last_price at indices 8 and 9.
func stableswapNGResults(t *testing.T, a *abi.ABI) []outbound.Result {
	t.Helper()
	base := stableswapPreNGResults(t, a)
	pack := func(v int64) outbound.Result {
		return outbound.Result{Success: true, ReturnData: packUint256(big.NewInt(v))}
	}
	return append(base,
		pack(1000100000000000000), // price_oracle
		pack(1000050000000000000), // last_price
	)
}

func TestStableswapHandler_SnapshotPreNG(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := RegisteredPool{
		ID:           1,
		Address:      common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:         KindStableswapPreNG,
		NCoins:       2,
		CoinTokenIDs: []int64{1, 2},
		CoinDecimals: []int{18, 18},
	}
	mc := &fakeMulticaller{results: stableswapPreNGResults(t, a)}
	ss, err := h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if ss.Stableswap == nil || ss.Cryptoswap != nil {
		t.Fatal("want stableswap snapshot, cryptoswap must be nil")
	}
	if ss.Stableswap.LastPrice != nil {
		t.Fatal("pre-NG must not populate last_price")
	}
	if ss.Stableswap.PriceOracle != nil {
		t.Fatal("pre-NG must not populate price_oracle")
	}
	if len(ss.Stableswap.Balances) != 2 {
		t.Fatalf("balances len = %d, want 2", len(ss.Stableswap.Balances))
	}
	if len(ss.Stableswap.SpotDy) != 2 {
		t.Fatalf("spot_dy len = %d, want 2 (ordered pairs (0,1),(1,0))", len(ss.Stableswap.SpotDy))
	}
	if ss.BlockNumber != 100 {
		t.Errorf("BlockNumber = %d, want 100", ss.BlockNumber)
	}
}

func TestStableswapHandler_SnapshotNG(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := RegisteredPool{
		ID:           2,
		Address:      common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:         KindStableswapNG,
		NCoins:       2,
		CoinTokenIDs: []int64{1, 2},
		CoinDecimals: []int{18, 6},
	}
	mc := &fakeMulticaller{results: stableswapNGResults(t, a)}
	ss, err := h.SnapshotState(context.Background(), mc, pool, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot NG: %v", err)
	}
	if ss.Stableswap == nil {
		t.Fatal("want stableswap snapshot")
	}
	if ss.Stableswap.LastPrice == nil {
		t.Fatal("NG must populate last_price")
	}
	if ss.Stableswap.PriceOracle == nil {
		t.Fatal("NG must populate price_oracle")
	}
	if len(ss.Stableswap.SpotDy) != 2 {
		t.Fatalf("spot_dy len = %d, want 2", len(ss.Stableswap.SpotDy))
	}
}

// capturingMulticaller records the calls it receives and returns preset results.
// It is used to assert which Target address each call uses.
type capturingMulticaller struct {
	captured []outbound.Call
	results  []outbound.Result
}

func (c *capturingMulticaller) Execute(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	c.captured = append(c.captured, calls...)
	return c.results, nil
}

func (c *capturingMulticaller) Address() common.Address {
	return common.Address{}
}

// totalSupplyCallIndex returns the index of the totalSupply call in a pre-NG 2-coin
// call list: balances(0), balances(1), get_virtual_price, totalSupply -> index 3.
const stableswapPreNG2CoinTotalSupplyIdx = 3

// TestStableswapHandler_SnapshotTotalSupplyTargetsLpToken verifies that when
// LpTokenAddress is set, the totalSupply call targets the LP token, not the pool.
// All other calls must still target the pool address.
func TestStableswapHandler_SnapshotTotalSupplyTargetsLpToken(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)

	poolAddr := common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022")
	lpAddr := common.HexToAddress("0x06325440D014e39736583c165C2963BA99fAf14E")
	pool := RegisteredPool{
		ID:             1,
		Address:        poolAddr,
		Kind:           KindStableswapPreNG,
		NCoins:         2,
		CoinTokenIDs:   []int64{1, 2},
		CoinDecimals:   []int{18, 18},
		LpTokenAddress: &lpAddr,
	}

	mc := &capturingMulticaller{results: stableswapPreNGResults(t, a)}
	_, err = h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if len(mc.captured) <= stableswapPreNG2CoinTotalSupplyIdx {
		t.Fatalf("captured %d calls, want at least %d", len(mc.captured), stableswapPreNG2CoinTotalSupplyIdx+1)
	}

	tsCall := mc.captured[stableswapPreNG2CoinTotalSupplyIdx]
	if tsCall.Target != lpAddr {
		t.Errorf("totalSupply call Target = %s, want LP token %s", tsCall.Target, lpAddr)
	}

	// All other calls must target the pool, not the LP token.
	for i, c := range mc.captured {
		if i == stableswapPreNG2CoinTotalSupplyIdx {
			continue
		}
		if c.Target != poolAddr {
			t.Errorf("call[%d].Target = %s, want pool %s", i, c.Target, poolAddr)
		}
	}
}

// TestStableswapHandler_SnapshotTotalSupplyTargetsPoolWhenNoLpToken verifies that
// when LpTokenAddress is nil (NG pools that are their own LP token), totalSupply
// targets the pool address.
func TestStableswapHandler_SnapshotTotalSupplyTargetsPoolWhenNoLpToken(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)

	poolAddr := common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022")
	pool := RegisteredPool{
		ID:             2,
		Address:        poolAddr,
		Kind:           KindStableswapPreNG,
		NCoins:         2,
		CoinTokenIDs:   []int64{1, 2},
		CoinDecimals:   []int{18, 18},
		LpTokenAddress: nil, // pool is its own LP token
	}

	mc := &capturingMulticaller{results: stableswapPreNGResults(t, a)}
	_, err = h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if len(mc.captured) <= stableswapPreNG2CoinTotalSupplyIdx {
		t.Fatalf("captured %d calls, want at least %d", len(mc.captured), stableswapPreNG2CoinTotalSupplyIdx+1)
	}

	tsCall := mc.captured[stableswapPreNG2CoinTotalSupplyIdx]
	if tsCall.Target != poolAddr {
		t.Errorf("totalSupply call Target = %s, want pool %s", tsCall.Target, poolAddr)
	}
}

func TestStableswapHandler_SnapshotRevertErrors(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := RegisteredPool{
		ID:           1,
		Address:      common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:         KindStableswapPreNG,
		NCoins:       2,
		CoinTokenIDs: []int64{1, 2},
		CoinDecimals: []int{18, 18},
	}

	// Build results where the first balances call (required, AllowFailure=false) reverts.
	baseResults := stableswapPreNGResults(t, a)
	revertResults := make([]outbound.Result, len(baseResults))
	for i := range revertResults {
		revertResults[i] = baseResults[i]
	}
	revertResults[0] = outbound.Result{Success: false, ReturnData: nil} // First balances call reverts

	mc := &fakeMulticaller{results: revertResults}
	_, err = h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Errorf("snapshot with required call revert should error, got nil")
	}
}
