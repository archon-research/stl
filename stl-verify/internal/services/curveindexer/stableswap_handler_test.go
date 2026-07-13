package curveindexer

import (
	"bytes"
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
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestStableswapHandler_RemoveLiquidityOne_ValidatesCoinIndex: the NG
// RemoveLiquidityOne token_id (int128) is stored as coin_index, so a value
// outside [0, NCoins) must be rejected rather than persisted as a silent
// data-quality anomaly. Mirrors the cryptoswap RemoveLiquidityOne guard.
func TestStableswapHandler_RemoveLiquidityOne_ValidatesCoinIndex(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := stableswapPoolNG() // NCoins = 2
	provider := common.HexToAddress("0x1111111111111111111111111111111111111111")

	cases := []struct {
		name    string
		tokenID *big.Int
		wantErr bool
	}{
		{"index 0", big.NewInt(0), false},
		{"index 1", big.NewInt(1), false},
		{"index equal to n_coins", big.NewInt(2), true},
		{"negative index", big.NewInt(-1), true},
		{"far out of range", big.NewInt(1 << 20), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := buildEventLog(t, a, "RemoveLiquidityOne", pool.Address,
				[]common.Hash{addrTopic(provider)},
				tc.tokenID, big.NewInt(100), big.NewInt(200), big.NewInt(1000))
			receipt := shared.TransactionReceipt{Logs: []shared.Log{log}, TransactionHash: log.TransactionHash}
			_, err := h.DecodeEvents(receipt, pool, testChainID, 100, 0, time.Unix(100, 0).UTC())
			if tc.wantErr && err == nil {
				t.Fatalf("token_id %s: expected out-of-range error, got nil", tc.tokenID)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("token_id %s: unexpected error: %v", tc.tokenID, err)
			}
		})
	}
}

// TestStableswapHandler_TokenExchange_ValidatesCoinIndex: sold_id/bought_id are
// int128 stored in INT columns, so an out-of-range index must fail the block
// rather than be silently truncated (same coinIndexOrError guard as the
// RemoveLiquidityOne path).
func TestStableswapHandler_TokenExchange_ValidatesCoinIndex(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)
	pool := newTestPool() // NCoins = 2
	buyer := common.HexToAddress("0xabc")

	cases := []struct {
		name           string
		soldID, bought int64
		wantErr        bool
	}{
		{"in range", 0, 1, false},
		{"sold_id == n_coins", 2, 0, true},
		{"bought_id out of range", 0, 5, true},
		{"negative sold_id", -1, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			receipt := buildReceiptWithTokenExchange(t, a, pool.Address, buyer,
				tc.soldID, big.NewInt(1000), tc.bought, big.NewInt(999), 0)
			_, err := h.DecodeEvents(receipt, pool, testChainID, 100, 0, time.Unix(100, 0).UTC())
			if tc.wantErr && err == nil {
				t.Fatalf("sold_id=%d bought_id=%d: expected out-of-range error, got nil", tc.soldID, tc.bought)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("sold_id=%d bought_id=%d: unexpected error: %v", tc.soldID, tc.bought, err)
			}
		})
	}
}

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

// curveMC returns a MockMulticaller that serves the given canned results for
// every Execute/ExecuteAtHash call, recording each call on mc.Invocations so
// snapshot tests can assert which Target address each sub-call uses.
func curveMC(results []outbound.Result) *testutil.MockMulticaller {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return results, nil
	}
	return mc
}

// snapshotCalls returns the calls of the snapshot's multicall batch, pinning the
// contract that SnapshotState issues exactly one batch: the capturing assertions
// index into it, and a second batch would otherwise escape them silently.
func snapshotCalls(t *testing.T, mc *testutil.MockMulticaller) []outbound.Call {
	t.Helper()
	if len(mc.Invocations) != 1 {
		t.Fatalf("snapshot issued %d multicall batches, want exactly 1", len(mc.Invocations))
	}
	return mc.Invocations[0].Calls
}

// packUint256 ABI-encodes a uint256 value as a 32-byte big-endian word.
func packUint256(v *big.Int) []byte {
	return common.LeftPadBytes(v.Bytes(), 32)
}

// packUint256Result wraps a uint256 value as a successful multicall result.
func packUint256Result(v int64) outbound.Result {
	return outbound.Result{Success: true, ReturnData: packUint256(big.NewInt(v))}
}

// packUint256Array2 ABI-encodes a uint256[2] return value.
func packUint256Array2(t *testing.T, a, b int64) outbound.Result {
	t.Helper()
	arrT, err := uint256ArrayType(2)
	if err != nil {
		t.Fatalf("uint256[2] type: %v", err)
	}
	args := abi.Arguments{{Type: arrT}}
	packed, err := args.Pack([2]*big.Int{big.NewInt(a), big.NewInt(b)})
	if err != nil {
		t.Fatalf("packing uint256[2]: %v", err)
	}
	return outbound.Result{Success: true, ReturnData: packed}
}

// stableswapPreNGResults builds canned multicall results for a 2-coin pre-NG pool.
// Order matches buildSnapshotCalls for NCoins=2, Kind=plain_pre_ng: the 8 core
// reads, then the extended reads (A_precise, admin_balances x2, calc_token_amount,
// calc_withdraw_one_coin x2), then the 6 config getters, then future_admin_fee.
func stableswapPreNGResults(t *testing.T, _ *abi.ABI) []outbound.Result {
	t.Helper()
	pack := packUint256Result
	return []outbound.Result{
		pack(1000000000000000000), // 0 balances(0)
		pack(1000000000000000000), // 1 balances(1)
		pack(1001000000000000000), // 2 get_virtual_price
		pack(2000000000000000000), // 3 totalSupply
		pack(900),                 // 4 A
		pack(4000000),             // 5 fee
		pack(999000000000000000),  // 6 get_dy(0,1)
		pack(998000000000000000),  // 7 get_dy(1,0)
		pack(90000),               // 8 A_precise
		pack(167049139334410),     // 9 admin_balances(0)
		pack(200000),              // 10 admin_balances(1)
		pack(1754802761188011498), // 11 calc_token_amount
		pack(1139623037379637920), // 12 calc_withdraw_one_coin(0)
		pack(1138000000000000000), // 13 calc_withdraw_one_coin(1)
		pack(20000),               // 14 initial_A
		pack(1731805535),          // 15 initial_A_time
		pack(90000),               // 16 future_A
		pack(1732495784),          // 17 future_A_time
		pack(5000000000),          // 18 admin_fee
		pack(1000000),             // 19 future_fee
		pack(5000000000),          // 20 future_admin_fee
	}
}

// stableswapNGResults builds canned results for a 2-coin NG pool. Order matches
// buildSnapshotCalls for NCoins=2, Kind=plain_ng: 8 core + price_oracle +
// last_price + extended (A_precise, admin_balances x2, calc_token_amount,
// calc_withdraw x2) + NG (stored_rates, ema_price, get_p) + 6 config getters +
// ma_exp_time + oracle_method.
func stableswapNGResults(t *testing.T, _ *abi.ABI) []outbound.Result {
	t.Helper()
	pack := packUint256Result
	return []outbound.Result{
		pack(1000000000000000000), // 0 balances(0)
		pack(1000000000000000000), // 1 balances(1)
		pack(1001000000000000000), // 2 get_virtual_price
		pack(2000000000000000000), // 3 totalSupply
		pack(150000),              // 4 A
		pack(4000000),             // 5 fee
		pack(999000000000000000),  // 6 get_dy(0,1)
		pack(998000000000000000),  // 7 get_dy(1,0)
		pack(1000100000000000000), // 8 price_oracle
		pack(1000050000000000000), // 9 last_price
		pack(150000),              // 10 A_precise
		pack(3102741508070431),    // 11 admin_balances(0)
		pack(300000),              // 12 admin_balances(1)
		pack(1858424247096721508), // 13 calc_token_amount
		pack(1076093673587716682), // 14 calc_withdraw_one_coin(0)
		pack(1075000000000000000), // 15 calc_withdraw_one_coin(1)
		packUint256Array2(t, 1000000000000000000, 1000000000000000000), // 16 stored_rates
		pack(999924337681242600), // 17 ema_price
		pack(999923231149457753), // 18 get_p
		pack(20000),              // 19 initial_A
		pack(1731805535),         // 20 initial_A_time
		pack(150000),             // 21 future_A
		pack(1732495784),         // 22 future_A_time
		pack(5000000000),         // 23 admin_fee
		pack(1000000),            // 24 future_fee
		pack(2597),               // 25 ma_exp_time
		pack(0),                  // 26 oracle_method
	}
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
		CoinDecimals: []int{18, 18},
		HasAPrecise:  true,
	}
	mc := curveMC(stableswapPreNGResults(t, a))
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

	// Extended fields populate for pre-NG (NG-only fields stay nil).
	st := ss.Stableswap
	if st.APrecise == nil || st.APrecise.Cmp(big.NewInt(90000)) != 0 {
		t.Errorf("a_precise = %v, want 90000", st.APrecise)
	}
	if len(st.AdminBalances) != 2 {
		t.Errorf("admin_balances len = %d, want 2", len(st.AdminBalances))
	}
	if st.CalcTokenAmount == nil {
		t.Error("calc_token_amount must populate")
	}
	if len(st.CalcWithdrawOneCoin) != 2 {
		t.Errorf("calc_withdraw_one_coin len = %d, want 2", len(st.CalcWithdrawOneCoin))
	}
	if st.StoredRates != nil || st.EmaPrice != nil || st.GetP != nil {
		t.Error("pre-NG must not populate NG-only stored_rates/ema_price/get_p")
	}

	// Config is built from the pre-NG config getters incl. future_admin_fee.
	if ss.StableswapConfig == nil {
		t.Fatal("pre-NG snapshot must build a config")
	}
	cfg := ss.StableswapConfig
	if cfg.InitialA.Cmp(big.NewInt(20000)) != 0 {
		t.Errorf("config initial_a = %v, want 20000", cfg.InitialA)
	}
	if cfg.FutureATime != 1732495784 {
		t.Errorf("config future_a_time = %d, want 1732495784", cfg.FutureATime)
	}
	if cfg.FutureAdminFee == nil || cfg.FutureAdminFee.Cmp(big.NewInt(5000000000)) != 0 {
		t.Errorf("pre-NG config future_admin_fee = %v, want 5000000000", cfg.FutureAdminFee)
	}
	if cfg.MaExpTime != nil || cfg.OracleMethod != nil {
		t.Error("pre-NG config must not populate NG-only ma_exp_time/oracle_method")
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
		CoinDecimals: []int{18, 6},
		HasAPrecise:  true,
	}
	mc := curveMC(stableswapNGResults(t, a))
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

	// NG-only extended fields populate.
	st := ss.Stableswap
	if len(st.StoredRates) != 2 {
		t.Errorf("stored_rates len = %d, want 2", len(st.StoredRates))
	}
	if st.EmaPrice == nil {
		t.Error("NG must populate ema_price")
	}
	if st.GetP == nil {
		t.Error("NG must populate get_p")
	}
	if st.APrecise == nil || len(st.AdminBalances) != 2 || st.CalcTokenAmount == nil {
		t.Error("NG must populate a_precise/admin_balances/calc_token_amount")
	}

	// NG config has ma_exp_time + oracle_method and NO future_admin_fee.
	if ss.StableswapConfig == nil {
		t.Fatal("NG snapshot must build a config")
	}
	cfg := ss.StableswapConfig
	if cfg.FutureAdminFee != nil {
		t.Errorf("NG config future_admin_fee = %v, want nil", cfg.FutureAdminFee)
	}
	if cfg.MaExpTime == nil || *cfg.MaExpTime != 2597 {
		t.Errorf("NG config ma_exp_time = %v, want 2597", cfg.MaExpTime)
	}
	if cfg.OracleMethod == nil || cfg.OracleMethod.Sign() != 0 {
		t.Errorf("NG config oracle_method = %v, want 0", cfg.OracleMethod)
	}
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
		CoinDecimals:   []int{18, 18},
		LpTokenAddress: &lpAddr,
		HasAPrecise:    true,
	}

	mc := curveMC(stableswapPreNGResults(t, a))
	_, err = h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	captured := snapshotCalls(t, mc)
	if len(captured) <= stableswapPreNG2CoinTotalSupplyIdx {
		t.Fatalf("captured %d calls, want at least %d", len(captured), stableswapPreNG2CoinTotalSupplyIdx+1)
	}

	tsCall := captured[stableswapPreNG2CoinTotalSupplyIdx]
	if tsCall.Target != lpAddr {
		t.Errorf("totalSupply call Target = %s, want LP token %s", tsCall.Target, lpAddr)
	}

	// All other calls must target the pool, not the LP token.
	for i, c := range captured {
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
// targets the pool address. Uses KindStableswapNG because that is the real-world
// case: NG pools embed the LP token into the pool contract itself.
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
		Kind:           KindStableswapNG,
		NCoins:         2,
		CoinDecimals:   []int{18, 18},
		LpTokenAddress: nil, // pool is its own LP token
		HasAPrecise:    true,
	}

	mc := curveMC(stableswapNGResults(t, a))
	_, err = h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	captured := snapshotCalls(t, mc)
	if len(captured) <= stableswapPreNG2CoinTotalSupplyIdx {
		t.Fatalf("captured %d calls, want at least %d", len(captured), stableswapPreNG2CoinTotalSupplyIdx+1)
	}

	tsCall := captured[stableswapPreNG2CoinTotalSupplyIdx]
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
		CoinDecimals: []int{18, 18},
		HasAPrecise:  true,
	}

	// Build results where the first balances call (required, AllowFailure=false) reverts.
	baseResults := stableswapPreNGResults(t, a)
	revertResults := make([]outbound.Result, len(baseResults))
	for i := range revertResults {
		revertResults[i] = baseResults[i]
	}
	revertResults[0] = outbound.Result{Success: false, ReturnData: nil} // First balances call reverts

	mc := curveMC(revertResults)
	_, err = h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Errorf("snapshot with required call revert should error, got nil")
	}
}

// preNG2CoinAPreciseIdx is the index of the A_precise call (call 8) in the pre-NG
// 2-coin call list: 8 core reads (indices 0-7) then A_precise at index 8.
const preNG2CoinAPreciseIdx = 8

// preNG2CoinInitialAIdx is the index of the initial_A config getter: after the 8
// core reads, A_precise (8), admin_balances x2 (9-10), calc_token_amount (11),
// calc_withdraw x2 (12-13) -> initial_A at index 14.
const preNG2CoinInitialAIdx = 14

// TestStableswapHandler_SnapshotExtendedRevertErrors verifies that an issued
// extended read (AllowFailure=true) reverting bubbles up as an error and stops the
// block rather than being swallowed into a nil field.
func TestStableswapHandler_SnapshotExtendedRevertErrors(t *testing.T) {
	_, a := newStableswapHandlerForTest(t)
	h := NewStableswapHandler(a)
	pool := stableswapPoolPreNG()

	results := stableswapPreNGResults(t, a)
	results[preNG2CoinAPreciseIdx] = outbound.Result{Success: false} // A_precise reverts

	mc := curveMC(results)
	_, err := h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Error("reverted extended read (A_precise) must error, got nil")
	}
}

// TestStableswapHandler_SnapshotConfigGetterRevertErrors verifies that a reverted
// required config getter bubbles up as an error and stops the block rather than
// silently dropping the config row.
func TestStableswapHandler_SnapshotConfigGetterRevertErrors(t *testing.T) {
	_, a := newStableswapHandlerForTest(t)
	h := NewStableswapHandler(a)
	pool := stableswapPoolPreNG()

	results := stableswapPreNGResults(t, a)
	results[preNG2CoinInitialAIdx] = outbound.Result{Success: false} // initial_A reverts

	mc := curveMC(results)
	_, err := h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Error("reverted required config getter must error, got nil")
	}
}

// TestStableswapHandler_SnapshotNoAPreciseGatesCall verifies that a pre-NG pool
// lacking A_precise (HasAPrecise=false, e.g. 3pool) issues no A_precise call,
// leaves a_precise a structural NULL, and keeps the decode cursor aligned so every
// other field still decodes correctly and no error is returned.
func TestStableswapHandler_SnapshotNoAPreciseGatesCall(t *testing.T) {
	_, a := newStableswapHandlerForTest(t)
	h := NewStableswapHandler(a)
	pool := stableswapPoolPreNG()
	pool.HasAPrecise = false

	// The canned pre-NG results include the A_precise entry at index preNG2CoinAPreciseIdx;
	// drop it so the result list matches the gated (one-shorter) call list.
	base := stableswapPreNGResults(t, a)
	results := make([]outbound.Result, 0, len(base)-1)
	results = append(results, base[:preNG2CoinAPreciseIdx]...)
	results = append(results, base[preNG2CoinAPreciseIdx+1:]...)

	aPreciseData, err := a.Pack("A_precise")
	if err != nil {
		t.Fatalf("packing A_precise: %v", err)
	}

	mc := curveMC(results)
	ss, err := h.SnapshotState(context.Background(), mc, pool, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	for i, c := range snapshotCalls(t, mc) {
		if bytes.Equal(c.CallData, aPreciseData) {
			t.Errorf("call[%d] is A_precise, but HasAPrecise=false must issue no A_precise call", i)
		}
	}

	st := ss.Stableswap
	if st == nil {
		t.Fatal("want stableswap snapshot")
	}
	if st.APrecise != nil {
		t.Errorf("a_precise = %v, want nil (structural NULL when gated off)", st.APrecise)
	}
	// Cursor stays aligned: fields after the gated-off A_precise still decode.
	if len(st.AdminBalances) != 2 {
		t.Errorf("admin_balances len = %d, want 2", len(st.AdminBalances))
	}
	if st.CalcTokenAmount == nil {
		t.Error("calc_token_amount must still populate")
	}
	if len(st.CalcWithdrawOneCoin) != 2 {
		t.Errorf("calc_withdraw_one_coin len = %d, want 2", len(st.CalcWithdrawOneCoin))
	}
	if ss.StableswapConfig == nil {
		t.Fatal("config must still build")
	}
	if ss.StableswapConfig.InitialA.Cmp(big.NewInt(20000)) != 0 {
		t.Errorf("config initial_a = %v, want 20000", ss.StableswapConfig.InitialA)
	}
	if ss.StableswapConfig.FutureAdminFee == nil || ss.StableswapConfig.FutureAdminFee.Cmp(big.NewInt(5000000000)) != 0 {
		t.Errorf("config future_admin_fee = %v, want 5000000000", ss.StableswapConfig.FutureAdminFee)
	}
}
