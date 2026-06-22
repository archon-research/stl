package curveindexer

import (
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
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
