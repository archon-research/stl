package curveindexer

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// packWords concatenates 32-byte big-endian representations of each value.
func packWords(vals ...*big.Int) []byte {
	var buf bytes.Buffer
	for _, v := range vals {
		buf.Write(common.LeftPadBytes(v.Bytes(), 32))
	}
	return buf.Bytes()
}

// word is a convenience shorthand.
func word(v int64) *big.Int { return big.NewInt(v) }

// buildLiquidityLog builds a shared.Log with the given topic0, indexed provider
// address in Topics[1], and hand-packed data words.
func buildLiquidityLog(
	addr common.Address,
	topic0 common.Hash,
	provider common.Address,
	data []byte,
	logIndex uint,
) shared.Log {
	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	return shared.Log{
		Address: addr.Hex(),
		Topics: []string{
			topic0.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + hex.EncodeToString(data),
		TransactionHash: txHash.Hex(),
		LogIndex:        hexUintStr(logIndex),
	}
}

func buildReceiptFromLog(log shared.Log) shared.TransactionReceipt {
	return shared.TransactionReceipt{
		Logs:            []shared.Log{log},
		TransactionHash: log.TransactionHash,
	}
}

// ---------------------------------------------------------------------------
// Classic (pre-NG) liquidity event tests
// ---------------------------------------------------------------------------

// TestStableswapPreNG_Classic2CoinAddLiquidity verifies that a classic 2-coin
// AddLiquidity log on a KindStableswapPreNG pool is decoded as LiquidityAdd.
// This is also the regression guard: previously it was only captured, not typed.
func TestStableswapPreNG_Classic2CoinAddLiquidity(t *testing.T) {
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
	provider := common.HexToAddress("0x1234000000000000000000000000000000000001")

	// classic AddLiquidity(address,uint256[2],uint256[2],uint256,uint256)
	topic0 := eventTopic0("AddLiquidity(address,uint256[2],uint256[2],uint256,uint256)")
	data := packWords(
		word(100), word(200), // token_amounts[0], [1]
		word(10), word(20), // fees[0], [1]
		word(1000), // invariant
		word(5000), // token_supply
	)
	log := buildLiquidityLog(pool.Address, topic0, provider, data, 7)
	receipt := buildReceiptFromLog(log)

	got, err := h.DecodeEvents(receipt, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Liquidity) != 1 {
		t.Fatalf("liquidity len = %d, want 1", len(got.Liquidity))
	}
	rec := got.Liquidity[0]
	if rec.Kind != LiquidityAdd {
		t.Errorf("Kind = %q, want add", rec.Kind)
	}
	if len(rec.TokenAmounts) != 2 {
		t.Errorf("TokenAmounts len = %d, want 2", len(rec.TokenAmounts))
	}
	if len(rec.Fees) != 2 {
		t.Errorf("Fees len = %d, want 2", len(rec.Fees))
	}
	if rec.Invariant == nil {
		t.Error("Invariant must be non-nil")
	}
	if rec.TokenSupply == nil {
		t.Error("TokenSupply must be non-nil")
	}
	if rec.Provider != provider {
		t.Errorf("Provider = %s, want %s", rec.Provider, provider)
	}
	if rec.LogIndex != 7 {
		t.Errorf("LogIndex = %d, want 7", rec.LogIndex)
	}
	// Capture net must also contain the event.
	if len(got.Captured) == 0 {
		t.Error("captured events must be non-empty")
	}
}

// TestStableswapPreNG_Classic3CoinRemoveLiquidity verifies a 3-coin RemoveLiquidity.
func TestStableswapPreNG_Classic3CoinRemoveLiquidity(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)

	pool := RegisteredPool{
		ID:      2,
		Address: common.HexToAddress("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"),
		Kind:    KindStableswapPreNG,
		NCoins:  3,
	}
	provider := common.HexToAddress("0x2222000000000000000000000000000000000002")

	// classic RemoveLiquidity(address,uint256[3],uint256[3],uint256)
	topic0 := eventTopic0("RemoveLiquidity(address,uint256[3],uint256[3],uint256)")
	data := packWords(
		word(100), word(200), word(300), // token_amounts
		word(10), word(20), word(30), // fees
		word(9000), // token_supply
	)
	log := buildLiquidityLog(pool.Address, topic0, provider, data, 2)
	receipt := buildReceiptFromLog(log)

	got, err := h.DecodeEvents(receipt, pool, 1, 200, 0, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Liquidity) != 1 {
		t.Fatalf("liquidity len = %d, want 1", len(got.Liquidity))
	}
	rec := got.Liquidity[0]
	if rec.Kind != LiquidityRemove {
		t.Errorf("Kind = %q, want remove", rec.Kind)
	}
	if len(rec.TokenAmounts) != 3 {
		t.Errorf("TokenAmounts len = %d, want 3", len(rec.TokenAmounts))
	}
	if rec.TokenSupply == nil {
		t.Error("TokenSupply must be non-nil")
	}
	if rec.TokenSupply.Int64() != 9000 {
		t.Errorf("TokenSupply = %s, want 9000", rec.TokenSupply)
	}
}

// TestStableswapPreNG_ClassicRemoveLiquidityOne verifies the classic variant:
// no coin_index, 2 token_amounts (LP burned + coin received).
func TestStableswapPreNG_ClassicRemoveLiquidityOne(t *testing.T) {
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
	provider := common.HexToAddress("0x3333000000000000000000000000000000000003")

	// classic RemoveLiquidityOne(address,uint256,uint256) — no coin_index
	topic0 := eventTopic0("RemoveLiquidityOne(address,uint256,uint256)")
	data := packWords(word(500), word(480)) // LP burned, coin received
	log := buildLiquidityLog(pool.Address, topic0, provider, data, 1)
	receipt := buildReceiptFromLog(log)

	got, err := h.DecodeEvents(receipt, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Liquidity) != 1 {
		t.Fatalf("liquidity len = %d, want 1", len(got.Liquidity))
	}
	rec := got.Liquidity[0]
	if rec.Kind != LiquidityRemoveOne {
		t.Errorf("Kind = %q, want remove_one", rec.Kind)
	}
	if rec.CoinIndex != nil {
		t.Errorf("CoinIndex must be nil for classic variant, got %d", *rec.CoinIndex)
	}
	if len(rec.TokenAmounts) != 2 {
		t.Errorf("TokenAmounts len = %d, want 2", len(rec.TokenAmounts))
	}
	if rec.TokenAmounts[0].Int64() != 500 {
		t.Errorf("TokenAmounts[0] = %s, want 500", rec.TokenAmounts[0])
	}
	if rec.TokenAmounts[1].Int64() != 480 {
		t.Errorf("TokenAmounts[1] = %s, want 480", rec.TokenAmounts[1])
	}
}

// ---------------------------------------------------------------------------
// NG RemoveLiquidityOne test (ABI-based with token_id fix)
// ---------------------------------------------------------------------------

// TestStableswapNG_RemoveLiquidityOneHasCoinIndex verifies the NG form includes
// token_id decoded as CoinIndex and that TokenSupply is non-nil.
func TestStableswapNG_RemoveLiquidityOneHasCoinIndex(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)

	pool := RegisteredPool{
		ID:      3,
		Address: common.HexToAddress("0x21E27a5E5513D6e65C4f830167390997aA84843a"),
		Kind:    KindStableswapNG,
		NCoins:  2,
	}
	provider := common.HexToAddress("0x4444000000000000000000000000000000000004")

	ev, ok := a.Events["RemoveLiquidityOne"]
	if !ok {
		t.Fatal("RemoveLiquidityOne not in NG ABI")
	}

	// Pack the NG RemoveLiquidityOne non-indexed fields:
	// token_id (int128), token_amount, coin_amount, token_supply
	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	packed, err := nonIndexed.Pack(
		big.NewInt(1),    // token_id = coin index 1
		big.NewInt(1000), // token_amount (LP burned)
		big.NewInt(990),  // coin_amount
		big.NewInt(8000), // token_supply
	)
	if err != nil {
		t.Fatalf("packing RemoveLiquidityOne: %v", err)
	}

	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	log := shared.Log{
		Address: pool.Address.Hex(),
		Topics: []string{
			ev.ID.Hex(),
			common.BytesToHash(provider.Bytes()).Hex(),
		},
		Data:            "0x" + hex.EncodeToString(packed),
		TransactionHash: txHash.Hex(),
		LogIndex:        hexUintStr(4),
	}
	receipt := buildReceiptFromLog(log)

	got, err := h.DecodeEvents(receipt, pool, 1, 300, 0, time.Unix(3, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Liquidity) != 1 {
		t.Fatalf("liquidity len = %d, want 1", len(got.Liquidity))
	}
	rec := got.Liquidity[0]
	if rec.Kind != LiquidityRemoveOne {
		t.Errorf("Kind = %q, want remove_one", rec.Kind)
	}
	if rec.CoinIndex == nil {
		t.Fatal("CoinIndex must be non-nil for NG variant")
	}
	if *rec.CoinIndex != 1 {
		t.Errorf("CoinIndex = %d, want 1", *rec.CoinIndex)
	}
	if rec.TokenSupply == nil {
		t.Error("TokenSupply must be non-nil for NG variant")
	}
	if rec.TokenSupply.Int64() != 8000 {
		t.Errorf("TokenSupply = %s, want 8000", rec.TokenSupply)
	}
	if len(rec.TokenAmounts) != 2 {
		t.Errorf("TokenAmounts len = %d, want 2", len(rec.TokenAmounts))
	}
	if rec.TokenAmounts[0].Int64() != 1000 {
		t.Errorf("TokenAmounts[0] = %s, want 1000 (LP burned)", rec.TokenAmounts[0])
	}
}

// ---------------------------------------------------------------------------
// Cryptoswap liquidity event tests
// ---------------------------------------------------------------------------

// TestCryptoswap_3CoinAddLiquidity verifies cryptoswap AddLiquidity with NCoins=3.
func TestCryptoswap_3CoinAddLiquidity(t *testing.T) {
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
	provider := common.HexToAddress("0x5555000000000000000000000000000000000005")

	// cryptoswap AddLiquidity(address,uint256[3],uint256,uint256,uint256)
	topic0 := eventTopic0("AddLiquidity(address,uint256[3],uint256,uint256,uint256)")
	data := packWords(
		word(1000), word(2000), word(3000), // token_amounts
		word(100),    // fee
		word(6000),   // token_supply
		word(999999), // packed_price_scale (ignored)
	)
	log := buildLiquidityLog(pool.Address, topic0, provider, data, 5)
	receipt := buildReceiptFromLog(log)

	got, err := h.DecodeEvents(receipt, pool, 1, 500, 0, time.Unix(5, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Liquidity) != 1 {
		t.Fatalf("liquidity len = %d, want 1", len(got.Liquidity))
	}
	rec := got.Liquidity[0]
	if rec.Kind != LiquidityAdd {
		t.Errorf("Kind = %q, want add", rec.Kind)
	}
	if len(rec.TokenAmounts) != 3 {
		t.Errorf("TokenAmounts len = %d, want 3", len(rec.TokenAmounts))
	}
	if rec.TokenSupply == nil {
		t.Error("TokenSupply must be non-nil")
	}
	if rec.TokenSupply.Int64() != 6000 {
		t.Errorf("TokenSupply = %s, want 6000", rec.TokenSupply)
	}
	if len(rec.Fees) != 1 {
		t.Errorf("Fees len = %d, want 1", len(rec.Fees))
	}
	if rec.Fees[0].Int64() != 100 {
		t.Errorf("Fees[0] = %s, want 100", rec.Fees[0])
	}
	if len(got.Captured) == 0 {
		t.Error("captured events must be non-empty")
	}
}

// TestCryptoswap_RemoveLiquidityOne verifies cryptoswap RemoveLiquidityOne
// correctly extracts CoinIndex from words[1] and coin_amount from words[2].
func TestCryptoswap_RemoveLiquidityOne(t *testing.T) {
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
	provider := common.HexToAddress("0x6666000000000000000000000000000000000006")

	// cryptoswap RemoveLiquidityOne(address,uint256,uint256,uint256,uint256,uint256)
	// words: token_amount, coin_index, coin_amount, approx_fee, packed_price_scale
	topic0 := eventTopic0("RemoveLiquidityOne(address,uint256,uint256,uint256,uint256,uint256)")
	data := packWords(
		word(800), // token_amount (LP burned)
		word(2),   // coin_index
		word(750), // coin_amount
		word(5),   // approx_fee (ignored)
		word(999), // packed_price_scale (ignored)
	)
	log := buildLiquidityLog(pool.Address, topic0, provider, data, 3)
	receipt := buildReceiptFromLog(log)

	got, err := h.DecodeEvents(receipt, pool, 1, 500, 0, time.Unix(5, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Liquidity) != 1 {
		t.Fatalf("liquidity len = %d, want 1", len(got.Liquidity))
	}
	rec := got.Liquidity[0]
	if rec.Kind != LiquidityRemoveOne {
		t.Errorf("Kind = %q, want remove_one", rec.Kind)
	}
	if rec.CoinIndex == nil {
		t.Fatal("CoinIndex must be non-nil for cryptoswap")
	}
	if *rec.CoinIndex != 2 {
		t.Errorf("CoinIndex = %d, want 2", *rec.CoinIndex)
	}
	if len(rec.TokenAmounts) != 2 {
		t.Errorf("TokenAmounts len = %d, want 2", len(rec.TokenAmounts))
	}
	if rec.TokenAmounts[0].Int64() != 800 {
		t.Errorf("TokenAmounts[0] = %s, want 800 (LP burned)", rec.TokenAmounts[0])
	}
	if rec.TokenAmounts[1].Int64() != 750 {
		t.Errorf("TokenAmounts[1] = %s, want 750 (coin received)", rec.TokenAmounts[1])
	}
}

// TestStableswapPreNG_ClassicRemoveLiquidityImbalance verifies a 3-coin
// RemoveLiquidityImbalance log is decoded as LiquidityRemoveImbalance with
// correct TokenAmounts, Fees, Invariant, TokenSupply, and Provider.
func TestStableswapPreNG_ClassicRemoveLiquidityImbalance(t *testing.T) {
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewStableswapHandler(a)

	pool := RegisteredPool{
		ID:      5,
		Address: common.HexToAddress("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"),
		Kind:    KindStableswapPreNG,
		NCoins:  3,
	}
	provider := common.HexToAddress("0x8888000000000000000000000000000000000008")

	// RemoveLiquidityImbalance(address,uint256[3],uint256[3],uint256,uint256)
	// non-indexed layout: token_amounts[3], fees[3], invariant, token_supply
	topic0 := eventTopic0("RemoveLiquidityImbalance(address,uint256[3],uint256[3],uint256,uint256)")
	data := packWords(
		word(111), word(222), word(333), // token_amounts[0..2]
		word(11), word(22), word(33), // fees[0..2]
		word(77777), // invariant
		word(99999), // token_supply
	)
	log := buildLiquidityLog(pool.Address, topic0, provider, data, 9)
	receipt := buildReceiptFromLog(log)

	got, err := h.DecodeEvents(receipt, pool, 1, 400, 0, time.Unix(4, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Liquidity) != 1 {
		t.Fatalf("liquidity len = %d, want 1", len(got.Liquidity))
	}
	rec := got.Liquidity[0]
	if rec.Kind != LiquidityRemoveImbalance {
		t.Errorf("Kind = %q, want remove_imbalance", rec.Kind)
	}
	if len(rec.TokenAmounts) != 3 {
		t.Errorf("TokenAmounts len = %d, want 3", len(rec.TokenAmounts))
	}
	if rec.TokenAmounts[0].Int64() != 111 || rec.TokenAmounts[1].Int64() != 222 || rec.TokenAmounts[2].Int64() != 333 {
		t.Errorf("TokenAmounts = [%s,%s,%s], want [111,222,333]", rec.TokenAmounts[0], rec.TokenAmounts[1], rec.TokenAmounts[2])
	}
	if len(rec.Fees) != 3 {
		t.Errorf("Fees len = %d, want 3", len(rec.Fees))
	}
	if rec.Fees[0].Int64() != 11 || rec.Fees[1].Int64() != 22 || rec.Fees[2].Int64() != 33 {
		t.Errorf("Fees = [%s,%s,%s], want [11,22,33]", rec.Fees[0], rec.Fees[1], rec.Fees[2])
	}
	if rec.Invariant == nil {
		t.Fatal("Invariant must be non-nil")
	}
	if rec.Invariant.Int64() != 77777 {
		t.Errorf("Invariant = %s, want 77777", rec.Invariant)
	}
	if rec.TokenSupply == nil {
		t.Fatal("TokenSupply must be non-nil")
	}
	if rec.TokenSupply.Int64() != 99999 {
		t.Errorf("TokenSupply = %s, want 99999", rec.TokenSupply)
	}
	if rec.Provider != provider {
		t.Errorf("Provider = %s, want %s", rec.Provider, provider)
	}
	if rec.LogIndex != 9 {
		t.Errorf("LogIndex = %d, want 9", rec.LogIndex)
	}
}

// TestCryptoswap_RemoveLiquidity verifies a 2-coin cryptoswap RemoveLiquidity
// log is decoded as LiquidityRemove with correct TokenAmounts, TokenSupply,
// and Provider; CoinIndex must be nil.
func TestCryptoswap_RemoveLiquidity(t *testing.T) {
	a, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	h := NewCryptoswapHandler(a)

	pool := RegisteredPool{
		ID:      20,
		Address: common.HexToAddress("0xA5407eAE9Ba41422680e2e00537571bcC53efBfD"),
		Kind:    KindCryptoswap,
		NCoins:  2,
	}
	provider := common.HexToAddress("0x9999000000000000000000000000000000000009")

	// RemoveLiquidity(address,uint256[2],uint256)
	// non-indexed layout: token_amounts[2], token_supply
	topic0 := eventTopic0("RemoveLiquidity(address,uint256[2],uint256)")
	data := packWords(
		word(4444), word(5555), // token_amounts[0..1]
		word(88888), // token_supply
	)
	log := buildLiquidityLog(pool.Address, topic0, provider, data, 6)
	receipt := buildReceiptFromLog(log)

	got, err := h.DecodeEvents(receipt, pool, 1, 600, 0, time.Unix(6, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Liquidity) != 1 {
		t.Fatalf("liquidity len = %d, want 1", len(got.Liquidity))
	}
	rec := got.Liquidity[0]
	if rec.Kind != LiquidityRemove {
		t.Errorf("Kind = %q, want remove", rec.Kind)
	}
	if len(rec.TokenAmounts) != 2 {
		t.Errorf("TokenAmounts len = %d, want 2", len(rec.TokenAmounts))
	}
	if rec.TokenAmounts[0].Int64() != 4444 || rec.TokenAmounts[1].Int64() != 5555 {
		t.Errorf("TokenAmounts = [%s,%s], want [4444,5555]", rec.TokenAmounts[0], rec.TokenAmounts[1])
	}
	if rec.TokenSupply == nil {
		t.Fatal("TokenSupply must be non-nil")
	}
	if rec.TokenSupply.Int64() != 88888 {
		t.Errorf("TokenSupply = %s, want 88888", rec.TokenSupply)
	}
	if rec.Provider != provider {
		t.Errorf("Provider = %s, want %s", rec.Provider, provider)
	}
	if rec.CoinIndex != nil {
		t.Errorf("CoinIndex must be nil for RemoveLiquidity, got %d", *rec.CoinIndex)
	}
	if rec.LogIndex != 6 {
		t.Errorf("LogIndex = %d, want 6", rec.LogIndex)
	}
}

// ---------------------------------------------------------------------------
// C3: missing provider topic error-path tests
// ---------------------------------------------------------------------------

// TestDecodeClassicLiquidity_MatchedSigMissingProvider verifies that when a log's
// topic0 matches a known classic liquidity signature but Topics[1] (provider) is
// absent, decodeClassicLiquidity returns a non-nil error rather than silently
// returning (nil, false, nil).
func TestDecodeClassicLiquidity_MatchedSigMissingProvider(t *testing.T) {
	pool := RegisteredPool{
		ID:      1,
		Address: common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:    KindStableswapPreNG,
		NCoins:  2,
	}
	// A classic AddLiquidity topic0 for a 2-coin pool.
	topic0 := eventTopic0("AddLiquidity(address,uint256[2],uint256[2],uint256,uint256)")
	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	log := shared.Log{
		Address:         pool.Address.Hex(),
		Topics:          []string{topic0.Hex()}, // only one topic -- provider missing
		Data:            "0x",
		TransactionHash: txHash.Hex(),
		LogIndex:        "0x0",
	}
	rec, matched, err := decodeClassicLiquidity(log, pool)
	if err == nil {
		t.Errorf("expected non-nil error for matched classic sig with missing provider, got rec=%v matched=%v", rec, matched)
	}
	if matched {
		t.Errorf("matched must be false when provider topic is missing")
	}
}

// TestDecodeClassicLiquidity_UnknownTopicMissingProvider verifies that when a log
// has an unknown topic0 and only one topic, decodeClassicLiquidity returns
// (nil, false, nil) -- the unknown-sig path must not error.
func TestDecodeClassicLiquidity_UnknownTopicMissingProvider(t *testing.T) {
	pool := RegisteredPool{
		ID:      1,
		Address: common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:    KindStableswapPreNG,
		NCoins:  2,
	}
	unknownTopic := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	log := shared.Log{
		Address:         pool.Address.Hex(),
		Topics:          []string{unknownTopic.Hex()}, // unknown sig, one topic
		Data:            "0x",
		TransactionHash: txHash.Hex(),
		LogIndex:        "0x0",
	}
	rec, matched, err := decodeClassicLiquidity(log, pool)
	if err != nil {
		t.Errorf("unexpected error for unknown topic with missing provider: %v", err)
	}
	if matched {
		t.Errorf("matched must be false for unknown topic0")
	}
	if rec != nil {
		t.Errorf("rec must be nil for unknown topic0, got %v", rec)
	}
}

// TestDecodeCryptoLiquidity_MatchedSigMissingProvider verifies that when a log's
// topic0 matches a known cryptoswap liquidity signature but Topics[1] (provider)
// is absent, decodeCryptoLiquidity returns a non-nil error.
func TestDecodeCryptoLiquidity_MatchedSigMissingProvider(t *testing.T) {
	pool := RegisteredPool{
		ID:      10,
		Address: common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:    KindCryptoswap,
		NCoins:  2,
	}
	// A cryptoswap AddLiquidity topic0 for a 2-coin pool.
	topic0 := eventTopic0("AddLiquidity(address,uint256[2],uint256,uint256,uint256)")
	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	log := shared.Log{
		Address:         pool.Address.Hex(),
		Topics:          []string{topic0.Hex()}, // only one topic -- provider missing
		Data:            "0x",
		TransactionHash: txHash.Hex(),
		LogIndex:        "0x0",
	}
	rec, matched, err := decodeCryptoLiquidity(log, pool)
	if err == nil {
		t.Errorf("expected non-nil error for matched cryptoswap sig with missing provider, got rec=%v matched=%v", rec, matched)
	}
	if matched {
		t.Errorf("matched must be false when provider topic is missing")
	}
}

// TestDecodeCryptoLiquidity_UnknownTopicMissingProvider verifies that when a log
// has an unknown topic0 and only one topic, decodeCryptoLiquidity returns
// (nil, false, nil) -- the unknown-sig path must not error.
func TestDecodeCryptoLiquidity_UnknownTopicMissingProvider(t *testing.T) {
	pool := RegisteredPool{
		ID:      10,
		Address: common.HexToAddress("0xD51a44d3FaE010294C616388b506AcdA1bfAAE46"),
		Kind:    KindCryptoswap,
		NCoins:  2,
	}
	unknownTopic := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	txHash := common.HexToHash("0xdeadbeef01020304050607080900010203040506070809000102030405060708")
	log := shared.Log{
		Address:         pool.Address.Hex(),
		Topics:          []string{unknownTopic.Hex()}, // unknown sig, one topic
		Data:            "0x",
		TransactionHash: txHash.Hex(),
		LogIndex:        "0x0",
	}
	rec, matched, err := decodeCryptoLiquidity(log, pool)
	if err != nil {
		t.Errorf("unexpected error for unknown topic with missing provider: %v", err)
	}
	if matched {
		t.Errorf("matched must be false for unknown topic0")
	}
	if rec != nil {
		t.Errorf("rec must be nil for unknown topic0, got %v", rec)
	}
}

// TestStableswapPreNG_ClassicTypedNotOnlyCapture is a regression guard asserting
// that a classic AddLiquidity log on a KindStableswapPreNG pool produces a
// LiquidityRecord (was previously only captured-not-typed).
func TestStableswapPreNG_ClassicTypedNotOnlyCapture(t *testing.T) {
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
	provider := common.HexToAddress("0x7777000000000000000000000000000000000007")
	topic0 := eventTopic0("AddLiquidity(address,uint256[2],uint256[2],uint256,uint256)")
	data := packWords(word(1), word(2), word(0), word(0), word(100), word(200))
	log := buildLiquidityLog(pool.Address, topic0, provider, data, 0)

	got, err := h.DecodeEvents(buildReceiptFromLog(log), pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Liquidity) == 0 {
		t.Error("classic AddLiquidity on PreNG pool must produce a typed LiquidityRecord (regression: was captured-only)")
	}
}
