package uniswapv3indexer

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// testPool is the fixture RegisteredPool for decode tests.
func testPool() RegisteredPool {
	return RegisteredPool{
		ID:             7,
		Address:        common.HexToAddress("0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8"),
		Token0:         common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
		Token1:         common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		Token0Decimals: 18,
		Token1Decimals: 6,
		Fee:            3000,
		TickSpacing:    60,
		DeployBlock:    12369739,
	}
}

const (
	chainID     = int64(1)
	blockNumber = int64(19000000)
	blockVer    = 0
)

var blockTS = time.Unix(1700000000, 0).UTC()

// addrTopic left-pads an address into a 32-byte topic word.
func addrTopic(a common.Address) common.Hash { return common.BytesToHash(a.Bytes()) }

// signedTopic encodes a signed value (e.g. int24 tick) into a 32-byte topic
// word using two's complement, matching how solidity stores indexed signed
// params. big.Int values are ABI-length-checked by go-ethereum's ParseTopics
// path only against the raw bytes, so callers must pre-wrap negatives here.
func signedTopic(v int64) common.Hash {
	bi := big.NewInt(v)
	if bi.Sign() < 0 {
		// Two's complement over 256 bits: 2^256 + v.
		mod := new(big.Int).Lsh(big.NewInt(1), 256)
		bi = new(big.Int).Add(mod, bi)
	}
	return common.BigToHash(bi)
}

// buildLog ABI-encodes an event's non-indexed args into Data and places the
// given indexed topics (already 32-byte hashes) after topic0.
func buildLog(
	t *testing.T,
	a *abi.ABI,
	eventName string,
	addr common.Address,
	logIndexHex string,
	indexedTopics []common.Hash,
	nonIndexedValues ...any,
) shared.Log {
	t.Helper()
	ev, ok := a.Events[eventName]
	if !ok {
		t.Fatalf("event %s not in ABI", eventName)
	}
	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	var data []byte
	if len(nonIndexed) > 0 {
		packed, err := nonIndexed.Pack(nonIndexedValues...)
		if err != nil {
			t.Fatalf("packing %s data: %v", eventName, err)
		}
		data = packed
	}
	topics := []string{ev.ID.Hex()}
	for _, h := range indexedTopics {
		topics = append(topics, h.Hex())
	}
	return shared.Log{
		Address:         addr.Hex(),
		Topics:          topics,
		Data:            "0x" + hex.EncodeToString(data),
		TransactionHash: common.HexToHash("0xfeed000000000000000000000000000000000000000000000000000000000001").Hex(),
		LogIndex:        logIndexHex,
	}
}

// buildRawLog constructs a log with an unrecognized topic0 (still routed to
// the pool address) so we can exercise the unknown-event capture path.
func buildRawLog(addr common.Address, logIndexHex string, topics []string, data string) shared.Log {
	return shared.Log{
		Address:         addr.Hex(),
		Topics:          topics,
		Data:            data,
		TransactionHash: common.HexToHash("0xfeed000000000000000000000000000000000000000000000000000000000002").Hex(),
		LogIndex:        logIndexHex,
	}
}

func receiptOf(logs ...shared.Log) shared.TransactionReceipt {
	return shared.TransactionReceipt{Logs: logs}
}

func poolABIForTest(t *testing.T) *abi.ABI {
	t.Helper()
	a, err := PoolABI()
	if err != nil {
		t.Fatalf("loading pool ABI: %v", err)
	}
	return a
}

func decodePayload(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshalling payload: %v", err)
	}
	return m
}

// ---------------------------------------------------------------------------
// Initialize
// ---------------------------------------------------------------------------

func TestDecodeEvents_Initialize(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sqrtPriceX96 := big.NewInt(1234567890123)
	log := buildLog(t, a, "Initialize", pool.Address, "0x0", nil, sqrtPriceX96, big.NewInt(-100))

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.PoolEvents) != 1 {
		t.Fatalf("PoolEvents = %d, want 1", len(got.PoolEvents))
	}
	ev := got.PoolEvents[0]
	if ev.EventName != entity.PoolEventInitialize {
		t.Errorf("EventName = %q, want %q", ev.EventName, entity.PoolEventInitialize)
	}
	if err := ev.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
	params := decodePayload(t, ev.Params)
	if params["sqrtPriceX96"] != sqrtPriceX96.String() {
		t.Errorf("params.sqrtPriceX96 = %v, want %s", params["sqrtPriceX96"], sqrtPriceX96.String())
	}
	if params["tick"] != "-100" {
		t.Errorf("params.tick = %v, want -100", params["tick"])
	}
}

// ---------------------------------------------------------------------------
// Mint
// ---------------------------------------------------------------------------

func TestDecodeEvents_Mint(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	owner := common.HexToAddress("0x2222222222222222222222222222222222222222")
	amount := big.NewInt(500000)
	amount0 := big.NewInt(1000)
	amount1 := big.NewInt(2000)

	log := buildLog(t, a, "Mint", pool.Address, "0x1",
		[]common.Hash{addrTopic(owner), signedTopic(-120), signedTopic(180)},
		sender, amount, amount0, amount1,
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.LiquidityEvents) != 1 {
		t.Fatalf("LiquidityEvents = %d, want 1", len(got.LiquidityEvents))
	}
	e := got.LiquidityEvents[0]
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if e.EventName != entity.LiquidityEventMint {
		t.Errorf("EventName = %q, want mint", e.EventName)
	}
	if e.Owner != owner {
		t.Errorf("Owner = %s, want %s", e.Owner, owner)
	}
	if e.Sender == nil || *e.Sender != sender {
		t.Errorf("Sender = %v, want %s", e.Sender, sender)
	}
	if e.Recipient != nil {
		t.Errorf("Recipient = %v, want nil for mint", e.Recipient)
	}
	if e.TickLower != -120 {
		t.Errorf("TickLower = %d, want -120", e.TickLower)
	}
	if e.TickUpper != 180 {
		t.Errorf("TickUpper = %d, want 180", e.TickUpper)
	}
	if e.Amount == nil || e.Amount.Cmp(amount) != 0 {
		t.Errorf("Amount = %v, want %s", e.Amount, amount)
	}
	if e.Amount0.Cmp(amount0) != 0 {
		t.Errorf("Amount0 = %s, want %s", e.Amount0, amount0)
	}
	if e.Amount1.Cmp(amount1) != 0 {
		t.Errorf("Amount1 = %s, want %s", e.Amount1, amount1)
	}
}

// ---------------------------------------------------------------------------
// Collect
// ---------------------------------------------------------------------------

func TestDecodeEvents_Collect(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	owner := common.HexToAddress("0x3333333333333333333333333333333333333333")
	recipient := common.HexToAddress("0x4444444444444444444444444444444444444444")
	amount0 := big.NewInt(111)
	amount1 := big.NewInt(222)

	log := buildLog(t, a, "Collect", pool.Address, "0x2",
		[]common.Hash{addrTopic(owner), signedTopic(-60), signedTopic(60)},
		recipient, amount0, amount1,
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.LiquidityEvents) != 1 {
		t.Fatalf("LiquidityEvents = %d, want 1", len(got.LiquidityEvents))
	}
	e := got.LiquidityEvents[0]
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if e.EventName != entity.LiquidityEventCollect {
		t.Errorf("EventName = %q, want collect", e.EventName)
	}
	if e.Owner != owner {
		t.Errorf("Owner = %s, want %s", e.Owner, owner)
	}
	if e.Recipient == nil || *e.Recipient != recipient {
		t.Errorf("Recipient = %v, want %s", e.Recipient, recipient)
	}
	if e.Sender != nil {
		t.Errorf("Sender = %v, want nil for collect", e.Sender)
	}
	if e.Amount != nil {
		t.Errorf("Amount = %v, want nil for collect", e.Amount)
	}
	if e.TickLower != -60 || e.TickUpper != 60 {
		t.Errorf("ticks = [%d,%d], want [-60,60]", e.TickLower, e.TickUpper)
	}
	if e.Amount0.Cmp(amount0) != 0 || e.Amount1.Cmp(amount1) != 0 {
		t.Errorf("amounts = [%s,%s], want [%s,%s]", e.Amount0, e.Amount1, amount0, amount1)
	}
}

// ---------------------------------------------------------------------------
// Burn
// ---------------------------------------------------------------------------

func TestDecodeEvents_Burn(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	owner := common.HexToAddress("0x5555555555555555555555555555555555555555")
	amount := big.NewInt(999)
	amount0 := big.NewInt(10)
	amount1 := big.NewInt(20)

	log := buildLog(t, a, "Burn", pool.Address, "0x3",
		[]common.Hash{addrTopic(owner), signedTopic(-240), signedTopic(-60)},
		amount, amount0, amount1,
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.LiquidityEvents) != 1 {
		t.Fatalf("LiquidityEvents = %d, want 1", len(got.LiquidityEvents))
	}
	e := got.LiquidityEvents[0]
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if e.EventName != entity.LiquidityEventBurn {
		t.Errorf("EventName = %q, want burn", e.EventName)
	}
	if e.Sender != nil {
		t.Errorf("Sender = %v, want nil for burn", e.Sender)
	}
	if e.Recipient != nil {
		t.Errorf("Recipient = %v, want nil for burn", e.Recipient)
	}
	if e.Amount == nil || e.Amount.Cmp(amount) != 0 {
		t.Errorf("Amount = %v, want %s", e.Amount, amount)
	}
	// Both ticks negative: covers two's-complement decode when neither indexed
	// int24 in the pair is positive.
	if e.TickLower != -240 || e.TickUpper != -60 {
		t.Errorf("ticks = [%d,%d], want [-240,-60]", e.TickLower, e.TickUpper)
	}
}

// ---------------------------------------------------------------------------
// Swap
// ---------------------------------------------------------------------------

func TestDecodeEvents_SwapNegativeAmount0(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sender := common.HexToAddress("0x6666666666666666666666666666666666666666")
	recipient := common.HexToAddress("0x7777777777777777777777777777777777777777")
	amount0 := big.NewInt(-500) // pool paid out token0
	amount1 := big.NewInt(1500)
	sqrtPriceX96, ok := new(big.Int).SetString("79228162514264337593543950336", 10)
	if !ok {
		t.Fatal("parsing sqrtPriceX96 literal")
	}
	liquidity := big.NewInt(123456789)

	log := buildLog(t, a, "Swap", pool.Address, "0x4",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		amount0, amount1, sqrtPriceX96, liquidity, big.NewInt(-887220),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Swaps) != 1 {
		t.Fatalf("Swaps = %d, want 1", len(got.Swaps))
	}
	s := got.Swaps[0]
	if err := s.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if s.Sender != sender || s.Recipient != recipient {
		t.Errorf("sender/recipient = %s/%s, want %s/%s", s.Sender, s.Recipient, sender, recipient)
	}
	if s.Amount0.Cmp(amount0) != 0 {
		t.Errorf("Amount0 = %s, want %s", s.Amount0, amount0)
	}
	if s.Amount0.Sign() >= 0 {
		t.Errorf("Amount0 sign = %d, want negative", s.Amount0.Sign())
	}
	if s.Amount1.Cmp(amount1) != 0 {
		t.Errorf("Amount1 = %s, want %s", s.Amount1, amount1)
	}
	if s.SqrtPriceX96.Cmp(sqrtPriceX96) != 0 {
		t.Errorf("SqrtPriceX96 = %s, want %s", s.SqrtPriceX96, sqrtPriceX96)
	}
	if s.Liquidity.Cmp(liquidity) != 0 {
		t.Errorf("Liquidity = %s, want %s", s.Liquidity, liquidity)
	}
	if s.Tick != -887220 {
		t.Errorf("Tick = %d, want -887220", s.Tick)
	}
	if s.PoolID != pool.ID {
		t.Errorf("PoolID = %d, want %d", s.PoolID, pool.ID)
	}
	if s.BlockNumber != blockNumber || s.BlockVersion != blockVer {
		t.Errorf("block identity = (%d,%d), want (%d,%d)", s.BlockNumber, s.BlockVersion, blockNumber, blockVer)
	}
	if !s.BlockTimestamp.Equal(blockTS) {
		t.Errorf("BlockTimestamp = %v, want %v", s.BlockTimestamp, blockTS)
	}
}

// TestDecodeEvents_CapturedPayloadStringifiesBigInts proves the decoded
// capture-net payload encodes a *big.Int above 2^53 as a lossless JSON string
// (not a bare number), so a float-parsing consumer keeps full precision
// (PR#519 review #5).
func TestDecodeEvents_CapturedPayloadStringifiesBigInts(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sender := common.HexToAddress("0x6666666666666666666666666666666666666666")
	recipient := common.HexToAddress("0x7777777777777777777777777777777777777777")
	// liquidity is uint128; use a value far above float64's 2^53 lossless range.
	huge, ok := new(big.Int).SetString("9007199254740993000000", 10)
	if !ok {
		t.Fatal("parsing large liquidity literal")
	}
	sqrtPriceX96, ok := new(big.Int).SetString("79228162514264337593543950336", 10)
	if !ok {
		t.Fatal("parsing sqrtPriceX96 literal")
	}

	log := buildLog(t, a, "Swap", pool.Address, "0x4",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		big.NewInt(1), big.NewInt(1), sqrtPriceX96, huge, big.NewInt(1),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Captured) != 1 {
		t.Fatalf("Captured = %d, want 1", len(got.Captured))
	}

	payload := decodePayload(t, got.Captured[0].Payload)
	if s, isStr := payload["liquidity"].(string); !isStr {
		t.Errorf("liquidity encoded as %T (%v), want JSON string", payload["liquidity"], payload["liquidity"])
	} else if s != huge.String() {
		t.Errorf("liquidity = %q, want %q", s, huge.String())
	}
}

func TestDecodeEvents_SwapPositiveAmounts(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sender := common.HexToAddress("0x6666666666666666666666666666666666666666")
	recipient := common.HexToAddress("0x7777777777777777777777777777777777777777")
	amount0 := big.NewInt(500)
	amount1 := big.NewInt(-1500)

	log := buildLog(t, a, "Swap", pool.Address, "0x0",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		amount0, amount1, big.NewInt(1), big.NewInt(1), big.NewInt(100),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Swaps) != 1 {
		t.Fatalf("Swaps = %d, want 1", len(got.Swaps))
	}
	s := got.Swaps[0]
	if s.Amount0.Sign() <= 0 {
		t.Errorf("Amount0 sign = %d, want positive", s.Amount0.Sign())
	}
	if s.Amount1.Sign() >= 0 {
		t.Errorf("Amount1 sign = %d, want negative", s.Amount1.Sign())
	}
	if s.Tick != 100 {
		t.Errorf("Tick = %d, want 100", s.Tick)
	}
}

// ---------------------------------------------------------------------------
// Flash
// ---------------------------------------------------------------------------

func TestDecodeEvents_Flash(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sender := common.HexToAddress("0x8888888888888888888888888888888888888888")
	recipient := common.HexToAddress("0x9999999999999999999999999999999999999999")

	log := buildLog(t, a, "Flash", pool.Address, "0x1",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		big.NewInt(100), big.NewInt(200), big.NewInt(1), big.NewInt(2),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.PoolEvents) != 1 {
		t.Fatalf("PoolEvents = %d, want 1", len(got.PoolEvents))
	}
	ev := got.PoolEvents[0]
	if err := ev.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if ev.EventName != entity.PoolEventFlash {
		t.Errorf("EventName = %q, want %q", ev.EventName, entity.PoolEventFlash)
	}
	params := decodePayload(t, ev.Params)
	for _, key := range []string{"sender", "recipient", "amount0", "amount1", "paid0", "paid1"} {
		if _, ok := params[key]; !ok {
			t.Errorf("params missing key %q", key)
		}
	}
}

// ---------------------------------------------------------------------------
// IncreaseObservationCardinalityNext
// ---------------------------------------------------------------------------

func TestDecodeEvents_IncreaseObservationCardinalityNext(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()

	log := buildLog(t, a, "IncreaseObservationCardinalityNext", pool.Address, "0x0", nil,
		uint16(100), uint16(200),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.PoolEvents) != 1 {
		t.Fatalf("PoolEvents = %d, want 1", len(got.PoolEvents))
	}
	ev := got.PoolEvents[0]
	if err := ev.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if ev.EventName != entity.PoolEventIncreaseObservationCardinalityNext {
		t.Errorf("EventName = %q, want %q", ev.EventName, entity.PoolEventIncreaseObservationCardinalityNext)
	}
	params := decodePayload(t, ev.Params)
	if params["observationCardinalityNextOld"] != float64(100) {
		t.Errorf("params.observationCardinalityNextOld = %v, want 100", params["observationCardinalityNextOld"])
	}
	if params["observationCardinalityNextNew"] != float64(200) {
		t.Errorf("params.observationCardinalityNextNew = %v, want 200", params["observationCardinalityNextNew"])
	}
}

// ---------------------------------------------------------------------------
// SetFeeProtocol
// ---------------------------------------------------------------------------

func TestDecodeEvents_SetFeeProtocol(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()

	log := buildLog(t, a, "SetFeeProtocol", pool.Address, "0x0", nil,
		uint8(0), uint8(0), uint8(4), uint8(4),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.PoolEvents) != 1 {
		t.Fatalf("PoolEvents = %d, want 1", len(got.PoolEvents))
	}
	ev := got.PoolEvents[0]
	if err := ev.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if ev.EventName != entity.PoolEventSetFeeProtocol {
		t.Errorf("EventName = %q, want %q", ev.EventName, entity.PoolEventSetFeeProtocol)
	}
	params := decodePayload(t, ev.Params)
	if params["feeProtocol0New"] != float64(4) || params["feeProtocol1New"] != float64(4) {
		t.Errorf("params new fees = %v/%v, want 4/4", params["feeProtocol0New"], params["feeProtocol1New"])
	}
}

// ---------------------------------------------------------------------------
// CollectProtocol
// ---------------------------------------------------------------------------

func TestDecodeEvents_CollectProtocol(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sender := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	recipient := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	log := buildLog(t, a, "CollectProtocol", pool.Address, "0x0",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		big.NewInt(50), big.NewInt(60),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.PoolEvents) != 1 {
		t.Fatalf("PoolEvents = %d, want 1", len(got.PoolEvents))
	}
	ev := got.PoolEvents[0]
	if err := ev.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if ev.EventName != entity.PoolEventCollectProtocol {
		t.Errorf("EventName = %q, want %q", ev.EventName, entity.PoolEventCollectProtocol)
	}
	// common.Address JSON-marshals via MarshalText as plain lowercase hex, not
	// the EIP-55 checksummed casing Hex()/String() return.
	wantSender, wantRecipient := strings.ToLower(sender.Hex()), strings.ToLower(recipient.Hex())
	params := decodePayload(t, ev.Params)
	if params["sender"] != wantSender || params["recipient"] != wantRecipient {
		t.Errorf("params sender/recipient = %v/%v, want %s/%s", params["sender"], params["recipient"], wantSender, wantRecipient)
	}
}

// ---------------------------------------------------------------------------
// Capture-net: every matching log is mirrored regardless of decode outcome.
// ---------------------------------------------------------------------------

func TestDecodeEvents_CaptureNetMirrorsKnownEvent(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sender := common.HexToAddress("0x6666666666666666666666666666666666666666")
	recipient := common.HexToAddress("0x7777777777777777777777777777777777777777")

	log := buildLog(t, a, "Swap", pool.Address, "0x9",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		big.NewInt(1), big.NewInt(-1), big.NewInt(1), big.NewInt(1), big.NewInt(5),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Captured) != 1 {
		t.Fatalf("Captured = %d, want 1", len(got.Captured))
	}
	c := got.Captured[0]
	if c.EventName != "Swap" {
		t.Errorf("Captured EventName = %q, want Swap", c.EventName)
	}
	if c.Address != pool.Address {
		t.Errorf("Captured Address = %s, want %s", c.Address, pool.Address)
	}
	if c.LogIndex != 9 {
		t.Errorf("Captured LogIndex = %d, want 9", c.LogIndex)
	}
	var payload map[string]any
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		t.Fatalf("unmarshalling captured payload: %v", err)
	}
	if _, ok := payload["sender"]; !ok {
		t.Errorf("captured payload for known event missing decoded field %q", "sender")
	}
}

func TestDecodeEvents_UnknownTopic0IsCapturedRaw(t *testing.T) {
	pool := testPool()
	unknownTopic0 := common.HexToHash("0xdeadbeef00000000000000000000000000000000000000000000000000000001").Hex()
	someTopic1 := addrTopic(common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")).Hex()
	log := buildRawLog(pool.Address, "0x2", []string{unknownTopic0, someTopic1}, "0x0102030405")

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Swaps) != 0 || len(got.LiquidityEvents) != 0 || len(got.PoolEvents) != 0 {
		t.Fatalf("unknown topic0 must not produce any typed entity: swaps=%d liquidity=%d poolEvents=%d",
			len(got.Swaps), len(got.LiquidityEvents), len(got.PoolEvents))
	}
	if len(got.Captured) != 1 {
		t.Fatalf("Captured = %d, want 1", len(got.Captured))
	}
	c := got.Captured[0]
	if c.EventName != unknownTopic0 {
		t.Errorf("Captured EventName = %q, want topic0 hex %q for unknown topic0", c.EventName, unknownTopic0)
	}
	if c.Address != pool.Address {
		t.Errorf("Captured Address = %s, want %s", c.Address, pool.Address)
	}
	if c.LogIndex != 2 {
		t.Errorf("Captured LogIndex = %d, want 2", c.LogIndex)
	}
	assertValidProtocolEventName(t, c.EventName)

	var payload struct {
		Topics []string `json:"topics"`
		Data   string   `json:"data"`
	}
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		t.Fatalf("unmarshalling raw captured payload: %v", err)
	}
	if len(payload.Topics) != 2 || payload.Topics[0] != unknownTopic0 || payload.Topics[1] != someTopic1 {
		t.Errorf("payload.topics = %v, want [%s, %s]", payload.Topics, unknownTopic0, someTopic1)
	}
	if payload.Data != "0x0102030405" {
		t.Errorf("payload.data = %q, want 0x0102030405", payload.Data)
	}
}

func TestDecodeEvents_ZeroTopicsLogIsCapturedWithSentinelName(t *testing.T) {
	pool := testPool()
	log := buildRawLog(pool.Address, "0x3", nil, "0x0a0b0c")

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Swaps) != 0 || len(got.LiquidityEvents) != 0 || len(got.PoolEvents) != 0 {
		t.Fatalf("zero-topics log must not produce any typed entity: swaps=%d liquidity=%d poolEvents=%d",
			len(got.Swaps), len(got.LiquidityEvents), len(got.PoolEvents))
	}
	if len(got.Captured) != 1 {
		t.Fatalf("Captured = %d, want 1", len(got.Captured))
	}
	c := got.Captured[0]
	if c.EventName == "" {
		t.Fatal("Captured EventName is empty for zero-topics log, want non-empty sentinel")
	}
	assertValidProtocolEventName(t, c.EventName)

	var payload struct {
		Topics []string `json:"topics"`
		Data   string   `json:"data"`
	}
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		t.Fatalf("unmarshalling raw captured payload: %v", err)
	}
	if len(payload.Topics) != 0 {
		t.Errorf("payload.topics = %v, want empty", payload.Topics)
	}
	if payload.Data != "0x0a0b0c" {
		t.Errorf("payload.data = %q, want 0x0a0b0c", payload.Data)
	}
}

// assertValidProtocolEventName confirms that a CapturedLog.EventName produced
// by the capture-net path would survive entity.NewProtocolEvent's validation
// (specifically the non-empty EventName check that made ""  poison-stall the
// worker: an aborted SaveBatch is redelivered by SQS and fails identically).
func assertValidProtocolEventName(t *testing.T, eventName string) {
	t.Helper()
	_, err := entity.NewProtocolEvent(
		int(chainID),
		1,
		blockNumber,
		blockVer,
		[]byte{0x01},
		0,
		[]byte{0x02},
		eventName,
		json.RawMessage(`{"topics":[]}`),
		blockTS,
	)
	if err != nil {
		t.Errorf("NewProtocolEvent rejected capture-net EventName %q: %v", eventName, err)
	}
}

func TestDecodeEvents_LogNotBelongingToPoolIsSkipped(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	otherAddr := common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd")
	sender := common.HexToAddress("0x6666666666666666666666666666666666666666")
	recipient := common.HexToAddress("0x7777777777777777777777777777777777777777")

	// A well-formed Swap log, but emitted by a contract other than the pool.
	log := buildLog(t, a, "Swap", otherAddr, "0x0",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		big.NewInt(1), big.NewInt(1), big.NewInt(1), big.NewInt(1), big.NewInt(1),
	)

	got, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Swaps) != 0 {
		t.Errorf("Swaps = %d, want 0 for log not belonging to pool", len(got.Swaps))
	}
	if len(got.Captured) != 0 {
		t.Errorf("Captured = %d, want 0 for log not belonging to pool", len(got.Captured))
	}
}

// ---------------------------------------------------------------------------
// Decode failure on a known topic0 must error, not be swallowed.
// ---------------------------------------------------------------------------

func TestDecodeEvents_KnownTopic0MalformedDataReturnsError(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	sender := common.HexToAddress("0x6666666666666666666666666666666666666666")
	recipient := common.HexToAddress("0x7777777777777777777777777777777777777777")

	// Build a valid Swap log, then truncate its data so the non-indexed args
	// (5 x 32-byte words) can no longer be unpacked.
	log := buildLog(t, a, "Swap", pool.Address, "0x0",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		big.NewInt(1), big.NewInt(1), big.NewInt(1), big.NewInt(1), big.NewInt(1),
	)
	log.Data = log.Data[:len(log.Data)-64] // drop the last word

	_, err := DecodeEvents(receiptOf(log), pool, chainID, blockNumber, blockVer, blockTS)
	if err == nil {
		t.Fatal("DecodeEvents: want error for malformed data on known topic0, got nil")
	}
}

// ---------------------------------------------------------------------------
// Multiple logs across one receipt: mix of belonging/not, known/unknown.
// ---------------------------------------------------------------------------

func TestDecodeEvents_MultipleLogsInReceipt(t *testing.T) {
	a := poolABIForTest(t)
	pool := testPool()
	otherAddr := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	sender := common.HexToAddress("0x6666666666666666666666666666666666666666")
	recipient := common.HexToAddress("0x7777777777777777777777777777777777777777")

	swapLog := buildLog(t, a, "Swap", pool.Address, "0x0",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		big.NewInt(1), big.NewInt(-1), big.NewInt(1), big.NewInt(1), big.NewInt(1),
	)
	foreignLog := buildLog(t, a, "Swap", otherAddr, "0x1",
		[]common.Hash{addrTopic(sender), addrTopic(recipient)},
		big.NewInt(1), big.NewInt(1), big.NewInt(1), big.NewInt(1), big.NewInt(1),
	)
	unknownTopic0 := common.HexToHash("0xabcdef0000000000000000000000000000000000000000000000000000000009").Hex()
	unknownLog := buildRawLog(pool.Address, "0x2", []string{unknownTopic0}, "0x")

	got, err := DecodeEvents(receiptOf(swapLog, foreignLog, unknownLog), pool, chainID, blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	if len(got.Swaps) != 1 {
		t.Errorf("Swaps = %d, want 1 (foreign log must be excluded)", len(got.Swaps))
	}
	if len(got.Captured) != 2 {
		t.Fatalf("Captured = %d, want 2 (swap + unknown, foreign excluded)", len(got.Captured))
	}
	if got.Captured[0].EventName != "Swap" {
		t.Errorf("Captured[0].EventName = %q, want Swap", got.Captured[0].EventName)
	}
	if got.Captured[1].EventName != unknownTopic0 {
		t.Errorf("Captured[1].EventName = %q, want topic0 hex %q (unknown topic0)", got.Captured[1].EventName, unknownTopic0)
	}
}
