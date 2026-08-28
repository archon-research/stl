package uniswapv4indexer

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

const (
	blockNumber = int64(23000000)
	blockVer    = 0
	// The singleton v4-core PoolManager and its StateView periphery on mainnet.
	poolManagerAddr = "0x000000000004444c5dc75cB358380D2e3dE08A90"
	stateViewAddr   = "0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"
	// PoolIds of the real mainnet logs replayed as decode fixtures.
	swapFixturePoolID     = "0xb86b55a845bae916f4d6b15087ec7f734276714652182f929817a8a177d3f837"
	modifyFixturePoolID   = "0x5244a02f00c673477f6bf1aef0f6b7f4af9cf50ebc22339f20a64f8a8cecba25"
	donateFixturePoolID   = "0xbd5baafef9eb4fcbc7e592b1b8e14d4a03716c705517baab4121a67b47017b97"
	protoFeeFixturePoolID = "0x864c2a36e2b28626149a99c3bd72fa57b197c15be63c7b7212d8095455edb090"
	fixtureTxHash         = "0xfeed000000000000000000000000000000000000000000000000000000000001"
)

var blockTS = time.Unix(1700000000, 0).UTC()

func poolManagerAddress() common.Address { return common.HexToAddress(poolManagerAddr) }

// decodeTestPool returns a RegisteredPool keyed by poolIDHash. Only the fields
// the decoder reads (ID, PoolIDHash) carry meaning here; the rest keep the
// mainnet ETH/wstETH shape so the fixture stays recognisable.
func decodeTestPool(id int64, poolIDHash string) RegisteredPool {
	return RegisteredPool{
		ID:                id,
		PoolManager:       poolManagerAddress(),
		StateView:         common.HexToAddress(stateViewAddr),
		PoolIDHash:        common.HexToHash(poolIDHash),
		Currency0:         common.Address{},
		Currency1:         common.HexToAddress(wstethAddress),
		Currency0Decimals: 18,
		Currency1Decimals: 18,
		Fee:               100,
		TickSpacing:       1,
		DeployBlock:       21743144,
		SnapshotSupported: true,
	}
}

func poolsByIDOf(pools ...RegisteredPool) map[common.Hash]RegisteredPool {
	byID := make(map[common.Hash]RegisteredPool, len(pools))
	for _, p := range pools {
		byID[p.PoolIDHash] = p
	}
	return byID
}

// rawLog builds a log verbatim from an on-chain topics/data capture, so the
// decode path is exercised against real PoolManager bytes rather than
// round-tripped Go values.
func rawLog(topics []string, data, logIndexHex string) shared.Log {
	return shared.Log{
		Address:         poolManagerAddr,
		Topics:          topics,
		Data:            data,
		TransactionHash: fixtureTxHash,
		LogIndex:        logIndexHex,
	}
}

// buildLog ABI-encodes an event's non-indexed args into Data and places the
// given indexed topics (already 32-byte hashes) after topic0.
func buildLog(t *testing.T, eventName string, indexedTopics []common.Hash, nonIndexedValues ...any) shared.Log {
	t.Helper()
	a := poolManagerABIForTest(t)
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
	return rawLog(topics, "0x"+hex.EncodeToString(data), "0x0")
}

func receiptOf(logs ...shared.Log) shared.TransactionReceipt {
	return shared.TransactionReceipt{Logs: logs}
}

// addrTopic left-pads an address into a 32-byte topic word.
func addrTopic(a common.Address) common.Hash { return common.BytesToHash(a.Bytes()) }

// corruptHexWord replaces one hex digit in the middle of a 32-byte word with a
// non-hex character. Corrupting the middle rather than the first digit is what
// makes the case interesting: go-ethereum truncates at the bad character and
// left-pads the remainder into a plausible-looking hash, not an obviously-empty
// one.
func corruptHexWord(word string) string { return word[:34] + "z" + word[35:] }

// decodeFixture runs DecodeEvents over one log against the given registry,
// failing the test on any decode error.
func decodeFixture(t *testing.T, log shared.Log, pools map[common.Hash]RegisteredPool) (DecodedEvents, map[int64]bool) {
	t.Helper()
	got, touched, err := DecodeEvents(receiptOf(log), pools, poolManagerAddress(), blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	return got, touched
}

func decodePayload(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshalling payload: %v", err)
	}
	return m
}

// swapFixtureLog is a verbatim mainnet PoolManager Swap log: amount0 positive
// (the swapper received currency0), amount1 negative (paid in), tick 94759,
// swap fee 10990 hundredths of a bip.
// swapDataWithTick repacks the Swap fixture's non-indexed block with a
// substituted tick, so a test can drive a value the int24 width cannot hold.
func swapDataWithTick(t *testing.T, tick *big.Int) string {
	t.Helper()
	a := poolManagerABIForTest(t)
	var nonIndexed abi.Arguments
	for _, arg := range a.Events["Swap"].Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	packed, err := nonIndexed.Pack(big.NewInt(-100), big.NewInt(200), big.NewInt(1234567890), big.NewInt(999), tick, big.NewInt(2500))
	if err != nil {
		t.Fatalf("packing Swap data: %v", err)
	}
	return "0x" + hex.EncodeToString(packed)
}

func swapFixtureLog() shared.Log {
	return rawLog(
		[]string{
			"0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
			swapFixturePoolID,
			"0x0000000000000000000000008f10b468b06c6fd214b65f87778827f7d113f996",
		},
		"0x00000000000000000000000000000000000000000000000001b0e10ad3ac424b"+
			"ffffffffffffffffffffffffffffffffffffffffffffffa9438a1d29cf000000"+
			"00000000000000000000000000000000000000722da9d0301a04d8baa4ff8cfd"+
			"0000000000000000000000000000000000000000000000c5b7e37c12962e4946"+
			"0000000000000000000000000000000000000000000000000000000000017227"+
			"0000000000000000000000000000000000000000000000000000000000002aee",
		"0x5",
	)
}

func TestDecodeEvents_Swap(t *testing.T) {
	pool := decodeTestPool(7, swapFixturePoolID)
	got, touched := decodeFixture(t, swapFixtureLog(), poolsByIDOf(pool))

	if len(got.Swaps) != 1 {
		t.Fatalf("Swaps = %d, want 1", len(got.Swaps))
	}
	swap := got.Swaps[0]
	if err := swap.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if swap.PoolID != pool.ID {
		t.Errorf("PoolID = %d, want %d", swap.PoolID, pool.ID)
	}
	if want := common.HexToAddress("0x8F10B468b06c6FD214B65F87778827F7D113f996"); swap.Sender != want {
		t.Errorf("Sender = %s, want %s", swap.Sender, want)
	}
	if want := big.NewInt(121844626556207691); swap.Amount0.Cmp(want) != 0 {
		t.Errorf("Amount0 = %s, want %s (swapper received currency0)", swap.Amount0, want)
	}
	wantAmount1, _ := new(big.Int).SetString("-1600000000000000000000", 10)
	if swap.Amount1.Cmp(wantAmount1) != 0 {
		t.Errorf("Amount1 = %s, want %s (negative int128: paid into the PoolManager)", swap.Amount1, wantAmount1)
	}
	wantSqrtPrice, _ := new(big.Int).SetString("9046142643671156900152450452733", 10)
	if swap.SqrtPriceX96.Cmp(wantSqrtPrice) != 0 {
		t.Errorf("SqrtPriceX96 = %s, want %s", swap.SqrtPriceX96, wantSqrtPrice)
	}
	wantLiquidity, _ := new(big.Int).SetString("3647259153468706670918", 10)
	if swap.Liquidity.Cmp(wantLiquidity) != 0 {
		t.Errorf("Liquidity = %s, want %s", swap.Liquidity, wantLiquidity)
	}
	if swap.Tick != 94759 {
		t.Errorf("Tick = %d, want 94759", swap.Tick)
	}
	if swap.Fee != 10990 {
		t.Errorf("Fee = %d, want 10990", swap.Fee)
	}
	if swap.LogIndex != 5 {
		t.Errorf("LogIndex = %d, want 5", swap.LogIndex)
	}
	if swap.TxHash != common.HexToHash(fixtureTxHash) {
		t.Errorf("TxHash = %s, want %s", swap.TxHash, fixtureTxHash)
	}
	if swap.BlockNumber != blockNumber || swap.BlockVersion != blockVer || !swap.BlockTimestamp.Equal(blockTS) {
		t.Errorf("block identity = (%d, %d, %v), want (%d, %d, %v)", swap.BlockNumber, swap.BlockVersion, swap.BlockTimestamp, blockNumber, blockVer, blockTS)
	}
	if !touched[pool.ID] {
		t.Errorf("touched = %v, want pool %d marked", touched, pool.ID)
	}
	if len(got.Captured) != 1 {
		t.Errorf("Captured = %d, want 1 (the swap mirrored into the capture net)", len(got.Captured))
	}
}

// modifyLiquidityFixtureLog is a verbatim mainnet ModifyLiquidity log: a
// full-range position (tickLower -887200, tickUpper 887200) with a non-zero
// caller-chosen salt.
func modifyLiquidityFixtureLog() shared.Log {
	return rawLog(
		[]string{
			"0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec",
			modifyFixturePoolID,
			"0x000000000000000000000000bf4195ab0b03e1eb3345dd1e83bed7650b1ed123",
		},
		"0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffff27660"+
			"00000000000000000000000000000000000000000000000000000000000d89a0"+
			"00000000000000000000000000000000000000000000000000537dfbf1461d0a"+
			"b219073c4ff49f0783bdeaf4d0e56adae19d2a87a11c63be935f1c021f923b4b",
		"0x2",
	)
}

func TestDecodeEvents_ModifyLiquidity(t *testing.T) {
	pool := decodeTestPool(11, modifyFixturePoolID)
	got, touched := decodeFixture(t, modifyLiquidityFixtureLog(), poolsByIDOf(pool))

	if len(got.LiquidityEvents) != 1 {
		t.Fatalf("LiquidityEvents = %d, want 1", len(got.LiquidityEvents))
	}
	e := got.LiquidityEvents[0]
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if e.PoolID != pool.ID {
		t.Errorf("PoolID = %d, want %d", e.PoolID, pool.ID)
	}
	if want := common.HexToAddress("0xBF4195ab0B03e1eB3345dd1e83BeD7650b1ed123"); e.Sender != want {
		t.Errorf("Sender = %s, want %s", e.Sender, want)
	}
	if e.TickLower != -887200 {
		t.Errorf("TickLower = %d, want -887200 (negative int24 two's complement)", e.TickLower)
	}
	if e.TickUpper != 887200 {
		t.Errorf("TickUpper = %d, want 887200", e.TickUpper)
	}
	if want := big.NewInt(23500944105151754); e.LiquidityDelta.Cmp(want) != 0 {
		t.Errorf("LiquidityDelta = %s, want %s", e.LiquidityDelta, want)
	}
	wantSalt := common.HexToHash("0xb219073c4ff49f0783bdeaf4d0e56adae19d2a87a11c63be935f1c021f923b4b")
	if e.Salt != wantSalt {
		t.Errorf("Salt = %s, want %s", e.Salt, wantSalt)
	}
	if e.LogIndex != 2 {
		t.Errorf("LogIndex = %d, want 2", e.LogIndex)
	}
	if !touched[pool.ID] {
		t.Errorf("touched = %v, want pool %d marked", touched, pool.ID)
	}
}

// TestDecodeEvents_CapturesBytes32FieldsAsHex pins the capture net's encoding of
// the ABI kind go-ethereum decodes as a bare [32]byte: a decimal byte array
// would leave params->>'id' unjoinable against uniswap_v4_pool.pool_id.
func TestDecodeEvents_CapturesBytes32FieldsAsHex(t *testing.T) {
	pool := decodeTestPool(11, modifyFixturePoolID)
	got, _ := decodeFixture(t, modifyLiquidityFixtureLog(), poolsByIDOf(pool))

	if len(got.Captured) != 1 {
		t.Fatalf("Captured = %d, want 1", len(got.Captured))
	}
	params := decodePayload(t, got.Captured[0].Payload)

	tests := []struct {
		key  string
		want string
	}{
		{key: "id", want: modifyFixturePoolID},
		{key: "salt", want: "0xb219073c4ff49f0783bdeaf4d0e56adae19d2a87a11c63be935f1c021f923b4b"},
	}
	for _, tt := range tests {
		got, ok := params[tt.key].(string)
		if !ok {
			t.Errorf("params[%q] = %v (%T), want a hex string", tt.key, params[tt.key], params[tt.key])
			continue
		}
		if got != tt.want {
			t.Errorf("params[%q] = %q, want %q", tt.key, got, tt.want)
		}
		if len(got) != 66 {
			t.Errorf("params[%q] = %q, want 0x plus 64 hex digits", tt.key, got)
		}
	}
}

// initializeFixtureLog is a verbatim mainnet Initialize log for the ETH/wstETH
// 0.01% pool: native ETH as currency0, tickSpacing 1, no hooks, opening tick
// -1750.
func initializeFixtureLog() shared.Log {
	return rawLog(
		[]string{
			"0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
			ethWstethPoolID,
			"0x0000000000000000000000000000000000000000000000000000000000000000",
			"0x0000000000000000000000007f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
		},
		"0x0000000000000000000000000000000000000000000000000000000000000064"+
			"0000000000000000000000000000000000000000000000000000000000000001"+
			"0000000000000000000000000000000000000000000000000000000000000000"+
			"0000000000000000000000000000000000000000ea8d9528cd7c59ab51c5ef72"+
			"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff92a",
		"0x1",
	)
}

func TestDecodeEvents_Initialize(t *testing.T) {
	pool := decodeTestPool(3, ethWstethPoolID)
	got, touched := decodeFixture(t, initializeFixtureLog(), poolsByIDOf(pool))

	if len(got.PoolEvents) != 1 {
		t.Fatalf("PoolEvents = %d, want 1", len(got.PoolEvents))
	}
	ev := got.PoolEvents[0]
	if err := ev.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if ev.EventName != entity.UniswapV4PoolEventInitialize {
		t.Errorf("EventName = %q, want %q", ev.EventName, entity.UniswapV4PoolEventInitialize)
	}
	params := decodePayload(t, ev.Params)
	wantParams := map[string]any{
		"fee":          "100",
		"tickSpacing":  "1",
		"tick":         "-1750",
		"sqrtPriceX96": "72590655224042927587984797554",
		"hooks":        "0x0000000000000000000000000000000000000000",
		"currency0":    "0x0000000000000000000000000000000000000000",
		// common.Address marshals through hexutil, i.e. lowercase, not EIP-55.
		"currency1": strings.ToLower(wstethAddress),
		"id":        ethWstethPoolID,
	}
	for key, want := range wantParams {
		if params[key] != want {
			t.Errorf("params[%q] = %v, want %v", key, params[key], want)
		}
	}
	if !touched[pool.ID] {
		t.Errorf("touched = %v, want pool %d marked", touched, pool.ID)
	}
}

// donateFixtureLog is a verbatim mainnet Donate log: 1528 wei of currency0 and
// nothing of currency1, so the fixture also pins which amount is which.
func donateFixtureLog() shared.Log {
	return rawLog(
		[]string{
			"0x29ef05caaff9404b7cb6d1c0e9bbae9eaa7ab2541feba1a9c4248594c08156cb",
			donateFixturePoolID,
			"0x0000000000000000000000007f6a0f0612d9e3a08bcd16772524d14bfa518888",
		},
		"0x00000000000000000000000000000000000000000000000000000000000005f8"+
			"0000000000000000000000000000000000000000000000000000000000000000",
		"0x2f1",
	)
}

func TestDecodeEvents_Donate(t *testing.T) {
	pool := decodeTestPool(4, donateFixturePoolID)

	got, touched := decodeFixture(t, donateFixtureLog(), poolsByIDOf(pool))

	if len(got.PoolEvents) != 1 {
		t.Fatalf("PoolEvents = %d, want 1", len(got.PoolEvents))
	}
	ev := got.PoolEvents[0]
	if err := ev.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if ev.EventName != entity.UniswapV4PoolEventDonate {
		t.Errorf("EventName = %q, want %q", ev.EventName, entity.UniswapV4PoolEventDonate)
	}
	if ev.LogIndex != 753 {
		t.Errorf("LogIndex = %d, want 753", ev.LogIndex)
	}
	params := decodePayload(t, ev.Params)
	wantParams := map[string]any{
		"id":      donateFixturePoolID,
		"sender":  "0x7f6a0f0612d9e3a08bcd16772524d14bfa518888",
		"amount0": "1528",
		"amount1": "0",
	}
	for key, want := range wantParams {
		if params[key] != want {
			t.Errorf("params[%q] = %v, want %v", key, params[key], want)
		}
	}
	if !touched[pool.ID] {
		t.Errorf("touched = %v, want pool %d marked", touched, pool.ID)
	}
}

// protocolFeeUpdatedFixtureLog is a verbatim mainnet ProtocolFeeUpdated log:
// 0x3e83e8 packs the maximum 1000 pips in both 12-bit halves.
func protocolFeeUpdatedFixtureLog() shared.Log {
	return rawLog(
		[]string{
			"0xe9c42593e71f84403b84352cd168d693e2c9fcd1fdbcc3feb21d92b43e6696f9",
			protoFeeFixturePoolID,
		},
		"0x00000000000000000000000000000000000000000000000000000000003e83e8",
		"0x1aa",
	)
}

func TestDecodeEvents_ProtocolFeeUpdated(t *testing.T) {
	pool := decodeTestPool(5, protoFeeFixturePoolID)

	got, touched := decodeFixture(t, protocolFeeUpdatedFixtureLog(), poolsByIDOf(pool))

	if len(got.PoolEvents) != 1 {
		t.Fatalf("PoolEvents = %d, want 1", len(got.PoolEvents))
	}
	ev := got.PoolEvents[0]
	if err := ev.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if ev.EventName != entity.UniswapV4PoolEventProtocolFeeUpdated {
		t.Errorf("EventName = %q, want %q", ev.EventName, entity.UniswapV4PoolEventProtocolFeeUpdated)
	}
	if ev.LogIndex != 426 {
		t.Errorf("LogIndex = %d, want 426", ev.LogIndex)
	}
	params := decodePayload(t, ev.Params)
	// 1000 pips zeroForOne in the low 12 bits, 1000 oneForZero in the high 12.
	if params["protocolFee"] != "4097000" || params["id"] != protoFeeFixturePoolID {
		t.Errorf("params = %v, want protocolFee=4097000 id=%s", params, protoFeeFixturePoolID)
	}
	if !touched[pool.ID] {
		t.Errorf("touched = %v, want pool %d marked", touched, pool.ID)
	}
}

// TestDecodeEvents_UnregisteredPoolIsSkipped pins the load-bearing filter: the
// PoolManager is a singleton emitting for thousands of pools, so a log for a
// PoolId we do not track must produce nothing at all — not even a capture-net
// entry, which would otherwise flood protocol_event.
func TestDecodeEvents_UnregisteredPoolIsSkipped(t *testing.T) {
	other := decodeTestPool(7, ethWstethPoolID)

	got, touched := decodeFixture(t, swapFixtureLog(), poolsByIDOf(other))

	if len(got.Swaps) != 0 {
		t.Errorf("Swaps = %d, want 0 for an unregistered PoolId", len(got.Swaps))
	}
	if len(got.Captured) != 0 {
		t.Errorf("Captured = %d, want 0: an unregistered pool's logs must not reach protocol_event", len(got.Captured))
	}
	if len(touched) != 0 {
		t.Errorf("touched = %v, want empty", touched)
	}
}

// TestDecodeEvents_ERC6909EventsAreSkipped covers the claim-token surface: it
// is keyed by currency id, not PoolId, so it cannot be attributed to a pool and
// is deliberately not captured.
func TestDecodeEvents_ERC6909EventsAreSkipped(t *testing.T) {
	pool := decodeTestPool(7, ethWstethPoolID)
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	currencyID := common.BigToHash(big.NewInt(12345))

	tests := []struct {
		name string
		log  shared.Log
	}{
		{name: "Transfer", log: buildLog(t, "Transfer",
			[]common.Hash{addrTopic(from), addrTopic(to), currencyID},
			from, big.NewInt(100))},
		{name: "Approval", log: buildLog(t, "Approval",
			[]common.Hash{addrTopic(from), addrTopic(to), currencyID},
			big.NewInt(100))},
		{name: "OperatorSet", log: buildLog(t, "OperatorSet",
			[]common.Hash{addrTopic(from), addrTopic(to)},
			true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, touched := decodeFixture(t, tt.log, poolsByIDOf(pool))
			if len(got.Captured) != 0 {
				t.Errorf("Captured = %d, want 0 for ERC6909 %s", len(got.Captured), tt.name)
			}
			if len(touched) != 0 {
				t.Errorf("touched = %v, want empty", touched)
			}
		})
	}
}

// TestDecodeEvents_GovernanceEventsAreCapturedOnly covers the two singleton
// events that carry no PoolId: they belong in the capture net but have no pool
// to attribute a typed row to.
func TestDecodeEvents_GovernanceEventsAreCapturedOnly(t *testing.T) {
	pool := decodeTestPool(7, ethWstethPoolID)
	user := common.HexToAddress("0x1a9C8182C09F50C8318d769245beA52c32BE35BC")
	controller := common.HexToAddress("0x89A5D5bF00a27D55c02951E49078a5C5771051dB")

	tests := []struct {
		name string
		log  shared.Log
	}{
		{name: "OwnershipTransferred", log: buildLog(t, "OwnershipTransferred",
			[]common.Hash{addrTopic(user), addrTopic(controller)})},
		{name: "ProtocolFeeControllerUpdated", log: buildLog(t, "ProtocolFeeControllerUpdated",
			[]common.Hash{addrTopic(controller)})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, touched := decodeFixture(t, tt.log, poolsByIDOf(pool))
			if len(got.Captured) != 1 {
				t.Fatalf("Captured = %d, want 1", len(got.Captured))
			}
			if got.Captured[0].EventName != tt.name {
				t.Errorf("captured EventName = %q, want %q (decoded, not raw topic0)", got.Captured[0].EventName, tt.name)
			}
			if len(got.PoolEvents) != 0 || len(got.Swaps) != 0 || len(got.LiquidityEvents) != 0 {
				t.Errorf("typed entities produced for a non-pool-keyed event: %+v", got)
			}
			if len(touched) != 0 {
				t.Errorf("touched = %v, want empty", touched)
			}
		})
	}
}

func TestDecodeEvents_UnknownTopic0IsCapturedRaw(t *testing.T) {
	pool := decodeTestPool(7, ethWstethPoolID)
	unknown := "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	log := rawLog([]string{unknown}, "0xabcd", "0x3")

	got, _ := decodeFixture(t, log, poolsByIDOf(pool))

	if len(got.Captured) != 1 {
		t.Fatalf("Captured = %d, want 1", len(got.Captured))
	}
	if got.Captured[0].EventName != unknown {
		t.Errorf("EventName = %q, want the raw topic0 %q", got.Captured[0].EventName, unknown)
	}
	payload := decodePayload(t, got.Captured[0].Payload)
	if payload["data"] != "0xabcd" {
		t.Errorf("payload.data = %v, want 0xabcd", payload["data"])
	}
}

func TestDecodeEvents_ZeroTopicsLogIsCapturedAsAnonymous(t *testing.T) {
	pool := decodeTestPool(7, ethWstethPoolID)
	log := rawLog(nil, "0x1234", "0x4")

	got, _ := decodeFixture(t, log, poolsByIDOf(pool))

	if len(got.Captured) != 1 {
		t.Fatalf("Captured = %d, want 1", len(got.Captured))
	}
	if got.Captured[0].EventName != "anonymous" {
		t.Errorf("EventName = %q, want the anonymous sentinel", got.Captured[0].EventName)
	}
}

func TestDecodeEvents_LogFromAnotherContractIsIgnored(t *testing.T) {
	pool := decodeTestPool(7, swapFixturePoolID)
	log := swapFixtureLog()
	log.Address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"

	got, touched := decodeFixture(t, log, poolsByIDOf(pool))

	if len(got.Swaps) != 0 || len(got.Captured) != 0 {
		t.Errorf("decoded %d swaps / %d captures for a log from another contract, want 0/0", len(got.Swaps), len(got.Captured))
	}
	if len(touched) != 0 {
		t.Errorf("touched = %v, want empty", touched)
	}
}

// TestDecodeEvents_MultipleReceiptLogsAccumulate proves the singleton decode
// path handles a routing transaction that hits two tracked pools plus untracked
// noise in a single receipt.
func TestDecodeEvents_MultipleReceiptLogsAccumulate(t *testing.T) {
	swapPool := decodeTestPool(7, swapFixturePoolID)
	modifyPool := decodeTestPool(11, modifyFixturePoolID)
	untracked := rawLog(
		[]string{
			"0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
			"0x00000000000000000000000000000000000000000000000000000000deadbeef",
			"0x0000000000000000000000008f10b468b06c6fd214b65f87778827f7d113f996",
		},
		swapFixtureLog().Data, "0x9",
	)

	got, touched, err := DecodeEvents(
		receiptOf(swapFixtureLog(), modifyLiquidityFixtureLog(), untracked),
		poolsByIDOf(swapPool, modifyPool), poolManagerAddress(), blockNumber, blockVer, blockTS,
	)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}

	if len(got.Swaps) != 1 || len(got.LiquidityEvents) != 1 {
		t.Errorf("Swaps = %d, LiquidityEvents = %d, want 1 and 1", len(got.Swaps), len(got.LiquidityEvents))
	}
	if len(got.Captured) != 2 {
		t.Errorf("Captured = %d, want 2 (the untracked pool's swap must not be captured)", len(got.Captured))
	}
	if len(touched) != 2 || !touched[swapPool.ID] || !touched[modifyPool.ID] {
		t.Errorf("touched = %v, want both %d and %d", touched, swapPool.ID, modifyPool.ID)
	}
}

func TestDecodeEvents_MalformedLogsReturnError(t *testing.T) {
	pool := decodeTestPool(7, swapFixturePoolID)

	tests := []struct {
		name    string
		log     shared.Log
		wantSub string
	}{
		{
			name:    "invalid log address",
			log:     shared.Log{Address: "0x123", Topics: []string{}, LogIndex: "0x0"},
			wantSub: "invalid log address",
		},
		{
			name: "unparseable log index",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.LogIndex = "zz"
				return l
			}(),
			wantSub: "log index",
		},
		{
			name: "truncated event data",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Data = "0xdead"
				return l
			}(),
			wantSub: "decoding Swap",
		},
		{
			name: "pool-keyed event without its id topic",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Topics = l.Topics[:1]
				return l
			}(),
			wantSub: "pool id",
		},
		{
			name: "short pool id topic",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Topics[1] = "0x01"
				return l
			}(),
			wantSub: "32-byte hex word",
		},
		{
			name: "pool id topic without the 0x prefix",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Topics[1] = strings.TrimPrefix(l.Topics[1], "0x") + "00"
				return l
			}(),
			wantSub: "32-byte hex word",
		},
		{
			name: "pool id topic with a non-hex character",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Topics[1] = corruptHexWord(l.Topics[1])
				return l
			}(),
			wantSub: "topic 1",
		},
		{
			name: "topic0 with a non-hex character",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Topics[0] = corruptHexWord(l.Topics[0])
				return l
			}(),
			wantSub: "topic 0",
		},
		{
			name: "indexed sender topic with a non-hex character",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Topics[2] = corruptHexWord(l.Topics[2])
				return l
			}(),
			wantSub: "topic 2",
		},
		{
			name: "transaction hash with a non-hex character",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.TransactionHash = corruptHexWord(l.TransactionHash)
				return l
			}(),
			wantSub: "transaction hash",
		},
		{
			name: "truncated transaction hash",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.TransactionHash = "0xfeed"
				return l
			}(),
			wantSub: "transaction hash",
		},
		{
			name: "tick wider than int64",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Data = swapDataWithTick(t, new(big.Int).Lsh(big.NewInt(1), 200))
				return l
			}(),
			wantSub: "tick does not fit in an int64",
		},
		{
			name: "tick outside int24",
			log: func() shared.Log {
				l := swapFixtureLog()
				l.Data = swapDataWithTick(t, big.NewInt(1<<23))
				return l
			}(),
			wantSub: "tick must be within",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeEvents(receiptOf(tt.log), poolsByIDOf(pool), poolManagerAddress(), blockNumber, blockVer, blockTS)
			if err == nil {
				t.Fatalf("DecodeEvents: want error for %s, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not mention %q", err, tt.wantSub)
			}
		})
	}
}

// TestDecodeEvents_EmptyDataIsAnError proves a log whose data block is missing
// stops the block: DecodeLog skips the non-indexed arguments entirely when data
// is empty, which would otherwise persist a Donate whose amounts are simply
// absent from params.
func TestDecodeEvents_EmptyDataIsAnError(t *testing.T) {
	pool := decodeTestPool(7, ethWstethPoolID)
	log := buildLog(t, "Donate",
		[]common.Hash{common.HexToHash(ethWstethPoolID), addrTopic(common.HexToAddress("0xccc"))},
		big.NewInt(10), big.NewInt(20),
	)
	log.Data = "0x"

	_, _, err := DecodeEvents(receiptOf(log), poolsByIDOf(pool), poolManagerAddress(), blockNumber, blockVer, blockTS)
	if err == nil {
		t.Fatal("DecodeEvents: want error for a Donate log with no data, got nil")
	}
	if !strings.Contains(err.Error(), "amount0") {
		t.Errorf("error %q does not name the missing field", err)
	}
}

// TestPoolManagerABI_RoutesOnlyEventsItDefines pins the routing tables to the
// ABI: a name in one of them that the ABI does not define would silently divert
// real logs into the unknown-topic0 capture net.
func TestPoolManagerABI_RoutesOnlyEventsItDefines(t *testing.T) {
	tests := []struct {
		name      string
		abi       *abi.ABI
		keyed     map[string]struct{}
		wantError bool
	}{
		{name: "real ABI and routing tables", abi: poolManagerABIForTest(t), keyed: poolKeyedEvents},
		{name: "ABI defining no events", abi: &abi.ABI{Events: map[string]abi.Event{}}, keyed: poolKeyedEvents, wantError: true},
		{
			name:      "keyed event with no builder",
			abi:       poolManagerABIForTest(t),
			keyed:     map[string]struct{}{"Swap": {}, "ModifyLiquidity": {}, "OwnershipTransferred": {}},
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := assertRoutedEventsExist(tt.abi, tt.keyed)
			if tt.wantError && err == nil {
				t.Fatal("assertRoutedEventsExist: want error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("assertRoutedEventsExist: %v", err)
			}
		})
	}
}

// TestDecodeEvents_EntityValidationFailureIsAnError proves a decoded log that
// cannot form a valid entity stops the block instead of being dropped, for
// every builder: a partially-valid block would look healthy while leaving holes.
func TestDecodeEvents_EntityValidationFailureIsAnError(t *testing.T) {
	poolIDTopic := common.HexToHash(ethWstethPoolID)
	sender := addrTopic(common.HexToAddress("0xaaa"))

	for _, tc := range []struct {
		name    string
		log     func(t *testing.T) shared.Log
		wantSub string
	}{
		{
			name: "swap with a zero sender",
			log: func(t *testing.T) shared.Log {
				return buildLog(t, "Swap", []common.Hash{poolIDTopic, addrTopic(common.Address{})},
					big.NewInt(1), big.NewInt(-1), big.NewInt(100), big.NewInt(1), big.NewInt(0), big.NewInt(500))
			},
			wantSub: "sender",
		},
		{
			name: "modify liquidity with an inverted tick range",
			log: func(t *testing.T) shared.Log {
				return buildLog(t, "ModifyLiquidity", []common.Hash{poolIDTopic, sender},
					big.NewInt(200), big.NewInt(-100), big.NewInt(5000), [32]byte{})
			},
			wantSub: "tickLower",
		},
		{
			name: "donate without a transaction hash",
			log: func(t *testing.T) shared.Log {
				log := buildLog(t, "Donate", []common.Hash{poolIDTopic, sender}, big.NewInt(10), big.NewInt(20))
				log.TransactionHash = common.Hash{}.Hex()
				return log
			},
			wantSub: "txHash",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := decodeTestPool(7, ethWstethPoolID)
			_, _, err := DecodeEvents(receiptOf(tc.log(t)), poolsByIDOf(pool), poolManagerAddress(), blockNumber, blockVer, blockTS)
			if err == nil {
				t.Fatalf("DecodeEvents: want error for %s, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}
