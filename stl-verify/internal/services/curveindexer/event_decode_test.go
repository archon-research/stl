package curveindexer

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// newStableswapHandlerForTest builds a handler with the real ABI.
func newStableswapHandlerForTest(t *testing.T) (*StableswapHandler, *abi.ABI) {
	t.Helper()
	a, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}
	return NewStableswapHandler(a), a
}

// stableswapPoolPreNG and stableswapPoolNG are fixture pools for decode tests.
func stableswapPoolPreNG() RegisteredPool {
	return RegisteredPool{
		ID:           1,
		Address:      common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"),
		Kind:         KindStableswapPreNG,
		NCoins:       2,
		CoinDecimals: []int{18, 18},
	}
}

func stableswapPoolNG() RegisteredPool {
	return RegisteredPool{
		ID:           2,
		Address:      common.HexToAddress("0x21E27a5E5513D6e65C4f830167390997aA84843a"),
		Kind:         KindStableswapNG,
		NCoins:       2,
		CoinDecimals: []int{18, 18},
	}
}

// buildEventLog ABI-encodes an event's non-indexed args into Data and places the
// given indexed topics (already 32-byte hashes) after topic0.
func buildEventLog(
	t *testing.T,
	a *abi.ABI,
	eventName string,
	addr common.Address,
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
	txHash := common.HexToHash("0xabc1230000000000000000000000000000000000000000000000000000000001")
	return shared.Log{
		Address:         addr.Hex(),
		Topics:          topics,
		Data:            "0x" + hex.EncodeToString(data),
		TransactionHash: txHash.Hex(),
		LogIndex:        "0x5",
	}
}

func addrTopic(a common.Address) common.Hash { return common.BytesToHash(a.Bytes()) }

func uintTopic(v int64) common.Hash { return common.BytesToHash(big.NewInt(v).Bytes()) }

// ---------------------------------------------------------------------------
// TokenExchangeUnderlying
// ---------------------------------------------------------------------------

func TestStableswapHandler_TokenExchangeUnderlyingSetsIsUnderlying(t *testing.T) {
	h, a := newStableswapHandlerForTest(t)
	pool := stableswapPoolPreNG()
	buyer := common.HexToAddress("0xabc")

	log := buildEventLog(t, a, "TokenExchangeUnderlying", pool.Address,
		[]common.Hash{addrTopic(buyer)},
		big.NewInt(1), big.NewInt(1000), big.NewInt(0), big.NewInt(999),
	)

	got, err := h.DecodeEvents(buildReceiptFromLog(log), pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Swaps) != 1 {
		t.Fatalf("swaps = %d, want 1", len(got.Swaps))
	}
	if !got.Swaps[0].IsUnderlying {
		t.Error("IsUnderlying = false, want true for TokenExchangeUnderlying")
	}
}

func TestStableswapHandler_TokenExchangeIsNotUnderlying(t *testing.T) {
	h, a := newStableswapHandlerForTest(t)
	pool := stableswapPoolPreNG()
	receipt := buildReceiptWithTokenExchange(
		t, a, pool.Address, common.HexToAddress("0xabc"),
		1, big.NewInt(1000), 0, big.NewInt(999), 3,
	)
	got, err := h.DecodeEvents(receipt, pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Swaps) != 1 {
		t.Fatalf("swaps = %d, want 1", len(got.Swaps))
	}
	if got.Swaps[0].IsUnderlying {
		t.Error("IsUnderlying = true, want false for TokenExchange")
	}
}

// ---------------------------------------------------------------------------
// Parameter events
// ---------------------------------------------------------------------------

func decodeParamsMap(t *testing.T, raw json.RawMessage) map[string]string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshalling params %s: %v", raw, err)
	}
	return m
}

func TestStableswapHandler_DecodeParameterEvents(t *testing.T) {
	pool := stableswapPoolPreNG()
	ngPool := stableswapPoolNG()

	cases := []struct {
		name       string
		eventName  string
		pool       RegisteredPool
		indexed    []common.Hash
		nonIndexed []any
		wantEvent  string
		wantParams map[string]string
	}{
		{
			name:       "RampA",
			eventName:  "RampA",
			pool:       pool,
			nonIndexed: []any{big.NewInt(20000), big.NewInt(90000), big.NewInt(1731805535), big.NewInt(1732495784)},
			wantEvent:  "ramp_a",
			wantParams: map[string]string{"old_a": "20000", "new_a": "90000", "initial_time": "1731805535", "future_time": "1732495784"},
		},
		{
			name:       "StopRampA",
			eventName:  "StopRampA",
			pool:       pool,
			nonIndexed: []any{big.NewInt(50000), big.NewInt(1732000000)},
			wantEvent:  "stop_ramp_a",
			wantParams: map[string]string{"a": "50000", "t": "1732000000"},
		},
		{
			name:       "NewFee_preNG",
			eventName:  "NewFee",
			pool:       pool,
			nonIndexed: []any{big.NewInt(4000000), big.NewInt(5000000000)},
			wantEvent:  "new_fee",
			wantParams: map[string]string{"fee": "4000000", "admin_fee": "5000000000"},
		},
		{
			name:       "CommitNewFee_preNG",
			eventName:  "CommitNewFee",
			pool:       pool,
			nonIndexed: []any{big.NewInt(1732500000), big.NewInt(4000000), big.NewInt(5000000000)},
			wantEvent:  "commit_new_fee",
			wantParams: map[string]string{"deadline": "1732500000", "fee": "4000000", "admin_fee": "5000000000"},
		},
		{
			name:       "ApplyNewFee_NG",
			eventName:  "ApplyNewFee",
			pool:       ngPool,
			nonIndexed: []any{big.NewInt(1000000), big.NewInt(20000000000)},
			wantEvent:  "apply_new_fee",
			wantParams: map[string]string{"fee": "1000000", "offpeg_fee_multiplier": "20000000000"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, a := newStableswapHandlerForTest(t)
			log := buildEventLog(t, a, tc.eventName, tc.pool.Address, tc.indexed, tc.nonIndexed...)
			got, err := h.DecodeEvents(buildReceiptFromLog(log), tc.pool, 1, 100, 0, time.Unix(1, 0).UTC())
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

func TestStableswapHandler_DecodeNewAdmin(t *testing.T) {
	h, a := newStableswapHandlerForTest(t)
	pool := stableswapPoolPreNG()
	admin := common.HexToAddress("0x000000000000000000000000000000000000dEaD")

	log := buildEventLog(t, a, "NewAdmin", pool.Address, []common.Hash{addrTopic(admin)})
	got, err := h.DecodeEvents(buildReceiptFromLog(log), pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.ParameterEvents) != 1 {
		t.Fatalf("parameter events = %d, want 1", len(got.ParameterEvents))
	}
	pe := got.ParameterEvents[0]
	if pe.EventName != "new_admin" {
		t.Errorf("event_name = %q, want new_admin", pe.EventName)
	}
	params := decodeParamsMap(t, pe.Params)
	if common.HexToAddress(params["admin"]) != admin {
		t.Errorf("params[admin] = %q, want %s", params["admin"], admin.Hex())
	}
}

func TestStableswapHandler_DecodeCommitNewAdmin(t *testing.T) {
	h, a := newStableswapHandlerForTest(t)
	pool := stableswapPoolPreNG()
	admin := common.HexToAddress("0x000000000000000000000000000000000000bEEF")
	deadline := int64(1732600000)

	log := buildEventLog(t, a, "CommitNewAdmin", pool.Address,
		[]common.Hash{uintTopic(deadline), addrTopic(admin)})
	got, err := h.DecodeEvents(buildReceiptFromLog(log), pool, 1, 100, 0, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.ParameterEvents) != 1 {
		t.Fatalf("parameter events = %d, want 1", len(got.ParameterEvents))
	}
	pe := got.ParameterEvents[0]
	if pe.EventName != "commit_new_admin" {
		t.Errorf("event_name = %q, want commit_new_admin", pe.EventName)
	}
	params := decodeParamsMap(t, pe.Params)
	if params["deadline"] != "1732600000" {
		t.Errorf("params[deadline] = %q, want 1732600000", params["deadline"])
	}
	if common.HexToAddress(params["admin"]) != admin {
		t.Errorf("params[admin] = %q, want %s", params["admin"], admin.Hex())
	}
}

// ---------------------------------------------------------------------------
// LP-token Transfer / Approval
// ---------------------------------------------------------------------------

func TestStableswapHandler_DecodeLpTransfer(t *testing.T) {
	h, a := newStableswapHandlerForTest(t)
	pool := stableswapPoolNG() // NG: pool is its own LP token
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

func TestStableswapHandler_DecodeLpApproval(t *testing.T) {
	h, a := newStableswapHandlerForTest(t)
	pool := stableswapPoolNG()
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
