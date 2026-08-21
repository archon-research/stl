package allocation_tracker

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func newTestExtractor(t *testing.T) *TransferExtractor {
	t.Helper()
	return NewTransferExtractor(defaultProxies(t))
}

func makeTransferLog(token, from, to common.Address, amount *big.Int, index uint) types.Log {
	return types.Log{
		Address: token,
		Topics: []common.Hash{
			transferEventTopic,
			common.BytesToHash(from.Bytes()),
			common.BytesToHash(to.Bytes()),
		},
		Data:  common.LeftPadBytes(amount.Bytes(), 32),
		Index: index,
	}
}

func TestTransferExtractor_Extract_NoLogs(t *testing.T) {
	ext := newTestExtractor(t)
	receipt := TransactionReceipt{
		TransactionHash: "0xabc",
		Logs:            nil,
	}
	events := ext.Extract(receipt)
	if len(events) != 0 {
		t.Errorf("expected 0 events from empty receipt, got %d", len(events))
	}
}

func TestTransferExtractor_Extract_NonTransferTopic(t *testing.T) {
	ext := newTestExtractor(t)
	receipt := TransactionReceipt{
		TransactionHash: "0xabc",
		Logs: []types.Log{
			{
				Address: common.HexToAddress("0x1111111111111111111111111111111111111111"),
				Topics: []common.Hash{
					{},
					common.BytesToHash(common.HexToAddress("0x01").Bytes()),
					common.BytesToHash(common.HexToAddress("0x02").Bytes()),
				},
				Data:  common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
				Index: 0,
			},
		},
	}
	events := ext.Extract(receipt)
	if len(events) != 0 {
		t.Errorf("expected 0 events from non-transfer log, got %d", len(events))
	}
}

func TestTransferExtractor_Extract_TransferToProxy(t *testing.T) {
	sparkProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	sender := common.HexToAddress("0x9999999999999999999999999999999999999999")
	token := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

	ext := newTestExtractor(t)
	amount := new(big.Int).SetUint64(1000000)

	receipt := TransactionReceipt{
		TransactionHash: "0xdeadbeef",
		Logs: []types.Log{
			makeTransferLog(token, sender, sparkProxy, amount, 5),
		},
	}

	events := ext.Extract(receipt)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	ev := events[0]
	if ev.Direction != DirectionIn {
		t.Errorf("expected direction IN, got %s", ev.Direction)
	}
	if ev.Star != "spark" {
		t.Errorf("expected star spark, got %s", ev.Star)
	}
	if ev.ProxyAddress != sparkProxy {
		t.Errorf("expected proxy %s, got %s", sparkProxy.Hex(), ev.ProxyAddress.Hex())
	}
	if ev.TokenAddress != token {
		t.Errorf("expected token %s, got %s", token.Hex(), ev.TokenAddress.Hex())
	}
	if ev.Amount.Cmp(amount) != 0 {
		t.Errorf("expected amount %s, got %s", amount.String(), ev.Amount.String())
	}
	if ev.LogIndex != 5 {
		t.Errorf("expected logIndex 5, got %d", ev.LogIndex)
	}
	if ev.From != sender {
		t.Errorf("expected from %s, got %s", sender.Hex(), ev.From.Hex())
	}
	if ev.To != sparkProxy {
		t.Errorf("expected to %s (the proxy), got %s", sparkProxy.Hex(), ev.To.Hex())
	}
}

func TestTransferExtractor_Extract_TransferFromProxy(t *testing.T) {
	sparkProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	receiver := common.HexToAddress("0x9999999999999999999999999999999999999999")
	token := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

	ext := newTestExtractor(t)
	amount := new(big.Int).SetUint64(5000000)

	receipt := TransactionReceipt{
		TransactionHash: "0xfeed",
		Logs: []types.Log{
			makeTransferLog(token, sparkProxy, receiver, amount, 0),
		},
	}

	events := ext.Extract(receipt)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Direction != DirectionOut {
		t.Errorf("expected direction OUT, got %s", events[0].Direction)
	}
	if events[0].From != sparkProxy {
		t.Errorf("expected from %s (the proxy), got %s", sparkProxy.Hex(), events[0].From.Hex())
	}
	if events[0].To != receiver {
		t.Errorf("expected to %s, got %s", receiver.Hex(), events[0].To.Hex())
	}
}

func TestTransferExtractor_Extract_ProxyToProxy(t *testing.T) {
	sparkProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	groveProxy := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")
	token := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

	ext := newTestExtractor(t)
	amount := new(big.Int).SetUint64(1000000)

	receipt := TransactionReceipt{
		TransactionHash: "0xboth",
		Logs: []types.Log{
			makeTransferLog(token, sparkProxy, groveProxy, amount, 1),
		},
	}

	events := ext.Extract(receipt)
	if len(events) != 2 {
		t.Fatalf("proxy-to-proxy should produce 2 events (out + in), got %d", len(events))
	}

	if events[0].Direction != DirectionOut || events[0].Star != "spark" {
		t.Errorf("first event should be OUT/spark, got %s/%s", events[0].Direction, events[0].Star)
	}
	if events[1].Direction != DirectionIn || events[1].Star != "grove" {
		t.Errorf("second event should be IN/grove, got %s/%s", events[1].Direction, events[1].Star)
	}

	// Both events carry the same transfer sides; only the direction differs, so
	// which side is the proxy and which the counterparty is a read-side question.
	for i, ev := range events {
		if ev.From != sparkProxy {
			t.Errorf("event %d from = %s, want %s", i, ev.From.Hex(), sparkProxy.Hex())
		}
		if ev.To != groveProxy {
			t.Errorf("event %d to = %s, want %s", i, ev.To.Hex(), groveProxy.Hex())
		}
	}
}

func TestTransferExtractor_Extract_TooFewTopics(t *testing.T) {
	ext := newTestExtractor(t)
	receipt := TransactionReceipt{
		TransactionHash: "0xabc",
		Logs: []types.Log{
			{
				Address: common.HexToAddress("0x1111111111111111111111111111111111111111"),
				Topics: []common.Hash{
					transferEventTopic,
				},
				Data:  common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
				Index: 0,
			},
		},
	}
	events := ext.Extract(receipt)
	if len(events) != 0 {
		t.Errorf("expected 0 events from log with < 3 topics, got %d", len(events))
	}
}
