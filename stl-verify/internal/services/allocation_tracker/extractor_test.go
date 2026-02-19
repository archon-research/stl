package allocation_tracker

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestParseHexInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"0x0", 0},
		{"0x1", 1},
		{"0xa", 10},
		{"0xff", 255},
		{"0x10", 16},
		{"", 0},
		{"0x", 0},
	}
	for _, tt := range tests {
		got := parseHexInt(tt.input)
		if got != tt.want {
			t.Errorf("parseHexInt(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestTransferExtractor_Extract_NoLogs(t *testing.T) {
	ext := NewTransferExtractor(DefaultProxies())
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
	ext := NewTransferExtractor(DefaultProxies())
	receipt := TransactionReceipt{
		TransactionHash: "0xabc",
		Logs: []Log{
			{
				Address: "0x1111111111111111111111111111111111111111",
				Topics: []string{
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x0000000000000000000000000000000000000000000000000000000000000001",
					"0x0000000000000000000000000000000000000000000000000000000000000002",
				},
				Data:     "0x0000000000000000000000000000000000000000000000000000000000000001",
				LogIndex: "0x0",
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
	token := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48") // USDC

	ext := NewTransferExtractor(DefaultProxies())

	amount := new(big.Int).SetUint64(1000000) // 1 USDC
	amountHex := common.BytesToHash(amount.Bytes()).Hex()

	receipt := TransactionReceipt{
		TransactionHash: "0xdeadbeef",
		Logs: []Log{
			{
				Address: token.Hex(),
				Topics: []string{
					transferEventTopic.Hex(),
					common.BytesToHash(sender.Bytes()).Hex(),
					common.BytesToHash(sparkProxy.Bytes()).Hex(),
				},
				Data:     amountHex,
				LogIndex: "0x5",
			},
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
	if ev.TxHash != "0xdeadbeef" {
		t.Errorf("expected txHash 0xdeadbeef, got %s", ev.TxHash)
	}
}

func TestTransferExtractor_Extract_TransferFromProxy(t *testing.T) {
	sparkProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	receiver := common.HexToAddress("0x9999999999999999999999999999999999999999")
	token := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

	ext := NewTransferExtractor(DefaultProxies())

	amount := new(big.Int).SetUint64(5000000)
	amountHex := common.BytesToHash(amount.Bytes()).Hex()

	receipt := TransactionReceipt{
		TransactionHash: "0xfeed",
		Logs: []Log{
			{
				Address: token.Hex(),
				Topics: []string{
					transferEventTopic.Hex(),
					common.BytesToHash(sparkProxy.Bytes()).Hex(),
					common.BytesToHash(receiver.Bytes()).Hex(),
				},
				Data:     amountHex,
				LogIndex: "0x0",
			},
		},
	}

	events := ext.Extract(receipt)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Direction != DirectionOut {
		t.Errorf("expected direction OUT, got %s", events[0].Direction)
	}
}

func TestTransferExtractor_Extract_ProxyToProxy(t *testing.T) {
	sparkProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	groveProxy := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")
	token := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

	ext := NewTransferExtractor(DefaultProxies())

	amount := new(big.Int).SetUint64(1000000)
	amountHex := common.BytesToHash(amount.Bytes()).Hex()

	receipt := TransactionReceipt{
		TransactionHash: "0xboth",
		Logs: []Log{
			{
				Address: token.Hex(),
				Topics: []string{
					transferEventTopic.Hex(),
					common.BytesToHash(sparkProxy.Bytes()).Hex(),
					common.BytesToHash(groveProxy.Bytes()).Hex(),
				},
				Data:     amountHex,
				LogIndex: "0x1",
			},
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
}

func TestTransferExtractor_Extract_TooFewTopics(t *testing.T) {
	ext := NewTransferExtractor(DefaultProxies())
	receipt := TransactionReceipt{
		TransactionHash: "0xabc",
		Logs: []Log{
			{
				Address: "0x1111111111111111111111111111111111111111",
				Topics: []string{
					transferEventTopic.Hex(),
				},
				Data:     "0x0000000000000000000000000000000000000000000000000000000000000001",
				LogIndex: "0x0",
			},
		},
	}
	events := ext.Extract(receipt)
	if len(events) != 0 {
		t.Errorf("expected 0 events from log with < 3 topics, got %d", len(events))
	}
}

func TestTransferExtractor_Extract_NonProxyTransfer(t *testing.T) {
	ext := NewTransferExtractor(DefaultProxies())
	receipt := TransactionReceipt{
		TransactionHash: "0xabc",
		Logs: []Log{
			{
				Address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
				Topics: []string{
					transferEventTopic.Hex(),
					common.BytesToHash(common.HexToAddress("0xaaaa").Bytes()).Hex(),
					common.BytesToHash(common.HexToAddress("0xbbbb").Bytes()).Hex(),
				},
				Data:     "0x0000000000000000000000000000000000000000000000000000000000000001",
				LogIndex: "0x0",
			},
		},
	}
	events := ext.Extract(receipt)
	if len(events) != 0 {
		t.Errorf("expected 0 events from non-proxy transfer, got %d", len(events))
	}
}
