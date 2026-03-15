package transfer_user_discovery

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/ethereum/go-ethereum/common"
	gethhexutil "github.com/ethereum/go-ethereum/common/hexutil"
)

func TestNewTransferExtractor_BuildsTrackedTokenLookup(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §4 requires the extractor to resolve
	// protocol addresses from a tracked receipt-token mapping.
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocol := common.HexToAddress("0x2222222222222222222222222222222222222222")
	from := common.HexToAddress("0x3333333333333333333333333333333333333333")
	to := common.HexToAddress("0x4444444444444444444444444444444444444444")
	txHash := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	extractor := NewTransferExtractor([]outbound.TrackedReceiptToken{
		{
			ReceiptTokenAddress: trackedToken,
			ProtocolAddress:     protocol,
		},
	})

	events := extractor.Extract([]shared.TransactionReceipt{
		makeReceipt(txHash, makeTransferLog(trackedToken, from, to, big.NewInt(123), 7)),
	}, 19000000, 3)

	if len(events) != 1 {
		t.Fatalf("expected 1 transfer event, got %d", len(events))
	}

	event := events[0]
	if event.TokenAddress != trackedToken {
		t.Fatalf("expected token address %s, got %s", trackedToken.Hex(), event.TokenAddress.Hex())
	}
	if event.ProtocolAddress != protocol {
		t.Fatalf("expected protocol address %s, got %s", protocol.Hex(), event.ProtocolAddress.Hex())
	}
	if event.From != from {
		t.Fatalf("expected from %s, got %s", from.Hex(), event.From.Hex())
	}
	if event.To != to {
		t.Fatalf("expected to %s, got %s", to.Hex(), event.To.Hex())
	}
	if event.Amount.Cmp(big.NewInt(123)) != 0 {
		t.Fatalf("expected amount 123, got %s", event.Amount.String())
	}
	if string(event.TxHash) != string(txHash.Bytes()) {
		t.Fatalf("expected tx hash %x, got %x", txHash.Bytes(), event.TxHash)
	}
	if event.LogIndex != 7 {
		t.Fatalf("expected log index 7, got %d", event.LogIndex)
	}
	if event.BlockNumber != 19000000 {
		t.Fatalf("expected block number 19000000, got %d", event.BlockNumber)
	}
	if event.BlockVersion != 3 {
		t.Fatalf("expected block version 3, got %d", event.BlockVersion)
	}
}

func TestTransferExtractor_Extract_IgnoresUntrackedTokenTransfers(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §4 acceptance criterion 2.
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	untrackedToken := common.HexToAddress("0x5555555555555555555555555555555555555555")
	protocol := common.HexToAddress("0x2222222222222222222222222222222222222222")
	from := common.HexToAddress("0x3333333333333333333333333333333333333333")
	to := common.HexToAddress("0x4444444444444444444444444444444444444444")

	extractor := NewTransferExtractor([]outbound.TrackedReceiptToken{{
		ReceiptTokenAddress: trackedToken,
		ProtocolAddress:     protocol,
	}})

	events := extractor.Extract([]shared.TransactionReceipt{
		makeReceipt(common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), makeTransferLog(untrackedToken, from, to, big.NewInt(1), 0)),
	}, 19000001, 0)

	if len(events) != 0 {
		t.Fatalf("expected no events for untracked token, got %d", len(events))
	}
}

func TestTransferExtractor_Extract_IgnoresNonTransferLogsOnTrackedTokens(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §4 requires filtering for ERC20 Transfer events.
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocol := common.HexToAddress("0x2222222222222222222222222222222222222222")

	extractor := NewTransferExtractor([]outbound.TrackedReceiptToken{{
		ReceiptTokenAddress: trackedToken,
		ProtocolAddress:     protocol,
	}})

	events := extractor.Extract([]shared.TransactionReceipt{
		makeReceipt(common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"), shared.Log{
			Address: trackedToken.Hex(),
			Topics: []string{
				common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").Hex(),
				common.BytesToHash(common.HexToAddress("0x3333333333333333333333333333333333333333").Bytes()).Hex(),
				common.BytesToHash(common.HexToAddress("0x4444444444444444444444444444444444444444").Bytes()).Hex(),
			},
			Data:     encodeUint256(big.NewInt(1)),
			LogIndex: "0x0",
		}),
	}, 19000002, 1)

	if len(events) != 0 {
		t.Fatalf("expected no events for non-transfer log, got %d", len(events))
	}
}

func TestTransferExtractor_Extract_IncludesBurnTransfersToZeroAddress(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §4 acceptance criterion 3.
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocol := common.HexToAddress("0x2222222222222222222222222222222222222222")
	from := common.HexToAddress("0x3333333333333333333333333333333333333333")
	zero := common.Address{}

	extractor := NewTransferExtractor([]outbound.TrackedReceiptToken{{
		ReceiptTokenAddress: trackedToken,
		ProtocolAddress:     protocol,
	}})

	events := extractor.Extract([]shared.TransactionReceipt{
		makeReceipt(common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"), makeTransferLog(trackedToken, from, zero, big.NewInt(99), 2)),
	}, 19000003, 4)

	if len(events) != 1 {
		t.Fatalf("expected 1 burn event, got %d", len(events))
	}
	if events[0].To != zero {
		t.Fatalf("expected burn transfer to zero address, got %s", events[0].To.Hex())
	}
}

func TestTransferExtractor_Extract_IncludesMintTransfersFromZeroAddress(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §4 acceptance criterion 4.
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocol := common.HexToAddress("0x2222222222222222222222222222222222222222")
	zero := common.Address{}
	to := common.HexToAddress("0x4444444444444444444444444444444444444444")

	extractor := NewTransferExtractor([]outbound.TrackedReceiptToken{{
		ReceiptTokenAddress: trackedToken,
		ProtocolAddress:     protocol,
	}})

	events := extractor.Extract([]shared.TransactionReceipt{
		makeReceipt(common.HexToHash("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"), makeTransferLog(trackedToken, zero, to, big.NewInt(77), 5)),
	}, 19000004, 2)

	if len(events) != 1 {
		t.Fatalf("expected 1 mint event, got %d", len(events))
	}
	if events[0].From != zero {
		t.Fatalf("expected mint transfer from zero address, got %s", events[0].From.Hex())
	}
}

func TestTransferExtractor_Extract_DecodesAllTrackedTransfersInSingleReceipt(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §4 acceptance criterion 5.
	trackedTokenA := common.HexToAddress("0x1111111111111111111111111111111111111111")
	trackedTokenB := common.HexToAddress("0x6666666666666666666666666666666666666666")
	protocolA := common.HexToAddress("0x2222222222222222222222222222222222222222")
	protocolB := common.HexToAddress("0x7777777777777777777777777777777777777777")
	fromA := common.HexToAddress("0x3333333333333333333333333333333333333333")
	toA := common.HexToAddress("0x4444444444444444444444444444444444444444")
	fromB := common.HexToAddress("0x8888888888888888888888888888888888888888")
	toB := common.HexToAddress("0x9999999999999999999999999999999999999999")
	txHash := common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")

	extractor := NewTransferExtractor([]outbound.TrackedReceiptToken{
		{ReceiptTokenAddress: trackedTokenA, ProtocolAddress: protocolA},
		{ReceiptTokenAddress: trackedTokenB, ProtocolAddress: protocolB},
	})

	events := extractor.Extract([]shared.TransactionReceipt{
		makeReceipt(
			txHash,
			makeTransferLog(trackedTokenA, fromA, toA, big.NewInt(11), 1),
			makeTransferLog(trackedTokenB, fromB, toB, big.NewInt(22), 2),
		),
	}, 19000005, 6)

	if len(events) != 2 {
		t.Fatalf("expected 2 transfer events, got %d", len(events))
	}

	if events[0].ProtocolAddress != protocolA {
		t.Fatalf("expected first protocol %s, got %s", protocolA.Hex(), events[0].ProtocolAddress.Hex())
	}
	if events[1].ProtocolAddress != protocolB {
		t.Fatalf("expected second protocol %s, got %s", protocolB.Hex(), events[1].ProtocolAddress.Hex())
	}
	if events[0].Amount.Cmp(big.NewInt(11)) != 0 {
		t.Fatalf("expected first amount 11, got %s", events[0].Amount.String())
	}
	if events[1].Amount.Cmp(big.NewInt(22)) != 0 {
		t.Fatalf("expected second amount 22, got %s", events[1].Amount.String())
	}
}

func makeReceipt(txHash common.Hash, logs ...shared.Log) shared.TransactionReceipt {
	return shared.TransactionReceipt{
		TransactionHash: txHash.Hex(),
		Logs:            logs,
	}
}

func makeTransferLog(token, from, to common.Address, amount *big.Int, logIndex int) shared.Log {
	return shared.Log{
		Address: token.Hex(),
		Topics: []string{
			transferEventTopic.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:     encodeUint256(amount),
		LogIndex: fmt.Sprintf("0x%x", logIndex),
	}
}

func encodeUint256(value *big.Int) string {
	return gethhexutil.Encode(common.LeftPadBytes(value.Bytes(), 32))
}
