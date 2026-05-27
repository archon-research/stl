package maple_indexer

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

const (
	syrupUSDCAddr = "0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b"
	otherAddr     = "0x9999999999999999999999999999999999999999"
	userA         = "0x1111111111111111111111111111111111111111"
	userB         = "0x2222222222222222222222222222222222222222"
	userC         = "0x3333333333333333333333333333333333333333"
)

// topicForAddress returns the 32-byte topic encoding of an address (left-padded).
func topicForAddress(addr string) string {
	return common.HexToHash(addr).Hex()
}

func newExtractor(t *testing.T) *EventExtractor {
	t.Helper()
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}
	return e
}

func TestEventExtractor_TopicHashesAreNonZero(t *testing.T) {
	e := newExtractor(t)
	if e.DepositTopic() == (common.Hash{}) {
		t.Fatal("Deposit topic is zero hash")
	}
	if e.WithdrawTopic() == (common.Hash{}) {
		t.Fatal("Withdraw topic is zero hash")
	}
	if e.TransferTopic() == (common.Hash{}) {
		t.Fatal("Transfer topic is zero hash")
	}
	if e.DepositTopic() == e.WithdrawTopic() || e.DepositTopic() == e.TransferTopic() ||
		e.WithdrawTopic() == e.TransferTopic() {
		t.Fatal("topic hashes are not distinct")
	}
}

func TestEventExtractor_IsRelevant_Deposit(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	log := shared.Log{
		Address: syrupUSDCAddr,
		Topics:  []string{e.DepositTopic().Hex(), topicForAddress(userA), topicForAddress(userB)},
	}
	if !e.IsRelevant(vault, log) {
		t.Fatal("expected deposit log to be relevant")
	}
}

func TestEventExtractor_IsRelevant_WrongAddress(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	log := shared.Log{
		Address: otherAddr,
		Topics:  []string{e.DepositTopic().Hex(), topicForAddress(userA), topicForAddress(userB)},
	}
	if e.IsRelevant(vault, log) {
		t.Fatal("log from unrelated address should not be relevant")
	}
}

func TestEventExtractor_IsRelevant_UnknownTopic(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	log := shared.Log{
		Address: syrupUSDCAddr,
		Topics:  []string{"0x" + "deadbeef" + "00000000000000000000000000000000000000000000000000000000"},
	}
	if e.IsRelevant(vault, log) {
		t.Fatal("log with unknown topic should not be relevant")
	}
}

func TestEventExtractor_IsRelevant_EmptyTopics(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	log := shared.Log{Address: syrupUSDCAddr, Topics: nil}
	if e.IsRelevant(vault, log) {
		t.Fatal("log without topics should not be relevant")
	}
}

func TestEventExtractor_ExtractTouchedAddresses_Deposit(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics: []string{
			e.DepositTopic().Hex(),
			topicForAddress(userA),
			topicForAddress(userB),
		},
	}}
	users, ok := e.ExtractTouchedAddresses(vault, logs)
	if !ok {
		t.Fatal("expected relevance")
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d: %v", len(users), users)
	}
	if _, ok := users[common.HexToAddress(userA)]; !ok {
		t.Fatal("userA missing")
	}
	if _, ok := users[common.HexToAddress(userB)]; !ok {
		t.Fatal("userB missing")
	}
}

func TestEventExtractor_ExtractTouchedAddresses_Withdraw(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics: []string{
			e.WithdrawTopic().Hex(),
			topicForAddress(userA), // sender — ignored
			topicForAddress(userB), // receiver — refresh
			topicForAddress(userC), // owner — refresh
		},
	}}
	users, ok := e.ExtractTouchedAddresses(vault, logs)
	if !ok {
		t.Fatal("expected relevance")
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users (receiver+owner), got %d: %v", len(users), users)
	}
	if _, ok := users[common.HexToAddress(userB)]; !ok {
		t.Fatal("receiver (userB) missing")
	}
	if _, ok := users[common.HexToAddress(userC)]; !ok {
		t.Fatal("owner (userC) missing")
	}
	if _, ok := users[common.HexToAddress(userA)]; ok {
		t.Fatal("sender (userA) must NOT be refreshed — that slot is the router/msg.sender")
	}
}

func TestEventExtractor_ExtractTouchedAddresses_Transfer(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics: []string{
			e.TransferTopic().Hex(),
			topicForAddress(userA),
			topicForAddress(userB),
		},
	}}
	users, ok := e.ExtractTouchedAddresses(vault, logs)
	if !ok {
		t.Fatal("expected relevance")
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
}

func TestEventExtractor_ExtractTouchedAddresses_MintIgnoresZeroFrom(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics: []string{
			e.TransferTopic().Hex(),
			topicForAddress("0x0000000000000000000000000000000000000000"),
			topicForAddress(userA),
		},
	}}
	users, ok := e.ExtractTouchedAddresses(vault, logs)
	if !ok {
		t.Fatal("expected relevance")
	}
	if _, hasZero := users[common.Address{}]; hasZero {
		t.Fatal("zero address must not be in refresh set (mint counter-party)")
	}
	if len(users) != 1 {
		t.Fatalf("expected only userA, got %d users: %v", len(users), users)
	}
}

func TestEventExtractor_ExtractTouchedAddresses_BurnIgnoresZeroTo(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics: []string{
			e.TransferTopic().Hex(),
			topicForAddress(userA),
			topicForAddress("0x0000000000000000000000000000000000000000"),
		},
	}}
	users, ok := e.ExtractTouchedAddresses(vault, logs)
	if !ok {
		t.Fatal("expected relevance")
	}
	if len(users) != 1 {
		t.Fatalf("expected only userA, got %d users: %v", len(users), users)
	}
}

func TestEventExtractor_ExtractTouchedAddresses_DedupesAcrossLogs(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	logs := []shared.Log{
		{
			Address: syrupUSDCAddr,
			Topics:  []string{e.DepositTopic().Hex(), topicForAddress(userA), topicForAddress(userB)},
		},
		{
			Address: syrupUSDCAddr,
			Topics:  []string{e.TransferTopic().Hex(), topicForAddress(userA), topicForAddress(userC)},
		},
	}
	users, _ := e.ExtractTouchedAddresses(vault, logs)
	if len(users) != 3 {
		t.Fatalf("expected dedup → 3 unique users, got %d: %v", len(users), users)
	}
}

func TestEventExtractor_ExtractTouchedAddresses_IgnoresWrongAddress(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	logs := []shared.Log{{
		Address: otherAddr,
		Topics:  []string{e.DepositTopic().Hex(), topicForAddress(userA), topicForAddress(userB)},
	}}
	_, ok := e.ExtractTouchedAddresses(vault, logs)
	if ok {
		t.Fatal("logs from non-vault address must not be relevant")
	}
}

func TestEventExtractor_ExtractTouchedAddresses_MalformedTopicsSkipped(t *testing.T) {
	e := newExtractor(t)
	vault := common.HexToAddress(syrupUSDCAddr)
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics:  []string{e.DepositTopic().Hex(), topicForAddress(userA)}, // missing owner topic
	}}
	users, ok := e.ExtractTouchedAddresses(vault, logs)
	if !ok {
		t.Fatal("relevance flag should be true (topic matched), even if payload malformed")
	}
	if len(users) != 0 {
		t.Fatalf("malformed log must contribute no users, got %d", len(users))
	}
}
