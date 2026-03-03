package allocation_tracker

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestEntryKey_Equality(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	addr3 := common.HexToAddress("0x3333333333333333333333333333333333333333")

	key1 := EntryKey{ContractAddress: addr1, WalletAddress: addr2}
	key2 := EntryKey{ContractAddress: addr1, WalletAddress: addr2}
	key3 := EntryKey{ContractAddress: addr1, WalletAddress: addr3}

	if key1 != key2 {
		t.Error("identical EntryKeys should be equal")
	}
	if key1 == key3 {
		t.Error("different WalletAddress should produce different keys")
	}
}

func TestEntryKey_MapKey(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	addr3 := common.HexToAddress("0x3333333333333333333333333333333333333333")

	m := make(map[EntryKey]string)
	m[EntryKey{ContractAddress: addr1, WalletAddress: addr2}] = "first"
	m[EntryKey{ContractAddress: addr1, WalletAddress: addr3}] = "second"

	if len(m) != 2 {
		t.Fatalf("same contract with different wallets should produce 2 map entries, got %d", len(m))
	}
	if m[EntryKey{ContractAddress: addr1, WalletAddress: addr2}] != "first" {
		t.Error("lookup by key failed")
	}
}

func TestTokenEntry_Key(t *testing.T) {
	contract := common.HexToAddress("0xaaaa")
	wallet := common.HexToAddress("0xbbbb")

	entry := &TokenEntry{
		ContractAddress: contract,
		WalletAddress:   wallet,
		Star:            "spark",
		Chain:           "ethereum",
	}

	key := entry.Key()
	if key.ContractAddress != contract {
		t.Errorf("expected contract %s, got %s", contract.Hex(), key.ContractAddress.Hex())
	}
	if key.WalletAddress != wallet {
		t.Errorf("expected wallet %s, got %s", wallet.Hex(), key.WalletAddress.Hex())
	}
}
