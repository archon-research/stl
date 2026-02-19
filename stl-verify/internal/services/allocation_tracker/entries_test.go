package allocation_tracker

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestBuildEntryLookup_Basic(t *testing.T) {
	entries := []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x1111"), WalletAddress: common.HexToAddress("0xaaaa")},
		{ContractAddress: common.HexToAddress("0x2222"), WalletAddress: common.HexToAddress("0xbbbb")},
	}

	lookup := BuildEntryLookup(entries)
	if len(lookup) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(lookup))
	}
}

func TestBuildEntryLookup_SameContractDifferentWallets(t *testing.T) {
	contract := common.HexToAddress("0x1111")
	entries := []*TokenEntry{
		{ContractAddress: contract, WalletAddress: common.HexToAddress("0xaaaa"), Star: "spark"},
		{ContractAddress: contract, WalletAddress: common.HexToAddress("0xbbbb"), Star: "grove"},
	}

	lookup := BuildEntryLookup(entries)
	if len(lookup) != 2 {
		t.Fatalf("same contract with different wallets should produce 2 entries, got %d", len(lookup))
	}
}

func TestBuildEntryLookup_DuplicateOverwrites(t *testing.T) {
	contract := common.HexToAddress("0x1111")
	wallet := common.HexToAddress("0xaaaa")

	entries := []*TokenEntry{
		{ContractAddress: contract, WalletAddress: wallet, Star: "first"},
		{ContractAddress: contract, WalletAddress: wallet, Star: "second"},
	}

	lookup := BuildEntryLookup(entries)
	if len(lookup) != 1 {
		t.Fatalf("duplicate key should produce 1 entry, got %d", len(lookup))
	}
	if lookup[EntryKey{ContractAddress: contract, WalletAddress: wallet}].Star != "second" {
		t.Error("last entry should win for duplicate keys")
	}
}

func TestDefaultTokenEntries_NotEmpty(t *testing.T) {
	entries := DefaultTokenEntries()
	if len(entries) == 0 {
		t.Fatal("DefaultTokenEntries should not be empty")
	}
}

func TestDefaultTokenEntries_AllHaveRequiredFields(t *testing.T) {
	entries := DefaultTokenEntries()
	zero := common.Address{}

	for i, e := range entries {
		if e.ContractAddress == zero {
			t.Errorf("entry %d: ContractAddress is zero", i)
		}
		if e.WalletAddress == zero {
			t.Errorf("entry %d: WalletAddress is zero", i)
		}
		if e.Star == "" {
			t.Errorf("entry %d (%s): Star is empty", i, e.ContractAddress.Hex())
		}
		if e.Chain == "" {
			t.Errorf("entry %d (%s): Chain is empty", i, e.ContractAddress.Hex())
		}
		if e.TokenType == "" {
			t.Errorf("entry %d (%s): TokenType is empty", i, e.ContractAddress.Hex())
		}
	}
}

func TestDefaultTokenEntries_NoDuplicateKeys(t *testing.T) {
	entries := DefaultTokenEntries()
	seen := make(map[EntryKey]int)

	for i, e := range entries {
		key := e.Key()
		if prev, ok := seen[key]; ok {
			t.Errorf("duplicate key at entry %d and %d: contract=%s wallet=%s",
				prev, i, e.ContractAddress.Hex(), e.WalletAddress.Hex())
		}
		seen[key] = i
	}
}

func TestDefaultTokenEntries_ChainsHaveIDs(t *testing.T) {
	entries := DefaultTokenEntries()
	for _, e := range entries {
		if _, ok := ChainNameToID[e.Chain]; !ok {
			t.Errorf("entry %s on chain %q: chain not in ChainNameToID", e.ContractAddress.Hex(), e.Chain)
		}
	}
}

func TestDefaultTokenEntries_StarsAreValid(t *testing.T) {
	validStars := map[string]bool{"spark": true, "grove": true}
	entries := DefaultTokenEntries()
	for _, e := range entries {
		if !validStars[e.Star] {
			t.Errorf("entry %s has invalid star %q", e.ContractAddress.Hex(), e.Star)
		}
	}
}

func TestEntriesForChain(t *testing.T) {
	entries := DefaultTokenEntries()
	eth := EntriesForChain(entries, "ethereum")
	if len(eth) == 0 {
		t.Fatal("expected ethereum entries")
	}
	for _, e := range eth {
		if e.Chain != "ethereum" {
			t.Errorf("EntriesForChain(ethereum) returned entry with chain %q", e.Chain)
		}
	}
}

func TestEntriesForChain_UnknownChain(t *testing.T) {
	entries := DefaultTokenEntries()
	result := EntriesForChain(entries, "nonexistent")
	if len(result) != 0 {
		t.Errorf("expected 0 entries for unknown chain, got %d", len(result))
	}
}
