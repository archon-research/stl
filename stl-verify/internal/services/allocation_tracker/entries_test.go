package allocation_tracker

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
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
	seen := make(map[struct {
		Chain string
		EntryKey
	}]int)

	for i, e := range entries {
		key := struct {
			Chain string
			EntryKey
		}{Chain: e.Chain, EntryKey: e.Key()}
		if prev, ok := seen[key]; ok {
			t.Errorf("duplicate key at entry %d and %d: contract=%s wallet=%s chain=%s",
				prev, i, e.ContractAddress.Hex(), e.WalletAddress.Hex(), e.Chain)
		}
		seen[key] = i
	}
}

func TestContractTokenEntryToAllocationEntry_NormalizesProtocol(t *testing.T) {
	tests := []struct {
		name     string
		contract axis_synome_contract.TokenEntry
		want     string
	}{
		{
			name: "aave family collapses to aave",
			contract: axis_synome_contract.TokenEntry{
				ContractAddress: "0x0000000000000000000000000000000000000001",
				WalletAddress:   "0x0000000000000000000000000000000000000002",
				Chain:           "mainnet",
				Protocol:        "aave-prime",
			},
			want: "aave",
		},
		{
			name: "sparklend protocol keeps legacy name",
			contract: axis_synome_contract.TokenEntry{
				ContractAddress: "0x0000000000000000000000000000000000000001",
				WalletAddress:   "0x0000000000000000000000000000000000000002",
				Chain:           "mainnet",
				Protocol:        "sparklend-protocol",
			},
			want: "sparklend",
		},
		{
			name: "steakhouse vault keeps legacy name",
			contract: axis_synome_contract.TokenEntry{
				ContractAddress: "0x0000000000000000000000000000000000000001",
				WalletAddress:   "0x0000000000000000000000000000000000000002",
				Chain:           "mainnet",
				Protocol:        "grove-x-steakhouse-ausd-morpho-vault",
			},
			want: "steakhouse",
		},
		{
			name: "mainnet spark savings stays sky",
			contract: axis_synome_contract.TokenEntry{
				ContractAddress: "0x0000000000000000000000000000000000000001",
				WalletAddress:   "0x0000000000000000000000000000000000000002",
				Chain:           "mainnet",
				Protocol:        "spark-savings-protocol",
			},
			want: "sky",
		},
		{
			name: "avalanche spark savings keeps spark",
			contract: axis_synome_contract.TokenEntry{
				ContractAddress: "0x0000000000000000000000000000000000000001",
				WalletAddress:   "0x0000000000000000000000000000000000000002",
				Chain:           "avalanche-c",
				Protocol:        "spark-savings-protocol",
			},
			want: "spark",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := contractTokenEntryToAllocationEntry("spark", &tt.contract)
			if got.Protocol != tt.want {
				t.Fatalf("Protocol = %q, want %q", got.Protocol, tt.want)
			}
		})
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
	eth := EntriesForChain(entries, "mainnet")
	if len(eth) == 0 {
		t.Fatal("expected mainnet entries")
	}
	for _, e := range eth {
		if e.Chain != "mainnet" {
			t.Errorf("EntriesForChain(mainnet) returned entry with chain %q", e.Chain)
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
