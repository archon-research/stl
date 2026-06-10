package allocation_tracker

import (
	"fmt"
	"sort"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
	"github.com/ethereum/go-ethereum/common"
)

// legacyProtocolAliases keeps the tracker's existing protocol vocabulary stable
// while Axis Synome emits more specific source labels.
var legacyProtocolAliases = map[string]string{
	"aave-core":                            "aave",
	"aave-core-v3":                         "aave",
	"aave-horizon":                         "aave",
	"aave-prime":                           "aave",
	"aave-v3":                              "aave",
	"agora-ausd":                           "agora",
	"ethena-protocol":                      "ethena",
	"fluid-finance-erc4626-vault":          "fluid",
	"grove-x-steakhouse-ausd-morpho-vault": "steakhouse",
	"grove-x-steakhouse-usdc-high-yield-vault-v2": "steakhouse",
	"morpho-blue-erc4626-vault":                   "morpho",
	"sparklend-protocol":                          "sparklend",
	"steakhouse-pyusd-morpho-vault":               "steakhouse",
}

// TokenEntriesFromContract converts the contract's assets-by-prime into allocation
// TokenEntries. It is pure (no I/O): callers load the contract once and pass it here, so
// entries and proxies come from a single read. It cross-checks each entry's star against
// its ASSETS_BY_PRIME key and validates the chain vocabulary, so a mis-keyed entry or an
// unrecognised chain fails loudly rather than being silently mis-attributed or dropped.
func TokenEntriesFromContract(contract *axis_synome_contract.Contract) ([]*TokenEntry, error) {
	entriesByStar := contract.GetAssetsByPrime()
	starKeys := make([]string, 0, len(entriesByStar))
	for star := range entriesByStar {
		starKeys = append(starKeys, star)
	}
	sort.Strings(starKeys)

	// The axis-synome export guarantees unique (chain, contract, wallet) token
	// entries (it hard-errors on duplicates) and NewService rejects any that
	// slip through, so entries are appended without local de-duplication.
	entries := make([]*TokenEntry, 0)
	for _, star := range starKeys {
		sourceEntries := entriesByStar[star]
		for i := range sourceEntries {
			src := &sourceEntries[i]
			if src.Star != "" && src.Star != star {
				return nil, fmt.Errorf(
					"token entry star %q does not match its ASSETS_BY_PRIME key %q (chain=%s contract=%s)",
					src.Star, star, src.Chain, src.ContractAddress,
				)
			}
			entries = append(entries, contractTokenEntryToAllocationEntry(star, src))
		}
	}

	sort.Slice(entries, func(i int, j int) bool {
		if entries[i].Chain != entries[j].Chain {
			return entries[i].Chain < entries[j].Chain
		}
		if entries[i].Star != entries[j].Star {
			return entries[i].Star < entries[j].Star
		}
		if entries[i].WalletAddress != entries[j].WalletAddress {
			return entries[i].WalletAddress.Hex() < entries[j].WalletAddress.Hex()
		}
		if entries[i].ContractAddress != entries[j].ContractAddress {
			return entries[i].ContractAddress.Hex() < entries[j].ContractAddress.Hex()
		}
		if entries[i].AllocationType != entries[j].AllocationType {
			return entries[i].AllocationType < entries[j].AllocationType
		}
		if entries[i].Protocol != entries[j].Protocol {
			return entries[i].Protocol < entries[j].Protocol
		}
		return entries[i].TokenType < entries[j].TokenType
	})

	chainCounts := make(map[string]int)
	for _, e := range entries {
		chainCounts[e.Chain]++
	}
	if err := validateChainVocabulary("token entries", chainCounts); err != nil {
		return nil, err
	}

	return entries, nil
}

func contractTokenEntryToAllocationEntry(star string, source *axis_synome_contract.TokenEntry) *TokenEntry {
	entryStar := source.Star
	if entryStar == "" {
		entryStar = star
	}

	return &TokenEntry{
		ContractAddress: common.HexToAddress(source.ContractAddress),
		WalletAddress:   common.HexToAddress(source.WalletAddress),
		AssetAddress:    optionalEntryAssetAddress(source.AssetAddress),
		Star:            entryStar,
		Chain:           source.Chain,
		Protocol:        legacyAllocationTrackerProtocol(source),
		AllocationType:  source.AllocationType,
		TokenType:       source.TokenType,
		// created_at_block is chain-observed, not carried by the axis-synome
		// contract; the allocation tracker owns it via knownCreatedAtBlocks.
		CreatedAtBlock: lookupCreatedAtBlock(source.Chain, common.HexToAddress(source.ContractAddress)),
	}
}

func legacyAllocationTrackerProtocol(source *axis_synome_contract.TokenEntry) string {
	if source.Protocol == "spark-savings-protocol" {
		// The legacy tracker grouped Avalanche savings positions under "spark"
		// and the mainnet savings positions under "sky".
		if source.Chain == "avalanche-c" {
			return "spark"
		}
		return "sky"
	}

	if alias, ok := legacyProtocolAliases[source.Protocol]; ok {
		return alias
	}

	return source.Protocol
}

func optionalEntryAssetAddress(value *string) *common.Address {
	if value == nil {
		return nil
	}

	address := common.HexToAddress(*value)
	return &address
}

func BuildEntryLookup(entries []*TokenEntry) map[EntryKey]*TokenEntry {
	m := make(map[EntryKey]*TokenEntry, len(entries))
	for _, e := range entries {
		m[e.Key()] = e
	}
	return m
}

func EntriesForChain(entries []*TokenEntry, chain string) []*TokenEntry {
	var result []*TokenEntry
	for _, e := range entries {
		if e.Chain == chain {
			result = append(result, e)
		}
	}
	return result
}

func EntriesForChainID(entries []*TokenEntry, chainID int64) []*TokenEntry {
	chain, ok := entity.ChainIDToName[chainID]
	if !ok {
		return nil
	}
	return EntriesForChain(entries, chain)
}

func ProxiesForChainID(proxies []ProxyConfig, chainID int64) []ProxyConfig {
	chain, ok := entity.ChainIDToName[chainID]
	if !ok {
		return nil
	}
	var result []ProxyConfig
	for _, p := range proxies {
		if p.Chain == chain {
			result = append(result, p)
		}
	}
	return result
}

// EntriesAndProxiesForChainID derives the token entries and proxies for a single chain
// from the contract: it converts (validating star/chain vocabulary), filters to chainID,
// and fails with a clear error when the chain has no tracked entries or no proxies. The
// worker calls this once per startup; keeping it here (rather than inline in main) makes
// the empty-set error paths unit-testable without standing up the full worker.
func EntriesAndProxiesForChainID(contract *axis_synome_contract.Contract, chainID int64) ([]*TokenEntry, []ProxyConfig, error) {
	allEntries, err := TokenEntriesFromContract(contract)
	if err != nil {
		return nil, nil, fmt.Errorf("token entries from contract: %w", err)
	}
	entries := EntriesForChainID(allEntries, chainID)
	if len(entries) == 0 {
		return nil, nil, fmt.Errorf("no token entries for chain ID %d", chainID)
	}

	allProxies, err := ProxiesFromContract(contract)
	if err != nil {
		return nil, nil, fmt.Errorf("proxies from contract: %w", err)
	}
	proxies := ProxiesForChainID(allProxies, chainID)
	if len(proxies) == 0 {
		return nil, nil, fmt.Errorf("no proxies for chain ID %d", chainID)
	}

	return entries, proxies, nil
}
