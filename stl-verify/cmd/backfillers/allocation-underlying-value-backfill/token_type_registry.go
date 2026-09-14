package main

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
)

// tokenTypeEntry is one axis-synome registry entry's classification of a
// (chain, contract_address) pair -- the same registry the live allocation
// tracker classifies positions from (internal/services/allocation_tracker.
// underlyingValuation). assetAddress is the address that type denominates in
// when it isn't itself (erc4626, atoken); nil for a type that has none.
type tokenTypeEntry struct {
	tokenType    string
	assetAddress *common.Address
}

// tokenTypeRegistry resolves a (chain, contract_address) pair to its
// axis-synome classification. A row with no receipt_token match is not
// necessarily a plain erc20: Curve LP shares, NAV/RWA shares and pre-cutover
// Uniswap-V3 rows all clear that check too, and self-denominating them (as if
// erc20) writes plausible-but-wrong data. This registry gives classify.go the
// same answer the live write path would have given, instead of assuming "no
// receipt_token row" means erc20.
type tokenTypeRegistry map[string]tokenTypeEntry

func loadTokenTypeRegistry() (tokenTypeRegistry, error) {
	contract, err := axis_synome_contract.LoadDefaultContract()
	if err != nil {
		return nil, fmt.Errorf("loading axis-synome contract: %w", err)
	}

	reg := make(tokenTypeRegistry)
	for _, entries := range contract.GetAssetsByPrime() {
		for _, e := range entries {
			var assetAddr *common.Address
			if e.AssetAddress != nil {
				addr := common.HexToAddress(*e.AssetAddress)
				assetAddr = &addr
			}
			reg[tokenTypeKey(e.Chain, common.HexToAddress(e.ContractAddress))] = tokenTypeEntry{
				tokenType:    e.TokenType,
				assetAddress: assetAddr,
			}
		}
	}
	return reg, nil
}

func tokenTypeKey(chain string, address common.Address) string {
	return strings.ToLower(chain) + "|" + strings.ToLower(address.Hex())
}

// lookup resolves a row's own chain and token address to its registry entry,
// the same identity the live tracker classifies positions from. ok is false
// for a row absent from the registry (retired/renamed since the contract was
// last regenerated).
func (r tokenTypeRegistry) lookup(chainID int64, address common.Address) (tokenTypeEntry, bool) {
	chain, err := entity.ChainName(chainID)
	if err != nil {
		return tokenTypeEntry{}, false
	}
	entry, ok := r[tokenTypeKey(chain, address)]
	return entry, ok
}

// isPlainERC20 reports whether the registry resolves this row's own chain and
// token address to token_type "erc20" -- one of three types the live tracker
// values (internal/services/allocation_tracker.underlyingValuation); the
// other two, erc4626 and atoken, are handled by registryPromotableEntry
// instead. A row absent from the registry is treated as unresolved, not
// erc20: writing NULL is always safe, writing a wrong self-denomination is
// not.
func (r tokenTypeRegistry) isPlainERC20(chainID int64, address common.Address) bool {
	entry, ok := r.lookup(chainID, address)
	return ok && entry.tokenType == "erc20"
}

// tokenDecimalsKey identifies one (chain, address) pair whose decimals must
// come from the token table: the axis-synome contract names an asset, never
// its decimals.
type tokenDecimalsKey struct {
	chainID int64
	address common.Address
}

// tokenDecimalsLookup answers tokenDecimalsKey with the token table's own
// decimals for that address -- the on-chain truth the write path descales
// the raw underlying amount by, as opposed to a value merely assumed from the
// registry or copied from the share token.
type tokenDecimalsLookup map[tokenDecimalsKey]int32

// registryPromotableEntry reports whether a non-receipt-token row's registry
// entry is one of the two types the live tracker denominates in something
// other than the token itself (erc4626, atoken) and has an asset_address to
// do it with. The NAV/RWA share types (buidl, securitize, superstate,
// centrifuge, proxy) and curve pool positions stay excluded: their balanceOf
// is a share count and asset_address is a pricing hint, not a redemption
// denomination, matching how the live tracker's own default case leaves them
// nil.
func registryPromotableEntry(c candidateRow, tokenTypes tokenTypeRegistry) (tokenTypeEntry, bool) {
	if c.isReceiptToken {
		return tokenTypeEntry{}, false
	}
	entry, ok := tokenTypes.lookup(c.chainID, c.tokenAddress)
	if !ok || entry.assetAddress == nil {
		return tokenTypeEntry{}, false
	}
	if entry.tokenType != "atoken" && entry.tokenType != "erc4626" {
		return tokenTypeEntry{}, false
	}
	return entry, true
}

// registryUnderlyingKeys collects the distinct (chain, asset_address) pairs
// applyRegistryUnderlyings will need decimals for, so the caller can resolve
// them all in one query instead of one per candidate.
func registryUnderlyingKeys(candidates []candidateRow, tokenTypes tokenTypeRegistry) []tokenDecimalsKey {
	seen := make(map[tokenDecimalsKey]bool)
	var keys []tokenDecimalsKey
	for _, c := range candidates {
		entry, ok := registryPromotableEntry(c, tokenTypes)
		if !ok {
			continue
		}
		key := tokenDecimalsKey{chainID: c.chainID, address: *entry.assetAddress}
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys
}

// applyRegistryUnderlyings promotes a non-receipt-token row whose registry
// type is erc4626 or atoken into the same underlyingAddress/
// underlyingDecimals/underlyingIsOneToOne shape a receipt_token match would
// produce, so it flows through classifyCandidates' existing aToken/erc4626
// branches unchanged. A row the registry promotes but decimals has no answer
// for (the asset_address is not yet a known token) is left unpromoted --
// classifyCandidates counts that as skippedNoUnderlying, the same outcome a
// receipt_token row with an unresolved underlying already gets.
func applyRegistryUnderlyings(candidates []candidateRow, tokenTypes tokenTypeRegistry, decimals tokenDecimalsLookup) []candidateRow {
	out := make([]candidateRow, len(candidates))
	copy(out, candidates)
	for i, c := range out {
		entry, ok := registryPromotableEntry(c, tokenTypes)
		if !ok {
			continue
		}
		dec, ok := decimals[tokenDecimalsKey{chainID: c.chainID, address: *entry.assetAddress}]
		if !ok {
			continue
		}
		out[i].underlyingAddress = entry.assetAddress
		out[i].underlyingDecimals = &dec
		out[i].underlyingIsOneToOne = entry.tokenType == "atoken"
	}
	return out
}
