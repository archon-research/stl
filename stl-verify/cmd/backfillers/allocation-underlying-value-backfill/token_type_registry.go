package main

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
)

// tokenTypeRegistry resolves a (chain, contract_address) pair to its
// axis-synome token_type (erc20, curve, uni_v3_pool, centrifuge, ...) -- the
// same registry the live allocation tracker classifies positions from
// (internal/services/allocation_tracker.underlyingValuation). A row with no
// receipt_token match is not necessarily a plain erc20: Curve LP shares,
// NAV/RWA shares and pre-cutover Uniswap-V3 rows all clear that check too,
// and self-denominating them (as if erc20) writes plausible-but-wrong data.
// This registry gives classify.go the same answer the live write path would
// have given, instead of assuming "no receipt_token row" means erc20.
type tokenTypeRegistry map[string]string

func loadTokenTypeRegistry() (tokenTypeRegistry, error) {
	contract, err := axis_synome_contract.LoadDefaultContract()
	if err != nil {
		return nil, fmt.Errorf("loading axis-synome contract: %w", err)
	}

	reg := make(tokenTypeRegistry)
	for _, entries := range contract.GetAssetsByPrime() {
		for _, e := range entries {
			reg[tokenTypeKey(e.Chain, common.HexToAddress(e.ContractAddress))] = e.TokenType
		}
	}
	return reg, nil
}

func tokenTypeKey(chain string, address common.Address) string {
	return strings.ToLower(chain) + "|" + strings.ToLower(address.Hex())
}

// isPlainERC20 reports whether the registry resolves this row's own chain and
// token address to token_type "erc20" -- the only type the live tracker
// denominates in itself (internal/services/allocation_tracker.underlyingValuation).
// A row absent from the registry (retired/renamed since the contract was last
// regenerated) is treated as unresolved, not erc20: writing NULL is always
// safe, writing a wrong self-denomination is not.
func (r tokenTypeRegistry) isPlainERC20(chainID int64, address common.Address) bool {
	chain, err := entity.ChainName(chainID)
	if err != nil {
		return false
	}
	return r[tokenTypeKey(chain, address)] == "erc20"
}
