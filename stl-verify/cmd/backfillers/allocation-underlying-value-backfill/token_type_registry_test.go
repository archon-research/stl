package main

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

var testRegistryUnderlying = common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb49")

func TestIsPlainERC20(t *testing.T) {
	registry := tokenTypeRegistry{
		tokenTypeKey("mainnet", testDirect): tokenTypeEntry{tokenType: "erc20"},
		tokenTypeKey("mainnet", testVault):  tokenTypeEntry{tokenType: "erc4626", assetAddress: &testRegistryUnderlying},
	}
	if !registry.isPlainERC20(1, testDirect) {
		t.Error("isPlainERC20 = false, want true for a registered erc20 entry")
	}
	if registry.isPlainERC20(1, testVault) {
		t.Error("isPlainERC20 = true, want false for a registered erc4626 entry")
	}
	if registry.isPlainERC20(1, common.HexToAddress("0x9999999999999999999999999999999999999999")) {
		t.Error("isPlainERC20 = true, want false for an address absent from the registry")
	}
}

// TestRegistryPromotableEntry_ATokenAndERC4626Promoted guards the two types
// the live tracker denominates in something other than the token itself
// (internal/services/allocation_tracker.underlyingValuation's erc4626 and
// atoken cases): both must be promotable from a non-receipt-token row.
func TestRegistryPromotableEntry_ATokenAndERC4626Promoted(t *testing.T) {
	tests := []string{"atoken", "erc4626"}
	for _, tokenType := range tests {
		t.Run(tokenType, func(t *testing.T) {
			c := baseCandidate()
			c.isReceiptToken = false
			registry := tokenTypeRegistry{
				tokenTypeKey("mainnet", c.tokenAddress): tokenTypeEntry{tokenType: tokenType, assetAddress: &testRegistryUnderlying},
			}

			entry, ok := registryPromotableEntry(c, registry)
			if !ok {
				t.Fatalf("registryPromotableEntry ok = false, want true for registry type %q", tokenType)
			}
			if entry.tokenType != tokenType {
				t.Errorf("entry.tokenType = %q, want %q", entry.tokenType, tokenType)
			}
			if entry.assetAddress == nil || *entry.assetAddress != testRegistryUnderlying {
				t.Errorf("entry.assetAddress = %v, want %s", entry.assetAddress, testRegistryUnderlying)
			}
		})
	}
}

// TestRegistryPromotableEntry_NavRwaAndCurveNeverPromoted guards the live
// tracker's default case: NAV/RWA share types and curve pool positions stay
// nil because their balanceOf is a share count and asset_address is a pricing
// hint, not a redemption denomination -- self-denominating them would write
// plausible-but-wrong data, so these must never be promoted even though the
// registry gives them a real asset_address.
func TestRegistryPromotableEntry_NavRwaAndCurveNeverPromoted(t *testing.T) {
	tests := []string{"curve", "centrifuge", "superstate", "proxy", "erc20", "uni_v3_pool"}
	for _, tokenType := range tests {
		t.Run(tokenType, func(t *testing.T) {
			c := baseCandidate()
			c.isReceiptToken = false
			registry := tokenTypeRegistry{
				tokenTypeKey("mainnet", c.tokenAddress): tokenTypeEntry{tokenType: tokenType, assetAddress: &testRegistryUnderlying},
			}

			if _, ok := registryPromotableEntry(c, registry); ok {
				t.Errorf("registryPromotableEntry ok = true, want false for registry type %q", tokenType)
			}
		})
	}
}

// TestRegistryPromotableEntry_ReceiptTokenRowNeverPromoted guards against
// double-resolving a row that already has a receipt_token match: the registry
// path exists only to catch rows receipt_token misses.
func TestRegistryPromotableEntry_ReceiptTokenRowNeverPromoted(t *testing.T) {
	c := baseCandidate()
	c.isReceiptToken = true
	registry := tokenTypeRegistry{
		tokenTypeKey("mainnet", c.tokenAddress): tokenTypeEntry{tokenType: "erc4626", assetAddress: &testRegistryUnderlying},
	}

	if _, ok := registryPromotableEntry(c, registry); ok {
		t.Error("registryPromotableEntry ok = true, want false for a row that already matched receipt_token")
	}
}

// TestRegistryPromotableEntry_AbsentFromRegistryNeverPromoted guards the
// unresolved case: a token absent from the axis-synome contract entirely
// must not be promoted -- writing NULL is always safe, writing a wrong
// self-denomination is not.
func TestRegistryPromotableEntry_AbsentFromRegistryNeverPromoted(t *testing.T) {
	c := baseCandidate()
	c.isReceiptToken = false

	if _, ok := registryPromotableEntry(c, tokenTypeRegistry{}); ok {
		t.Error("registryPromotableEntry ok = true, want false for a token absent from the registry")
	}
}

func TestApplyRegistryUnderlyings_PromotesWhenDecimalsResolved(t *testing.T) {
	c := baseCandidate()
	c.isReceiptToken = false
	tokenTypes := tokenTypeRegistry{
		tokenTypeKey("mainnet", c.tokenAddress): tokenTypeEntry{tokenType: "atoken", assetAddress: &testRegistryUnderlying},
	}
	decimals := tokenDecimalsLookup{
		{chainID: c.chainID, address: testRegistryUnderlying}: 6,
	}

	out := applyRegistryUnderlyings([]candidateRow{c}, tokenTypes, decimals)
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	got := out[0]
	if got.underlyingAddress == nil || *got.underlyingAddress != testRegistryUnderlying {
		t.Errorf("underlyingAddress = %v, want %s", got.underlyingAddress, testRegistryUnderlying)
	}
	if got.underlyingDecimals == nil || *got.underlyingDecimals != 6 {
		t.Errorf("underlyingDecimals = %v, want 6", got.underlyingDecimals)
	}
	if !got.underlyingIsOneToOne {
		t.Error("underlyingIsOneToOne = false, want true for a registry atoken entry")
	}
}

// TestApplyRegistryUnderlyings_LeavesRowUnpromotedWhenDecimalsUnresolved
// guards the case where the registry names an asset the token table does not
// yet know decimals for: the row must be left exactly as fetchCandidates
// produced it, not promoted with a guessed decimals count.
func TestApplyRegistryUnderlyings_LeavesRowUnpromotedWhenDecimalsUnresolved(t *testing.T) {
	c := baseCandidate()
	c.isReceiptToken = false
	tokenTypes := tokenTypeRegistry{
		tokenTypeKey("mainnet", c.tokenAddress): tokenTypeEntry{tokenType: "erc4626", assetAddress: &testRegistryUnderlying},
	}

	out := applyRegistryUnderlyings([]candidateRow{c}, tokenTypes, tokenDecimalsLookup{})
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	if out[0].underlyingAddress != nil {
		t.Errorf("underlyingAddress = %v, want nil when decimals could not be resolved", out[0].underlyingAddress)
	}
}

func TestRegistryUnderlyingKeys_DeduplicatesAcrossCandidates(t *testing.T) {
	a := baseCandidate()
	a.isReceiptToken = false
	a.tokenAddress = testVault
	b := baseCandidate()
	b.isReceiptToken = false
	b.tokenAddress = testDirect

	tokenTypes := tokenTypeRegistry{
		tokenTypeKey("mainnet", a.tokenAddress): tokenTypeEntry{tokenType: "erc4626", assetAddress: &testRegistryUnderlying},
		tokenTypeKey("mainnet", b.tokenAddress): tokenTypeEntry{tokenType: "atoken", assetAddress: &testRegistryUnderlying},
	}

	keys := registryUnderlyingKeys([]candidateRow{a, b}, tokenTypes)
	if len(keys) != 1 {
		t.Fatalf("len(keys) = %d, want 1 (both rows share one asset_address)", len(keys))
	}
	if keys[0] != (tokenDecimalsKey{chainID: a.chainID, address: testRegistryUnderlying}) {
		t.Errorf("keys[0] = %+v, want the shared underlying key", keys[0])
	}
}
