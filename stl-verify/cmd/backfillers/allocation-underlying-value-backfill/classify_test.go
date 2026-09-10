package main

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

var (
	testVault      = common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9")
	testUnderlying = common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	testDirect     = common.HexToAddress("0x1111111111111111111111111111111111111111")
)

// baseCandidate returns a minimal valid candidateRow a test tweaks from, so
// each case only sets the fields its scenario actually varies.
func baseCandidate() candidateRow {
	return candidateRow{
		chainID:       1,
		tokenAddress:  testDirect,
		tokenDecimals: 18,
		primeID:       1,
		balance:       big.NewInt(1_000_000_000_000_000_000),
	}
}

func TestClassifyCandidates_DirectHolding(t *testing.T) {
	c := baseCandidate()
	c.isReceiptToken = false

	out, stats, err := classifyCandidates([]candidateRow{c}, nil, cliConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.direct != 1 || len(out) != 1 {
		t.Fatalf("stats = %+v, len(out) = %d, want direct=1", stats, len(out))
	}
	if out[0].source != "direct" {
		t.Errorf("source = %q, want direct", out[0].source)
	}
	if out[0].position.Underlying.AssetAddress != testDirect {
		t.Errorf("direct holding must self-reference its own token as the underlying asset")
	}
}

func TestClassifyCandidates_AToken(t *testing.T) {
	underlying := testUnderlying
	c := baseCandidate()
	c.isReceiptToken = true
	c.underlyingIsOneToOne = true
	c.underlyingAddress = &underlying

	out, stats, err := classifyCandidates([]candidateRow{c}, nil, cliConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.aToken != 1 || len(out) != 1 {
		t.Fatalf("stats = %+v, len(out) = %d, want aToken=1", stats, len(out))
	}
	if out[0].position.Underlying.Value.Cmp(c.balance) != 0 {
		t.Errorf("aToken underlying value = %s, want 1:1 with balance %s", out[0].position.Underlying.Value, c.balance)
	}
}

func TestClassifyCandidates_ATokenSkippedWithoutRegistryUnderlying(t *testing.T) {
	c := baseCandidate()
	c.isReceiptToken = true
	c.underlyingIsOneToOne = true
	c.underlyingAddress = nil // registry could not resolve one

	out, stats, err := classifyCandidates([]candidateRow{c}, nil, cliConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.skippedNoUnderlying != 1 || len(out) != 0 {
		t.Fatalf("stats = %+v, len(out) = %d, want skippedNoUnderlying=1, 0 positions", stats, len(out))
	}
}

func erc4626Candidate() candidateRow {
	underlying := testUnderlying
	decimals := int32(6)
	c := baseCandidate()
	c.tokenAddress = testVault
	c.isReceiptToken = true
	c.underlyingIsOneToOne = false
	c.underlyingAddress = &underlying
	c.underlyingDecimals = &decimals
	c.protocolName = "Morpho Blue"
	return c
}

func TestClassifyCandidates_ERC4626PrefersArchiveOverPriceRatio(t *testing.T) {
	c := erc4626Candidate()
	// Price-ratio fallback data is present too, so this asserts precedence:
	// a resolved archive read must win even when a fallback is available.
	fallback := "999.000000"
	lag := int64(0)
	c.erc4626UnderlyingHuman = &fallback
	c.sharePriceBlockLag = &lag

	archiveRaw := big.NewInt(1_234_567_000) // 1234.567000 at 6 decimals
	archiveResults := map[int]*big.Int{0: archiveRaw}

	out, stats, err := classifyCandidates([]candidateRow{c}, archiveResults, cliConfig{maxPriceLag: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.convertedERC4626Archive != 1 || stats.convertedERC4626PriceRatio != 0 {
		t.Fatalf("stats = %+v, want archive=1 priceRatio=0", stats)
	}
	if out[0].source != sourceERC4626Archive {
		t.Errorf("source = %q, want %q", out[0].source, sourceERC4626Archive)
	}
	if out[0].position.Underlying.Value.Cmp(archiveRaw) != 0 {
		t.Errorf("underlying value = %s, want the archive-derived %s (not the price-ratio fallback)", out[0].position.Underlying.Value, archiveRaw)
	}
}

func TestClassifyCandidates_ERC4626FallsBackToPriceRatioWhenArchiveMissing(t *testing.T) {
	c := erc4626Candidate()
	human := "1234.567000"
	lag := int64(50)
	c.erc4626UnderlyingHuman = &human
	c.sharePriceBlockLag = &lag

	// No entry in archiveResults for index 0: the archive call reverted or
	// was never attempted, so this must fall back rather than skip.
	out, stats, err := classifyCandidates([]candidateRow{c}, map[int]*big.Int{}, cliConfig{maxPriceLag: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.convertedERC4626PriceRatio != 1 || stats.convertedERC4626Archive != 0 {
		t.Fatalf("stats = %+v, want priceRatio=1 archive=0", stats)
	}
	if out[0].source != sourceERC4626PriceRatio {
		t.Errorf("source = %q, want %q", out[0].source, sourceERC4626PriceRatio)
	}
	want, _ := humanToRaw("1234.567000", 6)
	if out[0].position.Underlying.Value.Cmp(want) != 0 {
		t.Errorf("underlying value = %s, want %s", out[0].position.Underlying.Value, want)
	}
}

func TestClassifyCandidates_ERC4626SkipsWhenNoPriceHistoryAndNoArchive(t *testing.T) {
	c := erc4626Candidate() // erc4626UnderlyingHuman left nil: no price history

	out, stats, err := classifyCandidates([]candidateRow{c}, map[int]*big.Int{}, cliConfig{maxPriceLag: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.skippedNoPriceHistory != 1 || len(out) != 0 {
		t.Fatalf("stats = %+v, len(out) = %d, want skippedNoPriceHistory=1", stats, len(out))
	}
}

func TestClassifyCandidates_ERC4626SkipsWhenPriceTooStaleAndNoArchive(t *testing.T) {
	c := erc4626Candidate()
	human := "1234.567000"
	lag := int64(200) // exceeds maxPriceLag below
	c.erc4626UnderlyingHuman = &human
	c.sharePriceBlockLag = &lag

	out, stats, err := classifyCandidates([]candidateRow{c}, map[int]*big.Int{}, cliConfig{maxPriceLag: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.skippedPriceTooStale != 1 || len(out) != 0 {
		t.Fatalf("stats = %+v, len(out) = %d, want skippedPriceTooStale=1", stats, len(out))
	}
}

func TestClassifyCandidates_ERC4626PriceRatioBadHumanStringErrors(t *testing.T) {
	c := erc4626Candidate()
	human := "not-a-number"
	lag := int64(0)
	c.erc4626UnderlyingHuman = &human
	c.sharePriceBlockLag = &lag

	_, _, err := classifyCandidates([]candidateRow{c}, map[int]*big.Int{}, cliConfig{maxPriceLag: 100})
	if err == nil {
		t.Fatal("expected an error for an unparsable price-ratio human string")
	}
}

// TestClassifyCandidates_ProtocolNameDrivesClassification_NotSymbol guards
// against keying classification on a token's display symbol: two different
// on-chain addresses can share one symbol (spDAI is a live example -- a
// SparkLend aToken at one address, a Morpho Blue vault at another), and only
// the resolved protocol name for THIS row's own address may decide the
// bucket.
func TestClassifyCandidates_ProtocolNameDrivesClassification_NotSymbol(t *testing.T) {
	underlying := testUnderlying
	decimals := int32(18)

	aToken := baseCandidate()
	aToken.tokenAddress = common.HexToAddress("0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b")
	aToken.isReceiptToken = true
	aToken.underlyingIsOneToOne = true // resolved from protocol name "SparkLend"
	aToken.underlyingAddress = &underlying
	aToken.protocolName = "SparkLend"

	vault := baseCandidate()
	vault.tokenAddress = common.HexToAddress("0x73e65dbd630f90604062f6e02fab9138e713edd9")
	vault.isReceiptToken = true
	vault.underlyingIsOneToOne = false // resolved from protocol name "Morpho Blue"
	vault.underlyingAddress = &underlying
	vault.underlyingDecimals = &decimals
	vault.protocolName = "Morpho Blue"
	archiveRaw := big.NewInt(2_000_000_000_000_000_000)

	out, stats, err := classifyCandidates([]candidateRow{aToken, vault}, map[int]*big.Int{1: archiveRaw}, cliConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.aToken != 1 || stats.convertedERC4626Archive != 1 {
		t.Fatalf("stats = %+v, want aToken=1 convertedERC4626Archive=1", stats)
	}
	if out[0].position.Underlying.Value.Cmp(aToken.balance) != 0 {
		t.Errorf("SparkLend-registered address must resolve 1:1, got %s", out[0].position.Underlying.Value)
	}
	if out[1].position.Underlying.Value.Cmp(archiveRaw) != 0 {
		t.Errorf("Morpho-Blue-registered address must resolve via the real conversion, got %s", out[1].position.Underlying.Value)
	}
}
