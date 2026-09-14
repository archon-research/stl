package main

import (
	"log/slog"
	"math/big"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// classificationStats tallies how each candidate row was resolved, so the
// operator can see which source produced each erc4626 conversion -- a real
// archive read or the price-ratio fallback -- and why any row was skipped.
// The ViaRegistry counters are the same routes, but for a row with no
// receipt_token match at all: the registry's own asset_address resolved it
// instead (see registryPromotableEntry).
type classificationStats struct {
	direct                                int
	aToken                                int
	aTokenViaRegistry                     int
	convertedERC4626Archive               int
	convertedERC4626ArchiveViaRegistry    int
	convertedERC4626PriceRatio            int
	convertedERC4626PriceRatioViaRegistry int
	skippedNoUnderlying                   int
	skippedNotPlainERC20                  int
	skippedNoPriceHistory                 int
	skippedPriceTooStale                  int
	skippedRatioNotExact                  int
}

// positionSource pairs a resolved position with how it was derived, purely
// for the dry-run preview log -- it never reaches the entity or the database.
type positionSource struct {
	position *entity.AllocationPosition
	source   string
}

// classifyCandidates turns each candidate into a position (or a documented
// skip). erc4626-like rows prefer archiveResults -- a real convertToAssets
// read at the row's own block -- and fall back to the price-ratio derivation
// only when the archive could not answer for that row.
func classifyCandidates(candidates []candidateRow, archiveResults map[int]*big.Int, tokenTypes tokenTypeRegistry, cfg cliConfig) ([]positionSource, classificationStats, error) {
	out := make([]positionSource, 0, len(candidates))
	var stats classificationStats

	for i, c := range candidates {
		var (
			pos  positionSource
			emit bool
			err  error
		)
		switch {
		case !c.isReceiptToken && c.underlyingAddress == nil:
			pos, emit = classifyDirectRow(c, tokenTypes, &stats)
		case c.underlyingIsOneToOne:
			pos, emit = classifyATokenRow(c, &stats)
		default:
			pos, emit, err = classifyERC4626Row(c, archiveResults[i], cfg, &stats)
		}
		if err != nil {
			return nil, classificationStats{}, err
		}
		if emit {
			out = append(out, pos)
		}
	}

	return out, stats, nil
}

// classifyDirectRow resolves a row with no receipt_token match and no registry
// underlying, tallying which of the two refusals applied when it resolves none.
func classifyDirectRow(c candidateRow, tokenTypes tokenTypeRegistry, stats *classificationStats) (positionSource, bool) {
	pos, skip := classifyDirect(c, tokenTypes)
	switch skip {
	case directSkipNoUnderlying:
		stats.skippedNoUnderlying++
	case directSkipNotPlainERC20:
		stats.skippedNotPlainERC20++
	default:
		stats.direct++
		return pos, true
	}
	return positionSource{}, false
}

// classifyATokenRow resolves a 1:1 aToken holding, counting it against the
// receipt_token or the registry route it arrived by.
func classifyATokenRow(c candidateRow, stats *classificationStats) (positionSource, bool) {
	pos, ok := aTokenPosition(c)
	if !ok {
		stats.skippedNoUnderlying++
		return positionSource{}, false
	}
	if c.isReceiptToken {
		stats.aToken++
	} else {
		stats.aTokenViaRegistry++
	}
	return pos, true
}

// classifyERC4626Row converts a share balance, tallying the documented skip
// when the conversion cannot be trusted.
func classifyERC4626Row(c candidateRow, archiveResult *big.Int, cfg cliConfig, stats *classificationStats) (positionSource, bool, error) {
	pos, skip, err := classifyERC4626(c, archiveResult, cfg)
	if err != nil {
		return positionSource{}, false, err
	}
	switch skip {
	case skipNoPriceHistory:
		stats.skippedNoPriceHistory++
	case skipPriceTooStale:
		stats.skippedPriceTooStale++
	case skipRatioNotExact:
		stats.skippedRatioNotExact++
	default:
		recordERC4626Conversion(stats, pos.source, c.isReceiptToken)
		return pos, true, nil
	}
	return positionSource{}, false, nil
}

// recordERC4626Conversion tallies a resolved erc4626 conversion into the
// (archive vs price-ratio) x (receipt_token vs registry) counter it came
// from.
func recordERC4626Conversion(stats *classificationStats, source string, isReceiptToken bool) {
	switch {
	case source == sourceERC4626Archive && isReceiptToken:
		stats.convertedERC4626Archive++
	case source == sourceERC4626Archive:
		stats.convertedERC4626ArchiveViaRegistry++
	case isReceiptToken:
		stats.convertedERC4626PriceRatio++
	default:
		stats.convertedERC4626PriceRatioViaRegistry++
	}
}

// directHoldingPosition denominates a non-receipt-token row in itself: the
// invariant "underlying_token_id IS NULL iff underlying_value IS NULL" forces
// a self-reference here, not NULL.
func directHoldingPosition(c candidateRow) positionSource {
	return positionSource{toEntity(c, c.tokenAddress, c.tokenDecimals, c.balance), "direct"}
}

// directSkip is why a non-receipt-token, non-registry-resolved candidate could
// not be self-denominated as a plain erc20 holding.
type directSkip int

const (
	directSkipNone directSkip = iota
	directSkipNoUnderlying
	directSkipNotPlainERC20
)

// classifyDirect resolves a row with no receipt_token match and no
// registry-resolved underlying: a plain erc20 holding denominated in itself,
// or a documented skip.
func classifyDirect(c candidateRow, tokenTypes tokenTypeRegistry) (positionSource, directSkip) {
	if entry, ok := tokenTypes.lookup(c.chainID, c.tokenAddress); ok && (entry.tokenType == "atoken" || entry.tokenType == "erc4626") {
		// The registry resolved a route for this row (see
		// registryPromotableEntry) but the asset_address it named isn't a
		// token we know decimals for yet -- an unresolved underlying, the
		// same outcome a receipt_token row with no registry match for its
		// own underlying already gets.
		return positionSource{}, directSkipNoUnderlying
	}
	if !tokenTypes.isPlainERC20(c.chainID, c.tokenAddress) {
		// Curve LP shares, NAV/RWA shares and uni_v3 pool/lp rows all clear
		// the receipt_token check too (it is seeded only for
		// SparkLend/Aave/Morpho/Maple); self-denominating them would write
		// plausible-but-wrong data the live tracker itself refuses to write
		// (see underlyingValuation's default case).
		return positionSource{}, directSkipNotPlainERC20
	}
	return directHoldingPosition(c), directSkipNone
}

// aTokenPosition resolves a 1:1 aToken holding: the raw underlying amount
// equals the raw balance -- only the denominating asset and its decimals
// differ, so this reads the underlying's own decimals rather than assuming
// they match the share token's. ok is false when the underlying address or
// its decimals are unresolved, which classifyCandidates counts as
// skippedNoUnderlying.
func aTokenPosition(c candidateRow) (positionSource, bool) {
	if c.underlyingAddress == nil || c.underlyingDecimals == nil {
		return positionSource{}, false
	}
	return positionSource{toEntity(c, *c.underlyingAddress, *c.underlyingDecimals, c.balance), "aToken"}, true
}

type erc4626Skip int

const (
	skipNone erc4626Skip = iota
	skipNoPriceHistory
	skipPriceTooStale
	skipRatioNotExact
)

const (
	sourceERC4626Archive    = "erc4626_archive"
	sourceERC4626PriceRatio = "erc4626_price_ratio"
)

// classifyERC4626 resolves one erc4626-like row's underlying value: a real
// archive conversion when the resolver found one for this row (archiveRaw),
// the price-ratio derivation otherwise. The ratio is read at-or-before the
// row's own block, so it IS a neighbour-block estimate whose error is bounded
// by maxPriceLag on both legs -- a row with neither source, or whose nearer
// leg is staler than that bound, is skipped rather than approximated further.
func classifyERC4626(c candidateRow, archiveRaw *big.Int, cfg cliConfig) (positionSource, erc4626Skip, error) {
	if archiveRaw != nil {
		// Guaranteed non-nil: the resolver only produces a result for rows
		// isERC4626ArchiveCandidate already required these on.
		pos := toEntity(c, *c.underlyingAddress, *c.underlyingDecimals, archiveRaw)
		return positionSource{pos, sourceERC4626Archive}, skipNone, nil
	}
	if c.erc4626UnderlyingHuman == nil || c.underlyingAddress == nil || c.underlyingDecimals == nil {
		return positionSource{}, skipNoPriceHistory, nil
	}
	if c.sharePriceBlockLag == nil || *c.sharePriceBlockLag > cfg.maxPriceLag {
		return positionSource{}, skipPriceTooStale, nil
	}
	if c.underlyingPriceBlockLag == nil || *c.underlyingPriceBlockLag > cfg.maxPriceLag {
		return positionSource{}, skipPriceTooStale, nil
	}
	underlyingRaw, err := humanToRaw(*c.erc4626UnderlyingHuman, *c.underlyingDecimals)
	if err != nil {
		// The division that produced this human string does not always
		// terminate (irrational-looking ratios can leave more fractional
		// digits than the token's decimals can hold exactly); that is a fact
		// about this one row, not a reason to abort the whole batch.
		slog.Warn("erc4626 price-ratio underlying is not an exact raw amount, skipping row",
			"block_number", c.blockNumber, "token", c.tokenAddress.Hex(), "error", err)
		return positionSource{}, skipRatioNotExact, nil
	}
	pos := toEntity(c, *c.underlyingAddress, *c.underlyingDecimals, underlyingRaw)
	return positionSource{pos, sourceERC4626PriceRatio}, skipNone, nil
}

func logClassification(stats classificationStats, total int) {
	slog.Info("classified",
		"positions_to_write", total,
		"direct_holdings", stats.direct,
		"atoken_holdings", stats.aToken,
		"atoken_holdings_via_registry", stats.aTokenViaRegistry,
		"erc4626_converted_via_archive", stats.convertedERC4626Archive,
		"erc4626_converted_via_archive_via_registry", stats.convertedERC4626ArchiveViaRegistry,
		"erc4626_converted_via_price_ratio", stats.convertedERC4626PriceRatio,
		"erc4626_converted_via_price_ratio_via_registry", stats.convertedERC4626PriceRatioViaRegistry,
		"skipped_no_resolved_underlying", stats.skippedNoUnderlying,
		"skipped_not_plain_erc20", stats.skippedNotPlainERC20,
		"skipped_erc4626_no_price_history", stats.skippedNoPriceHistory,
		"skipped_erc4626_price_too_stale", stats.skippedPriceTooStale,
		"skipped_erc4626_ratio_not_exact", stats.skippedRatioNotExact,
	)
}
