package main

import (
	"log/slog"
	"math/big"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// classificationStats tallies how each candidate row was resolved, so the
// operator can see which source produced each erc4626 conversion -- a real
// archive read or the price-ratio fallback -- and why any row was skipped.
type classificationStats struct {
	direct                     int
	aToken                     int
	convertedERC4626Archive    int
	convertedERC4626PriceRatio int
	skippedNoUnderlying        int
	skippedNotPlainERC20       int
	skippedNoPriceHistory      int
	skippedPriceTooStale       int
	skippedRatioNotExact       int
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
		switch {
		case !c.isReceiptToken:
			if !tokenTypes.isPlainERC20(c.chainID, c.tokenAddress) {
				// Curve LP shares, NAV/RWA shares and uni_v3 pool/lp rows all
				// clear the receipt_token check too (it is seeded only for
				// SparkLend/Aave/Morpho/Maple); self-denominating them would
				// write plausible-but-wrong data the live tracker itself
				// refuses to write (see underlyingValuation's default case).
				stats.skippedNotPlainERC20++
				continue
			}
			out = append(out, directHoldingPosition(c))
			stats.direct++

		case c.underlyingIsOneToOne:
			pos, ok := aTokenPosition(c)
			if !ok {
				stats.skippedNoUnderlying++
				continue
			}
			out = append(out, pos)
			stats.aToken++

		default:
			pos, skip, err := classifyERC4626(c, archiveResults[i], cfg)
			if err != nil {
				return nil, classificationStats{}, err
			}
			switch skip {
			case skipNoPriceHistory:
				stats.skippedNoPriceHistory++
			case skipPriceTooStale:
				stats.skippedPriceTooStale++
			case skipRatioNotExact:
				stats.skippedRatioNotExact++
			default:
				out = append(out, pos)
				if pos.source == sourceERC4626Archive {
					stats.convertedERC4626Archive++
				} else {
					stats.convertedERC4626PriceRatio++
				}
			}
		}
	}

	return out, stats, nil
}

// directHoldingPosition denominates a non-receipt-token row in itself: the
// invariant "underlying_token_id IS NULL iff underlying_value IS NULL" forces
// a self-reference here, not NULL.
func directHoldingPosition(c candidateRow) positionSource {
	return positionSource{toEntity(c, c.tokenAddress, c.tokenDecimals, c.balance), "direct"}
}

// aTokenPosition resolves a 1:1 aToken holding: the raw underlying amount
// equals the raw balance, only the denominating asset differs. ok is false
// when the registry has no resolved underlying for this receipt_token row,
// which classifyCandidates counts as skippedNoUnderlying.
func aTokenPosition(c candidateRow) (positionSource, bool) {
	if c.underlyingAddress == nil {
		return positionSource{}, false
	}
	return positionSource{toEntity(c, *c.underlyingAddress, c.tokenDecimals, c.balance), "aToken"}, true
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
		"erc4626_converted_via_archive", stats.convertedERC4626Archive,
		"erc4626_converted_via_price_ratio", stats.convertedERC4626PriceRatio,
		"skipped_receipt_token_without_registry_underlying", stats.skippedNoUnderlying,
		"skipped_not_plain_erc20", stats.skippedNotPlainERC20,
		"skipped_erc4626_no_price_history", stats.skippedNoPriceHistory,
		"skipped_erc4626_price_too_stale", stats.skippedPriceTooStale,
		"skipped_erc4626_ratio_not_exact", stats.skippedRatioNotExact,
	)
}
