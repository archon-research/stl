package main

import (
	"fmt"
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
	skippedNoPriceHistory      int
	skippedPriceTooStale       int
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
func classifyCandidates(candidates []candidateRow, archiveResults map[int]*big.Int, cfg cliConfig) ([]positionSource, classificationStats, error) {
	out := make([]positionSource, 0, len(candidates))
	var stats classificationStats

	for i, c := range candidates {
		switch {
		case !c.isReceiptToken:
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
// when the registry has no resolved underlying for this receipt_token row --
// an Aave-family protocol name is not proof that one exists, and
// dereferencing without this check panics on a real run.
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
)

const (
	sourceERC4626Archive    = "erc4626_archive"
	sourceERC4626PriceRatio = "erc4626_price_ratio"
)

// classifyERC4626 resolves one erc4626-like row's underlying value: a real
// archive conversion when the resolver found one for this row (archiveRaw),
// the price-ratio derivation otherwise. The ratio genuinely moves over time,
// so borrowing it from a neighbour block would bake an estimate into the
// table as though it were an observation -- a row with neither source is
// skipped, not approximated.
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
	underlyingRaw, err := humanToRaw(*c.erc4626UnderlyingHuman, *c.underlyingDecimals)
	if err != nil {
		return positionSource{}, skipNone, fmt.Errorf("erc4626 underlying for block %d: %w", c.blockNumber, err)
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
		"skipped_erc4626_no_price_history", stats.skippedNoPriceHistory,
		"skipped_erc4626_price_too_stale", stats.skippedPriceTooStale,
	)
}
