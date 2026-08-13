package orderbook

import (
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// parseLevel validates an exchange [price, size] pair and returns the original
// decimal strings unchanged. Both must be canonical fixed-point decimals
// (entity.IsCanonicalDecimal): signs, exponents and NaN/Inf are rejected so one
// price cannot key the book map under two spellings, and a violation fails the
// frame to force a resync. Price must be positive; size zero means "level
// removed".
func parseLevel(price, size string) (entity.PriceLevel, error) {
	if !entity.IsCanonicalDecimal(price) {
		return entity.PriceLevel{}, fmt.Errorf("invalid price %q: not a canonical decimal", price)
	}
	if entity.IsZeroDecimal(price) {
		return entity.PriceLevel{}, fmt.Errorf("invalid price %q: must be positive", price)
	}
	if !entity.IsCanonicalDecimal(size) {
		return entity.PriceLevel{}, fmt.Errorf("invalid size %q: not a canonical decimal", size)
	}
	return entity.PriceLevel{Price: price, Size: size}, nil
}

// applyDeltaLevels applies a delta's [price, size] pairs to one side of book in
// place (a zero size removes a level), validating each pair via parseLevel and
// failing the frame on the first malformed level. This is the hot path for WS
// deltas.
func applyDeltaLevels(book *entity.Orderbook, side entity.Side, raw [][]string) error {
	for i, pair := range raw {
		if len(pair) < 2 {
			return fmt.Errorf("level %d: expected [price, size], got %d fields", i, len(pair))
		}
		lvl, err := parseLevel(pair[0], pair[1])
		if err != nil {
			return fmt.Errorf("level %d: %w", i, err)
		}
		book.ApplyLevel(side, lvl.Price, lvl.Size)
	}
	return nil
}
