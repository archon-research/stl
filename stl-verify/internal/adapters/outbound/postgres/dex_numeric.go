package postgres

import (
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5/pgtype"
)

// Shared big.Int → NUMERIC conversion helpers for the per-DEX pool
// repositories (curve / uniswap_v3 / balancer). They live here, in the base
// shared PR, rather than in any single per-DEX repository file so that the
// per-DEX PRs do not depend on one another.

// BigIntToNullableNumeric returns a pgtype.Numeric for a possibly-nil raw
// integer big.Int (Exp = 0; storing the integer value as-is). A nil input
// serialises as SQL NULL.
func BigIntToNullableNumeric(b *big.Int) pgtype.Numeric {
	if b == nil {
		return pgtype.Numeric{}
	}
	return pgtype.Numeric{Int: new(big.Int).Set(b), Exp: 0, Valid: true}
}

// BigIntToNumericRequired returns a non-nullable pgtype.Numeric, erroring with
// the column name on nil — the caller's column has NOT NULL on it.
func BigIntToNumericRequired(b *big.Int, column string) (pgtype.Numeric, error) {
	if b == nil {
		return pgtype.Numeric{}, fmt.Errorf("%s must not be nil", column)
	}
	return pgtype.Numeric{Int: new(big.Int).Set(b), Exp: 0, Valid: true}, nil
}

// BigIntsToNumericArray converts a slice of *big.Int to a slice of
// pgtype.Numeric for NUMERIC[] columns where the column is NOT NULL. Nil
// entries in the slice are an error.
func BigIntsToNumericArray(bs []*big.Int) ([]pgtype.Numeric, error) {
	out := make([]pgtype.Numeric, len(bs))
	for i, b := range bs {
		if b == nil {
			return nil, fmt.Errorf("element %d must not be nil", i)
		}
		out[i] = pgtype.Numeric{Int: new(big.Int).Set(b), Exp: 0, Valid: true}
	}
	return out, nil
}

// BigIntsToNullableNumericArray converts a slice to a NUMERIC[] payload,
// returning nil so pgx serialises a SQL NULL when the input slice is nil.
// An empty (non-nil) slice yields an empty array, not NULL.
func BigIntsToNullableNumericArray(bs []*big.Int) (any, error) {
	if bs == nil {
		return nil, nil
	}
	return BigIntsToNumericArray(bs)
}

// BigIntsToNullableElementArrayOrNull converts a slice to a NUMERIC[] payload
// where a nil slice → SQL NULL column and nil elements → SQL NULL elements.
// Used by the NG oracle columns (price_oracle / last_price) where individual
// slots are legitimately absent: the EMA is uninitialised before the pool's
// first swap, or the indexed selector reverts on factory-v2-era pools.
func BigIntsToNullableElementArrayOrNull(bs []*big.Int) any {
	if bs == nil {
		return nil
	}
	return BigIntsToNullableElementNumericArray(bs)
}

// BigIntsToNullableElementNumericArray converts a slice of *big.Int to a
// NUMERIC[] payload where individual nil entries become SQL NULL elements
// (rather than erroring). Used by columns that are NOT NULL on the array
// itself but allow NULL per-element semantics — e.g. balancer_pool_state
// balances where phantom BPT slots have no real reserve.
func BigIntsToNullableElementNumericArray(bs []*big.Int) []pgtype.Numeric {
	out := make([]pgtype.Numeric, len(bs))
	for i, b := range bs {
		if b == nil {
			out[i] = pgtype.Numeric{Valid: false}
			continue
		}
		out[i] = pgtype.Numeric{Int: new(big.Int).Set(b), Exp: 0, Valid: true}
	}
	return out
}
