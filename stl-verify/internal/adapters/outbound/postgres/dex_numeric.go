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

// NumericToNullableBigInt converts a scanned NUMERIC into a *big.Int, returning
// nil when the value is SQL NULL. A positive Exp scales the mantissa up; a
// negative Exp is only valid here when the value is a whole number (the curve
// columns that use this store integers), in which case it scales down exactly.
func NumericToNullableBigInt(n pgtype.Numeric) (*big.Int, error) {
	if !n.Valid {
		return nil, nil
	}
	if n.Int == nil {
		return nil, fmt.Errorf("numeric is valid but has nil mantissa")
	}
	v := new(big.Int).Set(n.Int)
	switch {
	case n.Exp == 0:
		return v, nil
	case n.Exp > 0:
		scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil)
		return v.Mul(v, scale), nil
	default:
		scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-n.Exp)), nil)
		quo, rem := new(big.Int).QuoRem(v, scale, new(big.Int))
		if rem.Sign() != 0 {
			return nil, fmt.Errorf("numeric with exp %d is not a whole number", n.Exp)
		}
		return quo, nil
	}
}

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

// BigIntsToNullableNumericArray converts a slice to a NUMERIC[] payload.
// A nil input slice yields a nil FlatArray (SQL NULL); a non-nil slice
// (including empty) yields a valid array. pgx encodes a nil FlatArray as
// SQL NULL because its Dimensions() returns nil.
func BigIntsToNullableNumericArray(bs []*big.Int) (pgtype.FlatArray[pgtype.Numeric], error) {
	if bs == nil {
		return nil, nil
	}
	elems, err := BigIntsToNumericArray(bs)
	if err != nil {
		return nil, err
	}
	return pgtype.FlatArray[pgtype.Numeric](elems), nil
}
