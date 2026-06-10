package entity

import (
	"fmt"
	"math/big"
)

// requireNonNegBigInt validates that a required big.Int field is present and
// non-negative.
func requireNonNegBigInt(name string, v *big.Int) error {
	if v == nil {
		return fmt.Errorf("%s must not be nil", name)
	}
	if v.Sign() < 0 {
		return fmt.Errorf("%s must be non-negative, got %s", name, v)
	}
	return nil
}

// requireNonNegBigIntIfSet validates that an optional big.Int field is
// non-negative when present.
func requireNonNegBigIntIfSet(name string, v *big.Int) error {
	if v != nil && v.Sign() < 0 {
		return fmt.Errorf("%s must be non-negative, got %s", name, v)
	}
	return nil
}
