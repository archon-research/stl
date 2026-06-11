package entity

import (
	"fmt"
	"math/big"
	"time"
)

// normalizeSyncedAt enforces the snapshot timestamp convention: UTC,
// second precision. Exact synced_at equality is load-bearing — it is part of
// every maple_* state primary key and the dedup key in the
// processing-version triggers — so any construction site passing a non-UTC
// or sub-second time must not silently create near-duplicate snapshot rows.
func normalizeSyncedAt(t time.Time) time.Time {
	return t.UTC().Truncate(time.Second)
}

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
