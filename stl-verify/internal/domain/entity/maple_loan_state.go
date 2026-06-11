package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MapleLoanState is a snapshot of an active Open Term Loan at a sync cycle.
// PrincipalOwed is a raw integer in pool-asset decimals (6 for USDC/USDT);
// AcmRatio has 6 decimals (1445731 = 144.57%) and is nil when the API
// reports none (observed on active uncollateralized loans).
type MapleLoanState struct {
	MapleLoanID   int64
	SyncedAt      time.Time
	State         string // 'Active' (only Active is queried for MVP)
	PrincipalOwed *big.Int
	AcmRatio      *big.Int // nil when absent upstream
}

// NewMapleLoanState creates a new MapleLoanState entity with validation.
func NewMapleLoanState(mapleLoanID int64, syncedAt time.Time, state string, principalOwed, acmRatio *big.Int) (*MapleLoanState, error) {
	s := &MapleLoanState{
		MapleLoanID:   mapleLoanID,
		SyncedAt:      normalizeSyncedAt(syncedAt),
		State:         state,
		PrincipalOwed: principalOwed,
		AcmRatio:      acmRatio,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewMapleLoanState: %w", err)
	}
	return s, nil
}

// Validate checks that all fields have valid values.
func (s *MapleLoanState) Validate() error {
	if s.MapleLoanID <= 0 {
		return fmt.Errorf("mapleLoanID must be positive, got %d", s.MapleLoanID)
	}
	if s.SyncedAt.IsZero() {
		return fmt.Errorf("syncedAt must not be zero")
	}
	if s.State == "" {
		return fmt.Errorf("state must not be empty")
	}
	if err := requireNonNegBigInt("principalOwed", s.PrincipalOwed); err != nil {
		return err
	}
	if err := requireNonNegBigIntIfSet("acmRatio", s.AcmRatio); err != nil {
		return err
	}
	return nil
}
