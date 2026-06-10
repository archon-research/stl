package entity

import (
	"fmt"
	"math/big"
	"time"
)

// MapleLoanState is a snapshot of an active Open Term Loan at a sync cycle.
// PrincipalOwed is a raw integer in pool-asset decimals (6 for USDC/USDT);
// AcmRatio has 6 decimals (1445731 = 144.57%).
type MapleLoanState struct {
	MapleLoanID   int64
	SyncedAt      time.Time
	State         string // 'Active' (only Active is queried for MVP)
	PrincipalOwed *big.Int
	AcmRatio      *big.Int
}

// NewMapleLoanState creates a new MapleLoanState entity with validation.
func NewMapleLoanState(mapleLoanID int64, syncedAt time.Time, state string, principalOwed, acmRatio *big.Int) (*MapleLoanState, error) {
	s := &MapleLoanState{
		MapleLoanID:   mapleLoanID,
		SyncedAt:      syncedAt,
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
	if s.PrincipalOwed == nil {
		return fmt.Errorf("principalOwed must not be nil")
	}
	if s.PrincipalOwed.Sign() < 0 {
		return fmt.Errorf("principalOwed must be non-negative, got %s", s.PrincipalOwed)
	}
	if s.AcmRatio == nil {
		return fmt.Errorf("acmRatio must not be nil")
	}
	if s.AcmRatio.Sign() < 0 {
		return fmt.Errorf("acmRatio must be non-negative, got %s", s.AcmRatio)
	}
	return nil
}
