package maple

import (
	"fmt"
	"math/big"
	"time"
)

// FTLLoanState is a snapshot of a live fixed-term loan at a sync cycle.
// Amounts are raw integers: PrincipalOwed / InterestPaid / DrawdownAmount /
// ClaimableAmount in funds-asset decimals, CollateralAmount / CollateralRequired
// in collateral-asset decimals. InterestRate and CollateralRatio carry 6
// decimals, AcmRatio 6 decimals (nil when the API reports none). Pre-funding
// states report 0 amounts, which is a valid value, not absence.
//
// MaturityDate and NextPaymentDue are *time.Time so the API epoch-second
// sentinel 0 (pre-funding / none due) is stored as SQL NULL rather than
// 1970-01-01. Refinance-mutable terms (TermDays, InterestRate,
// PaymentsRemaining) live here, not in the registry, so a refinance is a new
// snapshot rather than a registry-immutability failure.
type FTLLoanState struct {
	LoanID              int64
	SyncedAt            time.Time
	State               string
	StateDetail         string // LoanStateDetail enum; "" when the API reports none
	PrincipalOwed       *big.Int
	InterestRate        *big.Int
	InterestPaid        *big.Int
	PaymentsRemaining   int64
	PaymentIntervalDays int64
	TermDays            int64
	MaturityDate        *time.Time // nil when the API reports epoch 0 (pre-funding)
	NextPaymentDue      *time.Time // nil when the API reports epoch 0 (none due)
	CollateralAmount    *big.Int
	CollateralRequired  *big.Int
	CollateralRatio     *big.Int
	DrawdownAmount      *big.Int
	ClaimableAmount     *big.Int
	AcmRatio            *big.Int // nil when the API reports none
	IsImpaired          bool
}

type FTLLoanStateParams struct {
	LoanID              int64
	SyncedAt            time.Time
	State               string
	StateDetail         string
	PrincipalOwed       *big.Int
	InterestRate        *big.Int
	InterestPaid        *big.Int
	PaymentsRemaining   int64
	PaymentIntervalDays int64
	TermDays            int64
	MaturityDate        *time.Time
	NextPaymentDue      *time.Time
	CollateralAmount    *big.Int
	CollateralRequired  *big.Int
	CollateralRatio     *big.Int
	DrawdownAmount      *big.Int
	ClaimableAmount     *big.Int
	AcmRatio            *big.Int
	IsImpaired          bool
}

// NewFTLLoanState creates a new FTLLoanState entity with validation.
func NewFTLLoanState(p FTLLoanStateParams) (*FTLLoanState, error) {
	s := &FTLLoanState{
		LoanID:              p.LoanID,
		SyncedAt:            NormalizeSyncedAt(p.SyncedAt),
		State:               p.State,
		StateDetail:         p.StateDetail,
		PrincipalOwed:       p.PrincipalOwed,
		InterestRate:        p.InterestRate,
		InterestPaid:        p.InterestPaid,
		PaymentsRemaining:   p.PaymentsRemaining,
		PaymentIntervalDays: p.PaymentIntervalDays,
		TermDays:            p.TermDays,
		MaturityDate:        normalizeOptionalTime(p.MaturityDate),
		NextPaymentDue:      normalizeOptionalTime(p.NextPaymentDue),
		CollateralAmount:    p.CollateralAmount,
		CollateralRequired:  p.CollateralRequired,
		CollateralRatio:     p.CollateralRatio,
		DrawdownAmount:      p.DrawdownAmount,
		ClaimableAmount:     p.ClaimableAmount,
		AcmRatio:            p.AcmRatio,
		IsImpaired:          p.IsImpaired,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewFTLLoanState: %w", err)
	}
	return s, nil
}

// normalizeOptionalTime returns nil unchanged and otherwise normalizes the
// timestamp to UTC, matching the snapshot timestamp convention.
func normalizeOptionalTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

// Validate checks that all fields have valid values.
func (s *FTLLoanState) Validate() error {
	if s.LoanID <= 0 {
		return fmt.Errorf("mapleFtlLoanID must be positive, got %d", s.LoanID)
	}
	if s.SyncedAt.IsZero() {
		return fmt.Errorf("syncedAt must not be zero")
	}
	if s.State == "" {
		return fmt.Errorf("state must not be empty")
	}
	for _, f := range []struct {
		name string
		v    *big.Int
	}{
		{"principalOwed", s.PrincipalOwed},
		{"interestRate", s.InterestRate},
		{"interestPaid", s.InterestPaid},
		{"collateralAmount", s.CollateralAmount},
		{"collateralRequired", s.CollateralRequired},
		{"collateralRatio", s.CollateralRatio},
		{"drawdownAmount", s.DrawdownAmount},
		{"claimableAmount", s.ClaimableAmount},
	} {
		if err := requireNonNegBigInt(f.name, f.v); err != nil {
			return err
		}
	}
	if err := requireNonNegBigIntIfSet("acmRatio", s.AcmRatio); err != nil {
		return err
	}
	for _, f := range []struct {
		name string
		v    int64
	}{
		{"paymentsRemaining", s.PaymentsRemaining},
		{"paymentIntervalDays", s.PaymentIntervalDays},
		{"termDays", s.TermDays},
	} {
		if err := requireNonNegInt64(f.name, f.v); err != nil {
			return err
		}
	}
	return nil
}
