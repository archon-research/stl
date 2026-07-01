package maple

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func validFTLLoanStateParams() FTLLoanStateParams {
	maturity := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	nextDue := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	return FTLLoanStateParams{
		LoanID:              1,
		SyncedAt:            time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC),
		State:               "Active",
		StateDetail:         "ActiveInArrears",
		PrincipalOwed:       big.NewInt(10000000),
		InterestRate:        big.NewInt(182000),
		InterestPaid:        big.NewInt(5000),
		PaymentsRemaining:   6,
		PaymentIntervalDays: 30,
		TermDays:            180,
		MaturityDate:        &maturity,
		NextPaymentDue:      &nextDue,
		CollateralAmount:    big.NewInt(21510),
		CollateralRequired:  big.NewInt(20000),
		CollateralRatio:     big.NewInt(1500000),
		DrawdownAmount:      big.NewInt(16917002739727),
		ClaimableAmount:     big.NewInt(0),
		AcmRatio:            big.NewInt(1445731),
		IsImpaired:          false,
	}
}

func TestFTLLoanState_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(p *FTLLoanStateParams)
		wantErr string
	}{
		{name: "valid state"},
		{name: "zero amounts ok (pre-funding)", mutate: func(p *FTLLoanStateParams) {
			p.PrincipalOwed = big.NewInt(0)
			p.InterestRate = big.NewInt(0)
			p.InterestPaid = big.NewInt(0)
			p.CollateralAmount = big.NewInt(0)
			p.CollateralRequired = big.NewInt(0)
			p.CollateralRatio = big.NewInt(0)
			p.DrawdownAmount = big.NewInt(0)
			p.ClaimableAmount = big.NewInt(0)
		}},
		{name: "nil acm ratio ok", mutate: func(p *FTLLoanStateParams) { p.AcmRatio = nil }},
		{name: "empty state detail ok", mutate: func(p *FTLLoanStateParams) { p.StateDetail = "" }},
		{name: "nil dates ok", mutate: func(p *FTLLoanStateParams) { p.MaturityDate = nil; p.NextPaymentDue = nil }},
		{name: "zero counts ok", mutate: func(p *FTLLoanStateParams) {
			p.PaymentsRemaining = 0
			p.PaymentIntervalDays = 0
			p.TermDays = 0
		}},
		{
			name:    "zero loan id",
			mutate:  func(p *FTLLoanStateParams) { p.LoanID = 0 },
			wantErr: "mapleFtlLoanID must be positive",
		},
		{
			name:    "zero synced at",
			mutate:  func(p *FTLLoanStateParams) { p.SyncedAt = time.Time{} },
			wantErr: "syncedAt must not be zero",
		},
		{
			name:    "empty state",
			mutate:  func(p *FTLLoanStateParams) { p.State = "" },
			wantErr: "state must not be empty",
		},
		{
			name:    "nil principal owed",
			mutate:  func(p *FTLLoanStateParams) { p.PrincipalOwed = nil },
			wantErr: "principalOwed must not be nil",
		},
		{
			name:    "negative interest rate",
			mutate:  func(p *FTLLoanStateParams) { p.InterestRate = big.NewInt(-1) },
			wantErr: "interestRate must be non-negative",
		},
		{
			name:    "nil collateral ratio",
			mutate:  func(p *FTLLoanStateParams) { p.CollateralRatio = nil },
			wantErr: "collateralRatio must not be nil",
		},
		{
			name:    "negative drawdown amount",
			mutate:  func(p *FTLLoanStateParams) { p.DrawdownAmount = big.NewInt(-1) },
			wantErr: "drawdownAmount must be non-negative",
		},
		{
			name:    "negative acm ratio",
			mutate:  func(p *FTLLoanStateParams) { p.AcmRatio = big.NewInt(-1) },
			wantErr: "acmRatio must be non-negative",
		},
		{
			name:    "negative payments remaining",
			mutate:  func(p *FTLLoanStateParams) { p.PaymentsRemaining = -1 },
			wantErr: "paymentsRemaining must be non-negative",
		},
		{
			name:    "negative term days",
			mutate:  func(p *FTLLoanStateParams) { p.TermDays = -1 },
			wantErr: "termDays must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := validFTLLoanStateParams()
			if tt.mutate != nil {
				tt.mutate(&p)
			}
			s := &FTLLoanState{
				LoanID:              p.LoanID,
				SyncedAt:            p.SyncedAt,
				State:               p.State,
				StateDetail:         p.StateDetail,
				PrincipalOwed:       p.PrincipalOwed,
				InterestRate:        p.InterestRate,
				InterestPaid:        p.InterestPaid,
				PaymentsRemaining:   p.PaymentsRemaining,
				PaymentIntervalDays: p.PaymentIntervalDays,
				TermDays:            p.TermDays,
				MaturityDate:        p.MaturityDate,
				NextPaymentDue:      p.NextPaymentDue,
				CollateralAmount:    p.CollateralAmount,
				CollateralRequired:  p.CollateralRequired,
				CollateralRatio:     p.CollateralRatio,
				DrawdownAmount:      p.DrawdownAmount,
				ClaimableAmount:     p.ClaimableAmount,
				AcmRatio:            p.AcmRatio,
				IsImpaired:          p.IsImpaired,
			}
			err := s.Validate()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewFTLLoanState_NormalizesSyncedAtAndDates(t *testing.T) {
	p := validFTLLoanStateParams()
	// Sub-second, non-UTC synced_at must normalize to UTC second precision.
	p.SyncedAt = time.Date(2026, 6, 10, 12, 0, 0, 999999999, time.FixedZone("UTC+2", 2*3600))
	// A zoned maturity must normalize to UTC but keep its instant.
	maturity := time.Date(2026, 12, 1, 8, 0, 0, 0, time.FixedZone("UTC+2", 2*3600))
	p.MaturityDate = &maturity

	s, err := NewFTLLoanState(p)
	if err != nil {
		t.Fatalf("NewFTLLoanState: %v", err)
	}

	wantSyncedAt := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	if !s.SyncedAt.Equal(wantSyncedAt) {
		t.Errorf("syncedAt = %v, want %v", s.SyncedAt, wantSyncedAt)
	}
	if s.SyncedAt.Location() != time.UTC {
		t.Errorf("syncedAt location = %v, want UTC", s.SyncedAt.Location())
	}
	if s.MaturityDate.Location() != time.UTC {
		t.Errorf("maturityDate location = %v, want UTC", s.MaturityDate.Location())
	}
	if !s.MaturityDate.Equal(maturity) {
		t.Errorf("maturityDate instant changed: %v vs %v", s.MaturityDate, maturity)
	}
}

func TestNewFTLLoanState_NilDatesStayNil(t *testing.T) {
	p := validFTLLoanStateParams()
	p.MaturityDate = nil
	p.NextPaymentDue = nil

	s, err := NewFTLLoanState(p)
	if err != nil {
		t.Fatalf("NewFTLLoanState: %v", err)
	}
	if s.MaturityDate != nil || s.NextPaymentDue != nil {
		t.Errorf("nil dates must stay nil, got %v / %v", s.MaturityDate, s.NextPaymentDue)
	}
}

func TestNewFTLLoanState_PropagatesValidationError(t *testing.T) {
	p := validFTLLoanStateParams()
	p.LoanID = 0
	if _, err := NewFTLLoanState(p); err == nil {
		t.Fatal("expected error, got nil")
	} else if !strings.Contains(err.Error(), "NewFTLLoanState") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
