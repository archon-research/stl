package reference_capital_backfill

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var (
	errProvider  = errors.New("provider boom")
	errRepo      = errors.New("repo boom")
	trackedStars = []string{"spark"}
)

type mockPrimeRepo struct {
	primes []entity.Prime
	err    error
}

func (m *mockPrimeRepo) GetPrimeIDByName(_ context.Context, name string) (int64, error) {
	for _, p := range m.primes {
		if p.Name == name {
			return p.ID, nil
		}
	}
	return 0, nil
}

func (m *mockPrimeRepo) ListPrimes(_ context.Context) ([]entity.Prime, error) {
	return m.primes, m.err
}

type mockSheetRepo struct {
	snapshots []entity.PrimeBalanceSheetSnapshot
	err       error
}

func (m *mockSheetRepo) SaveBalanceSheetSnapshots(
	_ context.Context, s []entity.PrimeBalanceSheetSnapshot,
) (inserted, newDays int, err error) {
	if m.err != nil {
		return 0, 0, m.err
	}
	m.snapshots = append(m.snapshots, s...)
	return len(s), len(s), nil
}

type mockProvider struct {
	days           []outbound.BalanceSheetDay
	err            error
	requestedStars []string
	requestedDays  int
}

func (m *mockProvider) FetchHistory(_ context.Context, stars []string, daysAgo int) ([]outbound.BalanceSheetDay, error) {
	m.requestedStars = stars
	m.requestedDays = daysAgo
	return m.days, m.err
}

func day(star, date string) outbound.BalanceSheetDay {
	return outbound.BalanceSheetDay{
		Star:            star,
		Date:            date,
		TreasuryBalance: "48142491.085806286854722044",
		Assets:          "3224022323.40",
		AllocatedAssets: "2718840719.96",
		IdleAssets:      "505181603.43",
		Debt:            "2642147590.40",
		BackstopCapital: "25000000",
	}
}

func newService(primes *mockPrimeRepo, sheets *mockSheetRepo, provider *mockProvider) *Service {
	return NewService(primes, sheets, provider, trackedStars, 365, 7, nil)
}

func TestRunStampsEachDayAtItsMidnightUTC(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	sheets := &mockSheetRepo{}
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "2025-08-19")}}

	if err := newService(primes, sheets, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	want := time.Date(2025, 8, 19, 0, 0, 0, 0, time.UTC)
	if got := sheets.snapshots[0].ObservedAt; !got.Equal(want) {
		t.Errorf("ObservedAt = %v, want %v", got, want)
	}
}

func TestRunCarriesTreasuryBalanceAtFullPrecision(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	sheets := &mockSheetRepo{}
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "2025-08-19")}}

	if err := newService(primes, sheets, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if got := sheets.snapshots[0].TreasuryBalanceUSD; got != "48142491.085806286854722044" {
		t.Errorf("TreasuryBalanceUSD = %q, want the 18-decimal value unrounded", got)
	}
}

// Both feeds are one provenance downstream, so they must record the same slug.
func TestRunRecordsTheSharedReferenceProvenance(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	sheets := &mockSheetRepo{}
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "2025-08-19")}}

	if err := newService(primes, sheets, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if got := sheets.snapshots[0].Source; got != entity.ReferenceDataSource {
		t.Errorf("Source = %q, want %q", got, entity.ReferenceDataSource)
	}
}

func TestRunRequestsOnlyTheTrackedPrimes(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	provider := &mockProvider{days: []outbound.BalanceSheetDay{
		day("spark", "2025-08-19"), day("grove", "2025-08-19"),
	}}
	service := NewService(primes, &mockSheetRepo{}, provider, []string{"grove", "spark"}, 365, 7, nil)

	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if len(provider.requestedStars) != 2 || provider.requestedDays != 365 {
		t.Errorf("requested %v over %d days, want [grove spark] over 365", provider.requestedStars, provider.requestedDays)
	}
}

func TestRunFailsOnAPrimeTheRegistryDoesNotKnow(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	sheets := &mockSheetRepo{}
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "2025-08-19"), day("newstar", "2025-08-19")}}

	err := newService(primes, sheets, provider).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error naming the unknown prime")
	}
	if len(sheets.snapshots) != 0 {
		t.Errorf("saved %d snapshots, want 0 — a failed run must persist nothing", len(sheets.snapshots))
	}
}

func TestRunFailsOnAnUnparseableDate(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "19-08-2025")}}

	if err := newService(primes, &mockSheetRepo{}, provider).Run(context.Background()); err == nil {
		t.Fatal("Run() = nil, want an error")
	}
}

func TestRunFailsWhenNoPrimesAreTracked(t *testing.T) {
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "2025-08-19")}}
	service := NewService(
		&mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}},
		&mockSheetRepo{}, provider, nil, 365, 7, nil,
	)

	if err := service.Run(context.Background()); err == nil {
		t.Fatal("Run() = nil, want an error")
	}
}

func TestRunPropagatesAProviderFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}

	err := newService(primes, &mockSheetRepo{}, &mockProvider{err: errProvider}).Run(context.Background())

	if !errors.Is(err, errProvider) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errProvider)
	}
}

func TestRunPropagatesAPersistenceFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "2025-08-19")}}

	err := newService(primes, &mockSheetRepo{err: errRepo}, provider).Run(context.Background())

	if !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errRepo)
	}
}

func TestRunPropagatesAPrimeListingFailure(t *testing.T) {
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "2025-08-19")}}

	err := newService(&mockPrimeRepo{err: errRepo}, &mockSheetRepo{}, provider).Run(context.Background())

	if !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errRepo)
	}
}

func TestRunRefusesToSeedWhenATrackedPrimeHasNoHistory(t *testing.T) {
	// One-shot, and the write is ON CONFLICT DO NOTHING: seeding spark alone
	// would leave grove's year permanently absent with nothing to signal it.
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	sheets := &mockSheetRepo{}
	provider := &mockProvider{days: []outbound.BalanceSheetDay{day("spark", "2025-08-19")}}
	service := NewService(primes, sheets, provider, []string{"grove", "spark"}, 365, 7, nil)

	err := service.Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error naming the uncovered prime")
	}
	if !strings.Contains(err.Error(), "grove") {
		t.Errorf("Run() = %v, want the error to name grove", err)
	}
	if len(sheets.snapshots) != 0 {
		t.Errorf("saved %d snapshots, want 0 — a partial backfill must persist nothing", len(sheets.snapshots))
	}
}
