package reference_capital_indexer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var (
	errProvider  = errors.New("provider boom")
	errRepo      = errors.New("repo boom")
	syncedAt     = time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	trackedStars = []string{"grove", "spark"}
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

type mockCapitalRepo struct {
	snapshots []entity.PrimeCapitalStackSnapshot
	err       error
}

func (m *mockCapitalRepo) SavePrimeCapitalSnapshots(_ context.Context, snapshots []entity.PrimeCapitalStackSnapshot) error {
	if m.err != nil {
		return m.err
	}
	m.snapshots = append(m.snapshots, snapshots...)
	return nil
}

type mockRiskProvider struct {
	rows      []outbound.RiskCapitalPrimeSnapshot
	err       error
	requested []string
}

func (m *mockRiskProvider) FetchPrimeSnapshots(
	_ context.Context,
	stars []string,
) ([]outbound.RiskCapitalPrimeSnapshot, error) {
	m.requested = stars
	return m.rows, m.err
}

func upstreamRow(star string) outbound.RiskCapitalPrimeSnapshot {
	ratio := "0.3705"
	return outbound.RiskCapitalPrimeSnapshot{
		Star:                       star,
		Exposure:                   "2098090654.81",
		RequiredRiskCapital:        "17837860.43",
		TotalRiskCapital:           "48142491.08",
		JuniorRiskCapital:          "48142491.08",
		SeniorRiskCapital:          "0",
		InternalJuniorRiskCapital:  "48142491.08",
		ExternalJuniorRiskCapital:  "0",
		TokenizedJuniorRiskCapital: "0",
		InternalSeniorRiskCapital:  "0",
		ExternalSeniorRiskCapital:  "0",
		EncumbranceRatio:           &ratio,
		ExposureShare:              "0.0084",
		EPIUtilization:             "0",
		SPJUtilization:             "0",
	}
}

func newService(primes *mockPrimeRepo, capital *mockCapitalRepo, provider *mockRiskProvider) *Service {
	return NewService(primes, capital, provider, trackedStars, 7, func() time.Time { return syncedAt }, nil, nil)
}

func TestRunPersistsASnapshotPerUpstreamPrime(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark"), upstreamRow("grove")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if got := len(capital.snapshots); got != 2 {
		t.Fatalf("saved %d snapshots, want 2", got)
	}
	if got := capital.snapshots[0].PrimeID; got != 1 {
		t.Errorf("PrimeID = %d, want 1", got)
	}
	if got := capital.snapshots[1].PrimeID; got != 2 {
		t.Errorf("PrimeID = %d, want 2", got)
	}
}

func TestRunStampsEveryRowOfACycleWithOneSyncedAtAndBuildID(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark"), upstreamRow("grove")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	for i, s := range capital.snapshots {
		if !s.SyncedAt.Equal(syncedAt) {
			t.Errorf("snapshot %d SyncedAt = %v, want %v", i, s.SyncedAt, syncedAt)
		}
		if s.BuildID != 7 {
			t.Errorf("snapshot %d BuildID = %d, want 7", i, s.BuildID)
		}
	}
}

func TestRunCarriesEveryUpstreamFigureOntoTheSnapshot(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	got := capital.snapshots[0]
	for _, tc := range []struct{ field, got, want string }{
		{"ExposureUSD", got.ExposureUSD, "2098090654.81"},
		{"RequiredRiskCapitalUSD", got.RequiredRiskCapitalUSD, "17837860.43"},
		{"TotalRiskCapitalUSD", got.TotalRiskCapitalUSD, "48142491.08"},
		{"JuniorRiskCapitalUSD", got.JuniorRiskCapitalUSD, "48142491.08"},
		{"SeniorRiskCapitalUSD", got.SeniorRiskCapitalUSD, "0"},
		{"ExposureShare", got.ExposureShare, "0.0084"},
		{"Source", got.Source, entity.ReferenceDataSource},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.field, tc.got, tc.want)
		}
	}
	if got.EncumbranceRatio == nil || *got.EncumbranceRatio != "0.3705" {
		t.Errorf("EncumbranceRatio = %v, want 0.3705", got.EncumbranceRatio)
	}
}

func TestRunKeepsAnAbsentEncumbranceRatioNullRatherThanZero(t *testing.T) {
	row := upstreamRow("spark")
	row.EncumbranceRatio = nil
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{row}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if capital.snapshots[0].EncumbranceRatio != nil {
		t.Errorf("EncumbranceRatio = %v, want nil", *capital.snapshots[0].EncumbranceRatio)
	}
}

func TestRunMatchesPrimeNamesCaseInsensitively(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "Spark"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if len(capital.snapshots) != 1 {
		t.Fatalf("saved %d snapshots, want 1", len(capital.snapshots))
	}
}

// A star the registry does not know must fail the cycle rather than be skipped:
// a skipped prime leaves a hole indistinguishable from one the monitor stopped
// covering.
func TestRunFailsOnAPrimeTheRegistryDoesNotKnow(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark"), upstreamRow("newstar")}}

	err := newService(primes, capital, provider).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error naming the unknown prime")
	}
	if len(capital.snapshots) != 0 {
		t.Errorf("saved %d snapshots, want 0 — a failed cycle must persist nothing", len(capital.snapshots))
	}
}

// The monitor reports primes STL does not track (obex, osero today). Asking
// only for the tracked ones keeps their snapshots out of the table entirely,
// rather than relying on the prime table to exclude them — it still carries a
// row for a prime STL has stopped tracking.
func TestRunAsksTheMonitorOnlyForTheTrackedPrimes(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark"), upstreamRow("grove")}}

	if err := newService(primes, &mockCapitalRepo{}, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if len(provider.requested) != 2 || provider.requested[0] != "grove" || provider.requested[1] != "spark" {
		t.Errorf("requested = %v, want [grove spark]", provider.requested)
	}
}

func TestRunRecordsTheTrackedPrimesTheMonitorDoesCover(t *testing.T) {
	// A tracked prime the monitor omits is a coverage fact, not a failure, so
	// the rest of the cycle is still recorded.
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if len(capital.snapshots) != 1 || capital.snapshots[0].PrimeID != 1 {
		t.Errorf("snapshots = %+v, want one for spark", capital.snapshots)
	}
}

func TestRunFailsWhenNoPrimesAreTracked(t *testing.T) {
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	service := NewService(
		&mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}},
		&mockCapitalRepo{},
		provider,
		nil,
		7,
		func() time.Time { return syncedAt },
		nil,
		nil,
	)

	if err := service.Run(context.Background()); err == nil {
		t.Fatal("Run() = nil, want an error")
	}
}

func TestRunFailsWhenTheMonitorReportsNoPrimes(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	capital := &mockCapitalRepo{}

	err := newService(primes, capital, &mockRiskProvider{}).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error — an empty feed is a broken feed, not a no-op")
	}
}

func TestRunFailsWhenNoPrimesExistToAttributeSnapshotsTo(t *testing.T) {
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newService(&mockPrimeRepo{}, &mockCapitalRepo{}, provider).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error")
	}
}

func TestRunPropagatesAProviderFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}

	err := newService(primes, &mockCapitalRepo{}, &mockRiskProvider{err: errProvider}).Run(context.Background())

	if !errors.Is(err, errProvider) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errProvider)
	}
}

func TestRunPropagatesAPersistenceFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newService(primes, &mockCapitalRepo{err: errRepo}, provider).Run(context.Background())

	if !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errRepo)
	}
}

func TestRunPropagatesAPrimeListingFailure(t *testing.T) {
	primes := &mockPrimeRepo{err: errRepo}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newService(primes, &mockCapitalRepo{}, provider).Run(context.Background())

	if !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errRepo)
	}
}
