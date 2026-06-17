package capital_metrics_fetcher

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type mockProvider struct {
	rows []outbound.RiskCapitalRow
	err  error
}

func (m *mockProvider) Name() string { return "mock-star" }

func (m *mockProvider) FetchRiskCapital(context.Context) ([]outbound.RiskCapitalRow, error) {
	return m.rows, m.err
}

type mockRepo struct {
	primes     []entity.Prime
	primesErr  error
	saveErr    error
	saved      []*entity.CapitalMetricsSnapshot
	saveCalled bool
}

func (m *mockRepo) GetPrimes(context.Context) ([]entity.Prime, error) {
	return m.primes, m.primesErr
}

func (m *mockRepo) SaveSnapshots(_ context.Context, snapshots []*entity.CapitalMetricsSnapshot) error {
	m.saveCalled = true
	if m.saveErr != nil {
		return m.saveErr
	}
	m.saved = append(m.saved, snapshots...)
	return nil
}

func newPrime(id int64, name string) entity.Prime {
	return entity.Prime{ID: id, Name: name, VaultAddress: common.Address{}}
}

func newService(t *testing.T, provider outbound.CapitalMetricsProvider, repo outbound.CapitalMetricsRepository) *Service {
	t.Helper()
	svc, err := NewService(ServiceConfig{BenchmarkSource: "https://example.com/star"}, provider, repo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func TestNewService_Validation(t *testing.T) {
	tests := []struct {
		name     string
		provider outbound.CapitalMetricsProvider
		repo     outbound.CapitalMetricsRepository
		source   string
		wantErr  bool
	}{
		{name: "valid", provider: &mockProvider{}, repo: &mockRepo{}, source: "src"},
		{name: "nil provider", provider: nil, repo: &mockRepo{}, source: "src", wantErr: true},
		{name: "nil repo", provider: &mockProvider{}, repo: nil, source: "src", wantErr: true},
		{name: "empty source", provider: &mockProvider{}, repo: &mockRepo{}, source: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(ServiceConfig{BenchmarkSource: tt.source}, tt.provider, tt.repo)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewService() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestService_FetchAndStore(t *testing.T) {
	tests := []struct {
		name        string
		primes      []entity.Prime
		primesErr   error
		rows        []outbound.RiskCapitalRow
		providerErr error
		saveErr     error
		wantErr     bool
		wantSaved   bool
		wantCount   int
	}{
		{
			name:      "no primes skips fetch and save",
			primes:    nil,
			wantSaved: false,
		},
		{
			name:      "primes error propagates",
			primesErr: errors.New("db down"),
			wantErr:   true,
		},
		{
			name:        "provider error propagates",
			primes:      []entity.Prime{newPrime(1, "spark")},
			providerErr: errors.New("upstream down"),
			wantErr:     true,
		},
		{
			name:   "matched prime is snapshotted",
			primes: []entity.Prime{newPrime(1, "Spark")},
			rows: []outbound.RiskCapitalRow{
				{Star: "spark", Exposure: "100", TotalRC: "200", FinancialRRC: "50", RiskToleranceRatio: "0.85"},
			},
			wantSaved: true,
			wantCount: 1,
		},
		{
			name:   "unmatched prime is skipped",
			primes: []entity.Prime{newPrime(1, "spark"), newPrime(2, "grove")},
			rows: []outbound.RiskCapitalRow{
				{Star: "spark", Exposure: "100", TotalRC: "200", FinancialRRC: "50"},
			},
			wantSaved: true,
			wantCount: 1,
		},
		{
			name:   "invalid decimal row is skipped",
			primes: []entity.Prime{newPrime(1, "spark"), newPrime(2, "grove")},
			rows: []outbound.RiskCapitalRow{
				{Star: "spark", Exposure: "not-a-number", TotalRC: "200", FinancialRRC: "50"},
				{Star: "grove", Exposure: "10", TotalRC: "20", FinancialRRC: "5"},
			},
			wantSaved: true,
			wantCount: 1,
		},
		{
			name:   "all skipped does not save",
			primes: []entity.Prime{newPrime(1, "spark")},
			rows: []outbound.RiskCapitalRow{
				{Star: "unknown", Exposure: "1", TotalRC: "2", FinancialRRC: "1"},
			},
			wantSaved: false,
		},
		{
			name:   "save error propagates",
			primes: []entity.Prime{newPrime(1, "spark")},
			rows: []outbound.RiskCapitalRow{
				{Star: "spark", Exposure: "100", TotalRC: "200", FinancialRRC: "50"},
			},
			saveErr: errors.New("write failed"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &mockProvider{rows: tt.rows, err: tt.providerErr}
			repo := &mockRepo{primes: tt.primes, primesErr: tt.primesErr, saveErr: tt.saveErr}
			svc := newService(t, provider, repo)

			err := svc.FetchAndStore(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("FetchAndStore() err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if repo.saveCalled != tt.wantSaved {
				t.Fatalf("saveCalled = %v, want %v", repo.saveCalled, tt.wantSaved)
			}
			if tt.wantSaved && len(repo.saved) != tt.wantCount {
				t.Fatalf("saved %d snapshots, want %d", len(repo.saved), tt.wantCount)
			}
		})
	}
}

func TestService_FetchAndStore_SnapshotFields(t *testing.T) {
	provider := &mockProvider{rows: []outbound.RiskCapitalRow{
		{Star: "  SPARK ", Exposure: " 100 ", TotalRC: "200", FinancialRRC: "50", RiskToleranceRatio: " "},
	}}
	repo := &mockRepo{primes: []entity.Prime{newPrime(7, "spark")}}
	svc := newService(t, provider, repo)

	if err := svc.FetchAndStore(context.Background()); err != nil {
		t.Fatalf("FetchAndStore: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(repo.saved))
	}

	got := repo.saved[0]
	if got.PrimeID != 7 {
		t.Errorf("PrimeID = %d, want 7", got.PrimeID)
	}
	if got.RiskCapital != "100" {
		t.Errorf("RiskCapital = %q, want trimmed %q", got.RiskCapital, "100")
	}
	if got.RiskToCapitalRatio != nil {
		t.Errorf("RiskToCapitalRatio = %v, want nil for blank upstream value", *got.RiskToCapitalRatio)
	}
	if got.BenchmarkSource != "https://example.com/star" {
		t.Errorf("BenchmarkSource = %q", got.BenchmarkSource)
	}
	if got.SyncedAt.IsZero() {
		t.Error("SyncedAt should be set")
	}
}
