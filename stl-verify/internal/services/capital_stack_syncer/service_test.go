package capital_stack_syncer

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

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

func (m *mockCapitalRepo) UpsertPrimeCapitalSnapshots(_ context.Context, snapshots []entity.PrimeCapitalStackSnapshot) error {
	if m.err != nil {
		return m.err
	}
	m.snapshots = append(m.snapshots, snapshots...)
	return nil
}

type mockRiskProvider struct {
	rows []outbound.RiskCapitalPrimeRow
	err  error
}

func (m *mockRiskProvider) FetchPrimeRows(_ context.Context) ([]outbound.RiskCapitalPrimeRow, error) {
	return m.rows, m.err
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestRun_NoProblems_SyncsSnapshots(t *testing.T) {
	// TODO: implement
}
