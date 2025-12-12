package memory

import (
	"errors"
	"sync"

	"github.com/archon-research/stl/internal/stl-verify/domain"
)

// RiskRepository is an in-memory implementation of interfaces.RiskRepository
// Useful for testing and development.
type RiskRepository struct {
	mu   sync.RWMutex
	data map[string]*domain.RiskModel
}

// NewRiskRepository creates a new in-memory RiskRepository
func NewRiskRepository() *RiskRepository {
	return &RiskRepository{
		data: make(map[string]*domain.RiskModel),
	}
}

func (r *RiskRepository) Save(risk *domain.RiskModel) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Store a copy to prevent external mutation
	copy := &domain.RiskModel{
		ID:    risk.ID,
		Score: risk.Score,
	}
	r.data[risk.ID] = copy
	return nil
}

func (r *RiskRepository) Get(id string) (*domain.RiskModel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	risk, ok := r.data[id]
	if !ok {
		return nil, errors.New("risk model not found")
	}

	// Return a copy to prevent external mutation
	return &domain.RiskModel{
		ID:    risk.ID,
		Score: risk.Score,
	}, nil
}

// Reset clears all data - useful for test setup/teardown
func (r *RiskRepository) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = make(map[string]*domain.RiskModel)
}
