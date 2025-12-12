package services

import (
	"github.com/archon-research/stl/internal/stl-verify/domain"
	"github.com/archon-research/stl/internal/stl-verify/interfaces"
)

type Verifier struct {
	repo interfaces.RiskRepository
}

func NewVerifier(repo interfaces.RiskRepository) *Verifier {
	return &Verifier{repo: repo}
}

func (v *Verifier) Verify(transactionID string) (*domain.RiskModel, error) {
	return &domain.RiskModel{ID: transactionID, Score: 0.1}, nil
}
