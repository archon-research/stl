package interfaces

import "github.com/archon-research/stl/internal/stl-verify/domain"

type VerifierService interface {
	Verify(transactionID string) (*domain.RiskModel, error)
}

type RiskRepository interface {
	Save(risk *domain.RiskModel) error
	Get(id string) (*domain.RiskModel, error)
}
