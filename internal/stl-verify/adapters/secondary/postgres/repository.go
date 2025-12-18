package postgres

import (
	"database/sql"

	"github.com/archon-research/stl/internal/stl-verify/domain"
)

// RiskRepository is the PostgreSQL implementation of interfaces.RiskRepository
type RiskRepository struct {
	db *sql.DB
}

// NewRiskRepository creates a new PostgreSQL-backed RiskRepository
func NewRiskRepository(db *sql.DB) *RiskRepository {
	return &RiskRepository{db: db}
}

func (r *RiskRepository) Save(risk *domain.RiskModel) error {
	_, err := r.db.Exec(
		"INSERT INTO risk_events (id, score) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET score = $2",
		risk.ID, risk.Score,
	)
	return err
}

func (r *RiskRepository) Get(id string) (*domain.RiskModel, error) {
	row := r.db.QueryRow("SELECT id, score FROM risk_events WHERE id = $1", id)

	var risk domain.RiskModel
	if err := row.Scan(&risk.ID, &risk.Score); err != nil {
		return nil, err
	}
	return &risk, nil
}
