package services

import (
	"context"

	"github.com/archon-research/stl/internal/stl-trade/ports"
)

type Executor struct {
	repo ports.OrderRepository
}

func NewExecutor(repo ports.OrderRepository) *Executor {
	return &Executor{repo: repo}
}

func (e *Executor) ExecuteOrder(ctx context.Context, orderID string) error {
	// Execute trade logic
	return nil
}
