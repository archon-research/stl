package services

import (
	"context"

	"github.com/archon-research/stl/internal/stl-trade/interfaces"
)

type Executor struct {
	repo interfaces.OrderRepository
}

func NewExecutor(repo interfaces.OrderRepository) *Executor {
	return &Executor{repo: repo}
}

func (e *Executor) ExecuteOrder(ctx context.Context, orderID string) error {
	// Execute trade logic
	return nil
}
