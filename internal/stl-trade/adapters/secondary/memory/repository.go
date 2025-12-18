package memory

import (
	"context"
	"sync"
)

// OrderRepository is an in-memory implementation of interfaces.OrderRepository
// Useful for testing and development.
type OrderRepository struct {
	mu     sync.RWMutex
	orders []interface{}
}

// NewOrderRepository creates a new in-memory OrderRepository
func NewOrderRepository() *OrderRepository {
	return &OrderRepository{
		orders: make([]interface{}, 0),
	}
}

func (r *OrderRepository) SaveOrder(ctx context.Context, order interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.orders = append(r.orders, order)
	return nil
}

// GetAll returns all saved orders - useful for test assertions
func (r *OrderRepository) GetAll() []interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]interface{}, len(r.orders))
	copy(result, r.orders)
	return result
}

// Reset clears all data - useful for test setup/teardown
func (r *OrderRepository) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.orders = make([]interface{}, 0)
}
