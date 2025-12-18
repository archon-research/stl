package testutil

import "context"

// Mock implementations for testing

// MockOrderRepository is a mock implementation of the OrderRepository interface
type MockOrderRepository struct {
	SaveOrderFunc func(ctx context.Context, order interface{}) error
	Orders        []interface{}
}

func (m *MockOrderRepository) SaveOrder(ctx context.Context, order interface{}) error {
	m.Orders = append(m.Orders, order)
	if m.SaveOrderFunc != nil {
		return m.SaveOrderFunc(ctx, order)
	}
	return nil
}

// MockRiskRepository is a mock implementation of the RiskRepository interface
type MockRiskRepository struct {
	SaveFunc func(risk interface{}) error
	GetFunc  func(id string) (interface{}, error)
}

func (m *MockRiskRepository) Save(risk interface{}) error {
	if m.SaveFunc != nil {
		return m.SaveFunc(risk)
	}
	return nil
}

func (m *MockRiskRepository) Get(id string) (interface{}, error) {
	if m.GetFunc != nil {
		return m.GetFunc(id)
	}
	return nil, nil
}
