package ports

import "context"

// Primary Port
type TradeService interface {
	ExecuteOrder(ctx context.Context, orderID string) error
}

// Secondary Port
type OrderRepository interface {
	SaveOrder(ctx context.Context, order interface{}) error
}
