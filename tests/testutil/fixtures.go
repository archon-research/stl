package testutil

// Fixtures provides test data for use across test suites

type TestOrder struct {
	ID     string
	Symbol string
	Amount float64
	Side   string
}

func SampleOrder() TestOrder {
	return TestOrder{
		ID:     "test-order-001",
		Symbol: "ETH/USD",
		Amount: 1.5,
		Side:   "BUY",
	}
}

func SampleOrders(count int) []TestOrder {
	orders := make([]TestOrder, count)
	for i := 0; i < count; i++ {
		orders[i] = TestOrder{
			ID:     "test-order-" + string(rune('0'+i)),
			Symbol: "ETH/USD",
			Amount: float64(i + 1),
			Side:   "BUY",
		}
	}
	return orders
}
