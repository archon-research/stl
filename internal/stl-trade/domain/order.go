package domain

// Order represents a trade order
type Order struct {
	ID     string
	Symbol string
	Amount float64
	Side   string // "BUY" or "SELL"
}

func (o *Order) IsValid() bool {
	return o.Amount > 0
}
