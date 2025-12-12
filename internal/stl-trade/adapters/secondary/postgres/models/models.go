package models

type Order struct {
	ID     int
	Symbol string
	Amount float64
	Side   string
	Status string
}
