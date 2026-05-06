package entity

import "time"

// PrimeCapitalStackSnapshot is a durable capital stack row for a prime.
type PrimeCapitalStackSnapshot struct {
	PrimeID              int64
	CapitalBuffer        string
	FirstLossCapital     string
	Timestamp            time.Time
	Source               string
	Version              int
	BenchmarkSource      string
	ReconciliationStatus string
	CreatedBy            string
	UpdatedBy            string
}
