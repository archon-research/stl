package entity

import (
	"fmt"
	"math/big"
	"time"
)

// CapitalMetricsSnapshot is a point-in-time capital-metrics observation for a
// prime, sourced from the upstream Star risk-capital monitor.
//
// Amounts are kept as the upstream decimal strings and stored verbatim in
// NUMERIC columns, so no float rounding is introduced in the indexer.
// capital_buffer is deliberately absent: it is derived on read as
// max(total_capital - first_loss_capital, 0) and must not be persisted
// independently of its inputs.
type CapitalMetricsSnapshot struct {
	PrimeID          int64
	RiskCapital      string
	TotalCapital     string
	FirstLossCapital string
	// RiskToCapitalRatio is nil when the upstream omits or blanks it.
	RiskToCapitalRatio *string
	BenchmarkSource    string
	SyncedAt           time.Time
}

func (s *CapitalMetricsSnapshot) Validate() error {
	if s.PrimeID <= 0 {
		return fmt.Errorf("prime_id must be positive")
	}
	if err := requireDecimal("risk_capital", s.RiskCapital); err != nil {
		return err
	}
	if err := requireDecimal("total_capital", s.TotalCapital); err != nil {
		return err
	}
	if err := requireDecimal("first_loss_capital", s.FirstLossCapital); err != nil {
		return err
	}
	if s.RiskToCapitalRatio != nil {
		if err := requireDecimal("risk_to_capital_ratio", *s.RiskToCapitalRatio); err != nil {
			return err
		}
	}
	if s.BenchmarkSource == "" {
		return fmt.Errorf("benchmark_source is required")
	}
	if s.SyncedAt.IsZero() {
		return fmt.Errorf("synced_at is required")
	}
	return nil
}

// requireDecimal rejects empty or non-numeric values before they reach a
// NUMERIC column, so malformed upstream data fails in the domain rather than as
// an opaque database error.
func requireDecimal(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if _, ok := new(big.Rat).SetString(value); !ok {
		return fmt.Errorf("%s is not a valid decimal: %q", field, value)
	}
	return nil
}
