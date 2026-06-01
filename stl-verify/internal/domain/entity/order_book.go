package entity

import (
	"errors"
	"fmt"
	"math/big"
	"time"
)

type OrderBookLevel struct {
	Price string
	Qty   string
}

type OrderBookSnapshot struct {
	Exchange   string
	Token      string
	CapturedAt time.Time
	Bids       []OrderBookLevel
	Asks       []OrderBookLevel
}

func (s OrderBookSnapshot) Validate() error {
	if s.Exchange == "" {
		return errors.New("exchange must not be empty")
	}
	if s.Token == "" {
		return errors.New("token must not be empty")
	}
	if s.CapturedAt.IsZero() {
		return errors.New("captured_at must not be zero")
	}
	for i, b := range s.Bids {
		if err := validateLevel(b); err != nil {
			return fmt.Errorf("bid at index %d: %w", i, err)
		}
	}
	for i, a := range s.Asks {
		if err := validateLevel(a); err != nil {
			return fmt.Errorf("ask at index %d: %w", i, err)
		}
	}
	if err := checkDescending(s.Bids); err != nil {
		return fmt.Errorf("bids: %w", err)
	}
	if err := checkAscending(s.Asks); err != nil {
		return fmt.Errorf("asks: %w", err)
	}
	if len(s.Bids) > 0 && len(s.Asks) > 0 {
		bestBid, _ := parseDecimal(s.Bids[0].Price)
		bestAsk, _ := parseDecimal(s.Asks[0].Price)
		if bestBid.Cmp(bestAsk) >= 0 {
			return fmt.Errorf("crossed book: best bid %s >= best ask %s", s.Bids[0].Price, s.Asks[0].Price)
		}
	}
	return nil
}

func validateLevel(l OrderBookLevel) error {
	p, err := parseDecimal(l.Price)
	if err != nil {
		return fmt.Errorf("price: %w", err)
	}
	if p.Sign() <= 0 {
		return fmt.Errorf("price must be positive, got %q", l.Price)
	}
	q, err := parseDecimal(l.Qty)
	if err != nil {
		return fmt.Errorf("qty: %w", err)
	}
	if q.Sign() < 0 {
		return fmt.Errorf("qty must be non-negative, got %q", l.Qty)
	}
	return nil
}

// parseDecimal parses s as an exact rational number.
// Rejects empty strings, NaN, Inf, and non-numeric input.
func parseDecimal(s string) (*big.Rat, error) {
	if s == "" {
		return nil, errors.New("empty string")
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", s)
	}
	return r, nil
}

// checkDescending verifies bids are ordered highest price first.
func checkDescending(levels []OrderBookLevel) error {
	for i := 1; i < len(levels); i++ {
		prev, _ := parseDecimal(levels[i-1].Price) // validated by validateLevel
		curr, _ := parseDecimal(levels[i].Price)
		if prev.Cmp(curr) < 0 {
			return fmt.Errorf("unsorted at index %d: %s < %s", i, levels[i-1].Price, levels[i].Price)
		}
	}
	return nil
}

// checkAscending verifies asks are ordered lowest price first.
func checkAscending(levels []OrderBookLevel) error {
	for i := 1; i < len(levels); i++ {
		prev, _ := parseDecimal(levels[i-1].Price) // validated by validateLevel
		curr, _ := parseDecimal(levels[i].Price)
		if prev.Cmp(curr) > 0 {
			return fmt.Errorf("unsorted at index %d: %s > %s", i, levels[i-1].Price, levels[i].Price)
		}
	}
	return nil
}
