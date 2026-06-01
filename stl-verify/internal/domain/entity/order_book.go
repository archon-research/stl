package entity

import (
	"errors"
	"fmt"
	"strconv"
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
	if err := checkSorted(s.Bids, false); err != nil {
		return fmt.Errorf("bids: %w", err)
	}
	if err := checkSorted(s.Asks, true); err != nil {
		return fmt.Errorf("asks: %w", err)
	}
	return nil
}

// checkSorted verifies levels are ordered: descending for bids, ascending for asks.
func checkSorted(levels []OrderBookLevel, ascending bool) error {
	for i := 1; i < len(levels); i++ {
		prev, err := strconv.ParseFloat(levels[i-1].Price, 64)
		if err != nil {
			return fmt.Errorf("invalid price at index %d: %w", i-1, err)
		}
		curr, err := strconv.ParseFloat(levels[i].Price, 64)
		if err != nil {
			return fmt.Errorf("invalid price at index %d: %w", i, err)
		}
		if ascending && prev > curr {
			return fmt.Errorf("unsorted at index %d: %s > %s", i, levels[i-1].Price, levels[i].Price)
		}
		if !ascending && prev < curr {
			return fmt.Errorf("unsorted at index %d: %s < %s", i, levels[i-1].Price, levels[i].Price)
		}
	}
	return nil
}
