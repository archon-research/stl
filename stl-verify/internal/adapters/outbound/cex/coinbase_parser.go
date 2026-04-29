package cex

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type CoinbaseParser struct {
	pairToSymbol map[string]string
}

func NewCoinbaseParser() *CoinbaseParser {
	cfg := Exchanges["coinbase"]
	reverse := make(map[string]string, len(cfg.Symbols))
	for symbol, pair := range cfg.Symbols {
		reverse[pair] = symbol
	}
	return &CoinbaseParser{pairToSymbol: reverse}
}

func (p *CoinbaseParser) Exchange() string { return "coinbase" }

func (p *CoinbaseParser) SubscribeMessage(pairs []string, _ int) ([]byte, error) {
	msg := struct {
		Type       string   `json:"type"`
		ProductIDs []string `json:"product_ids"`
		Channel    string   `json:"channel"`
	}{
		Type:       "subscribe",
		ProductIDs: pairs,
		Channel:    "level2",
	}
	return json.Marshal(msg)
}

func (p *CoinbaseParser) ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error) {
	var envelope struct {
		Channel string `json:"channel"`
		Events  []struct {
			Type      string `json:"type"`
			ProductID string `json:"product_id"`
			Updates   []struct {
				Side        string `json:"side"`
				PriceLevel  string `json:"price_level"`
				NewQuantity string `json:"new_quantity"`
			} `json:"updates"`
		} `json:"events"`
	}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return nil, fmt.Errorf("coinbase: unmarshal: %w", err)
	}
	if envelope.Channel != "l2_data" {
		return nil, nil
	}
	if len(envelope.Events) == 0 {
		return nil, nil
	}

	ev := envelope.Events[0]
	symbol, ok := p.pairToSymbol[ev.ProductID]
	if !ok {
		return nil, nil
	}

	var bids, asks []entity.OrderbookLevel
	for _, u := range ev.Updates {
		if u.NewQuantity == "0" {
			continue
		}
		price, err := strconv.ParseFloat(u.PriceLevel, 64)
		if err != nil {
			return nil, fmt.Errorf("coinbase: parse price %q: %w", u.PriceLevel, err)
		}
		qty, err := strconv.ParseFloat(u.NewQuantity, 64)
		if err != nil {
			return nil, fmt.Errorf("coinbase: parse qty %q: %w", u.NewQuantity, err)
		}
		level := entity.NewOrderbookLevel(price, qty)
		switch u.Side {
		case "bid":
			bids = append(bids, level)
		case "offer":
			asks = append(asks, level)
		}
	}

	return []entity.OrderbookSnapshot{{
		Exchange:   "coinbase",
		Symbol:     symbol,
		Bids:       bids,
		Asks:       asks,
		CapturedAt: time.Now(),
	}}, nil
}

func (p *CoinbaseParser) PingMessage() []byte { return nil }
