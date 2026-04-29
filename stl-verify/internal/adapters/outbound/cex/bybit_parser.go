package cex

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type BybitParser struct {
	pairToSymbol map[string]string
}

func NewBybitParser() *BybitParser {
	cfg := Exchanges["bybit"]
	reverse := make(map[string]string, len(cfg.Symbols))
	for symbol, pair := range cfg.Symbols {
		reverse[pair] = symbol
	}
	return &BybitParser{pairToSymbol: reverse}
}

func (p *BybitParser) Exchange() string { return "bybit" }

func (p *BybitParser) SubscribeMessage(pairs []string, depth int) ([]byte, error) {
	args := make([]string, len(pairs))
	for i, pair := range pairs {
		args[i] = fmt.Sprintf("orderbook.%d.%s", depth, pair)
	}
	msg := struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}{
		Op:   "subscribe",
		Args: args,
	}
	return json.Marshal(msg)
}

func (p *BybitParser) ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error) {
	var envelope struct {
		Topic string `json:"topic"`
		Type  string `json:"type"`
		Ts    int64  `json:"ts"`
		Data  struct {
			S string     `json:"s"`
			B [][]string `json:"b"`
			A [][]string `json:"a"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return nil, fmt.Errorf("bybit: unmarshal: %w", err)
	}
	if envelope.Topic == "" {
		return nil, nil
	}
	if envelope.Type != "snapshot" && envelope.Type != "delta" {
		return nil, nil
	}

	symbol, ok := p.pairToSymbol[envelope.Data.S]
	if !ok {
		return nil, nil
	}

	bids, err := parseLevels(envelope.Data.B)
	if err != nil {
		return nil, fmt.Errorf("bybit: parse bids: %w", err)
	}
	asks, err := parseLevels(envelope.Data.A)
	if err != nil {
		return nil, fmt.Errorf("bybit: parse asks: %w", err)
	}

	return []entity.OrderbookSnapshot{{
		Exchange:   "bybit",
		Symbol:     symbol,
		Bids:       bids,
		Asks:       asks,
		CapturedAt: time.UnixMilli(envelope.Ts),
	}}, nil
}

func (p *BybitParser) PingMessage() []byte {
	b, _ := json.Marshal(struct {
		Op string `json:"op"`
	}{Op: "ping"})
	return b
}
