package cex

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type KrakenParser struct {
	pairToSymbol map[string]string
}

func NewKrakenParser() *KrakenParser {
	cfg := Exchanges["kraken"]
	reverse := make(map[string]string, len(cfg.Symbols))
	for symbol, pair := range cfg.Symbols {
		reverse[pair] = symbol
	}
	return &KrakenParser{pairToSymbol: reverse}
}

func (p *KrakenParser) Exchange() string { return "kraken" }

func (p *KrakenParser) SubscribeMessage(pairs []string, depth int) ([]byte, error) {
	msg := struct {
		Method string `json:"method"`
		Params struct {
			Channel string   `json:"channel"`
			Symbol  []string `json:"symbol"`
			Depth   int      `json:"depth"`
		} `json:"params"`
	}{}
	msg.Method = "subscribe"
	msg.Params.Channel = "book"
	msg.Params.Symbol = pairs
	msg.Params.Depth = depth
	return json.Marshal(msg)
}

func (p *KrakenParser) ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error) {
	var envelope struct {
		Channel string `json:"channel"`
		Type    string `json:"type"`
		Data    []struct {
			Symbol    string `json:"symbol"`
			Bids      []struct {
				Price float64 `json:"price"`
				Qty   float64 `json:"qty"`
			} `json:"bids"`
			Asks []struct {
				Price float64 `json:"price"`
				Qty   float64 `json:"qty"`
			} `json:"asks"`
			Timestamp string `json:"timestamp"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return nil, fmt.Errorf("kraken: unmarshal: %w", err)
	}
	if envelope.Channel != "book" {
		return nil, nil
	}
	if envelope.Type != "snapshot" && envelope.Type != "update" {
		return nil, nil
	}
	if len(envelope.Data) == 0 {
		return nil, nil
	}

	d := envelope.Data[0]
	symbol, ok := p.pairToSymbol[d.Symbol]
	if !ok {
		return nil, nil
	}

	ts, err := time.Parse(time.RFC3339Nano, d.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("kraken: parse timestamp %q: %w", d.Timestamp, err)
	}

	bids := make([]entity.OrderbookLevel, 0, len(d.Bids))
	for _, b := range d.Bids {
		bids = append(bids, entity.NewOrderbookLevel(b.Price, b.Qty))
	}
	asks := make([]entity.OrderbookLevel, 0, len(d.Asks))
	for _, a := range d.Asks {
		asks = append(asks, entity.NewOrderbookLevel(a.Price, a.Qty))
	}

	return []entity.OrderbookSnapshot{{
		Exchange:   "kraken",
		Symbol:     symbol,
		Bids:       bids,
		Asks:       asks,
		CapturedAt: ts,
	}}, nil
}

func (p *KrakenParser) PingMessage() []byte {
	b, _ := json.Marshal(struct {
		Method string `json:"method"`
	}{Method: "ping"})
	return b
}
