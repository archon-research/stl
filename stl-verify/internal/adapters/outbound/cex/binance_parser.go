package cex

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type BinanceParser struct {
	pairToSymbol map[string]string
}

func NewBinanceParser() *BinanceParser {
	cfg := Exchanges["binance"]
	reverse := make(map[string]string, len(cfg.Symbols))
	for symbol, pair := range cfg.Symbols {
		reverse[strings.ToLower(pair)] = symbol
	}
	return &BinanceParser{pairToSymbol: reverse}
}

func (p *BinanceParser) Exchange() string { return "binance" }

func (p *BinanceParser) SubscribeMessage(pairs []string, depth int) ([]byte, error) {
	if depth != 5 && depth != 10 && depth != 20 {
		depth = 20
	}
	params := make([]string, len(pairs))
	for i, pair := range pairs {
		params[i] = fmt.Sprintf("%s@depth%d@100ms", strings.ToLower(pair), depth)
	}
	msg := struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     int      `json:"id"`
	}{
		Method: "SUBSCRIBE",
		Params: params,
		ID:     1,
	}
	return json.Marshal(msg)
}

func (p *BinanceParser) ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error) {
	var combined struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &combined); err != nil {
		return nil, fmt.Errorf("binance: unmarshal: %w", err)
	}
	if combined.Stream == "" {
		return nil, nil
	}

	pair := strings.SplitN(combined.Stream, "@", 2)[0]
	symbol, ok := p.pairToSymbol[pair]
	if !ok {
		return nil, nil
	}

	var depth struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.Unmarshal(combined.Data, &depth); err != nil {
		return nil, fmt.Errorf("binance: unmarshal depth: %w", err)
	}

	bids, err := parseLevels(depth.Bids)
	if err != nil {
		return nil, fmt.Errorf("binance: parse bids: %w", err)
	}
	asks, err := parseLevels(depth.Asks)
	if err != nil {
		return nil, fmt.Errorf("binance: parse asks: %w", err)
	}

	return []entity.OrderbookSnapshot{{
		Exchange:   "binance",
		Symbol:     symbol,
		Bids:       bids,
		Asks:       asks,
		CapturedAt: time.Now(),
	}}, nil
}

func (p *BinanceParser) PingMessage() []byte { return nil }

// parseLevels converts [["price","qty"], ...] into OrderbookLevel slices.
// Shared across parsers that use the same format.
func parseLevels(raw [][]string) ([]entity.OrderbookLevel, error) {
	levels := make([]entity.OrderbookLevel, 0, len(raw))
	for _, entry := range raw {
		if len(entry) < 2 {
			continue
		}
		price, err := strconv.ParseFloat(entry[0], 64)
		if err != nil {
			return nil, fmt.Errorf("parse price %q: %w", entry[0], err)
		}
		size, err := strconv.ParseFloat(entry[1], 64)
		if err != nil {
			return nil, fmt.Errorf("parse size %q: %w", entry[1], err)
		}
		levels = append(levels, entity.NewOrderbookLevel(price, size))
	}
	return levels, nil
}
