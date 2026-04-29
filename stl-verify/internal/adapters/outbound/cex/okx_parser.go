package cex

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type OKXParser struct {
	pairToSymbol map[string]string
}

func NewOKXParser() *OKXParser {
	cfg := Exchanges["okx"]
	reverse := make(map[string]string, len(cfg.Symbols))
	for symbol, pair := range cfg.Symbols {
		reverse[pair] = symbol
	}
	return &OKXParser{pairToSymbol: reverse}
}

func (p *OKXParser) Exchange() string { return "okx" }

func (p *OKXParser) SubscribeMessage(pairs []string, _ int) ([]byte, error) {
	type arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	}
	args := make([]arg, len(pairs))
	for i, pair := range pairs {
		args[i] = arg{Channel: "books", InstID: pair}
	}
	msg := struct {
		Op   string `json:"op"`
		Args []arg  `json:"args"`
	}{
		Op:   "subscribe",
		Args: args,
	}
	return json.Marshal(msg)
}

func (p *OKXParser) ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error) {
	// Handle plain text pong
	if string(msg) == "pong" {
		return nil, nil
	}

	var envelope struct {
		Arg struct {
			Channel string `json:"channel"`
			InstID  string `json:"instId"`
		} `json:"arg"`
		Action string `json:"action"`
		Data   []struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
			Ts   string     `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		return nil, fmt.Errorf("okx: unmarshal: %w", err)
	}
	if envelope.Arg.Channel != "books" {
		return nil, nil
	}
	if len(envelope.Data) == 0 {
		return nil, nil
	}

	symbol, ok := p.pairToSymbol[envelope.Arg.InstID]
	if !ok {
		return nil, nil
	}

	d := envelope.Data[0]
	tsMs, err := strconv.ParseInt(d.Ts, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("okx: parse ts %q: %w", d.Ts, err)
	}

	bids, err := parseLevels(d.Bids)
	if err != nil {
		return nil, fmt.Errorf("okx: parse bids: %w", err)
	}
	asks, err := parseLevels(d.Asks)
	if err != nil {
		return nil, fmt.Errorf("okx: parse asks: %w", err)
	}

	return []entity.OrderbookSnapshot{{
		Exchange:   "okx",
		Symbol:     symbol,
		Bids:       bids,
		Asks:       asks,
		CapturedAt: time.UnixMilli(tsMs),
	}}, nil
}

func (p *OKXParser) PingMessage() []byte { return []byte("ping") }
