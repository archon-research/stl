package orderbook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// instrumentsTimeout bounds the one startup call to a venue's public
// instruments endpoint; the worker cannot start until it answers.
const instrumentsTimeout = 10 * time.Second

var instrumentsClient = &http.Client{Timeout: instrumentsTimeout}

// validateSymbols rejects any configured symbol the venue does not currently
// trade, so a typo or a delisting fails the worker at startup instead of
// leaving it connected and silently persisting nothing for that symbol. Every
// offending symbol is named, so one restart is enough to fix the whole config.
func validateSymbols(ctx context.Context, exchange exchangeFeed, symbols []string) error {
	tradeable, err := exchange.instruments(ctx)
	if err != nil {
		return fmt.Errorf("fetching %s instruments: %w", exchange.name(), err)
	}
	var bad []string
	for _, s := range symbols {
		if !tradeable[s] {
			bad = append(bad, s)
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%s does not trade the configured symbols: %s", exchange.name(), strings.Join(bad, ", "))
	}
	return nil
}

// fetchJSON GETs url and decodes the JSON body into dst.
func fetchJSON(ctx context.Context, url string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("building request for %s: %w", url, err)
	}
	resp, err := instrumentsClient.Do(req)
	if err != nil {
		return fmt.Errorf("requesting %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("requesting %s: status %s", url, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		return fmt.Errorf("decoding %s: %w", url, err)
	}
	return nil
}
