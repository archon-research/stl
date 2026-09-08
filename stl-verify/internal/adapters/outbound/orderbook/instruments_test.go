package orderbook

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateSymbolsAcceptsTradeableSymbols(t *testing.T) {
	ex := &fakeExchange{instrs: symbolSet([]string{"BTC-USD", "ETH-USD"})}

	if err := validateSymbols(t.Context(), ex, []string{"BTC-USD", "ETH-USD"}); err != nil {
		t.Fatalf("validateSymbols: %v", err)
	}
}

// TestValidateSymbolsNamesEveryUntradeableSymbol: an operator fixing a ConfigMap
// needs every bad symbol in one pass, not one per crash loop.
func TestValidateSymbolsNamesEveryUntradeableSymbol(t *testing.T) {
	ex := &fakeExchange{instrs: symbolSet([]string{"BTC-USD"})}

	err := validateSymbols(t.Context(), ex, []string{"BTC-USD", "BTC-USDD", "ETH-USDD"})
	if err == nil {
		t.Fatal("expected an error for symbols the venue does not trade")
	}
	for _, want := range []string{"fake", "BTC-USDD", "ETH-USDD"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestValidateSymbolsFailsWhenInstrumentsUnavailable(t *testing.T) {
	ex := &fakeExchange{instrErr: errors.New("connection refused")}

	if err := validateSymbols(t.Context(), ex, []string{"BTC-USD"}); err == nil {
		t.Fatal("expected an error when the instruments endpoint is unavailable")
	}
}

func TestFetchJSONDecodesBody(t *testing.T) {
	base := newRESTTestServer(t, map[string]restResponse{"/x": {body: `{"code":"0"}`}})

	var got struct {
		Code string `json:"code"`
	}
	if err := fetchJSON(t.Context(), base+"/x", &got); err != nil {
		t.Fatalf("fetchJSON: %v", err)
	}
	if got.Code != "0" {
		t.Errorf("Code = %q, want 0", got.Code)
	}
}

func TestFetchJSONRejectsUnusableResponses(t *testing.T) {
	base := newRESTTestServer(t, map[string]restResponse{
		"/server-error": {status: 500, body: `{}`},
		"/malformed":    {body: `{"code":`},
	})

	tests := []struct {
		name string
		url  string
	}{
		{name: "non-200", url: base + "/server-error"},
		{name: "malformed JSON", url: base + "/malformed"},
		{name: "unreachable host", url: "http://127.0.0.1:1/x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dst struct{}
			if err := fetchJSON(t.Context(), tt.url, &dst); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
