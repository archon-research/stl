package coingecko

// simplePriceResponse represents the response from /simple/price endpoint.
// Example response:
//
//	{
//	  "ethereum": {
//	    "usd": 3456.78,
//	    "usd_market_cap": 415123456789,
//	    "last_updated_at": 1704067200
//	  }
//	}
type simplePriceResponse map[string]simplePriceData

type simplePriceData struct {
	USD          float64 `json:"usd"`
	USDMarketCap float64 `json:"usd_market_cap"`
	LastUpdated  int64   `json:"last_updated_at"`
}

// marketChartRangeResponse represents the response from /coins/{id}/market_chart/range endpoint.
// Example response:
//
//	{
//	  "prices": [[1704067200000, 3456.78], [1704070800000, 3460.12]],
//	  "market_caps": [[1704067200000, 415123456789], [1704070800000, 415234567890]],
//	  "total_volumes": [[1704067200000, 12345678901], [1704070800000, 12456789012]]
//	}
type marketChartRangeResponse struct {
	Prices       [][]float64 `json:"prices"`
	MarketCaps   [][]float64 `json:"market_caps"`
	TotalVolumes [][]float64 `json:"total_volumes"`
}

// coinGeckoError represents an error response from the CoinGecko API.
type coinGeckoError struct {
	Error string `json:"error"`
}
