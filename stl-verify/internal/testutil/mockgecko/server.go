// Package mockgecko is a local stand-in for the CoinGecko Pro API, so the
// offchain price pipeline runs in dev with no real key — the same role
// mockchain plays for the chain. It reproduces the behaviours the pipeline's
// correctness checks depend on, not just the happy path:
//
//   - market_chart/range is inclusive at BOTH ends: a 24h request returns 25
//     hourly points, and abutting windows share the seam hour (the trait
//     chunkWindows in the backfiller exists to compensate for).
//   - A range before the plan's entitlement horizon, or an unknown asset id,
//     answers HTTP 200 with EMPTY arrays — never an error. The backfiller's
//     coverage assertions are built on exactly this trap.
//   - A request without an API key is refused with the Pro API's 401 body.
//
// Prices are a deterministic per-asset walk around a real snapshot, so two
// requests for the same hour always agree, across processes and restarts.
package mockgecko

import (
	"encoding/json"
	"hash/fnv"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SeedAsset anchors one asset's synthetic series to realistic levels.
type SeedAsset struct {
	PriceUSD     float64
	MarketCapUSD float64
	VolumeUSD    float64
}

// DefaultSeed holds every asset in offchain_price_asset plus the natives
// VEC-539 will add, at levels captured from the live Pro API on 2026-09-08.
// Values only anchor the walk; they are not meant to stay current.
func DefaultSeed() map[string]SeedAsset {
	return map[string]SeedAsset{
		"bitcoin":               {78306, 1.5726e12, 2.9593e10},
		"coinbase-wrapped-btc":  {78307, 7.8126e9, 3.4517e8},
		"dai":                   {1.00, 4.6042e9, 2.7509e8},
		"ethereum":              {2472.06, 3.0166e11, 1.0857e10},
		"gnosis":                {116.83, 3.0837e8, 1.6973e6},
		"hyperliquid":           {83.10, 1.8486e10, 1.0972e9},
		"jito-staked-sol":       {133.93, 1.0593e9, 2.4254e7},
		"kelp-dao-restaked-eth": {2660.65, 1.0926e9, 5.2429e4},
		"lombard-staked-btc":    {78606, 8.0598e8, 1.5564e6},
		"paypal-usd":            {0.9998, 2.8889e9, 7.0636e7},
		"renzo-restaked-eth":    {2677.30, 1.1066e8, 1.6746e4},
		"ripple":                {1.40, 8.7573e10, 1.8778e9},
		"rocket-pool-eth":       {2889.87, 9.1641e8, 3.4985e5},
		"savings-dai":           {1.18, 1.6552e8, 1.6171e6},
		"solana":                {103.04, 6.0389e10, 2.9691e9},
		"susds":                 {1.11, 4.6719e9, 4.0959e5},
		"tbtc":                  {78285, 3.3935e8, 3.8224e6},
		"tether":                {0.9996, 1.8336e11, 5.3455e10},
		"usd-coin":              {0.9999, 7.4223e10, 1.3234e10},
		"usds":                  {0.9997, 9.8440e9, 1.3557e8},
		"weth":                  {2473.18, 5.0828e9, 2.0331e8},
		"wrapped-bitcoin":       {78240, 9.0864e9, 8.8141e7},
		"wrapped-eeth":          {2726.89, 5.2716e9, 1.6817e6},
		"wrapped-steth":         {3074.37, 1.1463e10, 2.5611e6},
	}
}

// Server implements the two CoinGecko Pro endpoints the pipeline calls.
type Server struct {
	seed map[string]SeedAsset

	// entitlementStart mimics the plan's historical horizon: points before it
	// are silently absent (HTTP 200, empty arrays). Zero = serve everything.
	entitlementStart time.Time
}

// NewServer builds a Server over the given seed (nil = DefaultSeed).
func NewServer(seed map[string]SeedAsset) *Server {
	if seed == nil {
		seed = DefaultSeed()
	}
	return &Server{seed: seed}
}

// SetEntitlementStart makes ranges before t come back empty, the way the real
// API answers a window outside the plan's historical entitlement.
func (s *Server) SetEntitlementStart(t time.Time) { s.entitlementStart = t }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("x-cg-pro-api-key") == "" && r.URL.Query().Get("x_cg_pro_api_key") == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{"error_code": 10002, "error_message": "API Key Missing"},
		})
		return
	}

	switch {
	case r.URL.Path == "/simple/price":
		s.simplePrice(w, r)
	case strings.HasPrefix(r.URL.Path, "/coins/") && strings.HasSuffix(r.URL.Path, "/market_chart/range"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/coins/"), "/market_chart/range")
		s.marketChartRange(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) simplePrice(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC().Truncate(time.Hour)
	response := map[string]map[string]any{}
	for id := range strings.SplitSeq(r.URL.Query().Get("ids"), ",") {
		asset, ok := s.seed[id]
		if !ok {
			continue // unknown ids are silently absent, like the real endpoint
		}
		price := s.priceAt(id, now)
		response[id] = map[string]any{
			"usd":             price,
			"usd_market_cap":  asset.MarketCapUSD * price / asset.PriceUSD,
			"usd_24h_vol":     asset.VolumeUSD,
			"last_updated_at": now.Unix(),
		}
	}
	writeJSON(w, response)
}

func (s *Server) marketChartRange(w http.ResponseWriter, r *http.Request, id string) {
	from, errFrom := strconv.ParseFloat(r.URL.Query().Get("from"), 64)
	to, errTo := strconv.ParseFloat(r.URL.Query().Get("to"), 64)
	asset, known := s.seed[id]

	type point = [2]float64
	prices, caps, vols := []point{}, []point{}, []point{}
	if known && errFrom == nil && errTo == nil {
		// Inclusive at both ends, hourly — the live API's measured behaviour.
		for ts := int64(from); ts <= int64(to); ts += 3600 {
			at := time.Unix(ts, 0).UTC()
			if !s.entitlementStart.IsZero() && at.Before(s.entitlementStart) {
				continue
			}
			price := s.priceAt(id, at)
			ms := float64(ts) * 1000
			prices = append(prices, point{ms, price})
			caps = append(caps, point{ms, asset.MarketCapUSD * price / asset.PriceUSD})
			vols = append(vols, point{ms, asset.VolumeUSD})
		}
	}
	// Unknown id or unparseable/unserved range: HTTP 200 with empty arrays,
	// never an error — the trap the backfiller's coverage checks guard against.
	writeJSON(w, map[string]any{"prices": prices, "market_caps": caps, "total_volumes": vols})
}

// priceAt is the deterministic walk: the seed level, a daily sinusoid (±2%),
// and per-hour hash noise (±0.5%). Keyed by absolute hour so every request —
// current or historical, from any process — agrees on the same value.
func (s *Server) priceAt(id string, at time.Time) float64 {
	hour := at.Unix() / 3600
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	_, _ = h.Write([]byte(strconv.FormatInt(hour, 10)))
	noise := (float64(h.Sum64()%2000)/1000 - 1) * 0.005
	cycle := 0.02 * math.Sin(2*math.Pi*float64(hour%24)/24)
	return s.seed[id].PriceUSD * (1 + cycle + noise)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
