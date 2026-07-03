package orderbook

import (
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/wsclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Config holds the tuning shared by every exchange adapter. The embedded
// wsclient.Config carries the transport settings (timeouts, keepalive pings,
// inbound buffer, logger); transport fields left zero pick up wsclient's
// defaults at dial time, and a negative PingInterval disables keepalive pings.
// The remaining fields are defaulted by DefaultConfig; zero values fall back to
// those defaults.
type Config struct {
	wsclient.Config

	// InitialBackoff is the delay before the first reconnect attempt.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential reconnect backoff.
	MaxBackoff time.Duration
	// BackoffFactor multiplies the backoff after each failed attempt. It must be
	// greater than 1; values at or below 1 fall back to the default.
	BackoffFactor float64

	// OutputBuffer is the per-Watch channel buffer. Because each update carries
	// the full book, updates are dropped (not corrupted) when the consumer falls
	// behind, so this need not be large.
	OutputBuffer int

	// MeterProvider supplies the provider's OpenTelemetry metrics. Defaults to
	// the global provider (a no-op until telemetry.InitMetrics runs).
	MeterProvider metric.MeterProvider
}

// DefaultConfig returns production-sensible defaults. Transport fields are left
// zero so wsclient applies its own defaults.
func DefaultConfig() Config {
	return Config{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     60 * time.Second,
		BackoffFactor:  2.0,
		OutputBuffer:   256,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = d.InitialBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = d.MaxBackoff
	}
	if c.BackoffFactor <= 1 {
		c.BackoffFactor = d.BackoffFactor
	}
	if c.OutputBuffer <= 0 {
		c.OutputBuffer = d.OutputBuffer
	}
	if c.MeterProvider == nil {
		c.MeterProvider = otel.GetMeterProvider()
	}
	return c
}
