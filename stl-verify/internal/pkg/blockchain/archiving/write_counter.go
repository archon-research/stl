package archiving

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	writeStatusSuccess = "success"
	writeStatusError   = "error"
	// writeStatusAbandoned marks a batch the drain gate refused, so a rollout
	// that drops writes is countable rather than only inferable from parity.
	writeStatusAbandoned = "abandoned"
	// WriteStatusLost marks a batch still being written when the drain budget
	// expired. Its queue message is already deleted, so nothing retries it.
	WriteStatusLost = "lost"
)

// WriteCounter records archive.writes.total for one (chain, source) pair. The
// call decorator and the shutdown drain both count through it, so every status
// lands on one meter scope with one attribute set.
type WriteCounter struct {
	counter metric.Int64Counter
	chain   string
	source  string
}

// NewWriteCounter builds the counter. A nil provider takes the global one, a
// nil logger falls back to slog.Default().
func NewWriteCounter(provider metric.MeterProvider, chain, source string, logger *slog.Logger) *WriteCounter {
	if provider == nil {
		provider = otel.GetMeterProvider()
	}
	if logger == nil {
		logger = slog.Default()
	}
	counter, err := provider.
		Meter("github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving").
		Int64Counter("archive.writes.total", metric.WithDescription("Raw SC call archive write attempts by status"))
	if err != nil {
		// Metrics must never break the archiving hot path, so a counter that
		// fails to construct is logged and left nil (Record no-ops on nil) rather
		// than failing construction. This intentionally differs from the
		// fail-hard rule used for core dependencies.
		logger.Error("building archive.writes.total counter; archive metrics disabled", "error", err)
	}
	return &WriteCounter{counter: counter, chain: chain, source: source}
}

// Record adds n writes under status. It records against a background context
// because the increment is independent of the archive operation's timeout.
func (c *WriteCounter) Record(status string, n int64) {
	if c == nil || c.counter == nil {
		return
	}
	c.counter.Add(context.Background(), n, metric.WithAttributes(
		attribute.String("chain", c.chain),
		attribute.String("source", c.source),
		attribute.String("status", status),
	))
}
