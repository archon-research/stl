// telemetry.go instruments the resolver.
//
// block_version.resolved.total: heights a replay asked the raw archive about, by outcome, so
// a red run's cause is readable without its logs. One of the five values is ambiguous alone:
// `not_archived` is a lagging head that clears itself or a hole below it, per the error line.
package blockversion

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/pkg/blockversion"

// resolveOutcome is what one ResolveBlockVersion call answered.
type resolveOutcome string

const (
	outcomeMemo        resolveOutcome = "memo"
	outcomeArchive     resolveOutcome = "archive"
	outcomeNotArchived resolveOutcome = "not_archived"
	outcomeMismatch    resolveOutcome = "mismatch"
	outcomeReadFailed  resolveOutcome = "read_failed"
)

// telemetry records what the resolver answered. Its zero value is the disabled state.
type telemetry struct {
	resolved metric.Int64Counter
}

// newTelemetry builds the counter off the global meter provider, and disables the metric
// rather than failing the replay when it cannot: a resolver that refuses to exist stops a
// repair job, which is a worse outcome than a missing series.
func newTelemetry(logger *slog.Logger) telemetry {
	resolved, err := otel.GetMeterProvider().Meter(instrumentationName).Int64Counter(
		"block_version.resolved.total",
		metric.WithDescription("Heights a replay asked the raw archive for a block_version, by outcome"),
	)
	if err != nil {
		logger.Error("building the block_version.resolved.total counter; the resolver's metric is disabled", "error", err)
		return telemetry{}
	}
	return telemetry{resolved: resolved}
}

func (t telemetry) record(ctx context.Context, outcome resolveOutcome) {
	if t.resolved == nil {
		return
	}
	t.resolved.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", string(outcome))))
}
