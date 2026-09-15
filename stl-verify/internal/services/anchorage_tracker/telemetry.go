package anchorage_tracker

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/anchorage_tracker"

// Telemetry counts the snapshot rows each poll persists. A poll that fetches
// zero packages returns no error, so the shared cronjob run metrics report a
// healthy run while anchorage_package_snapshot silently stops growing; this
// counter is what separates the two (VectorAnchorageNoSnapshotsStored).
type Telemetry struct {
	snapshotsStored metric.Int64Counter
}

// NewTelemetry creates a Telemetry using the global meter provider.
func NewTelemetry() (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider())
}

// NewTelemetryWithProvider creates a Telemetry with a custom meter provider.
func NewTelemetryWithProvider(mp metric.MeterProvider) (*Telemetry, error) {
	snapshotsStored, err := mp.Meter(instrumentationName).Int64Counter(
		"anchorage.snapshots.stored.total",
		metric.WithDescription("Snapshot rows written to anchorage_package_snapshot, counted once per poll (0 when the API returns an empty package list)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating snapshotsStored counter: %w", err)
	}
	return &Telemetry{snapshotsStored: snapshotsStored}, nil
}

// RecordSnapshotsStored counts the rows one poll persisted. It must also be
// called with 0, since a zero-row poll is the failure this metric exists to
// expose. Nil-safe so the service runs without telemetry wired.
func (t *Telemetry) RecordSnapshotsStored(ctx context.Context, count int) {
	if t == nil {
		return
	}
	t.snapshotsStored.Add(ctx, int64(count))
}
