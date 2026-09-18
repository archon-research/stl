package block_meta_loader

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/block_meta_loader"

// loaderMetrics records how much work one run found. block_meta_worklist is an UNLOGGED table a
// completed run clears, so its size is never observable from outside a run: what a run pages off it
// is the pending set, and that is the series the growth tripwire reads.
//
// recordPaged is nil-receiver-safe, so the service runs unchanged where telemetry is not wired.
type loaderMetrics struct {
	worklistRowsPaged    metric.Int64Counter
	runsCapped           metric.Int64Counter
	runsCappedNoProgress metric.Int64Counter
}

func newLoaderMetrics(chainID int64) (*loaderMetrics, error) {
	meter := otel.GetMeterProvider().Meter(instrumentationName)

	paged, err := meter.Int64Counter(
		"block_meta.worklist.rows.paged",
		metric.WithDescription("Rows taken off block_meta_worklist by a run, i.e. the pending set for that chain"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating block_meta.worklist.rows.paged counter: %w", err)
	}

	// A capped run leaves blocks pending and still succeeds, so the cap is the only place a schedule
	// that is falling behind becomes visible: a tick that never caps is keeping up, one that caps
	// every hour is not.
	capped, err := meter.Int64Counter(
		"block_meta.runs.capped",
		metric.WithDescription("Runs that stopped at MaxBlocks with blocks still pending for that chain"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating block_meta.runs.capped counter: %w", err)
	}

	// Absent blocks sort first and are never loaded, so a run capped on them alone repeats every tick.
	noProgress, err := meter.Int64Counter(
		"block_meta.runs.capped_without_progress",
		metric.WithDescription("Capped runs in which every block read was absent from the archive"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating block_meta.runs.capped_without_progress counter: %w", err)
	}

	m := &loaderMetrics{worklistRowsPaged: paged, runsCapped: capped, runsCappedNoProgress: noProgress}
	// Seeded so the series exists from process start: the tripwire reads a rate, and an unseeded
	// counter first appears at its first real increment (telemetry.SeedCounter carries the why).
	telemetry.SeedCounter(context.Background(), paged, chainAttr(chainID))
	telemetry.SeedCounter(context.Background(), capped, chainAttr(chainID))
	telemetry.SeedCounter(context.Background(), noProgress, chainAttr(chainID))
	return m, nil
}

func chainAttr(chainID int64) attribute.KeyValue {
	return attribute.Int64("chain_id", chainID)
}

func (m *loaderMetrics) recordPaged(ctx context.Context, chainID int64, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.worklistRowsPaged.Add(ctx, int64(n), metric.WithAttributes(chainAttr(chainID)))
}

// recordCapped counts a run that stopped at its cap. Nil-receiver-safe, like recordPaged.
func (m *loaderMetrics) recordCapped(ctx context.Context, chainID int64) {
	if m == nil {
		return
	}
	m.runsCapped.Add(ctx, 1, metric.WithAttributes(chainAttr(chainID)))
}

// recordCappedWithoutProgress counts a capped run whose every read was absent. Nil-receiver-safe.
func (m *loaderMetrics) recordCappedWithoutProgress(ctx context.Context, chainID int64) {
	if m == nil {
		return
	}
	m.runsCappedNoProgress.Add(ctx, 1, metric.WithAttributes(chainAttr(chainID)))
}
