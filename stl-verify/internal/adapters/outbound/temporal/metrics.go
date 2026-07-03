package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"

// cronjobMetrics records the outcome of every cronjob run from the single
// shared activity path. Because RunCronjob initialises OTel with
// ServiceName=cfg.Name, the exported series carry service_name="<cronjob>"
// automatically — so one set of alerts (stl/alerts/vector-cronjobs.yaml) keyed
// on service_name covers every cronjob, current and future, with no per-job
// wiring.
//
// All methods are nil-receiver-safe so cronjobs run unchanged when telemetry
// is not wired (unit tests, local runs without an OTLP endpoint).
type cronjobMetrics struct {
	runsTotal   metric.Int64Counter
	runDuration metric.Float64Histogram
}

func newCronjobMetrics() (*cronjobMetrics, error) {
	return newCronjobMetricsWithProvider(otel.GetMeterProvider())
}

func newCronjobMetricsWithProvider(mp metric.MeterProvider) (*cronjobMetrics, error) {
	meter := mp.Meter(instrumentationName)

	runsTotal, err := meter.Int64Counter(
		"cronjob.runs.total",
		metric.WithDescription("Total cronjob runs, labelled by terminal status (success|error)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating cronjob.runs.total counter: %w", err)
	}

	runDuration, err := meter.Float64Histogram(
		"cronjob.run.duration_seconds",
		metric.WithDescription("Duration of a cronjob run in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating cronjob.run.duration_seconds histogram: %w", err)
	}

	m := &cronjobMetrics{runsTotal: runsTotal, runDuration: runDuration}
	m.seedStatusSeries()
	return m, nil
}

// errSeed is a non-nil sentinel so StatusAttr yields status="error"; it is only
// used to seed the error series and is never surfaced as a real run outcome.
var errSeed = errors.New("seed")

// seedStatusSeries exports both terminal-status series of cronjob.runs.total at
// 0 at worker startup. Without it the {status="success"} series only appears on
// the first success, so Prometheus never observes the 0->1 transition and
// increase()/rate() report 0 successes for up to a full window after a pod
// (re)start. That trips VectorCronjobAllRunsFailing on every rollover (see
// alerts/vector-cronjobs.yaml). Seeding to 0 makes the first real increment
// visible to increase().
func (m *cronjobMetrics) seedStatusSeries() {
	ctx := context.Background()
	m.runsTotal.Add(ctx, 0, metric.WithAttributes(telemetry.StatusAttr(nil)))
	m.runsTotal.Add(ctx, 0, metric.WithAttributes(telemetry.StatusAttr(errSeed)))
}

// RecordRun records the outcome and duration of one cronjob run. nil-safe.
func (m *cronjobMetrics) RecordRun(ctx context.Context, duration time.Duration, err error) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(telemetry.StatusAttr(err))
	m.runsTotal.Add(ctx, 1, attrs)
	m.runDuration.Record(ctx, duration.Seconds(), attrs)
}
