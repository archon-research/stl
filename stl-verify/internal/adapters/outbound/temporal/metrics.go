package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
// RecordRun is nil-receiver-safe so cronjobs run unchanged when telemetry
// is not wired (unit tests, local runs without an OTLP endpoint). seedStatusSeries
// is construction-time only and always runs on a non-nil receiver.
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
		metric.WithDescription("Total cronjob runs, labelled by terminal status (success|error|canceled)"),
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

// runStatusValues are the terminal statuses a run can land on; must stay in
// sync with runStatusAttr and the counter description above.
var runStatusValues = []string{"success", "error", "canceled"}

// seedStatusSeries exports every terminal-status series of cronjob.runs.total
// at 0 at worker startup. Without it the {status="success"} series only appears
// on the first success, so Prometheus never observes the 0->1 transition and
// increase()/rate() report 0 successes for up to a full window after a pod
// (re)start. That trips VectorCronjobAllRunsFailing on every rollover (see
// stl/alerts/vector-cronjobs.yaml). Seeding to 0 makes the first real increment
// visible to increase().
func (m *cronjobMetrics) seedStatusSeries() {
	ctx := context.Background()
	for _, status := range runStatusValues {
		m.runsTotal.Add(ctx, 0, metric.WithAttributes(attribute.String("status", status)))
	}
}

// runStatusAttr classifies one run outcome for the runs/duration series. A
// failure that arrives with the activity context canceled is "canceled", not
// "error": the run was interrupted (worker shutdown during a deploy rollout,
// or a schedule cancel), not broken, and Temporal retries it on the next
// worker. Counting it as an error made VectorCronjobRunFailing fire on every
// deploy that landed while a run was in flight. A run that exceeds its own
// deadline (context.DeadlineExceeded) still counts as an error.
func runStatusAttr(ctx context.Context, err error) attribute.KeyValue {
	if err != nil && errors.Is(ctx.Err(), context.Canceled) {
		return attribute.String("status", "canceled")
	}
	return telemetry.StatusAttr(err)
}

// RecordRun records the outcome and duration of one cronjob run. nil-safe.
func (m *cronjobMetrics) RecordRun(ctx context.Context, duration time.Duration, err error) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(runStatusAttr(ctx, err))
	m.runsTotal.Add(ctx, 1, attrs)
	m.runDuration.Record(ctx, duration.Seconds(), attrs)
}
