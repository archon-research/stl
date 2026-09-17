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

// canceledStatusAttr is this counter's third terminal status, alongside
// telemetry's success and error. Both the seed and the recording path go
// through it so neither can spell it differently.
func canceledStatusAttr() attribute.KeyValue {
	return attribute.String("status", "canceled")
}

// runStatusValues are the terminal statuses a run can land on, held as the
// exact attributes runStatusAttr returns rather than as strings: a seed whose
// spelling the recorder never uses exports a phantom series at 0 forever while
// real runs land on an unseeded one, which reads as fixed and is not.
var runStatusValues = []attribute.KeyValue{
	telemetry.SuccessStatusAttr(),
	telemetry.ErrorStatusAttr(),
	canceledStatusAttr(),
}

// seedStatusSeries exports every terminal-status series of cronjob.runs.total
// at 0 at worker startup, so increase() can observe the first real increment
// (telemetry.SeedCounter carries the mechanism and the rollover it fixes).
// telemetry.SeedStatusCounter is the usual way to do this and is deliberately
// not used here: it seeds success and error, and this counter has a third
// terminal status, which would leave {status="canceled"} unseeded.
func (m *cronjobMetrics) seedStatusSeries() {
	ctx := context.Background()
	for _, status := range runStatusValues {
		telemetry.SeedCounter(ctx, m.runsTotal, status)
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
		return canceledStatusAttr()
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
