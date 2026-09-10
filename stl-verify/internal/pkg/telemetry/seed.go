package telemetry

import (
	"context"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SeedCounter exports c as a 0-valued series carrying attrs.
//
// An OTel cumulative counter's series does not exist until its first Add, so
// after a pod (re)start Prometheus first scrapes the series already at 1 and
// increase()/rate() never observe the 0->1 transition. Two production failure
// modes follow: an absence alert (`increase(...) == 0`) false-fires across a
// rollover because the first real increment is invisible for a full window
// (VectorCronjobAllRunsFailing), and a worker that dies before
// its first unit of work emits no series at all, so its stalled alert can
// never fire while the pod stays Running. Seeding at construction fixes both.
//
// Call this (or SeedStatusCounter) at construction for every counter an alert
// reads with an absence shape. A component constructed before telemetry is
// wired must register through OnMeterProviderReady instead: until the
// exporting provider is installed, Add lands on a delegating placeholder and
// the seed is dropped, while later real measurements still record — which
// looks exactly like the bug seeding prevents. Open-ended label sets (e.g. per-operation error
// counters) cannot be enumerated for seeding - pair those alerts with a
// kube-state Down companion instead.
func SeedCounter(ctx context.Context, c metric.Int64Counter, attrs ...attribute.KeyValue) {
	c.Add(ctx, 0, metric.WithAttributes(attrs...))
}

// SeedStatusCounter seeds the success and error series of c at 0, each also
// carrying base. A counter with further terminal statuses needs them seeded
// too, which this does not do. See SeedCounter for the rationale.
func SeedStatusCounter(ctx context.Context, c metric.Int64Counter, base ...attribute.KeyValue) {
	SeedCounter(ctx, c, slices.Concat(base, []attribute.KeyValue{SuccessStatusAttr()})...)
	SeedCounter(ctx, c, slices.Concat(base, []attribute.KeyValue{ErrorStatusAttr()})...)
}
