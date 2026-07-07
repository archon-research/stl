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
// (VectorCronjobAllRunsFailing, fixed by #529), and a worker that dies before
// its first unit of work emits no series at all, so its stalled alert can
// never fire while the pod stays Running. Seeding at construction fixes both.
//
// Call this (or SeedStatusCounter) at construction for every counter an alert
// reads with an absence shape. Open-ended label sets (e.g. per-operation error
// counters) cannot be enumerated for seeding - pair those alerts with a
// kube-state Down companion instead.
func SeedCounter(ctx context.Context, c metric.Int64Counter, attrs ...attribute.KeyValue) {
	c.Add(ctx, 0, metric.WithAttributes(attrs...))
}

// SeedStatusCounter seeds both terminal-status series (success and error) of c
// at 0, each also carrying base. See SeedCounter for the rationale.
func SeedStatusCounter(ctx context.Context, c metric.Int64Counter, base ...attribute.KeyValue) {
	SeedCounter(ctx, c, slices.Concat(base, []attribute.KeyValue{SuccessStatusAttr()})...)
	SeedCounter(ctx, c, slices.Concat(base, []attribute.KeyValue{ErrorStatusAttr()})...)
}
