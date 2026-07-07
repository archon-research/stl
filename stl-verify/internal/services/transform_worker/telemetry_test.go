package transform_worker

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
)

func TestTelemetry(t *testing.T) {
	if _, err := NewTelemetry(); err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	tel, err := NewTelemetryWithProvider(noop.NewMeterProvider())
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	ctx := context.Background()
	tel.RecordTableSuccess(ctx, "morpho_market_state", 5)
	tel.RecordTableFailure(ctx, "morpho_market_state")

	// nil receiver is a no-op, not a panic.
	var nilTel *Telemetry
	nilTel.RecordTableSuccess(ctx, "morpho_market_state", 1)
	nilTel.RecordTableFailure(ctx, "morpho_market_state")
}
