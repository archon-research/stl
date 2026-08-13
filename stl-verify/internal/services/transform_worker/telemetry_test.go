package transform_worker

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
)

func TestNewTelemetry(t *testing.T) {
	if _, err := NewTelemetry(); err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
}

func TestTelemetry_Record(t *testing.T) {
	tel, err := NewTelemetryWithProvider(noop.NewMeterProvider())
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	ctx := context.Background()
	tel.RecordTableSuccess(ctx, "morpho_market_state", 5, 4)
	tel.RecordTableBudgetExpired(ctx, "morpho_market_state", 2, 2)
	tel.RecordTableFailure(ctx, "morpho_market_state")
	tel.RecordDrainBudgetExceeded(ctx)
	tel.RecordQueueDepth(ctx, "morpho_market_state", 3, 12)
	tel.RecordParity(ctx, "morpho_market_state", 10, 9, 1)
}

func TestTelemetry_NilReceiverIsNoOp(t *testing.T) {
	ctx := context.Background()
	var nilTel *Telemetry
	nilTel.RecordTableSuccess(ctx, "morpho_market_state", 1, 1)
	nilTel.RecordTableBudgetExpired(ctx, "morpho_market_state", 1, 1)
	nilTel.RecordTableFailure(ctx, "morpho_market_state")
	nilTel.RecordDrainBudgetExceeded(ctx)
	nilTel.RecordQueueDepth(ctx, "morpho_market_state", 1, 1)
	nilTel.RecordParity(ctx, "morpho_market_state", 1, 1, 0)
}
