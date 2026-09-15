package position_daily_crystallizer

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestNewTelemetry(t *testing.T) {
	// Through the global provider, as the worker does, and through an injected one,
	// which is what makes the constructor testable at all.
	otel.SetMeterProvider(sdkmetric.NewMeterProvider())
	if _, err := NewTelemetry(); err != nil {
		t.Fatalf("NewTelemetry through the global provider: %v", err)
	}
	tel, err := NewTelemetryWithProvider(sdkmetric.NewMeterProvider())
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	for name, instrument := range map[string]any{
		"rowsWritten": tel.rowsWritten, "runs": tel.runs,
		"failures": tel.failures, "duration": tel.duration,
	} {
		if instrument == nil {
			t.Errorf("%s instrument is nil", name)
		}
	}
	tel.RecordRun(context.Background(), 5, time.Second)
	tel.RecordFailure(context.Background())
}

// A nil *Telemetry is the documented fallback for a worker that could not build
// instruments, so both methods must be safe on it.
func TestNilTelemetryIsSafe(t *testing.T) {
	var tel *Telemetry
	tel.RecordRun(context.Background(), 1, time.Second)
	tel.RecordFailure(context.Background())
}
