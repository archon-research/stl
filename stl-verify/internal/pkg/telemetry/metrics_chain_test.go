package telemetry_test

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/metrictest"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricsRecordBlockProcessedCarriesChainLabel(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	m, err := telemetry.NewMetrics("test-backup", "arbitrum")
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	ctx := context.Background()
	m.RecordBlockProcessed(ctx, "success")
	m.RecordProcessingLatency(ctx, 100*time.Millisecond, "success")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	metrictest.RequireChain(t, rm, "arbitrum", "blocks_processed_total", "processing_duration_seconds")
}
