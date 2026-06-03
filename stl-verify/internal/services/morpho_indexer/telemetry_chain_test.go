package morpho_indexer_test

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/metrictest"
	morpho "github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestMorphoTelemetryCarriesChainLabel(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tel, err := morpho.NewTelemetryWithProviders(tracenoop.NewTracerProvider(), mp, "arbitrum")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}

	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, 10*time.Millisecond, nil)
	tel.RecordError(ctx, "decode", context.DeadlineExceeded)
	tel.RecordRPCCall(ctx, "eth_call", 5*time.Millisecond, nil)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	metrictest.RequireChain(t, rm, "arbitrum",
		"morpho.blocks.processed",
		"morpho.errors.total",
		"morpho.rpc.duration_seconds",
	)
}
