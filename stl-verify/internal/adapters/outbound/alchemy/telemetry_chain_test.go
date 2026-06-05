package alchemy_test

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/pkg/metrictest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestAlchemyTelemetryCarriesChainLabel(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tel, err := alchemy.NewTelemetryWithProviders(tracenoop.NewTracerProvider(), mp, "base")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}

	ctx := context.Background()
	tel.RecordRequest(ctx, "eth_getBlockByNumber", 5*time.Millisecond, nil)
	tel.RecordRetry(ctx, "eth_getBlockByNumber", 1)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	metrictest.RequireChain(t, rm, "base",
		"alchemy.client.requests.total",
		"alchemy.client.retries.total",
		"alchemy.client.request.duration", // backs the watcher p99 latency alert + dashboard panels
	)
}
