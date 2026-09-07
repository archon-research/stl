package oracle_price_worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/metrictest"
	oracle "github.com/archon-research/stl/stl-verify/internal/services/oracle_price_worker"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestOracleTelemetryCarriesChainLabel(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tel, err := oracle.NewTelemetryWithProviders(tracenoop.NewTracerProvider(), mp, "optimism")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}

	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, 10*time.Millisecond, nil)
	tel.RecordError(ctx, "fetch", context.DeadlineExceeded)
	tel.RecordRPCCall(ctx, "eth_call", 5*time.Millisecond, nil)
	tel.RecordPricesChanged(ctx, "chainlink-eth-usd", 2)
	tel.RecordUnitSuccess(ctx, "chainlink-eth-usd")
	tel.RecordUnitReads(ctx, "chainlink-eth-usd", 1, 2)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	metrictest.RequireChain(t, rm, "optimism",
		"oracle.blocks.processed",
		"oracle.errors.total",
		"oracle.rpc.duration_seconds",
		"oracle.prices.changed",
		"oracle.unit.last_success_timestamp_seconds",
		"oracle.unit.prices_fetched",
		"oracle.unit.reads_failed",
	)
}
