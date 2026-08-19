package anchorage_tracker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry()
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	if tel == nil {
		t.Fatal("telemetry is nil")
	}
}

// TestService_RecordsSnapshotsStoredWhenAPIReturnsNoPackages is the regression
// test for the silent-empty failure: the Anchorage API answered 200 with an
// empty package list for two months, Run returned nil, and nothing recorded
// that anchorage_package_snapshot had stopped growing.
func TestService_RecordsSnapshotsStoredWhenAPIReturnsNoPackages(t *testing.T) {
	reader, tel := newTestTelemetry(t)
	svc := NewService(&mockClient{}, &mockSnapshotRepo{}, &mockOperationRepo{}, 1, nil, tel)

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	got, ok := collectSnapshotsStored(t, reader)
	if !ok {
		t.Fatal("anchorage.snapshots.stored.total series absent; the alert matches on increase() == 0 and would never fire")
	}
	if got != 0 {
		t.Errorf("snapshots stored = %d, want 0", got)
	}
}

func TestService_RecordsSnapshotsStoredOnHealthyPoll(t *testing.T) {
	reader, tel := newTestTelemetry(t)
	client := &mockClient{packages: []Package{newTestPackage()}}
	svc := NewService(client, &mockSnapshotRepo{}, &mockOperationRepo{}, 1, nil, tel)

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	got, ok := collectSnapshotsStored(t, reader)
	if !ok {
		t.Fatal("anchorage.snapshots.stored.total series absent")
	}
	if got != 1 {
		t.Errorf("snapshots stored = %d, want 1", got)
	}
}

func TestTelemetry_NilReceiverSafe(t *testing.T) {
	var tel *Telemetry
	tel.RecordSnapshotsStored(context.Background(), 1)
}

func TestNewTelemetryWithProvider_InstrumentError(t *testing.T) {
	_, err := NewTelemetryWithProvider(failingMeterProvider{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "instrument creation failed") {
		t.Errorf("error = %q", err.Error())
	}
}

func newTestTelemetry(t *testing.T) (*sdkmetric.ManualReader, *Telemetry) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp)
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}
	return reader, tel
}

func collectSnapshotsStored(t *testing.T, reader *sdkmetric.ManualReader) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "anchorage.snapshots.stored.total" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("snapshots.stored data type = %T, want Sum[int64]", m.Data)
			}
			var total int64
			for _, dp := range data.DataPoints {
				total += dp.Value
			}
			return total, true
		}
	}
	return 0, false
}

type failingMeter struct {
	noop.Meter
}

func (failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, errors.New("instrument creation failed")
}

type failingMeterProvider struct {
	noop.MeterProvider
}

func (failingMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return failingMeter{}
}
