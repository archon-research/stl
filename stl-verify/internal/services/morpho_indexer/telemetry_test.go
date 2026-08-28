package morpho_indexer

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// newRecordingTelemetry returns a Telemetry wired to an in-memory metric reader
// so tests can record calls and inspect the resulting metrics.
func newRecordingTelemetry(t *testing.T) (*Telemetry, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProviders(tracenoop.NewTracerProvider(), mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders() error: %v", err)
	}
	return tel, reader
}

// collectHistogramBounds collects the named float64 histogram from reader and
// returns its bucket upper bounds.
func collectHistogramBounds(t *testing.T, reader sdkmetric.Reader, name string) []float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Histogram[float64]", name, m.Data)
			}
			if len(hist.DataPoints) != 1 {
				t.Fatalf("metric %q has %d data points, want 1", name, len(hist.DataPoints))
			}
			return hist.DataPoints[0].Bounds
		}
	}
	t.Fatalf("metric %q not found", name)
	return nil
}

// TestSecondsHistograms_UseSecondsBuckets guards against the bucket-boundary bug
// behind the VectorMorphoIndexerRPCLatencyHigh alert. Without explicit
// boundaries the SDK applies its default millisecond-scale buckets
// ([0,5,10,...]), so a seconds-valued metric collapses into the (0,5] bucket and
// histogram_quantile(0.99,...) interpolates to 0.99*5 = 4.95s, tripping the >3s
// alert permanently. Every seconds histogram must instead use
// telemetry.SecondsDurationBuckets.
func TestSecondsHistograms_UseSecondsBuckets(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, 30*time.Millisecond, nil)
	tel.RecordRPCCall(ctx, "getMarketState", 30*time.Millisecond, nil)

	for _, name := range []string{
		"morpho.block.duration_seconds",
		"morpho.rpc.duration_seconds",
	} {
		if bounds := collectHistogramBounds(t, reader, name); !slices.Equal(bounds, telemetry.SecondsDurationBuckets) {
			t.Errorf("%s bounds = %v, want %v", name, bounds, telemetry.SecondsDurationBuckets)
		}
	}
}

// TestRecordAdapterMembershipObservation_LabelsTypeAndProvenance pins the label
// vocabulary the VectorMorphoV2UnknownAdapters and
// VectorMorphoV2LazyAdapterRegistrations rules select on. Renaming a value here
// silently un-fires those alerts. observed_via deliberately carries the same five
// values as morpho_adapter_membership.observed_via, so the metric and the table
// answer provenance questions in one vocabulary.
func TestRecordAdapterMembershipObservation_LabelsTypeAndProvenance(t *testing.T) {
	tests := []struct {
		name        string
		adapterType *entity.MorphoAdapterType
		observedVia entity.MembershipSource
		wantType    string
	}{
		{"market adapter seeded at discovery", adapterTypeFor(entity.MorphoAdapterTypeMarketV1), entity.MembershipFromDiscovery, "market_v1"},
		{"nested vault adapter via AddAdapter", adapterTypeFor(entity.MorphoAdapterTypeVaultV1), entity.MembershipFromAddAdapter, "vault_v1"},
		{"unclassifiable adapter inferred from an Allocate", adapterTypeFor(entity.MorphoAdapterTypeUnknown), entity.MembershipFromAllocation, "unknown"},
		{"a removal carries no classification at all", nil, entity.MembershipFromRemoveAdapter, "unprobed"},
		{"adapter seeded by the bootstrap", adapterTypeFor(entity.MorphoAdapterTypeMarketV1), entity.MembershipFromBootstrapSeed, "market_v1"},
		{"adapter type added to the enum but not the label map", adapterTypeFor(entity.MorphoAdapterType(3)), entity.MembershipFromAddAdapter, "type_3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel, reader := newRecordingTelemetry(t)
			tel.RecordAdapterMembershipObservation(context.Background(), tt.adapterType, tt.observedVia)

			points := counterPoints(t, reader, "morpho.v2.adapter.registrations")
			if len(points) != 1 {
				t.Fatalf("got %d data points, want 1", len(points))
			}
			want := attribute.NewSet(
				attribute.String("chain", "mainnet"),
				attribute.String("adapter.type", tt.wantType),
				attribute.String("observed_via", string(tt.observedVia)),
			)
			if !points[0].Attributes.Equals(&want) {
				t.Errorf("attributes = %v, want %v", points[0].Attributes.Encoded(attribute.DefaultEncoder()), want.Encoded(attribute.DefaultEncoder()))
			}
			if points[0].Value != 1 {
				t.Errorf("value = %d, want 1", points[0].Value)
			}
		})
	}
}

// TestRecordV2Snapshot_LabelsSnapshotType pins the label vocabulary the
// VectorMorphoV2NoSnapshotsWritten rule selects on.
func TestRecordV2Snapshot_LabelsSnapshotType(t *testing.T) {
	for _, snapshotType := range []v2SnapshotType{v2SnapshotAdapterState, v2SnapshotVaultCap, v2SnapshotVaultFee} {
		t.Run(string(snapshotType), func(t *testing.T) {
			tel, reader := newRecordingTelemetry(t)
			tel.RecordV2Snapshot(context.Background(), snapshotType)

			points := counterPoints(t, reader, "morpho.v2.snapshots.written")
			if len(points) != 1 {
				t.Fatalf("got %d data points, want 1", len(points))
			}
			want := attribute.NewSet(
				attribute.String("chain", "mainnet"),
				attribute.String("snapshot.type", string(snapshotType)),
			)
			if !points[0].Attributes.Equals(&want) {
				t.Errorf("attributes = %v, want %v", points[0].Attributes.Encoded(attribute.DefaultEncoder()), want.Encoded(attribute.DefaultEncoder()))
			}
			if points[0].Value != 1 {
				t.Errorf("value = %d, want 1", points[0].Value)
			}
		})
	}
}

// TestRecordUnprobeableCandidate_LabelsReason pins the label vocabulary a
// discard alert selects on, and that the address itself stays out of it — the
// WARN carries the identifier, the counter stays low-cardinality.
func TestRecordUnprobeableCandidate_LabelsReason(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	tel.RecordUnprobeableCandidate(context.Background(), UnprobeableGasExhausted)

	points := counterPoints(t, reader, "morpho.vault.candidates.unprobeable")
	if len(points) != 1 {
		t.Fatalf("got %d data points, want 1", len(points))
	}
	want := attribute.NewSet(
		attribute.String("chain", "mainnet"),
		attribute.String("reason", string(UnprobeableGasExhausted)),
	)
	if !points[0].Attributes.Equals(&want) {
		t.Errorf("attributes = %v, want %v", points[0].Attributes.Encoded(attribute.DefaultEncoder()), want.Encoded(attribute.DefaultEncoder()))
	}
	if points[0].Value != 1 {
		t.Errorf("value = %d, want 1", points[0].Value)
	}
}

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry("mainnet")
	if err != nil {
		t.Fatalf("NewTelemetry() returned error: %v", err)
	}
	if tel == nil {
		t.Fatal("NewTelemetry() returned nil")
	}

	exerciseAllMethods(t, tel)
}

func TestNewTelemetryWithProviders(t *testing.T) {
	tel, err := NewTelemetryWithProviders(tracenoop.NewTracerProvider(), metricnoop.NewMeterProvider(), "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders() returned error: %v", err)
	}
	if tel == nil {
		t.Fatal("NewTelemetryWithProviders() returned nil")
	}

	exerciseAllMethods(t, tel)
}

// exerciseAllMethods calls every public method, covering both the success and
// error-status branches of the recorders.
func exerciseAllMethods(t *testing.T, tel *Telemetry) {
	t.Helper()
	ctx := context.Background()
	someErr := errors.New("e")
	tel.RecordBlockProcessed(ctx, time.Second, nil)
	tel.RecordBlockProcessed(ctx, time.Second, someErr)
	tel.RecordEventProcessed(ctx, "Supply")
	tel.RecordRPCCall(ctx, "getMarketState", time.Millisecond, nil)
	tel.RecordRPCCall(ctx, "getMarketState", time.Millisecond, someErr)
	tel.RecordError(ctx, "op", someErr)
	tel.RecordAdapterMembershipObservation(ctx, adapterTypeFor(entity.MorphoAdapterTypeMarketV1), entity.MembershipFromAddAdapter)
	tel.RecordV2Snapshot(ctx, v2SnapshotAdapterState)
	tel.RecordUnprobeableCandidate(ctx, UnprobeableGasExhausted)

	_, span := tel.StartBlockSpan(ctx, 1)
	span.End()
	_, span = tel.StartSpan(ctx, "test.span", attribute.String("key", "value"))
	span.End()
}

func TestTelemetry_NilSafe(t *testing.T) {
	var tel *Telemetry // nil pointer
	ctx := context.Background()
	someErr := errors.New("test error")

	// All methods must be no-ops on a nil receiver: no panics.
	t.Run("RecordBlockProcessed", func(t *testing.T) {
		tel.RecordBlockProcessed(ctx, time.Second, nil)
		tel.RecordBlockProcessed(ctx, time.Second, someErr)
	})

	t.Run("RecordEventProcessed", func(t *testing.T) {
		tel.RecordEventProcessed(ctx, "Supply")
	})

	t.Run("RecordRPCCall", func(t *testing.T) {
		tel.RecordRPCCall(ctx, "getMarketState", 100*time.Millisecond, nil)
		tel.RecordRPCCall(ctx, "getMarketState", 100*time.Millisecond, someErr)
	})

	t.Run("RecordError", func(t *testing.T) {
		tel.RecordError(ctx, "processBlock", someErr)
		tel.RecordError(ctx, "processBlock", nil)
	})

	t.Run("RecordAdapterMembershipObservation", func(t *testing.T) {
		tel.RecordAdapterMembershipObservation(ctx, adapterTypeFor(entity.MorphoAdapterTypeUnknown), entity.MembershipFromAllocation)
	})

	t.Run("RecordV2Snapshot", func(t *testing.T) {
		tel.RecordV2Snapshot(ctx, v2SnapshotVaultCap)
	})

	t.Run("RecordUnprobeableCandidate", func(t *testing.T) {
		tel.RecordUnprobeableCandidate(ctx, UnprobeableGasExhausted)
	})

	t.Run("StartBlockSpan", func(t *testing.T) {
		retCtx, span := tel.StartBlockSpan(ctx, 12345)
		if retCtx == nil {
			t.Error("StartBlockSpan returned nil context")
		}
		if span == nil {
			t.Error("StartBlockSpan returned nil span")
		}
		span.End()
	})

	t.Run("StartSpan", func(t *testing.T) {
		retCtx, span := tel.StartSpan(ctx, "test.span", attribute.String("key", "value"))
		if retCtx == nil {
			t.Error("StartSpan returned nil context")
		}
		if span == nil {
			t.Error("StartSpan returned nil span")
		}
		span.End()
	})

	t.Run("SetSpanError", func(t *testing.T) {
		// SetSpanError is a package-level function, not a method.
		span := telemetry.NoopSpan()
		telemetry.SetSpanError(span, nil, "should be no-op")
		telemetry.SetSpanError(span, someErr, "test error description")
	})
}

// adapterTypeFor is the address-of helper the nil-able classification label needs.
func adapterTypeFor(t entity.MorphoAdapterType) *entity.MorphoAdapterType { return new(t) }
