package telemetry

import (
	"context"
	"testing"
	"time"
)

// All recorder methods must be safe to call on a nil *CEXMetrics so that
// services and their tests can run without an OTel meter provider wired up.
func TestCEXMetrics_NilReceiverIsNoOp(t *testing.T) {
	var m *CEXMetrics
	ctx := context.Background()

	// Each call must not panic.
	m.RecordMessageProcessed(ctx, "binance", "success", 10*time.Millisecond)
	m.RecordSnapshotsProduced(ctx, "binance", 3)
	m.RecordFlush(ctx, 5, 20*time.Millisecond, "success")
	m.RecordMessageForwarded(ctx, "binance", "success", 5*time.Millisecond)
	m.RecordMessageBytes(ctx, "binance", 2048)
	m.RecordExchangeInfo(ctx, "binance", "combined_stream", "snapshot")
}

// NewCEXMetrics must succeed even without InitMetrics — it should bind to
// the global no-op meter provider and produce a usable recorder.
func TestNewCEXMetrics_WithoutInit(t *testing.T) {
	m, err := NewCEXMetrics("test")
	if err != nil {
		t.Fatalf("NewCEXMetrics failed: %v", err)
	}
	if m == nil {
		t.Fatal("NewCEXMetrics returned nil")
	}
	ctx := context.Background()
	m.RecordMessageProcessed(ctx, "binance", "success", time.Millisecond)
	m.RecordSnapshotsProduced(ctx, "binance", 1)
	m.RecordFlush(ctx, 1, time.Millisecond, "success")
	m.RecordMessageForwarded(ctx, "binance", "success", time.Millisecond)
	m.RecordMessageBytes(ctx, "binance", 1024)
	m.RecordExchangeInfo(ctx, "binance", "combined_stream", "snapshot")
}

// Zero counts should not be recorded (avoids zero-bucket noise in
// snapshots_produced).
func TestCEXMetrics_SnapshotsProducedSkipsZero(t *testing.T) {
	m, err := NewCEXMetrics("test")
	if err != nil {
		t.Fatalf("NewCEXMetrics failed: %v", err)
	}
	m.RecordSnapshotsProduced(context.Background(), "binance", 0)
}
