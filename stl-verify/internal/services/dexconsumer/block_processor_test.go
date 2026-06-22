package dexconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// fakeCache implements outbound.BlockCacheReader; only GetReceipts is exercised
// by the block processor, so the embedded interface leaves the rest unimplemented.
type fakeCache struct {
	outbound.BlockCacheReader
	receipts json.RawMessage
	err      error
	gotChain int64
	gotBlock int64
	gotVer   int
}

func (c *fakeCache) GetReceipts(_ context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	c.gotChain, c.gotBlock, c.gotVer = chainID, blockNumber, version
	return c.receipts, c.err
}

func receiptsJSON(t *testing.T, n int) json.RawMessage {
	t.Helper()
	rs := make([]shared.TransactionReceipt, n)
	for i := range rs {
		rs[i] = shared.TransactionReceipt{TransactionHash: "0xabc"}
	}
	b, err := json.Marshal(rs)
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	return b
}

func fakeCacheWithReceipts(t *testing.T, n int) outbound.BlockCacheReader {
	t.Helper()
	return &fakeCache{receipts: receiptsJSON(t, n)}
}

func TestBlockProcessor_ProcessesEachReceiptWithBlockCoordinates(t *testing.T) {
	cache := &fakeCache{receipts: receiptsJSON(t, 3)}
	var calls int
	var gotChain, gotBlock int64
	var gotVer int
	var gotTS time.Time
	bp := NewBlockProcessor(cache, nil, func(_ context.Context, _ shared.TransactionReceipt, chainID, blockNumber int64, version int, ts time.Time) error {
		calls++
		gotChain, gotBlock, gotVer, gotTS = chainID, blockNumber, version, ts
		return nil
	})

	event := outbound.BlockEvent{ChainID: 8453, BlockNumber: 100, Version: 2, BlockTimestamp: 1_700_000_000}
	if err := bp.ProcessBlockEvent(context.Background(), event); err != nil {
		t.Fatalf("ProcessBlockEvent: %v", err)
	}

	if calls != 3 {
		t.Errorf("receipt handler called %d times, want 3", calls)
	}
	if cache.gotChain != 8453 || cache.gotBlock != 100 || cache.gotVer != 2 {
		t.Errorf("GetReceipts(%d,%d,%d), want (8453,100,2)", cache.gotChain, cache.gotBlock, cache.gotVer)
	}
	if gotChain != 8453 || gotBlock != 100 || gotVer != 2 {
		t.Errorf("handler got block coords (%d,%d,%d), want (8453,100,2)", gotChain, gotBlock, gotVer)
	}
	if want := time.Unix(1_700_000_000, 0).UTC(); !gotTS.Equal(want) || gotTS.Location() != time.UTC {
		t.Errorf("handler got timestamp %v (%v), want %v UTC", gotTS, gotTS.Location(), want)
	}
}

func TestBlockProcessor_NoReceipts_DoesNotCallHandler(t *testing.T) {
	cache := &fakeCache{receipts: receiptsJSON(t, 0)}
	called := false
	bp := NewBlockProcessor(cache, nil, func(context.Context, shared.TransactionReceipt, int64, int64, int, time.Time) error {
		called = true
		return nil
	})
	if err := bp.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1}); err != nil {
		t.Fatalf("ProcessBlockEvent: %v", err)
	}
	if called {
		t.Error("receipt handler must not be called when the block has no receipts")
	}
}

func TestBlockProcessor_CacheError_IsWrapped(t *testing.T) {
	sentinel := errors.New("redis down")
	cache := &fakeCache{err: sentinel}
	bp := NewBlockProcessor(cache, nil, failHandler(t))
	err := bp.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1})
	if err == nil {
		t.Fatal("expected error when cache read fails")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the cache error", err)
	}
	if !strings.Contains(err.Error(), "fetching receipts from cache") {
		t.Errorf("error %q should describe the failing step", err)
	}
}

// A nil result from the fallback reader means neither Redis nor the S3 archive
// had the block; that is an error so the message redelivers, not a silent skip.
func TestBlockProcessor_ReceiptsMissingFromCacheAndS3_Errors(t *testing.T) {
	cache := &fakeCache{receipts: nil}
	bp := NewBlockProcessor(cache, nil, failHandler(t))
	err := bp.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 42, Version: 0})
	if err == nil {
		t.Fatal("expected error when receipts are absent from both cache and S3")
	}
	if !strings.Contains(err.Error(), "cache or S3") {
		t.Errorf("error %q should indicate both cache and S3 were consulted", err)
	}
}

func TestBlockProcessor_UndecodableReceipts_Errors(t *testing.T) {
	cache := &fakeCache{receipts: json.RawMessage(`{not an array}`)}
	bp := NewBlockProcessor(cache, nil, failHandler(t))
	err := bp.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1})
	if err == nil {
		t.Fatal("expected error when receipts JSON is undecodable")
	}
	if !strings.Contains(err.Error(), "unmarshalling receipts") {
		t.Errorf("error %q should describe the unmarshalling step", err)
	}
}

func TestBlockProcessor_AggregatesReceiptErrorsAndProcessesAll(t *testing.T) {
	cache := &fakeCache{receipts: receiptsJSON(t, 3)}
	errA := errors.New("receipt 0 boom")
	errC := errors.New("receipt 2 boom")
	var calls int
	bp := NewBlockProcessor(cache, nil, func(context.Context, shared.TransactionReceipt, int64, int64, int, time.Time) error {
		defer func() { calls++ }()
		switch calls {
		case 0:
			return errA
		case 2:
			return errC
		default:
			return nil
		}
	})

	err := bp.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1})
	if err == nil {
		t.Fatal("expected a joined error")
	}
	if calls != 3 {
		t.Errorf("handler called %d times, want 3 (a failing receipt must not abort the rest)", calls)
	}
	if !errors.Is(err, errA) || !errors.Is(err, errC) {
		t.Errorf("joined error %v must wrap both failing receipts", err)
	}
}

func TestBlockProcessor_StopsOnContextCancellation(t *testing.T) {
	cache := &fakeCache{receipts: receiptsJSON(t, 5)}
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	bp := NewBlockProcessor(cache, nil, func(context.Context, shared.TransactionReceipt, int64, int64, int, time.Time) error {
		calls++
		cancel() // a deadline/shutdown fires after the first receipt
		return nil
	})

	err := bp.ProcessBlockEvent(ctx, outbound.BlockEvent{ChainID: 1, BlockNumber: 1})
	if err == nil {
		t.Fatal("expected an error when the context is cancelled mid-block")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error %v should wrap context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("processReceipt called %d times, want 1 (loop must stop once ctx is cancelled)", calls)
	}
}

func TestBlockProcessor_RecordsTelemetryByStatus(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})

	tel, err := dextelemetry.NewTelemetry("curve", 1)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	okCache := &fakeCache{receipts: receiptsJSON(t, 1)}
	okBP := NewBlockProcessor(okCache, tel, func(context.Context, shared.TransactionReceipt, int64, int64, int, time.Time) error { return nil })
	if err := okBP.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1}); err != nil {
		t.Fatalf("happy ProcessBlockEvent: %v", err)
	}

	failCache := &fakeCache{err: errors.New("boom")}
	failBP := NewBlockProcessor(failCache, tel, func(context.Context, shared.TransactionReceipt, int64, int64, int, time.Time) error { return nil })
	if err := failBP.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 2}); err == nil {
		t.Fatal("expected error from failing cache")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	success, errCount := readBlockCountersByStatus(t, &rm, "curve.blocks.processed")
	if success != 1 {
		t.Errorf("status=success count = %d, want 1", success)
	}
	if errCount != 1 {
		t.Errorf("status=error count = %d, want 1", errCount)
	}
	if got := readSumCount(t, &rm, "curve.errors.total"); got != 1 {
		t.Errorf("curve.errors.total = %d, want 1", got)
	}
}

func failHandler(t *testing.T) ReceiptHandler {
	t.Helper()
	return func(context.Context, shared.TransactionReceipt, int64, int64, int, time.Time) error {
		t.Error("receipt handler must not be called on this path")
		return nil
	}
}

func readBlockCountersByStatus(t *testing.T, rm *metricdata.ResourceMetrics, name string) (success, errCount int64) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s: unexpected metric type %T", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				status, _ := dp.Attributes.Value("status")
				switch status.AsString() {
				case "success":
					success += dp.Value
				case "error":
					errCount += dp.Value
				}
			}
			return success, errCount
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0, 0
}

func readSumCount(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s: unexpected metric type %T", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func TestProcessBlockEvent_FinalizerRunsAfterReceipts(t *testing.T) {
	var order []string
	rh := func(ctx context.Context, r shared.TransactionReceipt, chainID, blockNumber int64, version int, ts time.Time) error {
		order = append(order, "receipt")
		return nil
	}
	fin := func(ctx context.Context, event outbound.BlockEvent) error {
		order = append(order, "finalize")
		return nil
	}
	p := NewBlockProcessorWithFinalizer(fakeCacheWithReceipts(t, 2), nil, rh, fin)
	err := p.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"receipt", "receipt", "finalize"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestProcessBlockEvent_FinalizerErrorPropagates(t *testing.T) {
	rh := func(ctx context.Context, r shared.TransactionReceipt, chainID, blockNumber int64, version int, ts time.Time) error {
		return nil
	}
	fin := func(ctx context.Context, event outbound.BlockEvent) error { return errors.New("boom") }
	p := NewBlockProcessorWithFinalizer(fakeCacheWithReceipts(t, 1), nil, rh, fin)
	err := p.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected finalize error to propagate, got %v", err)
	}
}
