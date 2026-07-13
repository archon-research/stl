package dexconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// fakeReorg is a test ReorgChecker returning a fixed verdict / error.
type fakeReorg struct {
	out   bool
	err   error
	calls int
}

func (f *fakeReorg) ReorgedOut(context.Context, int64, common.Hash) (bool, error) {
	f.calls++
	return f.out, f.err
}

// errHandler returns a BlockHandler that always fails with err.
func errHandler(err error) BlockHandler {
	return func(context.Context, outbound.BlockEvent, []shared.TransactionReceipt) error { return err }
}

// okHandler returns a BlockHandler that always succeeds.
func okHandler() BlockHandler {
	return func(context.Context, outbound.BlockEvent, []shared.TransactionReceipt) error { return nil }
}

// The version-0 block hash observed reorged out of staging (block 25512663).
const reorgedEventHashHex = "0x5a94ae0445960dc11f7bf6048201a2ad600f2ad2fac663b4c45fd9ede414424f"

// reorgedEvent builds a well-formed BlockEvent for the reorg tests.
func reorgedEvent() outbound.BlockEvent {
	return outbound.BlockEvent{ChainID: 1, BlockNumber: 25512663, Version: 0, BlockHash: reorgedEventHashHex, BlockTimestamp: 1}
}

// A handler error on a block PROVEN reorged out (finalized chain holds a
// different block) is acked (nil), not retried/DLQ'd.
func TestProcessBlockEvent_SkipsReorgedOutBlockOnHandlerError(t *testing.T) {
	cache := fakeCacheWithReceipts(t, 1)
	rc := &fakeReorg{out: true}
	bp := NewBlockProcessor(cache, nil, errHandler(errors.New("block not found")), WithReorgChecker(rc))

	if err := bp.ProcessBlockEvent(context.Background(), reorgedEvent()); err != nil {
		t.Fatalf("reorged-out block must be acked (nil), got: %v", err)
	}
	if rc.calls != 1 {
		t.Errorf("reorg checker called %d times, want 1", rc.calls)
	}
}

// A handler error on a block that is NOT reorged out (still canonical, or not
// yet finalized so unjudgeable) must propagate — the failure is real.
func TestProcessBlockEvent_PropagatesErrorWhenNotReorgedOut(t *testing.T) {
	cache := fakeCacheWithReceipts(t, 1)
	rc := &fakeReorg{out: false} // canonical, or above the finalized head
	bp := NewBlockProcessor(cache, nil, errHandler(errors.New("db timeout")), WithReorgChecker(rc))

	if err := bp.ProcessBlockEvent(context.Background(), reorgedEvent()); err == nil {
		t.Fatal("a block not proven reorged out must propagate its handler error (retry), got nil")
	}
}

// If the reorg check cannot be determined, never drop the block: propagate the
// handler error so it retries rather than risk acking a still-canonical block.
func TestProcessBlockEvent_PropagatesErrorWhenReorgCheckFails(t *testing.T) {
	cache := fakeCacheWithReceipts(t, 1)
	rc := &fakeReorg{err: errors.New("rpc unavailable")}
	bp := NewBlockProcessor(cache, nil, errHandler(errors.New("block not found")), WithReorgChecker(rc))

	if err := bp.ProcessBlockEvent(context.Background(), reorgedEvent()); err == nil {
		t.Fatal("an inconclusive reorg check must propagate the error (retry), got nil")
	}
}

// Without a reorg checker wired, the original always-retry behaviour holds.
func TestProcessBlockEvent_NoCheckerPropagatesError(t *testing.T) {
	cache := fakeCacheWithReceipts(t, 1)
	bp := NewBlockProcessor(cache, nil, errHandler(errors.New("block not found")))

	if err := bp.ProcessBlockEvent(context.Background(), reorgedEvent()); err == nil {
		t.Fatal("without a reorg checker, a handler error must propagate, got nil")
	}
}

// A malformed event hash is a defect, not a reorg: propagate (retry) and never
// even consult the checker.
func TestProcessBlockEvent_MalformedHashPropagatesWithoutConsultingChecker(t *testing.T) {
	cache := fakeCacheWithReceipts(t, 1)
	rc := &fakeReorg{out: true} // would skip if consulted — it must not be
	ev := reorgedEvent()
	ev.BlockHash = "0xabc" // malformed -> ParsedBlockHash errors
	bp := NewBlockProcessor(cache, nil, errHandler(errors.New("block not found")), WithReorgChecker(rc))

	if err := bp.ProcessBlockEvent(context.Background(), ev); err == nil {
		t.Fatal("a malformed block hash must not be treated as reorged; the error must propagate")
	}
	if rc.calls != 0 {
		t.Errorf("reorg checker consulted %d times despite an unusable hash, want 0", rc.calls)
	}
}

// The happy path must not pay for the reorg check (no wasted RPC per block).
func TestProcessBlockEvent_HappyPathDoesNotConsultChecker(t *testing.T) {
	cache := fakeCacheWithReceipts(t, 1)
	rc := &fakeReorg{out: true}
	bp := NewBlockProcessor(cache, nil, okHandler(), WithReorgChecker(rc))

	if err := bp.ProcessBlockEvent(context.Background(), reorgedEvent()); err != nil {
		t.Fatalf("ProcessBlockEvent: %v", err)
	}
	if rc.calls != 0 {
		t.Errorf("reorg checker consulted %d times on the success path, want 0", rc.calls)
	}
}

// A reorg-skip must increment reorg_skipped AND must NOT be counted as a
// processed block: counting it status="success" would keep blocks_processed
// above zero and hide a skip storm from the Stalled alert.
func TestProcessBlockEvent_ReorgSkipMetrics(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		if err := mp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	})

	tel, err := dextelemetry.NewTelemetry("curve", 1)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	bp := NewBlockProcessor(fakeCacheWithReceipts(t, 1), tel,
		errHandler(errors.New("block not found")), WithReorgChecker(&fakeReorg{out: true}))
	if err := bp.ProcessBlockEvent(context.Background(), reorgedEvent()); err != nil {
		t.Fatalf("reorged-out block must be acked, got: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got := readSumCount(t, &rm, "curve.reorg.skipped"); got != 1 {
		t.Errorf("curve.reorg.skipped = %d, want 1", got)
	}
	// blocks.processed must not have been recorded at all for the skip.
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "curve.blocks.processed" {
				t.Errorf("a reorg-skip was recorded as a processed block (%s); it must not inflate the success rate", m.Name)
			}
		}
	}
}

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

// failHandler returns a BlockHandler that fails the test if it is ever called.
func failHandler(t *testing.T) BlockHandler {
	t.Helper()
	return func(context.Context, outbound.BlockEvent, []shared.TransactionReceipt) error {
		t.Error("BlockHandler must not be called on this path")
		return nil
	}
}

// TestBlockProcessor_HandlerReceivesAllReceiptsAndBlockCoordinates: the handler
// is called once with the full receipts slice and the correct event coordinates.
func TestBlockProcessor_HandlerReceivesAllReceiptsAndBlockCoordinates(t *testing.T) {
	cache := &fakeCache{receipts: receiptsJSON(t, 3)}
	var calls int
	var gotEvent outbound.BlockEvent
	var gotReceipts []shared.TransactionReceipt

	bp := NewBlockProcessor(cache, nil, func(_ context.Context, ev outbound.BlockEvent, recs []shared.TransactionReceipt) error {
		calls++
		gotEvent = ev
		gotReceipts = recs
		return nil
	})

	event := outbound.BlockEvent{ChainID: 8453, BlockNumber: 100, Version: 2, BlockTimestamp: 1_700_000_000}
	if err := bp.ProcessBlockEvent(context.Background(), event); err != nil {
		t.Fatalf("ProcessBlockEvent: %v", err)
	}

	if calls != 1 {
		t.Errorf("handler called %d times, want 1", calls)
	}
	if cache.gotChain != 8453 || cache.gotBlock != 100 || cache.gotVer != 2 {
		t.Errorf("GetReceipts(%d,%d,%d), want (8453,100,2)", cache.gotChain, cache.gotBlock, cache.gotVer)
	}
	if gotEvent.ChainID != 8453 || gotEvent.BlockNumber != 100 || gotEvent.Version != 2 {
		t.Errorf("handler got event coords (%d,%d,%d), want (8453,100,2)",
			gotEvent.ChainID, gotEvent.BlockNumber, gotEvent.Version)
	}
	wantTS := time.Unix(1_700_000_000, 0).UTC()
	ts := time.Unix(gotEvent.BlockTimestamp, 0).UTC()
	if !ts.Equal(wantTS) {
		t.Errorf("handler got timestamp %v, want %v", ts, wantTS)
	}
	if len(gotReceipts) != 3 {
		t.Errorf("handler got %d receipts, want 3", len(gotReceipts))
	}
	// Verify receipt identity: the handler receives the exact objects from the cache,
	// not a re-serialised copy with lost fields.
	if len(gotReceipts) > 0 && gotReceipts[0].TransactionHash != "0xabc" {
		t.Errorf("gotReceipts[0].TransactionHash = %q, want %q", gotReceipts[0].TransactionHash, "0xabc")
	}
}

// TestBlockProcessor_EmptyBlock_StillCallsHandler: a block with zero receipts
// must still invoke the handler once with an empty slice so the coordinator can
// take sweep snapshots.
func TestBlockProcessor_EmptyBlock_StillCallsHandler(t *testing.T) {
	cache := &fakeCache{receipts: receiptsJSON(t, 0)}
	var calls int
	var gotLen int
	bp := NewBlockProcessor(cache, nil, func(_ context.Context, _ outbound.BlockEvent, recs []shared.TransactionReceipt) error {
		calls++
		gotLen = len(recs)
		return nil
	})

	if err := bp.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1}); err != nil {
		t.Fatalf("ProcessBlockEvent: %v", err)
	}
	if calls != 1 {
		t.Errorf("handler called %d times, want 1 (empty block must still call handler)", calls)
	}
	if gotLen != 0 {
		t.Errorf("handler got %d receipts, want 0", gotLen)
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

// TestBlockProcessor_HandlerError_PropagatesForRedelivery: when the handler
// returns an error the SQS message must NOT be acked; ProcessBlockEvent returns
// that same error so the message redelivers.
func TestBlockProcessor_HandlerError_PropagatesForRedelivery(t *testing.T) {
	cache := fakeCacheWithReceipts(t, 1)
	handlerErr := errors.New("handler boom")
	bp := NewBlockProcessor(cache, nil, func(context.Context, outbound.BlockEvent, []shared.TransactionReceipt) error {
		return handlerErr
	})
	err := bp.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1})
	if err == nil {
		t.Fatal("expected error from failing handler")
	}
	if !errors.Is(err, handlerErr) {
		t.Errorf("error %v does not wrap the handler error", err)
	}
}

func TestBlockProcessor_RecordsTelemetryByStatus(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		if err := mp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	})

	tel, err := dextelemetry.NewTelemetry("curve", 1)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	okCache := fakeCacheWithReceipts(t, 1)
	okBP := NewBlockProcessor(okCache, tel, func(context.Context, outbound.BlockEvent, []shared.TransactionReceipt) error { return nil })
	if err := okBP.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1}); err != nil {
		t.Fatalf("happy ProcessBlockEvent: %v", err)
	}

	failCache := &fakeCache{err: errors.New("boom")}
	failBP := NewBlockProcessor(failCache, tel, func(context.Context, outbound.BlockEvent, []shared.TransactionReceipt) error { return nil })
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

// TestBlockProcessor_RecordsErrorOperationLabel: a cache error records
// curve.errors.total with operation="fetchReceipts".
func TestBlockProcessor_RecordsErrorOperationLabel(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		if err := mp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	})

	tel, err := dextelemetry.NewTelemetry("curve", 1)
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}

	failCache := &fakeCache{err: errors.New("redis down")}
	bp := NewBlockProcessor(failCache, tel, failHandler(t))
	if err := bp.ProcessBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 1}); err == nil {
		t.Fatal("expected error from failing cache")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "curve.errors.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("curve.errors.total: unexpected metric type %T", m.Data)
			}
			if len(sum.DataPoints) == 0 {
				t.Fatal("curve.errors.total: no datapoints")
			}
			op, _ := sum.DataPoints[0].Attributes.Value("operation")
			if got := op.AsString(); got != "fetchReceipts" {
				t.Errorf("operation attribute = %q, want %q", got, "fetchReceipts")
			}
			return
		}
	}
	t.Fatal("metric curve.errors.total not found")
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
