package sqsutil

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

type visibilityChange struct {
	handle     string
	visibility time.Duration
}

type visibilityRelease struct {
	handles    []string
	visibility time.Duration
	deadline   time.Time
}

type mockConsumer struct {
	mu                 sync.Mutex
	batches            [][]outbound.SQSMessage
	deletedHandles     []string
	visibilityReleases []visibilityRelease
	deleteErrFor       map[string]error
	visibilityErr      error
	visibilityPartial  map[string]error
	visibilityRefusals map[string]error
	visibilityTimeout  time.Duration // 0 -> a safe default well above the handler budget

	receive      func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
	beforeDelete func()
	onRelease    func()
}

func (m *mockConsumer) VisibilityTimeout() time.Duration {
	if m.visibilityTimeout > 0 {
		return m.visibilityTimeout
	}
	return 300 * time.Second
}

func (m *mockConsumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	if m.receive != nil {
		return m.receive(ctx, maxMessages)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.batches) == 0 {
		return nil, nil
	}
	batch := m.batches[0]
	m.batches = m.batches[1:]
	return batch, nil
}

// DeleteMessage refuses a cancelled context the way the AWS SDK does, so a
// test catches cleanup that is still riding the shutdown-cancelled context.
func (m *mockConsumer) DeleteMessage(ctx context.Context, handle string) error {
	if m.beforeDelete != nil {
		m.beforeDelete()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err, ok := m.deleteErrFor[handle]; ok {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedHandles = append(m.deletedHandles, handle)
	return nil
}

func (m *mockConsumer) ChangeMessageVisibilityBatch(ctx context.Context, handles []string, visibility time.Duration) (map[string]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deadline, _ := ctx.Deadline()
	m.recordRelease(visibilityRelease{handles: slices.Clone(handles), visibility: visibility, deadline: deadline})
	if m.onRelease != nil {
		m.onRelease()
	}
	if m.visibilityErr != nil {
		return m.visibilityPartial, m.visibilityErr
	}
	return m.refusalsFor(handles), nil
}

func (m *mockConsumer) recordRelease(release visibilityRelease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.visibilityReleases = append(m.visibilityReleases, release)
}

func (m *mockConsumer) refusalsFor(handles []string) map[string]error {
	refusals := make(map[string]error)
	for _, handle := range handles {
		if err, ok := m.visibilityRefusals[handle]; ok {
			refusals[handle] = err
		}
	}
	return refusals
}

func (m *mockConsumer) Close() error { return nil }

func (m *mockConsumer) deleted() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.deletedHandles)
}

func (m *mockConsumer) released() []visibilityChange {
	var changes []visibilityChange
	for _, release := range m.releaseCalls() {
		for _, handle := range release.handles {
			changes = append(changes, visibilityChange{handle: handle, visibility: release.visibility})
		}
	}
	return changes
}

func (m *mockConsumer) releaseCalls() []visibilityRelease {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.visibilityReleases)
}

func makeMsg(id, handle string, event outbound.BlockEvent) outbound.SQSMessage {
	body, _ := json.Marshal(event)
	return outbound.SQSMessage{MessageID: id, ReceiptHandle: handle, Body: string(body)}
}

func testConfig(consumer *mockConsumer) Config {
	return Config{
		Consumer:    consumer,
		MaxMessages: 1,
		Logger:      slog.Default(),
		ChainID:     1,
	}
}

func startRunLoop(consumer *mockConsumer, logger *slog.Logger, handler BlockEventHandler) (context.CancelFunc, <-chan struct{}) {
	return startLoop(runLoopConfig(consumer, logger), handler)
}

func runLoopConfig(consumer *mockConsumer, logger *slog.Logger) Config {
	return Config{
		Consumer:     consumer,
		MaxMessages:  1,
		PollInterval: 10 * time.Millisecond,
		Logger:       logger,
		ChainID:      1,
	}
}

func startLoop(cfg Config, handler BlockEventHandler) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunLoop(ctx, cfg, handler)
	}()
	return cancel, done
}

func noopHandler(context.Context, outbound.BlockEvent) error { return nil }

func awaitLoopExit(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunLoop did not return after cancellation")
	}
}

func signalOnce(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func blockEvent(number int64) outbound.BlockEvent {
	return outbound.BlockEvent{ChainID: 1, BlockNumber: number, Version: 0, BlockHash: "0xabc"}
}

func recordingConfig(consumer *mockConsumer) (Config, *testutil.SlogRecorder) {
	recorder := &testutil.SlogRecorder{}
	cfg := testConfig(consumer)
	cfg.Logger = slog.New(recorder)
	return cfg, recorder
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertNoErrorLogs(t *testing.T, recorder *testutil.SlogRecorder) {
	t.Helper()
	if logged := recorder.MessagesAt(slog.LevelError); len(logged) > 0 {
		t.Errorf("expected no ERROR records on the shutdown path, got %v", logged)
	}
}

type settleKey struct {
	op     string
	status string
}

// Keyed off the wire name and labels rather than the package constants: the
// alert rules match on exactly these strings.
func collectSettleCounter(t *testing.T, reader sdkmetric.Reader) map[settleKey]int64 {
	t.Helper()
	counts := make(map[settleKey]int64)
	for _, dp := range settleDataPoints(t, reader) {
		op, _ := dp.Attributes.Value("op")
		status, _ := dp.Attributes.Value("status")
		counts[settleKey{op: op.AsString(), status: status.AsString()}] += dp.Value
	}
	return counts
}

func collectSettleChains(t *testing.T, reader sdkmetric.Reader) []string {
	t.Helper()
	var chains []string
	for _, dp := range settleDataPoints(t, reader) {
		chain, _ := dp.Attributes.Value("chain")
		chains = append(chains, chain.AsString())
	}
	return chains
}

func settleDataPoints(t *testing.T, reader sdkmetric.Reader) []metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	var points []metricdata.DataPoint[int64]
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "sqs.message.settles.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Sum[int64]", m.Name, m.Data)
			}
			points = append(points, sum.DataPoints...)
		}
	}
	return points
}

func installManualMeterProvider(t *testing.T) sdkmetric.Reader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = mp.Shutdown(context.Background())
	})
	return reader
}
