package psm3_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/psm3"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	testBlockNum       = 31000000
	testChainID  int64 = 8453
)

var testPSM3Address = common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E")

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeBlockQuerier returns a fixed block number.
type fakeBlockQuerier struct {
	mu       sync.Mutex
	blockNum uint64
	err      error
}

func newFakeBlockQuerier(blockNum uint64) *fakeBlockQuerier {
	return &fakeBlockQuerier{blockNum: blockNum}
}

func (f *fakeBlockQuerier) BlockNumber(_ context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blockNum, f.err
}

// fakePSM3Caller is a controllable in-memory PSM3Caller.
type fakePSM3Caller struct {
	mu           sync.Mutex
	resolveErr   error
	readErr      error
	state        entity.PSM3State
	resolveBlock *big.Int
	readBlocks   []int64
	readAttempts int
}

func newFakePSM3Caller() *fakePSM3Caller {
	return &fakePSM3Caller{
		state: entity.PSM3State{
			USDSBalance:    big.NewInt(1_000_000),
			SUSDSBalance:   big.NewInt(2_000_000),
			USDCBalance:    big.NewInt(3_000_000),
			TotalAssets:    big.NewInt(6_000_000),
			ConversionRate: big.NewInt(1_050_000),
		},
	}
}

func (f *fakePSM3Caller) ResolveImmutables(_ context.Context, blockNumber *big.Int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if blockNumber == nil {
		// Mirror the production Multicaller, which rejects nil block numbers.
		return errors.New("block number is required")
	}
	if f.resolveErr != nil {
		return f.resolveErr
	}
	f.resolveBlock = new(big.Int).Set(blockNumber)
	return nil
}

// ReadState is now hash-pinned (VEC-471). makeBlockEvents encodes the block
// number into BlockHash as 0x%064x, so decoding the hash back to an int64 lets
// the existing block-number assertions keep working unchanged while proving the
// service threaded the block hash through, not the number.
func (f *fakePSM3Caller) ReadState(_ context.Context, blockHash common.Hash) (*entity.PSM3State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readAttempts++
	if f.readErr != nil {
		return nil, f.readErr
	}
	f.readBlocks = append(f.readBlocks, blockHash.Big().Int64())
	state := f.state
	return &state, nil
}

func (f *fakePSM3Caller) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.readBlocks)
}

// attemptCount counts all ReadState calls, including ones that returned an error.
func (f *fakePSM3Caller) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readAttempts
}

// fakePSM3Repo is a controllable in-memory snapshot repository.
type fakePSM3Repo struct {
	mu      sync.Mutex
	saved   []*entity.PSM3Reserves
	saveErr error
}

func (r *fakePSM3Repo) SaveReserves(_ context.Context, snap *entity.PSM3Reserves) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	r.saved = append(r.saved, snap)
	return nil
}

func (r *fakePSM3Repo) savedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saved)
}

func (r *fakePSM3Repo) allSaved() []*entity.PSM3Reserves {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*entity.PSM3Reserves(nil), r.saved...)
}

// fakeSQSConsumer is a controllable in-memory SQS consumer.
type fakeSQSConsumer struct {
	mu       sync.Mutex
	messages []outbound.SQSMessage
	served   int
	deleted  []string
}

func newFakeSQSConsumer(events []outbound.BlockEvent) *fakeSQSConsumer {
	msgs := make([]outbound.SQSMessage, len(events))
	for i, e := range events {
		body, _ := json.Marshal(e)
		msgs[i] = outbound.SQSMessage{
			MessageID:     fmt.Sprintf("msg-%d", i),
			ReceiptHandle: fmt.Sprintf("handle-%d", i),
			Body:          string(body),
		}
	}
	return &fakeSQSConsumer{messages: msgs}
}

func (f *fakeSQSConsumer) ReceiveMessages(_ context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.served >= len(f.messages) {
		return nil, nil
	}
	end := min(f.served+maxMessages, len(f.messages))
	batch := f.messages[f.served:end]
	f.served = end
	return batch, nil
}

func (f *fakeSQSConsumer) DeleteMessage(_ context.Context, receiptHandle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, receiptHandle)
	return nil
}

func (f *fakeSQSConsumer) Close() error { return nil }

func (f *fakeSQSConsumer) VisibilityTimeout() time.Duration { return 30 * time.Second }

func (f *fakeSQSConsumer) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleted)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makeBlockEvents(startBlock int64, count int, version int) []outbound.BlockEvent {
	events := make([]outbound.BlockEvent, count)
	for i := range events {
		events[i] = outbound.BlockEvent{
			ChainID:        testChainID,
			BlockNumber:    startBlock + int64(i),
			Version:        version,
			BlockHash:      fmt.Sprintf("0x%064x", startBlock+int64(i)),
			ParentHash:     fmt.Sprintf("0x%064x", startBlock+int64(i)-1),
			BlockTimestamp: 1700000000 + startBlock + int64(i),
			ReceivedAt:     time.Now(),
		}
	}
	return events
}

func defaultConfig(sweepEveryN int) psm3.Config {
	return psm3.Config{
		SweepEveryNBlocks: sweepEveryN,
		ChainID:           testChainID,
		PSM3Address:       testPSM3Address,
		MaxMessages:       10,
		PollInterval:      10 * time.Millisecond,
	}
}

func newService(t *testing.T, cfg psm3.Config, caller *fakePSM3Caller, repo *fakePSM3Repo, consumer *fakeSQSConsumer) *psm3.Service {
	t.Helper()
	svc, err := psm3.NewService(cfg, caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	return svc
}

// waitFor polls cond until it returns true or the deadline expires.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out: " + msg)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNewService_Validation(t *testing.T) {
	caller := newFakePSM3Caller()
	repo := &fakePSM3Repo{}
	consumer := newFakeSQSConsumer(nil)
	querier := newFakeBlockQuerier(testBlockNum)

	tests := []struct {
		name    string
		create  func() (*psm3.Service, error)
		wantErr string
	}{
		{"nil caller", func() (*psm3.Service, error) {
			return psm3.NewService(defaultConfig(1), nil, repo, consumer, querier)
		}, "caller"},
		{"nil repo", func() (*psm3.Service, error) {
			return psm3.NewService(defaultConfig(1), caller, nil, consumer, querier)
		}, "repository"},
		{"nil consumer", func() (*psm3.Service, error) {
			return psm3.NewService(defaultConfig(1), caller, repo, nil, querier)
		}, "sqs consumer"},
		{"nil block querier", func() (*psm3.Service, error) {
			return psm3.NewService(defaultConfig(1), caller, repo, consumer, nil)
		}, "block querier"},
		{"zero chain id", func() (*psm3.Service, error) {
			cfg := defaultConfig(1)
			cfg.ChainID = 0
			return psm3.NewService(cfg, caller, repo, consumer, querier)
		}, "chain ID"},
		{"negative chain id", func() (*psm3.Service, error) {
			cfg := defaultConfig(1)
			cfg.ChainID = -1
			return psm3.NewService(cfg, caller, repo, consumer, querier)
		}, "chain ID"},
		{"zero psm3 address", func() (*psm3.Service, error) {
			cfg := defaultConfig(1)
			cfg.PSM3Address = common.Address{}
			return psm3.NewService(cfg, caller, repo, consumer, querier)
		}, "psm3 address"},
		{"negative sweep blocks", func() (*psm3.Service, error) {
			return psm3.NewService(defaultConfig(-1), caller, repo, consumer, querier)
		}, "sweep every n blocks"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.create()
			if err == nil {
				t.Fatalf("expected error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestNewService_Defaults(t *testing.T) {
	cfg := psm3.Config{ChainID: testChainID, PSM3Address: testPSM3Address}
	svc, err := psm3.NewService(cfg, newFakePSM3Caller(), &fakePSM3Repo{}, newFakeSQSConsumer(nil), newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

func TestStart_ResolveImmutablesError(t *testing.T) {
	caller := newFakePSM3Caller()
	caller.resolveErr = errors.New("usds() mismatch")

	svc := newService(t, defaultConfig(1), caller, &fakePSM3Repo{}, newFakeSQSConsumer(nil))

	err := svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when immutable resolution fails")
	}
	if !strings.Contains(err.Error(), "resolve psm3 immutables") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestStart_BlockQuerierError(t *testing.T) {
	bq := newFakeBlockQuerier(testBlockNum)
	bq.err = errors.New("RPC node unreachable")

	svc, err := psm3.NewService(defaultConfig(1), newFakePSM3Caller(), &fakePSM3Repo{}, newFakeSQSConsumer(nil), bq)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when block querier fails")
	}
	if !strings.Contains(err.Error(), "get latest block") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestStart_ResolvesImmutablesAtLatestBlock(t *testing.T) {
	caller := newFakePSM3Caller()
	svc := newService(t, defaultConfig(1), caller, &fakePSM3Repo{}, newFakeSQSConsumer(nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	cancel()
	_ = svc.Stop()

	if caller.resolveBlock == nil || caller.resolveBlock.Int64() != testBlockNum {
		t.Errorf("ResolveImmutables block = %v, want %d (pinned to the latest block)", caller.resolveBlock, testBlockNum)
	}
}

func TestStart_Stop_Clean(t *testing.T) {
	svc := newService(t, defaultConfig(1), newFakePSM3Caller(), &fakePSM3Repo{}, newFakeSQSConsumer(nil))

	ctx, cancel := context.WithCancel(context.Background())
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

func TestStop_WithoutStart(t *testing.T) {
	svc := newService(t, defaultConfig(1), newFakePSM3Caller(), &fakePSM3Repo{}, newFakeSQSConsumer(nil))
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Sweep tests
// ---------------------------------------------------------------------------

func TestSweep_WritesSnapshot(t *testing.T) {
	caller := newFakePSM3Caller()
	repo := &fakePSM3Repo{}
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, 1, 0))
	svc := newService(t, defaultConfig(1), caller, repo, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	waitFor(t, func() bool { return repo.savedCount() >= 1 }, "snapshot not saved")
	cancel()
	_ = svc.Stop()

	snap := repo.allSaved()[0]
	if snap.ChainID != testChainID {
		t.Errorf("ChainID = %d, want %d", snap.ChainID, testChainID)
	}
	if snap.Address != testPSM3Address {
		t.Errorf("Address = %s, want %s", snap.Address.Hex(), testPSM3Address.Hex())
	}
	if snap.BlockNumber != testBlockNum {
		t.Errorf("BlockNumber = %d, want %d", snap.BlockNumber, testBlockNum)
	}
	if snap.Source != "sweep" {
		t.Errorf("Source = %q, want %q", snap.Source, "sweep")
	}
	wantTS := time.Unix(1700000000+testBlockNum, 0).UTC()
	if !snap.BlockTimestamp.Equal(wantTS) {
		t.Errorf("BlockTimestamp = %v, want %v", snap.BlockTimestamp, wantTS)
	}
	if snap.State.USDSBalance.Cmp(caller.state.USDSBalance) != 0 ||
		snap.State.SUSDSBalance.Cmp(caller.state.SUSDSBalance) != 0 ||
		snap.State.USDCBalance.Cmp(caller.state.USDCBalance) != 0 ||
		snap.State.TotalAssets.Cmp(caller.state.TotalAssets) != 0 ||
		snap.State.ConversionRate.Cmp(caller.state.ConversionRate) != 0 {
		t.Errorf("snapshot state %+v does not match caller state %+v", snap.State, caller.state)
	}

	// The read must be pinned to the event's block.
	if got := caller.readBlocks[0]; got != testBlockNum {
		t.Errorf("ReadState block = %d, want %d", got, testBlockNum)
	}

	// ACK: the message must have been deleted.
	if got := consumer.deleteCount(); got != 1 {
		t.Errorf("expected 1 deleted message, got %d", got)
	}
}

func TestSweep_BlockVersionPassthrough(t *testing.T) {
	caller := newFakePSM3Caller()
	repo := &fakePSM3Repo{}
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, 1, 3))
	svc := newService(t, defaultConfig(1), caller, repo, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	waitFor(t, func() bool { return repo.savedCount() >= 1 }, "snapshot not saved")
	cancel()
	_ = svc.Stop()

	if got := repo.allSaved()[0].BlockVersion; got != 3 {
		t.Errorf("BlockVersion = %d, want 3 (reorg re-emission must flow through)", got)
	}
}

func TestSweep_Cadence(t *testing.T) {
	// 10 blocks with sweep every 3: first block triggers an immediate read
	// (blocksSinceSweep initialized to N-1), then blocks 4, 7, 10 = 4 sweeps.
	const sweepN = 3
	const numBlocks = 10

	caller := newFakePSM3Caller()
	repo := &fakePSM3Repo{}
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, numBlocks, 0))
	svc := newService(t, defaultConfig(sweepN), caller, repo, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// All 10 messages are processed once all 10 are deleted (non-sweep blocks ACK too).
	waitFor(t, func() bool { return consumer.deleteCount() >= numBlocks }, "messages not all processed")
	cancel()
	_ = svc.Stop()

	if got := repo.savedCount(); got != 4 {
		t.Errorf("expected 4 snapshots with sweep every %d blocks over %d blocks, got %d", sweepN, numBlocks, got)
	}
	wantBlocks := []int64{testBlockNum, testBlockNum + 3, testBlockNum + 6, testBlockNum + 9}
	for i, snap := range repo.allSaved() {
		if i < len(wantBlocks) && snap.BlockNumber != wantBlocks[i] {
			t.Errorf("sweep %d at block %d, want %d", i, snap.BlockNumber, wantBlocks[i])
		}
	}
	if got := caller.readCount(); got != 4 {
		t.Errorf("expected 4 chain reads, got %d", got)
	}
}

func TestSweep_PersistentError_DoesNotStorm(t *testing.T) {
	// A sweep that keeps failing must not turn every subsequent block into a
	// sweep attempt: the cadence budget is consumed even on error. With sweep
	// every 3 over 10 blocks, only blocks 1, 4, 7, 10 may attempt a read (4),
	// never one-per-block (10).
	const sweepN = 3
	const numBlocks = 10

	caller := newFakePSM3Caller()
	caller.readErr = errors.New("multicall RPC failure")
	repo := &fakePSM3Repo{}
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, numBlocks, 0))
	svc := newService(t, defaultConfig(sweepN), caller, repo, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Every block is ACKed (deleted), including the 4 failed sweeps — a failed
	// sweep skips the interval rather than NACKing.
	waitFor(t, func() bool { return consumer.deleteCount() >= numBlocks }, "blocks not all processed")
	cancel()
	_ = svc.Stop()

	if got := caller.attemptCount(); got != 4 {
		t.Errorf("expected 4 read attempts (no per-block storm), got %d", got)
	}
	if got := repo.savedCount(); got != 0 {
		t.Errorf("expected 0 saved snapshots on persistent read failure, got %d", got)
	}
}

func TestSweep_ReadStateError_NoSaveACKsAndSkips(t *testing.T) {
	caller := newFakePSM3Caller()
	caller.readErr = errors.New("multicall RPC failure")
	repo := &fakePSM3Repo{}
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, 1, 0))
	svc := newService(t, defaultConfig(1), caller, repo, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// A failed sweep ACKs (deletes) the block and skips the interval — it must
	// not NACK, which would head-of-line block the chain's FIFO group.
	waitFor(t, func() bool { return consumer.deleteCount() >= 1 }, "block not ACKed after failed sweep")
	cancel()
	_ = svc.Stop()

	if got := repo.savedCount(); got != 0 {
		t.Errorf("expected 0 saved snapshots on read failure, got %d", got)
	}
}

// TestSweep_MissingBlockHash_NoCallerReadACKsAndSkips: an event with an empty
// BlockHash must fail loud before ever reaching the PSM3 caller, instead of
// silently defaulting to the zero hash (common.HexToHash never errors). Like
// every other sweep failure, this is logged and ACKed rather than NACKed (see
// processBlock's cadence-clock rationale), so the observable signal is "no
// chain read, no snapshot saved" rather than a returned error.
func TestSweep_MissingBlockHash_NoCallerReadACKsAndSkips(t *testing.T) {
	caller := newFakePSM3Caller()
	repo := &fakePSM3Repo{}
	events := []outbound.BlockEvent{{
		ChainID:        testChainID,
		BlockNumber:    testBlockNum,
		Version:        0,
		BlockHash:      "",
		ParentHash:     fmt.Sprintf("0x%064x", testBlockNum-1),
		BlockTimestamp: 1700000000 + testBlockNum,
		ReceivedAt:     time.Now(),
	}}
	consumer := newFakeSQSConsumer(events)
	svc := newService(t, defaultConfig(1), caller, repo, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	waitFor(t, func() bool { return consumer.deleteCount() >= 1 }, "block not ACKed after missing block hash")
	cancel()
	_ = svc.Stop()

	if got := caller.attemptCount(); got != 0 {
		t.Errorf("expected 0 ReadState attempts (guard must fire before the caller), got %d", got)
	}
	if got := repo.savedCount(); got != 0 {
		t.Errorf("expected 0 saved snapshots on missing block hash, got %d", got)
	}
}

func TestSweep_SaveReservesError_ACKsAndSkips(t *testing.T) {
	caller := newFakePSM3Caller()
	repo := &fakePSM3Repo{saveErr: errors.New("database write failed")}
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, 1, 0))
	svc := newService(t, defaultConfig(1), caller, repo, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	waitFor(t, func() bool { return consumer.deleteCount() >= 1 }, "block not ACKed after failed save")
	cancel()
	_ = svc.Stop()

	if got := repo.savedCount(); got != 0 {
		t.Errorf("expected 0 saved snapshots, got %d", got)
	}
}

func TestSweep_RecordsSuccessMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := psm3.NewTelemetryWithProvider(mp, "base")
	if err != nil {
		t.Fatalf("telemetry: %v", err)
	}

	cfg := defaultConfig(1)
	cfg.Telemetry = tel
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, 1, 0))
	svc := newService(t, cfg, newFakePSM3Caller(), &fakePSM3Repo{}, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, func() bool { return consumer.deleteCount() >= 1 }, "block not processed")
	cancel()
	_ = svc.Stop()

	if got := sweepCount(t, reader, "success"); got != 1 {
		t.Errorf("psm3.sweeps.total{status=success} = %d, want 1", got)
	}
}

func TestSweep_RecordsErrorMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := psm3.NewTelemetryWithProvider(mp, "base")
	if err != nil {
		t.Fatalf("telemetry: %v", err)
	}

	caller := newFakePSM3Caller()
	caller.readErr = errors.New("multicall RPC failure")
	cfg := defaultConfig(1)
	cfg.Telemetry = tel
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, 1, 0))
	svc := newService(t, cfg, caller, &fakePSM3Repo{}, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, func() bool { return consumer.deleteCount() >= 1 }, "block not processed")
	cancel()
	_ = svc.Stop()

	if got := sweepCount(t, reader, "error"); got != 1 {
		t.Errorf("psm3.sweeps.total{status=error} = %d, want 1", got)
	}
}

// sweepCount sums psm3.sweeps.total data points matching the given status.
func sweepCount(t *testing.T, reader sdkmetric.Reader, status string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "psm3.sweeps.total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("psm3.sweeps.total is %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				if s, _ := dp.Attributes.Value("status"); s.AsString() == status {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func TestSweep_InvalidStateFromCaller_NoSave(t *testing.T) {
	caller := newFakePSM3Caller()
	caller.state.USDSBalance = nil // structural defect in the caller's response
	repo := &fakePSM3Repo{}
	consumer := newFakeSQSConsumer(makeBlockEvents(testBlockNum, 1, 0))
	svc := newService(t, defaultConfig(1), caller, repo, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	waitFor(t, func() bool { return consumer.deleteCount() >= 1 }, "block not ACKed after invalid state")
	cancel()
	_ = svc.Stop()

	if got := repo.savedCount(); got != 0 {
		t.Errorf("expected 0 saved snapshots for invalid state, got %d", got)
	}
}
