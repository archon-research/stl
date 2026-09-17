package archivingwire

import (
	"context"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// drainTestBudget is short enough to assert on a drain's wall clock without
// spending the production DrainTimeout on every run of the suite.
const drainTestBudget = 50 * time.Millisecond

const (
	testChain  = "mainnet"
	testSource = "oracle-price"
)

func TestNewDrain_WaitsOutItsBudgetThenWarnsOnce(t *testing.T) {
	gate := gateHoldingAStuckWrite(t)
	writes, _ := newTestWriteCounter(t)
	recorder := &testutil.SlogRecorder{}

	started := time.Now()
	newDrain(gate, slog.New(recorder), writes, drainTestBudget)()

	elapsed := time.Since(started)
	if elapsed >= 2*drainTestBudget {
		t.Errorf("expected the drain bounded by %s, it took %s", drainTestBudget, elapsed)
	}
	if elapsed < drainTestBudget {
		t.Errorf("expected the drain to wait out its %s budget, it gave up after %s", drainTestBudget, elapsed)
	}
	if got := recorder.CountWarn("archive drain budget expired"); got != 1 {
		t.Errorf("expected one abandoned-drain warning, got %d", got)
	}
}

// A write the budget kills is gone for good: its queue message is already
// deleted, and it reaches neither the success nor the error count.
func TestNewDrain_CountsTheWritesItAbandonsAsLost(t *testing.T) {
	gate := gateHoldingAStuckWrite(t)
	writes, reader := newTestWriteCounter(t)

	newDrain(gate, slog.New(&testutil.SlogRecorder{}), writes, drainTestBudget)()

	if got, _ := counterForStatus(t, reader, "archive.writes.total", archiving.WriteStatusLost); got != 1 {
		t.Errorf("archive.writes.total{status=%s} = %d, want 1", archiving.WriteStatusLost, got)
	}
}

// A drain that finishes inside its budget lost nothing, so it must leave the
// lost count alone rather than stamping every clean shutdown with a zero: a
// zero-valued series still reads as a lost-writes series that exists.
func TestNewDrain_CountsNothingLostWhenTheWritesFinish(t *testing.T) {
	gate := archiving.NewDrainGate(nil)
	writes, reader := newTestWriteCounter(t)
	gate.Go(func() {})

	newDrain(gate, slog.New(&testutil.SlogRecorder{}), writes, time.Minute)()

	if got, ok := counterForStatus(t, reader, "archive.writes.total", archiving.WriteStatusLost); ok {
		t.Errorf("archive.writes.total{status=%s} exists with value %d after a clean drain, want no data point at all",
			archiving.WriteStatusLost, got)
	}
}

// DrainTimeout is sized for the slowest write, not for the common shutdown:
// paying it on every rollout would add most of a minute to each pod's exit.
func TestNewDrain_ReturnsWithoutSpendingItsBudgetWhenNothingIsInFlight(t *testing.T) {
	gate := archiving.NewDrainGate(nil)
	writes, _ := newTestWriteCounter(t)

	started := time.Now()
	newDrain(gate, slog.New(&testutil.SlogRecorder{}), writes, time.Minute)()

	if elapsed := time.Since(started); elapsed > drainTestBudget {
		t.Errorf("a drain with nothing in flight took %s; it must return without spending its %s budget",
			elapsed, time.Minute)
	}
}

// A zero budget would abandon every write in flight on every shutdown, so it
// must floor rather than expire the drain the moment it starts.
func TestNewDrain_FloorsANonPositiveBudget(t *testing.T) {
	gate := archiving.NewDrainGate(nil)
	writes, reader := newTestWriteCounter(t)
	landed := make(chan struct{})
	gate.Go(func() { time.Sleep(20 * time.Millisecond); close(landed) })

	newDrain(gate, slog.New(&testutil.SlogRecorder{}), writes, 0)()

	<-landed
	if got, ok := counterForStatus(t, reader, "archive.writes.total", archiving.WriteStatusLost); ok {
		t.Errorf("archive.writes.total{status=%s} = %d: a zero budget abandoned a write that was about to land",
			archiving.WriteStatusLost, got)
	}
}

// A write that lands after the drain gave up must not record its own outcome on
// top: the drain already counted its batch as lost, and the runbook's parity
// identity reads that second status for the same batch as an unexplained hole.
func TestNewDrain_LeavesALateLandingWriteOneStatus(t *testing.T) {
	provider, reader := newTestMeterProvider(t)
	release := make(chan struct{})
	writeStarted := make(chan struct{})
	gate := archiving.NewDrainGate(nil)
	mc := newArchivingMulticaller(t, gate, provider, blockingArchiver{running: writeStarted, release: release})

	if _, err := mc.Execute(context.Background(), oneCall(), big.NewInt(21_000_000)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	<-writeStarted

	newDrain(gate, slog.New(&testutil.SlogRecorder{}), archiving.NewWriteCounter(provider, testChain, testSource, nil), drainTestBudget)()
	close(release)
	gate.Wait()

	if got, _ := counterForStatus(t, reader, "archive.writes.total", archiving.WriteStatusLost); got != 1 {
		t.Errorf("archive.writes.total{status=%s} = %d, want 1", archiving.WriteStatusLost, got)
	}
	if got, ok := counterForStatus(t, reader, "archive.writes.total", "success"); ok {
		t.Errorf("the abandoned write also recorded status=success (%d): its batch is counted twice", got)
	}
}

// blockingArchiver holds one archive write open until release is closed,
// reporting through running that the write is provably in flight.
type blockingArchiver struct {
	running chan struct{}
	release <-chan struct{}
}

func (a blockingArchiver) Archive(ctx context.Context, _ outbound.CallBatchRecord) error {
	close(a.running)
	select {
	case <-a.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newArchivingMulticaller(t *testing.T, gate *archiving.DrainGate, provider metric.MeterProvider, arch outbound.CallArchiver) outbound.Multicaller {
	t.Helper()
	inner := testutil.NewMockMulticaller()
	inner.ExecuteFn = func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true}}, nil
	}
	return archiving.NewMulticaller(inner, arch, archiving.Config{
		Source:        testSource,
		ChainID:       1,
		Chain:         testChain,
		Gate:          gate,
		MeterProvider: provider,
	})
}

func oneCall() []outbound.Call {
	return []outbound.Call{{Target: common.HexToAddress("0x1"), CallData: []byte{0x01, 0x02, 0x03, 0x04}}}
}

// The wait exists to keep archiving usable: a write it gives up on keeps
// running, and the next unit of work must still be able to schedule its own.
func TestNewWait_LeavesTheGateOpenWhenItsBudgetExpires(t *testing.T) {
	gate := gateHoldingAStuckWrite(t)
	recorder := &testutil.SlogRecorder{}

	newWait(gate, slog.New(recorder), drainTestBudget)()

	if !gate.Go(func() {}) {
		t.Error("expected the gate still open after the wait budget expired")
	}
	if got := recorder.CountWarn("outlasted the wait budget"); got != 1 {
		t.Errorf("expected one warning about the writes left running, got %d", got)
	}
}

// gateHoldingAStuckWrite returns a gate whose single write is provably running
// and never finishes until the test ends.
func gateHoldingAStuckWrite(t *testing.T) *archiving.DrainGate {
	t.Helper()
	gate := archiving.NewDrainGate(nil)
	stuck := make(chan struct{})
	running := make(chan struct{})
	t.Cleanup(func() { close(stuck); gate.Wait() })
	gate.Go(func() { close(running); <-stuck })
	<-running
	return gate
}

func newTestWriteCounter(t *testing.T) (*archiving.WriteCounter, sdkmetric.Reader) {
	t.Helper()
	provider, reader := newTestMeterProvider(t)
	return archiving.NewWriteCounter(provider, testChain, testSource, nil), reader
}

func newTestMeterProvider(t *testing.T) (*sdkmetric.MeterProvider, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider, reader
}

// counterForStatus returns the int64 counter value whose data point carries
// status=want and whether that data point exists at all, asserting chain and
// source labels are present.
func counterForStatus(t *testing.T, reader sdkmetric.Reader, name, want string) (int64, bool) {
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
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if status, _ := dp.Attributes.Value("status"); status.AsString() != want {
					continue
				}
				if c, _ := dp.Attributes.Value("chain"); c.AsString() != testChain {
					t.Errorf("chain label = %q, want %s", c.AsString(), testChain)
				}
				if s, _ := dp.Attributes.Value("source"); s.AsString() != testSource {
					t.Errorf("source label = %q, want %s", s.AsString(), testSource)
				}
				return dp.Value, true
			}
		}
	}
	return 0, false
}

func TestEnabled(t *testing.T) {
	tests := []struct {
		name   string
		envVal string
		want   bool
	}{
		{name: "true enables", envVal: "true", want: true},
		{name: "1 enables", envVal: "1", want: true},
		{name: "TitleCase True enables", envVal: "True", want: true},
		{name: "false disables", envVal: "false", want: false},
		{name: "unset disables", envVal: "", want: false},
		{name: "unrecognised value disables", envVal: "yes", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ARCHIVE_SC_CALLS", tt.envVal)
			if got := Enabled(); got != tt.want {
				t.Fatalf("Enabled() = %v, want %v for env=%q", got, tt.want, tt.envVal)
			}
		})
	}
}

func TestNewS3WrapFromEnvRequiresBucket(t *testing.T) {
	t.Setenv("ARCHIVE_SC_CALLS", "true")
	t.Setenv("RAW_SC_BUCKET", "")
	_, _, _, err := NewS3WrapFromEnv(t.Context(), nil, 1, 47, "oracle-price")
	if err == nil {
		t.Fatal("expected error when RAW_SC_BUCKET is empty")
	}
}

// TestBootstrapDisabled verifies the identity wrap and no-op wait and drain
// returned when archiving is off, so callers can wire them unconditionally.
func TestBootstrapDisabled(t *testing.T) {
	t.Setenv("ARCHIVE_SC_CALLS", "false")

	wrap, wait, drain, err := Bootstrap(context.Background(), nil, 1, 47, "oracle-price")
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if wrap == nil || wait == nil || drain == nil {
		t.Fatal("disabled Bootstrap must return non-nil wrap, wait and drain")
	}

	mc := testutil.NewMockMulticaller()
	if got := wrap(mc); got != mc {
		t.Fatal("disabled wrap must return the multicaller unchanged")
	}
	wait()  // must not panic
	drain() // must not panic
}

// TestBootstrapErrorWhenMisconfigured verifies that enabling archiving without a
// bucket surfaces a wrapped error rather than silently disabling.
func TestBootstrapErrorWhenMisconfigured(t *testing.T) {
	t.Setenv("ARCHIVE_SC_CALLS", "true")
	t.Setenv("RAW_SC_BUCKET", "")

	wrap, wait, drain, err := Bootstrap(context.Background(), nil, 1, 47, "oracle-price")
	if err == nil {
		t.Fatal("expected error when RAW_SC_BUCKET is empty")
	}
	if wrap != nil || wait != nil || drain != nil {
		t.Fatal("error path must return nil wrap, wait and drain")
	}
}
