package multicall

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	trappingTarget = common.HexToAddress("0x4ECeF7bd1eD0c9f64a3a5c1a785A3Bb39DC5dF6A")
	quietTarget    = common.HexToAddress("0x1111111111111111111111111111111111111111")
	narrowedBlock  = big.NewInt(25827558)
	narrowedHash   = common.HexToHash("0xabc0")
)

// The boundary follows the node's eth_call cap: at mainnet's, two traps in one
// batch still leave Multicall3 its 1/64 to finish, three do not.
const trapsExhaustingOneBatch = 3

func probeCalls(target common.Address, n int) []outbound.Call {
	calls := make([]outbound.Call, n)
	for i := range calls {
		calls[i] = outbound.Call{Target: target, AllowFailure: true, CallData: []byte{byte(i), 0, 0, 0}}
	}
	return calls
}

// trappingNode answers like a node in front of a contract that traps on every
// selector: a batch holding trapsExhaustingOneBatch or more calls to the
// trapping target exhausts gas, a trapping call alone answers Success:false,
// and every other call echoes its selector back with Success:true.
func trappingNode(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	trapped := 0
	for _, c := range calls {
		if c.Target == trappingTarget {
			trapped++
		}
	}
	if trapped >= trapsExhaustingOneBatch {
		return nil, testutil.GasExhaustedRPCError()
	}
	out := make([]outbound.Result, len(calls))
	for i, c := range calls {
		if c.Target != trappingTarget {
			out[i] = outbound.Result{Success: true, ReturnData: c.CallData}
		}
	}
	return out, nil
}

func newNarrowing(t *testing.T, opts ...NarrowingOption) (*Narrowing, *testutil.MockMulticaller) {
	t.Helper()
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = trappingNode
	quiet := WithNarrowingLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	return NewNarrowing(mc, append([]NarrowingOption{quiet}, opts...)...), mc
}

func captureWarnings(t *testing.T) (*bytes.Buffer, NarrowingOption) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return &buf, WithNarrowingLogger(logger)
}

func recordingTelemetry(t *testing.T) (*Telemetry, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}
	return tel, reader
}

func TestNarrowing_PassesAnAnsweredBatchThroughUntouched(t *testing.T) {
	n, mc := newNarrowing(t)
	calls := probeCalls(quietTarget, 4)

	results, err := n.Execute(context.Background(), calls, narrowedBlock)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(results) != 4 || !results[3].Success || !bytes.Equal(results[3].ReturnData, calls[3].CallData) {
		t.Errorf("results = %+v, want the inner answers in order", results)
	}
	if mc.CallCount != 1 {
		t.Errorf("inner calls = %d, want 1: an answered batch is never split", mc.CallCount)
	}
}

func TestNarrowing_SplitsAGasExhaustedBatchUntilEveryCallAnswers(t *testing.T) {
	n, mc := newNarrowing(t)
	calls := append(probeCalls(quietTarget, 4), probeCalls(trappingTarget, 4)...)

	results, err := n.Execute(context.Background(), calls, narrowedBlock)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(results) != len(calls) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(calls))
	}
	for i, r := range results {
		if wantSuccess := calls[i].Target == quietTarget; r.Success != wantSuccess {
			t.Errorf("results[%d].Success = %v, want %v (target %s)", i, r.Success, wantSuccess, calls[i].Target.Hex())
		}
	}
	if mc.CallCount <= 1 {
		t.Errorf("inner calls = %d, want more than one: the batch must have been re-issued", mc.CallCount)
	}
}

func TestNarrowing_KeepsCallOrderAcrossASplit(t *testing.T) {
	n, _ := newNarrowing(t)
	calls := append(probeCalls(trappingTarget, 4), probeCalls(quietTarget, 4)...)

	results, err := n.Execute(context.Background(), calls, narrowedBlock)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for i := 4; i < 8; i++ {
		if !bytes.Equal(results[i].ReturnData, calls[i].CallData) {
			t.Errorf("results[%d] answered %x, want the selector %x: order must survive the split", i, results[i].ReturnData, calls[i].CallData)
		}
	}
}

func TestNarrowing_PropagatesGasExhaustionOfASingleCall(t *testing.T) {
	n, mc := newNarrowing(t)
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, testutil.GasExhaustedRPCError()
	}

	results, err := n.Execute(context.Background(), probeCalls(trappingTarget, 4), narrowedBlock)
	if !errors.Is(err, testutil.GasExhaustedRPCError()) {
		t.Fatalf("want the node's gas error to propagate once a single call still exhausts, got %v (results=%+v)", err, results)
	}
	if results != nil {
		t.Errorf("results = %+v, want nil on error", results)
	}
}

func TestNarrowing_PropagatesAnyOtherErrorWithoutSplitting(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "throttled", err: testutil.ThrottledRPCError()},
		{name: "revert", err: testutil.RPCError{Code: 3, Msg: "execution reverted"}},
		{name: "plain transport", err: errors.New("connection reset by peer")},
		{name: "context cancelled", err: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, mc := newNarrowing(t)
			mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				return nil, tt.err
			}

			_, err := n.Execute(context.Background(), probeCalls(trappingTarget, 4), narrowedBlock)
			if !errors.Is(err, tt.err) {
				t.Fatalf("want the error to propagate, got %v", err)
			}
			if mc.CallCount != 1 {
				t.Errorf("inner calls = %d, want 1: only gas exhaustion and oversized requests narrow", mc.CallCount)
			}
		})
	}
}

func TestNarrowing_ErrorMidSplitPropagates(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "throttled", err: testutil.ThrottledRPCError()},
		{name: "context cancelled", err: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, mc := newNarrowing(t)
			mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) == 1 {
					return nil, tt.err
				}
				return nil, testutil.GasExhaustedRPCError()
			}

			_, err := n.Execute(context.Background(), probeCalls(trappingTarget, 4), narrowedBlock)
			if !errors.Is(err, tt.err) {
				t.Fatalf("want the error to propagate from inside the split, got %v", err)
			}
		})
	}
}

func TestNarrowing_SplitsAnOversizedRequest(t *testing.T) {
	const maxCallsPerRequest = 3
	n, _ := newNarrowing(t)
	n.inner.(*testutil.MockMulticaller).ExecuteFn = func(ctx context.Context, calls []outbound.Call, block *big.Int) ([]outbound.Result, error) {
		if len(calls) > maxCallsPerRequest {
			return nil, rpc.HTTPError{StatusCode: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		return trappingNode(ctx, calls, block)
	}
	calls := probeCalls(quietTarget, 10)

	results, err := n.Execute(context.Background(), calls, narrowedBlock)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(results) != len(calls) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(calls))
	}
}

func TestNarrowing_HashPinnedReadsNarrowToo(t *testing.T) {
	n, mc := newNarrowing(t)
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return trappingNode(ctx, calls, nil)
	}

	results, err := n.ExecuteAtHash(context.Background(), probeCalls(trappingTarget, 4), narrowedHash)
	if err != nil {
		t.Fatalf("ExecuteAtHash: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("len(results) = %d, want 4", len(results))
	}
	for _, inv := range mc.Invocations {
		if !inv.ViaHash || inv.BlockHash != narrowedHash {
			t.Errorf("a re-issued batch left the hash-pinned path: %+v", inv)
		}
	}
}

func TestNarrowing_WarnsOncePerNarrowedBatch(t *testing.T) {
	buf, withLogger := captureWarnings(t)
	n, _ := newNarrowing(t, withLogger)

	if _, err := n.Execute(context.Background(), probeCalls(trappingTarget, 4), narrowedBlock); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	logged := buf.String()
	if got := strings.Count(logged, "level=WARN"); got != 1 {
		t.Fatalf("WARN lines = %d, want exactly 1 for the batch; got %q", got, logged)
	}
	for _, want := range []string{"narrowed", "reason=gas_exhausted", "outcome=answered", "block=25827558", strings.ToLower(trappingTarget.Hex()[2:10])} {
		if !strings.Contains(strings.ToLower(logged), want) {
			t.Errorf("WARN must carry %q; got %q", want, logged)
		}
	}
}

// A fifty-candidate backfill batch is address-sorted, so the trapping address
// can sit anywhere in it; the WARN must name it, not the head of the batch.
func TestNarrowing_NamesTheNarrowestRefusedBatchAsCulprits(t *testing.T) {
	buf, withLogger := captureWarnings(t)
	n, _ := newNarrowing(t, withLogger)
	var calls []outbound.Call
	for i := 1; i <= 9; i++ {
		calls = append(calls, probeCalls(common.BigToAddress(big.NewInt(int64(i))), 1)...)
	}
	calls = append(calls, probeCalls(trappingTarget, 4)...)

	if _, err := n.Execute(context.Background(), calls, narrowedBlock); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	logged := strings.ToLower(buf.String())
	if !strings.Contains(logged, "culprits=["+strings.ToLower(trappingTarget.Hex())+"]") {
		t.Errorf("WARN must name only the trapping address as culprit; got %q", logged)
	}
}

func TestNarrowing_ReportsAnAbortedNarrowing(t *testing.T) {
	buf, withLogger := captureWarnings(t)
	tel, reader := recordingTelemetry(t)
	n, mc := newNarrowing(t, withLogger, WithNarrowingTelemetry(tel))
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 1 {
			return nil, testutil.ThrottledRPCError()
		}
		return nil, testutil.GasExhaustedRPCError()
	}

	_, err := n.Execute(context.Background(), probeCalls(trappingTarget, 4), narrowedBlock)
	if !errors.Is(err, testutil.ThrottledRPCError()) {
		t.Fatalf("want the throttle to propagate, got %v", err)
	}
	for _, want := range []string{"outcome=aborted", "error="} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("an aborted narrowing must still WARN with %q; got %q", want, buf.String())
		}
	}
	if got := testutil.CounterValue(t, reader, "multicall.batches.narrowed", map[string]string{"reason": "gas_exhausted"}); got != 1 {
		t.Errorf("multicall.batches.narrowed = %d, want 1: an aborted narrowing counts too", got)
	}
}

func TestNarrowing_StaysQuietWhenNothingNarrows(t *testing.T) {
	buf, withLogger := captureWarnings(t)
	n, mc := newNarrowing(t, withLogger)
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, testutil.ThrottledRPCError()
	}

	_, _ = n.Execute(context.Background(), probeCalls(trappingTarget, 4), narrowedBlock)
	if buf.Len() != 0 {
		t.Errorf("a propagated transport error must not be logged as narrowing; got %q", buf.String())
	}
}

func TestNarrowing_CountsNarrowedBatchesByReason(t *testing.T) {
	tests := []struct {
		name   string
		refuse func(calls []outbound.Call) error
		reason string
	}{
		{name: "gas exhaustion", refuse: func([]outbound.Call) error { return testutil.GasExhaustedRPCError() }, reason: "gas_exhausted"},
		{name: "oversized request", refuse: func([]outbound.Call) error { return rpc.HTTPError{StatusCode: http.StatusRequestEntityTooLarge} }, reason: "request_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel, reader := recordingTelemetry(t)
			n, mc := newNarrowing(t, WithNarrowingTelemetry(tel))
			mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, block *big.Int) ([]outbound.Result, error) {
				if len(calls) > 2 {
					return nil, tt.refuse(calls)
				}
				return trappingNode(ctx, calls, block)
			}

			if _, err := n.Execute(context.Background(), probeCalls(quietTarget, 4), narrowedBlock); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got := testutil.CounterValue(t, reader, "multicall.batches.narrowed", map[string]string{"reason": tt.reason, "chain": "mainnet"}); got != 1 {
				t.Errorf("multicall.batches.narrowed{reason=%s} = %d, want 1", tt.reason, got)
			}
		})
	}
}

func TestNarrowing_ForwardsAddress(t *testing.T) {
	n, mc := newNarrowing(t)
	if n.Address() != mc.Addr {
		t.Errorf("Address() = %s, want the inner multicaller's %s", n.Address().Hex(), mc.Addr.Hex())
	}
}
