package uniswapv4bootstrap

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func testWindowPolicy(initial, minSize, maxSize int64) windowPolicy {
	return windowPolicy{initial: initial, min: minSize, max: maxSize}
}

func testScanner(t *testing.T, client outbound.LogScanClient, policy windowPolicy) *logWindowScanner {
	t.Helper()
	return &logWindowScanner{
		client: client,
		filter: outbound.LogFilter{
			Address: common.HexToAddress(poolManagerAddr),
			Topic0:  common.HexToHash("0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"),
			Topic1:  []common.Hash{common.HexToHash(poolAIDHash)},
		},
		policy: policy,
		logger: testLogger(),
	}
}

// collectRanges records the [from, to] of every window emit saw.
func collectRanges(ranges *[][2]int64) func(logWindow) error {
	return func(w logWindow) error {
		*ranges = append(*ranges, [2]int64{w.from, w.to})
		return nil
	}
}

func TestWindowPolicy_ShrinkHalvesAndFloorsAtMin(t *testing.T) {
	p := testWindowPolicy(1000, 10, 4000)
	tests := []struct{ in, want int64 }{
		{1000, 500},
		{21, 10},
		{10, 10},
		{1, 10},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.in), func(t *testing.T) {
			if got := p.shrink(tt.in); got != tt.want {
				t.Errorf("shrink(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestWindowPolicy_GrowIsGradualAndCapsAtMax(t *testing.T) {
	p := testWindowPolicy(1000, 10, 2000)
	tests := []struct{ in, want int64 }{
		{100, 125},
		{1000, 1250},
		{1900, 2000},
		{2000, 2000},
		// A window of 1 must still grow, which integer 5/4 alone would not do.
		{1, 2},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.in), func(t *testing.T) {
			if got := p.grow(tt.in); got != tt.want {
				t.Errorf("grow(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestScan_WalksTheWholeRangeInInitialSizedWindows(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))

	var ranges [][2]int64
	stats, err := scanner.scan(context.Background(), 1000, 1249, collectRanges(&ranges))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	want := [][2]int64{{1000, 1099}, {1100, 1199}, {1200, 1249}}
	if fmt.Sprint(ranges) != fmt.Sprint(want) {
		t.Errorf("windows = %v, want %v", ranges, want)
	}
	if stats.windows != 3 {
		t.Errorf("stats.windows = %d, want 3", stats.windows)
	}
	if stats.narrowings != 0 {
		t.Errorf("stats.narrowings = %d, want 0", stats.narrowings)
	}
}

func TestScan_ClampsTheLastWindowToTheScanEnd(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	// Anything wider than 25 blocks is refused, so the 100-block window would be
	// refused had the scan end not clamped it to the range's own 25.
	client.GetLogsFn = func(f outbound.LogFilter) ([]outbound.FilteredLog, error) {
		if f.ToBlock-f.FromBlock+1 > 25 {
			return nil, fmt.Errorf("provider says no: %w", outbound.ErrLogRangeTooLarge)
		}
		return nil, nil
	}
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))

	var ranges [][2]int64
	stats, err := scanner.scan(context.Background(), 1000, 1024, collectRanges(&ranges))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	if len(ranges) != 1 || ranges[0] != [2]int64{1000, 1024} {
		t.Fatalf("windows = %v, want one [1000 1024] window", ranges)
	}
	if stats.narrowings != 0 {
		t.Errorf("stats.narrowings = %d, want 0: the range itself is already inside the ceiling", stats.narrowings)
	}
}

func TestScan_ShrinksARefusedWindowFromItsClampedSpan(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	client.GetLogsFn = func(f outbound.LogFilter) ([]outbound.FilteredLog, error) {
		if f.ToBlock-f.FromBlock+1 > 4 {
			return nil, fmt.Errorf("provider says no: %w", outbound.ErrLogRangeTooLarge)
		}
		return nil, nil
	}
	// The nominal window is 20x the range left to scan, so every request is
	// clamped to the same 5 blocks until the nominal size drops below the clamp.
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))

	if _, err := scanner.scan(context.Background(), 1000, 1004, func(logWindow) error { return nil }); err != nil {
		t.Fatalf("scan: %v", err)
	}

	seen := map[[2]int64]int{}
	for _, f := range client.Filters {
		seen[[2]int64{f.FromBlock, f.ToBlock}]++
	}
	for span, n := range seen {
		if n > 1 {
			t.Errorf("blocks %d-%d were requested %d times; a refused window must shrink from the span actually asked for", span[0], span[1], n)
		}
	}
}

func TestScan_BisectsUntilTheProviderAcceptsTheWindow(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	// Only the first stretch is dense, so the bisect count below covers exactly
	// one narrowing sequence rather than the whole scan's.
	client.GetLogsFn = func(f outbound.LogFilter) ([]outbound.FilteredLog, error) {
		if f.FromBlock == 1000 && f.ToBlock-f.FromBlock+1 > 25 {
			return nil, fmt.Errorf("provider says no: %w", outbound.ErrLogRangeTooLarge)
		}
		return nil, nil
	}
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))

	var ranges [][2]int64
	stats, err := scanner.scan(context.Background(), 1000, 1200, collectRanges(&ranges))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	if len(ranges) == 0 || ranges[0] != [2]int64{1000, 1024} {
		t.Fatalf("first window = %v, want [1000 1024] after two bisects", ranges)
	}
	if stats.narrowings != 2 {
		t.Errorf("stats.narrowings = %d, want 2 (100 → 50 → 25)", stats.narrowings)
	}
	for i, f := range client.Filters[:3] {
		if f.FromBlock != 1000 {
			t.Errorf("attempt %d fromBlock = %d, want 1000: a refused window retries the same start", i, f.FromBlock)
		}
	}
}

func TestScan_GrowsBackAfterASuccessfulWindow(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	client.GetLogsFn = func(f outbound.LogFilter) ([]outbound.FilteredLog, error) {
		if f.FromBlock == 1000 && f.ToBlock-f.FromBlock+1 > 50 {
			return nil, fmt.Errorf("too wide: %w", outbound.ErrLogRangeTooLarge)
		}
		return nil, nil
	}
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 1000))

	var ranges [][2]int64
	if _, err := scanner.scan(context.Background(), 1000, 2000, collectRanges(&ranges)); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if len(ranges) < 2 {
		t.Fatalf("windows = %v, want at least two", ranges)
	}
	first := ranges[0][1] - ranges[0][0] + 1
	second := ranges[1][1] - ranges[1][0] + 1
	if first != 50 {
		t.Errorf("first window = %d blocks, want 50 after the bisect", first)
	}
	if second <= first {
		t.Errorf("second window = %d blocks, want it above the first (%d): success must widen the window", second, first)
	}
}

func TestScan_RefusalAtTheMinimumWindowFailsTheScan(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return nil, fmt.Errorf("still too big: %w", outbound.ErrLogRangeTooLarge)
	}
	scanner := testScanner(t, client, testWindowPolicy(4, 1, 4))

	_, err := scanner.scan(context.Background(), 1000, 1003, func(logWindow) error { return nil })
	if !errors.Is(err, outbound.ErrLogRangeTooLarge) {
		t.Fatalf("error = %v, want it to wrap ErrLogRangeTooLarge", err)
	}
}

func TestScan_NonRangeErrorFailsImmediately(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	boom := errors.New("archive node unavailable")
	client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) { return nil, boom }
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))

	_, err := scanner.scan(context.Background(), 1000, 2000, func(logWindow) error { return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap the upstream failure", err)
	}
	if len(client.Filters) != 1 {
		t.Errorf("attempts = %d, want 1: a non-range error must not be bisected around", len(client.Filters))
	}
}

func TestScan_EmitErrorStopsTheScan(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))
	boom := errors.New("decode failed")

	_, err := scanner.scan(context.Background(), 1000, 2000, func(logWindow) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap the emit failure", err)
	}
	if len(client.Filters) != 1 {
		t.Errorf("windows queried = %d, want 1: the scan must stop at the first emit failure", len(client.Filters))
	}
}

func TestScan_CancelledContextStopsBeforeTheNextWindow(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := scanner.scan(ctx, 1000, 2000, func(logWindow) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestScan_CarriesTheFilterAndCountsLogs(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	client.GetLogsFn = func(f outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{{LogIndex: "0x0"}, {LogIndex: "0x1"}}, nil
	}
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))

	var got int
	stats, err := scanner.scan(context.Background(), 1000, 1199, func(w logWindow) error {
		got += len(w.logs)
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got != 4 || stats.logs != 4 {
		t.Errorf("logs emitted = %d, stats.logs = %d, want 4 each", got, stats.logs)
	}
	for i, f := range client.Filters {
		if f.Address != common.HexToAddress(poolManagerAddr) {
			t.Errorf("window %d address = %s, want the PoolManager", i, f.Address)
		}
		if len(f.Topic1) != 1 || f.Topic1[0] != common.HexToHash(poolAIDHash) {
			t.Errorf("window %d topic1 = %v, want the registered pool id set", i, f.Topic1)
		}
	}
}

func TestScan_EmptyRangeQueriesNothing(t *testing.T) {
	client := newFakeLogScanClient(0, nil)
	scanner := testScanner(t, client, testWindowPolicy(100, 1, 100))

	stats, err := scanner.scan(context.Background(), 2000, 1999, func(logWindow) error {
		t.Error("emit must not be called for an empty range")
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if stats.windows != 0 || len(client.Filters) != 0 {
		t.Errorf("stats = %+v, filters = %d, want nothing queried", stats, len(client.Filters))
	}
}
