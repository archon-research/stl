# Multicall Telemetry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit OpenTelemetry metrics on multicall usage so SC call volume, batch sizes, and archive write rates are observable in Grafana, to inform the semaphore-removal and per-Execute batching decisions.

**Architecture:** Two layers. A `multicall.batch.size` histogram on the base multicall client (records every Execute regardless of archiving), and an `archive.writes.total` counter on the archiving decorator (records actual archive writes where archiving is on). Both mirror the existing `alchemy.Telemetry` pattern and use the global OTEL meter provider, injectable for tests.

**Tech Stack:** Go, OpenTelemetry (`go.opentelemetry.io/otel`, `sdk/metric`), existing `internal/pkg/telemetry` conventions.

---

## File Structure

- Create: `stl-verify/internal/pkg/blockchain/multicall/telemetry.go` — `multicall.Telemetry` (batch-size histogram).
- Create: `stl-verify/internal/pkg/blockchain/multicall/telemetry_test.go` — histogram unit tests.
- Modify: `stl-verify/internal/pkg/blockchain/multicall/client.go` — functional-option telemetry, record in `Execute`.
- Modify: `stl-verify/internal/pkg/blockchain/multicall/client_test.go` (create if absent) — Execute records batch size.
- Modify: `stl-verify/internal/pkg/blockchain/archiving/multicaller.go` — `archive.writes.total` counter.
- Modify: `stl-verify/internal/pkg/blockchain/archiving/multicaller_test.go` — counter assertions.
- Modify: `stl-verify/internal/pkg/blockchain/archiving/archivingwire/wire.go` — resolve chain name, set on decorator Config.
- Modify: 4 worker cmds — wire `multicall.WithTelemetry`.

Key facts (verified):
- `multicall.NewClient(ethClient *ethclient.Client, addr common.Address) (outbound.Multicaller, error)`, concrete `*Client`.
- `entity.ChainName(chainID int64) (string, error)` is the chain-name helper.
- Test meter pattern: `sdkmetric.NewManualReader()` + `sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))`, then `reader.Collect`.
- The seconds histogram view in `telemetry.InitMetrics` only matches `Unit:"s"` instruments, so a `Unit:"{call}"` histogram keeps its own bucket boundaries.

---

### Task 1: multicall.Telemetry batch-size histogram

**Files:**
- Create: `stl-verify/internal/pkg/blockchain/multicall/telemetry.go`
- Test: `stl-verify/internal/pkg/blockchain/multicall/telemetry_test.go`

- [ ] **Step 1: Write the failing test**

```go
package multicall

import (
	"context"
	"slices"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// histDataPoint returns the single histogram data point for the named metric.
func histDataPoint(t *testing.T, reader sdkmetric.Reader, name string) metricdata.HistogramDataPoint[int64] {
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
			hist, ok := m.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Histogram[int64]", name, m.Data)
			}
			if len(hist.DataPoints) != 1 {
				t.Fatalf("metric %q has %d data points, want 1", name, len(hist.DataPoints))
			}
			return hist.DataPoints[0]
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.HistogramDataPoint[int64]{}
}

func TestRecordBatch_CountSumAndChain(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}
	tel.RecordBatch(context.Background(), 3)
	tel.RecordBatch(context.Background(), 7)

	dp := histDataPoint(t, reader, "multicall.batch.size")
	if dp.Count != 2 {
		t.Errorf("count = %d, want 2 (two Execute calls)", dp.Count)
	}
	if dp.Sum != 10 {
		t.Errorf("sum = %d, want 10 (3+7 individual calls)", dp.Sum)
	}
	val, ok := dp.Attributes.Value("chain")
	if !ok || val.AsString() != "mainnet" {
		t.Errorf("chain attribute = %v (ok=%v), want mainnet", val.AsString(), ok)
	}
	if !slices.Equal(dp.Bounds, batchSizeBuckets) {
		t.Errorf("bounds = %v, want %v", dp.Bounds, batchSizeBuckets)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd stl-verify && go test ./internal/pkg/blockchain/multicall/ -run TestRecordBatch -v`
Expected: FAIL to compile (`NewTelemetryWithProvider`, `batchSizeBuckets` undefined).

- [ ] **Step 3: Write minimal implementation**

```go
// telemetry.go provides OpenTelemetry instrumentation for the multicall client.
//
//	multicall.batch.size: histogram of calls per Execute(). _count is the number
//	of multicalls (= batched S3 PUTs/sec when archiving batches per Execute),
//	_sum is the number of individual SC calls (= current per-call S3 PUTs/sec),
//	and the bucket distribution gives the worst-case burst a single Execute can
//	produce.
package multicall

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"

// batchSizeBuckets are explicit boundaries for the call-count histogram. Unlike
// the seconds-unit latency histograms (see telemetry.SecondsDurationBuckets),
// this counts calls per multicall, which range from 1 to several hundred. The
// boundaries give resolution across that range so histogram_quantile resolves a
// meaningful p99/p100 burst size. The unit "{call}" keeps the seconds view in
// telemetry.InitMetrics from overriding these.
var batchSizeBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}

// Telemetry records multicall metrics. The zero value is unusable; build with
// NewTelemetry or NewTelemetryWithProvider.
type Telemetry struct {
	batchSize metric.Int64Histogram
	chainAttr attribute.KeyValue
}

// NewTelemetry builds Telemetry against the global meter provider. chain is the
// chain name (e.g. "mainnet") attached as the `chain` label on every metric.
func NewTelemetry(chain string) (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider(), chain)
}

// NewTelemetryWithProvider builds Telemetry against a caller-supplied meter
// provider. Used by tests to inject a manual reader.
func NewTelemetryWithProvider(mp metric.MeterProvider, chain string) (*Telemetry, error) {
	meter := mp.Meter(instrumentationName)
	batchSize, err := meter.Int64Histogram(
		"multicall.batch.size",
		metric.WithDescription("Number of calls in a single multicall Execute"),
		metric.WithUnit("{call}"),
		metric.WithExplicitBucketBoundaries(batchSizeBuckets...),
	)
	if err != nil {
		return nil, err
	}
	return &Telemetry{batchSize: batchSize, chainAttr: attribute.String("chain", chain)}, nil
}

// RecordBatch records one multicall Execute holding size individual calls.
func (t *Telemetry) RecordBatch(ctx context.Context, size int) {
	t.batchSize.Record(ctx, int64(size), metric.WithAttributes(t.chainAttr))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd stl-verify && go test ./internal/pkg/blockchain/multicall/ -run TestRecordBatch -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add stl-verify/internal/pkg/blockchain/multicall/telemetry.go stl-verify/internal/pkg/blockchain/multicall/telemetry_test.go
git commit -m "feat(vec-81): multicall batch-size telemetry"
```

---

### Task 2: Wire telemetry into the multicall client

**Files:**
- Modify: `stl-verify/internal/pkg/blockchain/multicall/client.go`
- Test: `stl-verify/internal/pkg/blockchain/multicall/client_test.go` (create)

- [ ] **Step 1: Write the failing test**

```go
package multicall

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// TestExecuteRecordsBatchSize verifies a non-empty Execute records the batch
// size even when the underlying RPC has no node (the record happens before the
// network call). An empty batch records nothing.
func TestExecuteRecordsBatchSize(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	c := &Client{address: common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"), telemetry: tel}
	// recordBatch is the single instrumentation point used by Execute; call it
	// directly so the test does not need a live ethclient.
	c.recordBatch(context.Background(), 5)

	dp := histDataPoint(t, reader, "multicall.batch.size")
	if dp.Count != 1 || dp.Sum != 5 {
		t.Errorf("count=%d sum=%d, want 1 and 5", dp.Count, dp.Sum)
	}
}

func TestRecordBatchNilTelemetryIsNoOp(t *testing.T) {
	c := &Client{} // no telemetry
	c.recordBatch(context.Background(), 5) // must not panic
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd stl-verify && go test ./internal/pkg/blockchain/multicall/ -run TestExecute -v`
Expected: FAIL to compile (`telemetry` field, `recordBatch` undefined).

- [ ] **Step 3: Add the field, option, and recording to client.go**

Add the telemetry field to the struct (modify the existing `Client` struct):

```go
type Client struct {
	ethClient *ethclient.Client
	address   common.Address
	abi       *abi.ABI
	telemetry *Telemetry // optional; nil disables recording
}
```

Add the option type and constructor variadic. Replace the existing `NewClient` signature and return:

```go
// Option configures a Client at construction.
type Option func(*Client)

// WithTelemetry attaches metrics recording to the client.
func WithTelemetry(t *Telemetry) Option {
	return func(c *Client) { c.telemetry = t }
}

func NewClient(ethClient *ethclient.Client, multicall3Address common.Address, opts ...Option) (outbound.Multicaller, error) {
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load multicall3 ABI: %w", err)
	}

	c := &Client{
		ethClient: ethClient,
		address:   multicall3Address,
		abi:       multicallABI,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// recordBatch records the batch size when telemetry is attached. nil telemetry
// is a no-op so non-instrumented callers pay nothing.
func (c *Client) recordBatch(ctx context.Context, size int) {
	if c.telemetry != nil {
		c.telemetry.RecordBatch(ctx, size)
	}
}
```

In `Execute`, record the batch size right after the empty-batch guard, before packing (so every real multicall is counted, independent of whether the RPC later succeeds):

```go
	if len(calls) == 0 {
		return []outbound.Result{}, nil
	}

	c.recordBatch(ctx, len(calls))

	data, err := c.abi.Pack("aggregate3", calls)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd stl-verify && go test ./internal/pkg/blockchain/multicall/ -v`
Expected: PASS (all tests, including the existing Execute tests).

- [ ] **Step 5: Commit**

```bash
git add stl-verify/internal/pkg/blockchain/multicall/client.go stl-verify/internal/pkg/blockchain/multicall/client_test.go
git commit -m "feat(vec-81): record batch size on multicall Execute"
```

---

### Task 3: archive.writes.total counter on the decorator

**Files:**
- Modify: `stl-verify/internal/pkg/blockchain/archiving/multicaller.go`
- Test: `stl-verify/internal/pkg/blockchain/archiving/multicaller_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestExecuteRecordsArchiveWriteStatus(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tests := []struct {
		name       string
		archiveErr error
		wantStatus string
	}{
		{"success", nil, "success"},
		{"error", errors.New("s3 down"), "error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inner := &stubMulticaller{results: []outbound.Result{{Success: true}}}
			arch := &stubArchiver{err: tc.archiveErr}
			m := NewMulticaller(inner, arch, Config{
				Source:        "test-source",
				ChainID:       1,
				Chain:         "mainnet",
				MeterProvider: mp,
				Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
			})

			_, err := m.Execute(context.Background(), []outbound.Call{{Target: common.Address{}, CallData: []byte{0x01, 0x02, 0x03, 0x04}}}, big.NewInt(100))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			m.Close() // drain the fire-and-forget archive write

			got := counterValueForStatus(t, reader, "archive.writes.total", tc.wantStatus)
			if got != 1 {
				t.Errorf("archive.writes.total{status=%q} = %d, want 1", tc.wantStatus, got)
			}
		})
	}
}

// counterValueForStatus returns the int64 counter value whose data point carries
// status=want, asserting chain and source labels are present.
func counterValueForStatus(t *testing.T, reader sdkmetric.Reader, name, want string) int64 {
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
				status, _ := dp.Attributes.Value("status")
				if status.AsString() != want {
					continue
				}
				if c, _ := dp.Attributes.Value("chain"); c.AsString() != "mainnet" {
					t.Errorf("chain label = %q, want mainnet", c.AsString())
				}
				if s, _ := dp.Attributes.Value("source"); s.AsString() != "test-source" {
					t.Errorf("source label = %q, want test-source", s.AsString())
				}
				return dp.Value
			}
		}
	}
	return 0
}
```

Add a `stubMulticaller` and `stubArchiver` if the test file does not already have equivalents (check existing helpers first; reuse them if present). Minimal forms:

```go
type stubMulticaller struct {
	results []outbound.Result
}

func (s *stubMulticaller) Execute(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error) {
	return s.results, nil
}
func (s *stubMulticaller) Address() common.Address { return common.Address{} }

type stubArchiver struct{ err error }

func (s *stubArchiver) Archive(context.Context, outbound.CallRecord) error { return s.err }
```

Required new test imports: `errors`, `io`, `log/slog`, `math/big`, `go.opentelemetry.io/otel/sdk/metric` (as `sdkmetric`), `go.opentelemetry.io/otel/sdk/metric/metricdata`, `github.com/ethereum/go-ethereum/common`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd stl-verify && go test ./internal/pkg/blockchain/archiving/ -run TestExecuteRecordsArchiveWriteStatus -v`
Expected: FAIL to compile (`Config.Chain`, `Config.MeterProvider`, counter not recorded).

- [ ] **Step 3: Add chain, meter provider, and counter to the decorator**

Add fields to `Config` (modify the existing struct):

```go
	// Chain is the chain name (e.g. "mainnet") used as the `chain` metric label.
	Chain string
	// MeterProvider builds the archive.writes.total counter. nil uses the global
	// provider; tests inject a manual reader.
	MeterProvider metric.MeterProvider
```

Add a counter field to `Multicaller`:

```go
type Multicaller struct {
	inner    outbound.Multicaller
	archiver outbound.CallArchiver
	cfg      Config
	writes   metric.Int64Counter
}
```

In `NewMulticaller`, default the provider and build the counter (insert before the final `return`):

```go
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	writes, err := cfg.MeterProvider.
		Meter("github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving").
		Int64Counter("archive.writes.total", metric.WithDescription("Raw SC call archive write attempts by status"))
	if err != nil {
		// Counter construction only fails on a bad instrument definition, a
		// programming error; a no-op counter keeps archiving working.
		cfg.Logger.Error("building archive.writes.total counter; archive metrics disabled", "error", err)
	}
	return &Multicaller{inner: inner, archiver: arch, cfg: cfg, writes: writes}
```

In `scheduleArchive`, record the outcome after the `Archive` call. Replace the existing `if err := m.archiver.Archive(...)` block with:

```go
		archiveCtx, cancel := context.WithTimeout(ctx, archiveTimeout)
		defer cancel()
		err := m.archiver.Archive(archiveCtx, record)
		m.recordWrite(archiveCtx, err)
		if err != nil {
			m.cfg.Logger.Error("archiving SC call failed",
				"error", err,
				"source", record.Source,
				"block", record.BlockNumber,
				"contract", record.ContractAddress,
				"selector", record.Selector,
			)
		}
```

Add the recording helper:

```go
// recordWrite increments archive.writes.total with the outcome status. A nil
// counter (construction failed) is a no-op.
func (m *Multicaller) recordWrite(ctx context.Context, err error) {
	if m.writes == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	m.writes.Add(ctx, 1, metric.WithAttributes(
		attribute.String("chain", m.cfg.Chain),
		attribute.String("source", m.cfg.Source),
		attribute.String("status", status),
	))
}
```

Add imports to `multicaller.go`: `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/attribute`, `go.opentelemetry.io/otel/metric`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd stl-verify && go test ./internal/pkg/blockchain/archiving/ -v`
Expected: PASS (new test plus existing decorator tests).

- [ ] **Step 5: Commit**

```bash
git add stl-verify/internal/pkg/blockchain/archiving/multicaller.go stl-verify/internal/pkg/blockchain/archiving/multicaller_test.go
git commit -m "feat(vec-81): archive.writes.total counter on archiving decorator"
```

---

### Task 4: Set chain name on the decorator in archivingwire

**Files:**
- Modify: `stl-verify/internal/pkg/blockchain/archiving/archivingwire/wire.go`

- [ ] **Step 1: Resolve chain name and set Config.Chain**

In `NewS3WrapFromEnv`, resolve the chain name from the chainID and set it on the Config. Add the import `"github.com/archon-research/stl/stl-verify/internal/domain/entity"`. After loading the bucket, add:

```go
	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return nil, nil, fmt.Errorf("resolving chain name for archiving metrics: %w", err)
	}
```

Then add `Chain: chainName,` to the `archiving.Config{...}` literal inside `wrap`:

```go
		return archiving.NewMulticaller(inner, archiver, archiving.Config{
			Source:  source,
			ChainID: chainID,
			Chain:   chainName,
			BuildID: buildID,
			Wait:    &wg,
			Sem:     sem,
			Logger:  logger,
		})
```

- [ ] **Step 2: Run the wire tests**

Run: `cd stl-verify && go test ./internal/pkg/blockchain/archiving/archivingwire/ -v`
Expected: PASS. If an existing test calls `NewS3WrapFromEnv` with a chainID that `entity.ChainName` rejects, update that test to use a valid chainID (e.g. 1).

- [ ] **Step 3: Commit**

```bash
git add stl-verify/internal/pkg/blockchain/archiving/archivingwire/wire.go
git commit -m "feat(vec-81): label archive writes with chain name"
```

---

### Task 5: Wire multicall telemetry into the indexer workers

**Files:**
- Modify: `stl-verify/cmd/workers/prime-allocation-indexer/main.go:221`
- Modify: `stl-verify/cmd/workers/morpho-indexer/main.go:223`
- Modify: `stl-verify/cmd/workers/oracle-price-indexer/main.go:255`
- Modify: `stl-verify/cmd/workers/prime-debt-indexer/main.go:185`

These are the long-running watchers that issue SC calls; wiring all four gives fleet-wide steady-state volume. Backfillers and `aavelike_position_tracker` use the identical one-line pattern and can be added later if their bursty load matters.

- [ ] **Step 1: prime-allocation-indexer**

`entity` is already imported here? Confirm with `grep '"github.com/archon-research/stl/stl-verify/internal/domain/entity"' cmd/workers/prime-allocation-indexer/main.go`; add the import if missing. Replace the `mc, err := multicall.NewClient(rawClient, blockchain.Multicall3)` line with:

```go
	chainName, err := entity.ChainName(cfg.chainID)
	if err != nil {
		return fmt.Errorf("resolving chain name: %w", err)
	}
	mcTel, err := multicall.NewTelemetry(chainName)
	if err != nil {
		return fmt.Errorf("multicall telemetry: %w", err)
	}
	mc, err := multicall.NewClient(rawClient, blockchain.Multicall3, multicall.WithTelemetry(mcTel))
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}
```

- [ ] **Step 2: morpho-indexer**

`chainName` is already computed at `main.go:268`, but the multicall client at `:223` is constructed before it. Move chain-name resolution above the multicall construction (or compute it locally), then replace the `mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)` line with:

```go
	mcChainName, err := entity.ChainName(cfg.chainID)
	if err != nil {
		return fmt.Errorf("resolving chain name: %w", err)
	}
	mcTel, err := multicall.NewTelemetry(mcChainName)
	if err != nil {
		return fmt.Errorf("multicall telemetry: %w", err)
	}
	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3, multicall.WithTelemetry(mcTel))
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}
```

If the later `chainName` at `:268` now duplicates this, reuse `mcChainName` there and delete the redundant computation.

- [ ] **Step 3: oracle-price-indexer**

The multicall client at `:255` is inside a closure/loop. Resolve chain name once near `:220` where `chainName` already exists, then replace the `mc, err = multicall.NewClient(ethClient, blockchain.Multicall3)` line with:

```go
				mcTel, telErr := multicall.NewTelemetry(chainName)
				if telErr != nil {
					return fmt.Errorf("multicall telemetry: %w", telErr)
				}
				mc, err = multicall.NewClient(ethClient, blockchain.Multicall3, multicall.WithTelemetry(mcTel))
```

Confirm `chainName` is in scope at `:255`; if not, hoist its declaration so it is.

- [ ] **Step 4: prime-debt-indexer**

Add the `entity` and (if missing) `common` imports as needed. Replace the `mc, err := multicall.NewClient(ethClient, common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"))` line with:

```go
	chainName, err := entity.ChainName(cfg.chainID)
	if err != nil {
		return fmt.Errorf("resolving chain name: %w", err)
	}
	mcTel, err := multicall.NewTelemetry(chainName)
	if err != nil {
		return fmt.Errorf("multicall telemetry: %w", err)
	}
	mc, err := multicall.NewClient(ethClient, common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"), multicall.WithTelemetry(mcTel))
	if err != nil {
		return fmt.Errorf("multicall client: %w", err)
	}
```

- [ ] **Step 5: Build and vet all four**

Run: `cd stl-verify && go build ./cmd/workers/... && go vet ./cmd/workers/...`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add stl-verify/cmd/workers/prime-allocation-indexer/main.go stl-verify/cmd/workers/morpho-indexer/main.go stl-verify/cmd/workers/oracle-price-indexer/main.go stl-verify/cmd/workers/prime-debt-indexer/main.go
git commit -m "feat(vec-81): wire multicall telemetry into indexer workers"
```

---

### Task 6: Full verification and review

- [ ] **Step 1: Run the full check suite**

Run: `cd stl-verify && make test-race && go build ./...`
Expected: PASS, no build errors.

- [ ] **Step 2: Spawn review subagents per CLAUDE.md**

Spawn in parallel: `pr-review-toolkit:code-reviewer`, `pr-review-toolkit:silent-failure-hunter`, a general-purpose architecture reviewer, and a general-purpose code-quality reviewer. Scope: the files in this plan. Apply blocking and should-fix items.

- [ ] **Step 3: Confirm Grafana queries**

After deploy, validate in Grafana:
- `rate(multicall_batch_size_count[5m])` (multicalls/sec)
- `rate(multicall_batch_size_sum[5m])` (individual calls/sec)
- `histogram_quantile(1.0, sum by (le) (rate(multicall_batch_size_bucket[5m])))` (burst size)
- `sum by (status) (rate(archive_writes_total[5m]))` (archive write rate by status)

---

## Self-Review

**Spec coverage:**
- `multicall.batch.size` histogram with chain label → Task 1, recorded in Task 2. Covered.
- `archive.writes.total` counter with chain/source/status → Task 3, chain set in Task 4. Covered.
- Functional-option wiring, nil-safe, global provider → Task 2 (option), Task 5 (cmds). Covered.
- Decorator wires in one place → Task 3/4 (archivingwire). Covered.
- Out-of-scope (duration, explicit counters, batching, semaphore removal) → not implemented. Correct.

**Placeholder scan:** No TBD/TODO. Each code step shows complete code. The one conditional ("add import if missing", "confirm in scope") is a verification instruction with the concrete command, not a placeholder.

**Type consistency:** `NewTelemetryWithProvider(mp, chain)` and `RecordBatch(ctx, size)` consistent across Tasks 1-2. `Config.Chain` / `Config.MeterProvider` defined in Task 3 and set in Task 4. `WithTelemetry(*Telemetry)` defined in Task 2, used in Task 5. `entity.ChainName(int64) (string, error)` used consistently. Histogram is `metricdata.Histogram[int64]` (Int64Histogram), counter is `metricdata.Sum[int64]` (Int64Counter), matching the test collectors.
