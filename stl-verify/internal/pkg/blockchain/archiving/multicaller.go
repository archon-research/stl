package archiving

import (
	"context"
	"log/slog"
	"math/big"
	"runtime/debug"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rawsckey"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const archiveTimeout = 30 * time.Second

// Config holds the static metadata stamped onto every archived call.
type Config struct {
	Source  string
	ChainID int64
	BuildID int64
	// Chain is the chain name (e.g. "mainnet") used as the `chain` metric label.
	Chain string
	// Wait tracks in-flight background archive writes; shared across decorators
	// and drained on shutdown. It is safe to drain with a plain sync.WaitGroup
	// because every service stops its message loop (joining the goroutine that
	// calls Execute) before the drain runs, so no Add races the Wait.
	Wait *sync.WaitGroup
	// MeterProvider builds the archive.writes.total counter. nil uses the global
	// provider; tests inject a manual reader.
	MeterProvider metric.MeterProvider
	Logger        *slog.Logger
	Clock         func() time.Time // injectable for tests; defaults to time.Now
}

// Multicaller decorates an inner outbound.Multicaller. When the inner call
// succeeds it archives the whole batch in the background (fire-and-forget),
// recording each call regardless of its individual success flag. When the inner
// call itself errors nothing is archived, since there is no result batch to
// record.
type Multicaller struct {
	inner    outbound.Multicaller
	archiver outbound.CallArchiver
	cfg      Config
	writes   metric.Int64Counter
}

// NewMulticaller wraps inner so its calls are archived via arch.
func NewMulticaller(inner outbound.Multicaller, arch outbound.CallArchiver, cfg Config) *Multicaller {
	if cfg.Wait == nil {
		cfg.Wait = &sync.WaitGroup{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	writes, err := cfg.MeterProvider.
		Meter("github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving").
		Int64Counter("archive.writes.total", metric.WithDescription("Raw SC call archive write attempts by status"))
	if err != nil {
		// Metrics must never break the archiving hot path, so a counter that
		// fails to construct is logged and left nil (recordWrite no-ops on nil)
		// rather than failing NewMulticaller. This intentionally differs from the
		// fail-hard rule used for core dependencies.
		cfg.Logger.Error("building archive.writes.total counter; archive metrics disabled", "error", err)
	}
	return &Multicaller{inner: inner, archiver: arch, cfg: cfg, writes: writes}
}

// Execute forwards to the inner multicaller, then archives the entire
// (call, result) batch in a single detached background goroutine. Archiving
// never blocks the caller and never affects the returned results or error.
func (m *Multicaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	results, err := m.inner.Execute(ctx, calls, blockNumber)
	if err != nil {
		return results, err
	}

	n := len(calls)
	if len(results) != n {
		// The inner multicaller returned a different number of results than
		// calls; archive only the prefix that has both. Surface the anomaly
		// because an archive feature must not silently drop calls.
		m.cfg.Logger.Warn("multicaller result count does not match call count; trailing calls will not be archived",
			"source", m.cfg.Source,
			"block", blockNumber,
			"calls", n,
			"results", len(results),
		)
		if len(results) < n {
			n = len(results)
		}
	}
	if n == 0 {
		// Nothing to archive — don't schedule a goroutine that would write a
		// phantom empty object.
		return results, nil
	}

	blockVersion, _ := BlockVersionFromContext(ctx)
	mcAddr := m.inner.Address().Hex()
	detached := context.WithoutCancel(ctx)
	record := m.buildBatchRecord(calls[:n], results[:n], blockNumber, blockVersion, mcAddr)
	m.scheduleArchive(detached, record)
	// err is provably nil here (the err != nil path returned above); archiving
	// never affects the returned error.
	return results, nil
}

// Address forwards to the inner multicaller.
func (m *Multicaller) Address() common.Address { return m.inner.Address() }

// Close blocks until all in-flight archive writes complete. Call during
// graceful shutdown before the process exits. Production binaries drain via the
// shared sync.WaitGroup returned by archivingwire; this is the drain handle for
// a directly-constructed decorator (tests).
func (m *Multicaller) Close() { m.cfg.Wait.Wait() }

// recordWrite increments archive.writes.total with the outcome status. A nil
// counter (construction failed) is a no-op. It records against a background
// context because the counter increment is independent of the archive
// operation's timeout.
func (m *Multicaller) recordWrite(err error) {
	if m.writes == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	m.writes.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("chain", m.cfg.Chain),
		attribute.String("source", m.cfg.Source),
		attribute.String("status", status),
	))
}

func (m *Multicaller) buildBatchRecord(calls []outbound.Call, results []outbound.Result, blockNumber *big.Int, blockVersion int, mcAddr string) outbound.CallBatchRecord {
	var bn int64
	if blockNumber != nil {
		bn = blockNumber.Int64()
	}
	entries := make([]outbound.CallEntry, len(calls))
	for i := range calls {
		entries[i] = outbound.CallEntry{
			ContractAddress: calls[i].Target.Hex(),
			Selector:        rawsckey.Selector(calls[i].CallData),
			CallData:        append([]byte(nil), calls[i].CallData...),
			Success:         results[i].Success,
			Response:        append([]byte(nil), results[i].ReturnData...),
		}
	}
	return outbound.CallBatchRecord{
		ChainID:      m.cfg.ChainID,
		BlockNumber:  bn,
		BlockVersion: blockVersion,
		BuildID:      m.cfg.BuildID,
		Source:       m.cfg.Source,
		Multicaller:  mcAddr,
		Timestamp:    m.cfg.Clock().UTC(),
		Calls:        entries,
	}
}

func (m *Multicaller) scheduleArchive(ctx context.Context, record outbound.CallBatchRecord) {
	// Tracked by the shared WaitGroup so graceful shutdown drains this write. No
	// Add-after-Wait race: each service stops its message loop before draining,
	// so Execute (hence this Go) can never run concurrently with the drain.
	m.cfg.Wait.Go(func() {
		// Archiving is fire-and-forget: a panic here must never escape and crash
		// the worker, since archiving must not affect the hot path.
		defer func() {
			if r := recover(); r != nil {
				m.cfg.Logger.Error("panic while archiving SC call batch",
					"panic", r,
					"source", record.Source,
					"block", record.BlockNumber,
					"block_version", record.BlockVersion,
					"calls", len(record.Calls),
					"stack", string(debug.Stack()),
				)
			}
		}()

		archiveCtx, cancel := context.WithTimeout(ctx, archiveTimeout)
		defer cancel()
		err := m.archiver.Archive(archiveCtx, record)
		m.recordWrite(err)
		if err != nil {
			// A failed write is a permanent, unretried loss of an archived
			// batch, so surface it at error level rather than burying it in
			// warnings.
			m.cfg.Logger.Error("archiving SC call batch failed",
				"error", err,
				"source", record.Source,
				"block", record.BlockNumber,
				"block_version", record.BlockVersion,
				"calls", len(record.Calls),
			)
		}
	})
}
