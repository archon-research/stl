package archiving

import (
	"context"
	"log/slog"
	"math/big"
	"runtime/debug"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rawsckey"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
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
	// Gate tracks in-flight archive writes, shared across decorators. A handler
	// the SQS drain abandoned outlives its message loop, so Execute can still
	// run while the deferred drain waits; the gate refuses those writes.
	Gate *DrainGate
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
	writes   *WriteCounter
}

var _ outbound.Multicaller = (*Multicaller)(nil)

// NewMulticaller wraps inner so its calls are archived via arch.
func NewMulticaller(inner outbound.Multicaller, arch outbound.CallArchiver, cfg Config) *Multicaller {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Gate == nil {
		cfg.Gate = NewDrainGate(cfg.Logger)
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	writes := NewWriteCounter(cfg.MeterProvider, cfg.Chain, cfg.Source, cfg.Logger)
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
	m.archiveBatch(ctx, calls, results, blockNumber)
	return results, nil
}

// ExecuteAtHash forwards to the inner multicaller's hash-pinned path, then
// archives the batch the same way Execute does. It passes a nil blockNumber;
// archiveBatch recovers the real number from the context (see there).
func (m *Multicaller) ExecuteAtHash(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	results, err := m.inner.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return results, err
	}
	m.archiveBatch(ctx, calls, results, nil)
	return results, nil
}

// archiveBatch validates the call/result counts and schedules the detached
// background archive write. Shared by Execute and ExecuteAtHash so both stay
// in lockstep on truncation handling, metrics, and the fire-and-forget
// contract.
func (m *Multicaller) archiveBatch(ctx context.Context, calls []outbound.Call, results []outbound.Result, blockNumber *big.Int) {
	if blockNumber == nil {
		// The hash-pinned ExecuteAtHash path has no blockNumber argument; recover
		// it from the context, where live workers stamp it via WithBlockNumber.
		// Number-pinned Execute callers pass it positionally and it is unchanged.
		if bn, ok := BlockNumberFromContext(ctx); ok {
			blockNumber = big.NewInt(bn)
		}
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
		return
	}

	mcAddr := m.inner.Address().Hex()
	if blockNumber == nil {
		// rawsckey keys objects by block, so archiving at block 0 collides every
		// such batch under one key (the VEC-471 bug this fix pays down). No caller
		// legitimately archives without a height, so surface a regression rather
		// than silently writing genesis. Warn, not error: archiving stays
		// fire-and-forget and must not break the caller.
		m.cfg.Logger.Warn("archiving hash-pinned SC call batch with no resolvable block number; keying at block 0",
			"source", m.cfg.Source,
			"multicaller", mcAddr,
		)
	}

	blockVersion, _ := BlockVersionFromContext(ctx)
	detached := context.WithoutCancel(ctx)
	record := m.buildBatchRecord(calls[:n], results[:n], blockNumber, blockVersion, mcAddr)
	m.scheduleArchive(detached, record)
}

// Address forwards to the inner multicaller.
func (m *Multicaller) Address() common.Address { return m.inner.Address() }

// Close blocks until all in-flight archive writes complete. Production binaries
// drain via the budgeted drain archivingwire returns; this is the drain handle
// for a directly-constructed decorator (tests).
func (m *Multicaller) Close() { m.cfg.Gate.Wait() }

// recordWrite increments archive.writes.total with the outcome status.
func (m *Multicaller) recordWrite(err error) {
	status := writeStatusSuccess
	if err != nil {
		status = writeStatusError
	}
	m.writes.Record(status, 1)
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
	if m.cfg.Gate.Go(func() { m.archiveRecord(ctx, record) }) {
		return
	}
	m.writes.Record(writeStatusAbandoned, 1)
	m.cfg.Logger.Warn("archive drain already began; dropping this SC call batch",
		"source", record.Source,
		"block", record.BlockNumber,
		"block_version", record.BlockVersion,
		"calls", len(record.Calls),
	)
}

func (m *Multicaller) archiveRecord(ctx context.Context, record outbound.CallBatchRecord) {
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
		// A failed write is a permanent, unretried loss of an archived batch, so
		// surface it at error level rather than burying it in warnings.
		m.cfg.Logger.Error("archiving SC call batch failed",
			"error", err,
			"source", record.Source,
			"block", record.BlockNumber,
			"block_version", record.BlockVersion,
			"calls", len(record.Calls),
		)
	}
}
