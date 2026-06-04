// Package archiving wraps an outbound.Multicaller so every batch of contract
// calls is recorded to an outbound.CallArchiver for VEC-81 raw SC-call audit
// archival. The decorator is transparent: it forwards Execute unchanged and
// archives on a best-effort basis (failures are logged but never break the
// caller).
package archiving

import (
	"context"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Decoder optionally decodes a raw (Call, Result) pair into a structured value
// and the ABI method name. An indexer that holds the relevant ABI can supply
// one to enrich the archive; returning ok=false signals "no decoding
// available, archive raw bytes only".
//
// Decoders run on the hot path. They MUST be fast and MUST NOT panic — the
// decorator recovers panics defensively but a panicking decoder will still
// poison its archive line for that call.
type Decoder func(call outbound.Call, result outbound.Result) (decoded any, method string, ok bool)

// Clock returns the current wall-clock time. Injected for tests.
type Clock func() time.Time

// Option configures a Multicaller.
type Option func(*Multicaller)

// WithDecoder installs an optional Decoder. Without one, archived records
// store raw bytes only (Method = "", Decoded = nil).
func WithDecoder(d Decoder) Option {
	return func(m *Multicaller) { m.decoder = d }
}

// WithClock overrides the wall-clock source. Tests only.
func WithClock(c Clock) Option {
	return func(m *Multicaller) { m.now = c }
}

// WithLogger overrides the slog.Logger used for archive failures.
func WithLogger(l *slog.Logger) Option {
	return func(m *Multicaller) {
		if l != nil {
			m.logger = l
		}
	}
}

// Multicaller decorates an inner outbound.Multicaller, forwarding Execute
// unchanged and archiving each batch on the side. It implements
// outbound.Multicaller so existing services pick it up via the existing
// MulticallerFactory wiring with zero call-site changes.
type Multicaller struct {
	inner    outbound.Multicaller
	archiver outbound.CallArchiver
	chainID  int64
	buildID  int
	source   string
	decoder  Decoder
	now      Clock
	logger   *slog.Logger
}

// Compile-time interface check.
var _ outbound.Multicaller = (*Multicaller)(nil)

// New wraps inner so every Execute call is archived to archiver under the
// given source identifier and build_id. inner, archiver and a non-empty source
// are required.
func New(
	inner outbound.Multicaller,
	archiver outbound.CallArchiver,
	chainID int64,
	buildID int,
	source string,
	opts ...Option,
) *Multicaller {
	m := &Multicaller{
		inner:    inner,
		archiver: archiver,
		chainID:  chainID,
		buildID:  buildID,
		source:   source,
		now:      time.Now,
		logger:   slog.Default().With("component", "archiving-multicaller", "source", source),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Address forwards to the inner caller. The archive captures this on its own
// per-batch via the Multicaller meta field.
func (m *Multicaller) Address() common.Address {
	return m.inner.Address()
}

// Execute forwards to the inner caller and, on success, archives the batch.
// Archive failures are logged and swallowed (fail-open) — by the time we
// archive, the indexer's call has already succeeded and the hot path must not
// be broken by S3 hiccups.
func (m *Multicaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	results, err := m.inner.Execute(ctx, calls, blockNumber)
	if err != nil {
		// Do not archive failed batches: there are no canonical results
		// to record, and the indexer will retry the same call which will
		// produce its own archive on success.
		return results, err
	}
	m.archive(ctx, calls, results, blockNumber)
	return results, nil
}

func (m *Multicaller) archive(ctx context.Context, calls []outbound.Call, results []outbound.Result, blockNumber *big.Int) {
	if m.archiver == nil || len(calls) == 0 {
		return
	}

	bn := int64(0)
	if blockNumber != nil {
		bn = blockNumber.Int64()
	}

	bv, _ := BlockVersionFromContext(ctx)

	batch := outbound.CallBatch{
		ChainID:      m.chainID,
		BlockNumber:  bn,
		BlockVersion: bv,
		BuildID:      m.buildID,
		Source:       m.source,
		Multicaller:  m.inner.Address(),
		Timestamp:    m.now().UTC(),
		Records:      m.buildRecords(calls, results),
	}

	if err := m.archiver.ArchiveBatch(ctx, batch); err != nil {
		m.logger.WarnContext(ctx, "archive batch failed (fail-open: ingest continues)",
			"chain_id", batch.ChainID,
			"block_number", batch.BlockNumber,
			"block_version", batch.BlockVersion,
			"build_id", batch.BuildID,
			"calls", len(batch.Records),
			"error", err,
		)
	}
}

func (m *Multicaller) buildRecords(calls []outbound.Call, results []outbound.Result) []outbound.CallRecord {
	records := make([]outbound.CallRecord, 0, len(calls))
	for i, call := range calls {
		var res outbound.Result
		if i < len(results) {
			res = results[i]
		}
		method, decoded := m.decodeSafely(call, res)
		records = append(records, outbound.CallRecord{
			ContractAddress: call.Target,
			Method:          method,
			CallData:        call.CallData,
			Response:        res.ReturnData,
			Success:         res.Success,
			Decoded:         decoded,
		})
	}
	return records
}

// decodeSafely runs the optional decoder under a recover so a panic in
// user-supplied decoding code never poisons the hot path.
func (m *Multicaller) decodeSafely(call outbound.Call, res outbound.Result) (method string, decoded any) {
	if m.decoder == nil {
		return "", nil
	}
	defer func() {
		if r := recover(); r != nil {
			m.logger.Warn("decoder panicked; archiving raw bytes only",
				"target", call.Target.Hex(),
				"recover", r,
			)
			method = ""
			decoded = nil
		}
	}()
	d, mn, ok := m.decoder(call, res)
	if !ok {
		return "", nil
	}
	return mn, d
}
