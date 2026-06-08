package archiving

import (
	"context"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rawsckey"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
)

const archiveTimeout = 30 * time.Second

// Config holds the static metadata stamped onto every archived call.
type Config struct {
	Source  string
	ChainID int64
	BuildID int64
	Wait    *sync.WaitGroup // shared across decorators; drained on shutdown
	Logger  *slog.Logger
	Clock   func() time.Time // injectable for tests; defaults to time.Now
}

// Multicaller decorates an inner outbound.Multicaller, archiving every
// successful call in the background (fire-and-forget).
type Multicaller struct {
	inner    outbound.Multicaller
	archiver outbound.CallArchiver
	cfg      Config
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
	return &Multicaller{inner: inner, archiver: arch, cfg: cfg}
}

// Execute forwards to the inner multicaller, then archives each call/result
// pair in a detached background goroutine. Archiving never blocks the caller
// and never affects the returned results or error.
func (m *Multicaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	results, err := m.inner.Execute(ctx, calls, blockNumber)
	if err != nil {
		return results, err
	}

	blockVersion, _ := BlockVersionFromContext(ctx)
	mcAddr := m.inner.Address().Hex()
	detached := context.WithoutCancel(ctx)

	for i := range calls {
		if i >= len(results) {
			break
		}
		m.scheduleArchive(detached, m.buildRecord(calls[i], results[i], blockNumber, blockVersion, mcAddr))
	}
	return results, err
}

// Address forwards to the inner multicaller.
func (m *Multicaller) Address() common.Address { return m.inner.Address() }

// Close blocks until all in-flight archive writes complete. Call during
// graceful shutdown before the process exits.
func (m *Multicaller) Close() { m.cfg.Wait.Wait() }

func (m *Multicaller) buildRecord(call outbound.Call, result outbound.Result, blockNumber *big.Int, blockVersion int, mcAddr string) outbound.CallRecord {
	var bn int64
	if blockNumber != nil {
		bn = blockNumber.Int64()
	}
	return outbound.CallRecord{
		ChainID:         m.cfg.ChainID,
		BlockNumber:     bn,
		BlockVersion:    blockVersion,
		BuildID:         m.cfg.BuildID,
		Source:          m.cfg.Source,
		Multicaller:     mcAddr,
		Timestamp:       m.cfg.Clock().UTC(),
		ContractAddress: call.Target.Hex(),
		Selector:        rawsckey.Selector(call.CallData),
		CallData:        append([]byte(nil), call.CallData...),
		Success:         result.Success,
		Response:        append([]byte(nil), result.ReturnData...),
	}
}

func (m *Multicaller) scheduleArchive(ctx context.Context, record outbound.CallRecord) {
	m.cfg.Wait.Add(1)
	go func() {
		defer m.cfg.Wait.Done()
		archiveCtx, cancel := context.WithTimeout(ctx, archiveTimeout)
		defer cancel()
		if err := m.archiver.Archive(archiveCtx, record); err != nil {
			m.cfg.Logger.Warn("archiving SC call failed",
				"error", err,
				"source", record.Source,
				"block", record.BlockNumber,
				"contract", record.ContractAddress,
				"selector", record.Selector,
			)
		}
	}()
}
