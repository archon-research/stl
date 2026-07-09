package archiving

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Reader decorates an outbound.StateReader so successful read batches are
// archived in the background, keyed by the pin's (number, version). Unlike the
// legacy Multicaller decorator there is no context recovery and no block-0
// fallback: a pin cannot exist without a block number, so the PR #520 keying
// collision is unrepresentable here.
type Reader struct {
	inner         outbound.StateReader
	transportAddr common.Address
	core
}

var _ outbound.StateReader = (*Reader)(nil)

// NewReader wraps inner so its reads are archived via arch. transportAddr is
// the transport identity stamped on records (the Multicall3 address).
func NewReader(inner outbound.StateReader, transportAddr common.Address, arch outbound.CallArchiver, cfg Config) *Reader {
	return &Reader{inner: inner, transportAddr: transportAddr, core: newCore(arch, cfg)}
}

func (r *Reader) Read(ctx context.Context, pin outbound.BlockPin, calls []outbound.Call) ([]outbound.Result, error) {
	results, err := r.inner.Read(ctx, pin, calls)
	if err != nil {
		return results, err
	}
	// The StateReader contract guarantees len(results) == len(calls), so no
	// truncation handling is needed on this path.
	if len(calls) == 0 {
		return results, nil
	}
	record := r.buildBatchRecord(calls, results, big.NewInt(pin.Number()), pin.Version(), r.transportAddr.Hex())
	r.scheduleArchive(context.WithoutCancel(ctx), record)
	return results, nil
}

// Close blocks until all in-flight archive writes complete (drain handle for a
// directly-constructed decorator; production drains via archivingwire).
func (r *Reader) Close() { r.cfg.Wait.Wait() }
