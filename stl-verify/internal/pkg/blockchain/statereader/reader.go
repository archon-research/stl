// Package statereader adapts the Multicaller transport to the StateReader
// port: it owns the pin-mode dispatch and the result-count invariant.
package statereader

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type Reader struct {
	mc outbound.Multicaller
}

var _ outbound.StateReader = (*Reader)(nil)

func New(mc outbound.Multicaller) *Reader {
	return &Reader{mc: mc}
}

// TransportAddress exposes the underlying transport identity (the Multicall3
// address) for archiving records; it is not part of the StateReader port.
func (r *Reader) TransportAddress() common.Address {
	return r.mc.Address()
}

func (r *Reader) Read(ctx context.Context, pin outbound.BlockPin, calls []outbound.Call) ([]outbound.Result, error) {
	if err := validatePin(pin); err != nil {
		return nil, err
	}
	var (
		results []outbound.Result
		err     error
	)
	if h, ok := pin.Hash(); ok {
		results, err = r.mc.ExecuteAtHash(ctx, calls, h)
	} else {
		results, err = r.mc.Execute(ctx, calls, big.NewInt(pin.Number()))
	}
	if err != nil {
		return nil, fmt.Errorf("state read at block %d: %w", pin.Number(), err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("state read at block %d: %d calls returned %d results", pin.Number(), len(calls), len(results))
	}
	return results, nil
}

func validatePin(pin outbound.BlockPin) error {
	if pin.IsZero() {
		return fmt.Errorf("state read with an unset block pin")
	}
	if pin.Number() <= 0 {
		return fmt.Errorf("state read with non-positive block number %d", pin.Number())
	}
	// Unreachable via the constructors; guards a hand-rolled pin from a future
	// package-internal caller.
	if h, ok := pin.Hash(); ok && h == (common.Hash{}) {
		return fmt.Errorf("reorg-safe pin with zero hash at block %d", pin.Number())
	}
	return nil
}
