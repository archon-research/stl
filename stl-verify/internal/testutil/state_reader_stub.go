package testutil

import (
	"context"
	"errors"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// StateReaderStub implements outbound.StateReader for service tests. It
// records every pin it is handed, so a test can assert the property VEC-471
// protects directly: that a read was pinned to the expected mode and hash.
type StateReaderStub struct {
	mu     sync.Mutex
	ReadFn func(ctx context.Context, pin outbound.BlockPin, calls []outbound.Call) ([]outbound.Result, error)
	pins   []outbound.BlockPin
}

var _ outbound.StateReader = (*StateReaderStub)(nil)

func (s *StateReaderStub) Read(ctx context.Context, pin outbound.BlockPin, calls []outbound.Call) ([]outbound.Result, error) {
	s.mu.Lock()
	s.pins = append(s.pins, pin)
	s.mu.Unlock()
	if s.ReadFn == nil {
		return nil, errors.New("Read not stubbed")
	}
	return s.ReadFn(ctx, pin, calls)
}

// Pins returns a copy of every pin handed to Read, in call order.
func (s *StateReaderStub) Pins() []outbound.BlockPin {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]outbound.BlockPin, len(s.pins))
	copy(out, s.pins)
	return out
}

func (s *StateReaderStub) CallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pins)
}
