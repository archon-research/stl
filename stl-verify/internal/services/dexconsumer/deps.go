package dexconsumer

import (
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// CommonDeps are the outbound ports every DEX worker needs regardless of
// protocol. It is the service-layer validation surface for the per-worker
// constructors: dexbootstrap.Deps is the cmd-layer bundle that supplies these
// but cannot be imported from the service layer. The protocol's own pool
// repository is validated by the worker itself, since its type differs per DEX.
type CommonDeps struct {
	SQSConsumer  outbound.SQSConsumer
	CacheReader  outbound.BlockCacheReader
	Multicaller  outbound.Multicaller
	TxManager    outbound.TxManager
	TokenRepo    outbound.TokenRepository
	ProtocolRepo outbound.ProtocolRepository
	EventRepo    outbound.EventRepository
}

// Validate fails fast at worker startup if any shared dependency is missing,
// turning a would-be nil-pointer panic on the hot path into a clear boot-time
// error.
func (d CommonDeps) Validate() error {
	switch {
	case d.SQSConsumer == nil:
		return fmt.Errorf("consumer is required")
	case d.CacheReader == nil:
		return fmt.Errorf("cache is required")
	case d.Multicaller == nil:
		return fmt.Errorf("multicaller is required")
	case d.TxManager == nil:
		return fmt.Errorf("txManager is required")
	case d.TokenRepo == nil:
		return fmt.Errorf("tokenRepo is required")
	case d.ProtocolRepo == nil:
		return fmt.Errorf("protocolRepo is required")
	case d.EventRepo == nil:
		return fmt.Errorf("eventRepo is required")
	}
	return nil
}
