package dexconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ProtocolDescriptor identifies a DEX protocol in the protocol registry.
// GetOrCreateProtocol matches on (chain_id, Address); Name and ProtocolType are
// written only when the row is first created. Address must therefore match the
// row seeded by db/migrations/20260521_100000_create_dex_prereqs.sql — a wrong
// address silently creates a second, divergent protocol row (see
// ProtocolIDResolver for why that matters).
type ProtocolDescriptor struct {
	Address      common.Address
	Name         string
	ProtocolType string
	DeployBlock  int64
}

// ProtocolIDResolver resolves a worker's protocol_id once and caches it for the
// process lifetime. The first caller creates/loads the row under a mutex; the
// atomic fast-path then serves every subsequent call without locking. A worker
// handles a single chain, so one cached id is correct.
//
// Correctness depends on the descriptor naming a pre-seeded, committed row (see
// ProtocolDescriptor): Resolve caches the id while still inside the caller's
// transaction, so if GetOrCreateProtocol had to INSERT a new row and that
// transaction later rolled back, the cache would hold an id whose row no longer
// exists, failing the protocol_event FK on every later block until restart.
// Against the seeded rows GetOrCreateProtocol only ever reads a committed id, so
// the in-transaction cache is safe.
type ProtocolIDResolver struct {
	repo outbound.ProtocolRepository
	desc ProtocolDescriptor

	id atomic.Int64
	mu sync.Mutex
}

func NewProtocolIDResolver(repo outbound.ProtocolRepository, desc ProtocolDescriptor) *ProtocolIDResolver {
	return &ProtocolIDResolver{repo: repo, desc: desc}
}

// Resolve returns the cached protocol_id, loading it via GetOrCreateProtocol
// within tx on first use.
func (r *ProtocolIDResolver) Resolve(ctx context.Context, tx pgx.Tx, chainID int64) (int64, error) {
	if id := r.id.Load(); id != 0 {
		return id, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if id := r.id.Load(); id != 0 {
		return id, nil
	}
	id, err := r.repo.GetOrCreateProtocol(ctx, tx, chainID, r.desc.Address, r.desc.Name, r.desc.ProtocolType, r.desc.DeployBlock)
	if err != nil {
		return 0, fmt.Errorf("getting %s protocol_id: %w", r.desc.Name, err)
	}
	r.id.Store(id)
	return id, nil
}

// ProtocolEventInput is the protocol-agnostic shape of one decoded event the
// worker wants persisted to protocol_event. The worker marshals its decoded
// event into Payload and sets the identity fields (chain, block, tx, log index,
// event name, contract) from it.
type ProtocolEventInput struct {
	ContractAddress common.Address
	ChainID         int64
	BlockNumber     int64
	BlockVersion    int
	BlockTimestamp  time.Time
	TxHash          common.Hash
	LogIndex        uint
	EventName       string
	Payload         json.RawMessage
}

// ProtocolEventWriter persists decoded events to protocol_event, resolving the
// worker's protocol_id on the way.
type ProtocolEventWriter struct {
	resolver  *ProtocolIDResolver
	eventRepo outbound.EventRepository
}

func NewProtocolEventWriter(resolver *ProtocolIDResolver, eventRepo outbound.EventRepository) *ProtocolEventWriter {
	return &ProtocolEventWriter{resolver: resolver, eventRepo: eventRepo}
}

// Save resolves the protocol_id, builds a validated ProtocolEvent, and persists
// it within tx. Persistence is ON CONFLICT DO NOTHING, so a redelivered block
// is idempotent.
func (w *ProtocolEventWriter) Save(ctx context.Context, tx pgx.Tx, in ProtocolEventInput) error {
	protocolID, err := w.resolver.Resolve(ctx, tx, in.ChainID)
	if err != nil {
		return err
	}
	evt, err := entity.NewProtocolEvent(
		int(in.ChainID),
		protocolID,
		in.BlockNumber,
		in.BlockVersion,
		in.TxHash.Bytes(),
		int(in.LogIndex),
		in.ContractAddress.Bytes(),
		in.EventName,
		in.Payload,
		in.BlockTimestamp,
	)
	if err != nil {
		return fmt.Errorf("building protocol_event: %w", err)
	}
	return w.eventRepo.SaveEvent(ctx, tx, evt)
}
