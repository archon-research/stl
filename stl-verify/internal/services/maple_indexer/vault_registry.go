package maple_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// VaultRegistry is an in-memory mirror of the `maple_vault` DB rows.
//
// Maple ships Syrup as a small, fixed set of vaults (SyrupUSDC + SyrupUSDT
// today), so unlike morpho_indexer.VaultRegistry there is no
// auto-discovery, no `notVaults` negative cache, no probing path —
// the registry is read-only after LoadFromDB. When Maple launches a
// new Syrup vault, the deploy is followed by a seed migration that
// adds the row; the next worker restart picks it up.
type VaultRegistry struct {
	mu     sync.RWMutex
	vaults map[common.Address]*entity.MapleVault
	logger *slog.Logger
}

// NewVaultRegistry creates an empty registry. Call LoadFromDB before use.
func NewVaultRegistry(logger *slog.Logger) *VaultRegistry {
	return &VaultRegistry{
		vaults: make(map[common.Address]*entity.MapleVault),
		logger: logger.With("component", "maple-vault-registry"),
	}
}

// LoadFromDB replaces the in-memory registry with the current set of
// vaults persisted for the given chain. Idempotent; safe to call on
// every service start.
func (r *VaultRegistry) LoadFromDB(ctx context.Context, repo outbound.MapleRepository, chainID int64) error {
	loaded, err := repo.GetAllVaults(ctx, chainID)
	if err != nil {
		return fmt.Errorf("loading maple vaults: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if loaded != nil {
		r.vaults = loaded
	} else {
		r.vaults = make(map[common.Address]*entity.MapleVault)
	}

	r.logger.Info("loaded maple vaults from database", "count", len(r.vaults))
	return nil
}

// GetVault returns the vault entity for the given address, or nil if unknown.
func (r *VaultRegistry) GetVault(address common.Address) *entity.MapleVault {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.vaults[address]
}

// All returns a snapshot slice of every registered vault. The slice is a
// copy of the underlying map values, so callers can iterate without
// holding the registry lock.
func (r *VaultRegistry) All() []*entity.MapleVault {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*entity.MapleVault, 0, len(r.vaults))
	for _, v := range r.vaults {
		out = append(out, v)
	}
	return out
}

// Count returns the number of registered vaults.
func (r *VaultRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.vaults)
}
