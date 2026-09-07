package fluid_vault_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// VaultRegistry holds the set of in-scope Fluid vaults (plain single-collateral
// / single-debt vaults with the targeted debt token). It is loaded from the
// FluidVaultRepository at startup and updated as new vaults are discovered.
//
// notVault is a negative cache of addresses that emitted a vault-shaped log but
// were classified out of scope (smart/DEX vault, or a non-targeted debt token),
// so repeat appearances short-circuit without re-reading the resolver.
type VaultRegistry struct {
	mu       sync.RWMutex
	vaults   map[common.Address]*entity.FluidVault
	notVault map[common.Address]struct{}
	logger   *slog.Logger
}

func NewVaultRegistry(logger *slog.Logger) *VaultRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	return &VaultRegistry{
		vaults:   make(map[common.Address]*entity.FluidVault),
		notVault: make(map[common.Address]struct{}),
		logger:   logger.With("component", "fluid-vault-registry"),
	}
}

// LoadFromDB replaces the in-memory set with all known vaults for the chain.
func (r *VaultRegistry) LoadFromDB(ctx context.Context, repo outbound.FluidVaultRepository, chainID int64) error {
	loaded, err := repo.GetAllVaults(ctx, chainID)
	if err != nil {
		return fmt.Errorf("loading fluid vaults: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if loaded == nil {
		loaded = make(map[common.Address]*entity.FluidVault)
	}
	r.vaults = loaded
	r.logger.Info("loaded fluid vaults from database", "count", len(r.vaults))
	return nil
}

func (r *VaultRegistry) IsKnownVault(address common.Address) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.vaults[address]
	return ok
}

func (r *VaultRegistry) IsKnownNotVault(address common.Address) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.notVault[address]
	return ok
}

func (r *VaultRegistry) MarkNotVault(address common.Address) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notVault[address] = struct{}{}
}

func (r *VaultRegistry) GetVault(address common.Address) *entity.FluidVault {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.vaults[address]
}

func (r *VaultRegistry) RegisterVault(vault *entity.FluidVault) {
	addr := common.BytesToAddress(vault.Address)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.vaults[addr] = vault
	delete(r.notVault, addr)
	r.logger.Info("registered fluid vault", "address", addr.Hex(), "vaultType", vault.VaultType)
}

func (r *VaultRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.vaults)
}
