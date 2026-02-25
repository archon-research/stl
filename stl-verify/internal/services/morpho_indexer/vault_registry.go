package morpho_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// VaultRegistry manages the set of known MetaMorpho vault addresses.
// It is loaded from the database on startup and updated dynamically when
// new vaults are discovered from event logs.
type VaultRegistry struct {
	mu        sync.RWMutex
	vaults    map[common.Address]*entity.MorphoVault
	notVaults map[common.Address]struct{} // addresses confirmed to not be vaults
	logger    *slog.Logger
}

// NewVaultRegistry creates a new empty VaultRegistry.
func NewVaultRegistry(logger *slog.Logger) *VaultRegistry {
	return &VaultRegistry{
		vaults:    make(map[common.Address]*entity.MorphoVault),
		notVaults: make(map[common.Address]struct{}),
		logger:    logger.With("component", "vault-registry"),
	}
}

// LoadFromDB loads all known vaults for the given chain from the database.
func (r *VaultRegistry) LoadFromDB(ctx context.Context, repo outbound.MorphoRepository, chainID int64) error {
	loaded, err := repo.GetAllVaults(ctx, chainID)
	if err != nil {
		return fmt.Errorf("loading vaults: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.vaults = loaded

	r.logger.Info("loaded vaults from database", "count", len(r.vaults))
	return nil
}

// IsKnownVault returns true if the address is a known MetaMorpho vault.
func (r *VaultRegistry) IsKnownVault(address common.Address) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.vaults[address]
	return ok
}

// IsKnownNotVault returns true if the address has been tried and confirmed to not be a vault.
func (r *VaultRegistry) IsKnownNotVault(address common.Address) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.notVaults[address]
	return ok
}

// MarkNotVault records an address as confirmed not a vault, so it won't be retried.
func (r *VaultRegistry) MarkNotVault(address common.Address) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notVaults[address] = struct{}{}
}

// GetVault returns the vault entity for the given address, or nil if unknown.
func (r *VaultRegistry) GetVault(address common.Address) *entity.MorphoVault {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.vaults[address]
}

// RegisterVault adds a vault to the in-memory registry.
func (r *VaultRegistry) RegisterVault(address common.Address, vault *entity.MorphoVault) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.vaults[address] = vault
	delete(r.notVaults, address)
	r.logger.Info("registered new vault",
		"address", address.Hex(),
		"name", vault.Name,
		"version", vault.VaultVersion)
}

// Count returns the number of registered vaults.
func (r *VaultRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.vaults)
}
