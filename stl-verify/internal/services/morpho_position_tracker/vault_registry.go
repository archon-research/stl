package morpho_position_tracker

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
	mu     sync.RWMutex
	vaults map[common.Address]*entity.MorphoVault
	logger *slog.Logger
}

// NewVaultRegistry creates a new empty VaultRegistry.
func NewVaultRegistry(logger *slog.Logger) *VaultRegistry {
	return &VaultRegistry{
		vaults: make(map[common.Address]*entity.MorphoVault),
		logger: logger.With("component", "vault-registry"),
	}
}

// LoadFromDB loads all known vault addresses from the database.
func (r *VaultRegistry) LoadFromDB(ctx context.Context, repo outbound.MorphoRepository) error {
	addresses, err := repo.GetAllVaultAddresses(ctx)
	if err != nil {
		return fmt.Errorf("loading vault addresses: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, addr := range addresses {
		vault, err := repo.GetVaultByAddress(ctx, addr)
		if err != nil {
			r.logger.Warn("failed to load vault details", "address", addr.Hex(), "error", err)
			continue
		}
		if vault != nil {
			r.vaults[addr] = vault
		}
	}

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

// DetectVaultVersion determines if a vault is V1.1 or V2 based on AccrueInterest event data length.
// V1.1 AccrueInterest has 64 bytes (2 uint256: newTotalAssets, feeShares).
// V2 AccrueInterest has 128 bytes (4 uint256: newTotalAssets, interest, feeShares, feeAssets).
// If detection fails, defaults to V1.
func DetectVaultVersion(accrueInterestDataLen int) entity.MorphoVaultVersion {
	if accrueInterestDataLen >= 128 {
		return entity.MorphoVaultV2
	}
	return entity.MorphoVaultV1
}
