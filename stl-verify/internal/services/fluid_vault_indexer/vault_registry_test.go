package fluid_vault_indexer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func fluidVaultFixture(addr common.Address) *entity.FluidVault {
	return &entity.FluidVault{
		ID:                7,
		ChainID:           1,
		ProtocolID:        2,
		Address:           addr.Bytes(),
		VaultType:         "10000",
		CollateralTokenID: 3,
		DebtTokenID:       4,
		CreatedAtBlock:    100,
	}
}

func TestVaultRegistry_LoadFromDB(t *testing.T) {
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	repo := &stubFluidRepo{vaults: map[common.Address]*entity.FluidVault{addr: fluidVaultFixture(addr)}}
	r := NewVaultRegistry(testLogger())

	if err := r.LoadFromDB(context.Background(), repo, 1); err != nil {
		t.Fatalf("LoadFromDB: %v", err)
	}
	if !r.IsKnownVault(addr) {
		t.Errorf("expected vault to be known after load")
	}
	if r.Count() != 1 {
		t.Errorf("count = %d, want 1", r.Count())
	}
}

func TestVaultRegistry_LoadFromDB_Error(t *testing.T) {
	repo := &stubFluidRepo{getAllErr: errors.New("boom")}
	r := NewVaultRegistry(testLogger())
	if err := r.LoadFromDB(context.Background(), repo, 1); err == nil {
		t.Fatal("expected error")
	}
}

func TestVaultRegistry_RegisterAndNotVault(t *testing.T) {
	addr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	r := NewVaultRegistry(testLogger())

	r.MarkNotVault(addr)
	if !r.IsKnownNotVault(addr) {
		t.Errorf("expected addr marked not-vault")
	}

	// Registering a previously not-vault address clears the negative cache.
	r.RegisterVault(fluidVaultFixture(addr))
	if !r.IsKnownVault(addr) {
		t.Errorf("expected vault known after register")
	}
	if r.IsKnownNotVault(addr) {
		t.Errorf("register should clear not-vault marker")
	}
	if got := r.GetVault(addr); got == nil || got.ID != 7 {
		t.Errorf("GetVault returned %v", got)
	}
}
