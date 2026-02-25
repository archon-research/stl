package morpho_indexer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestVaultRegistry(t *testing.T) {
	logger := slog.Default()
	registry := NewVaultRegistry(logger)

	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	vault := &entity.MorphoVault{
		ID:           1,
		Name:         "Test Vault",
		VaultVersion: entity.MorphoVaultV1,
	}

	if registry.IsKnownVault(addr) {
		t.Error("empty registry should not know any vault")
	}
	if registry.Count() != 0 {
		t.Errorf("Count() = %d, want 0", registry.Count())
	}

	registry.RegisterVault(addr, vault)

	if !registry.IsKnownVault(addr) {
		t.Error("should know registered vault")
	}
	if registry.Count() != 1 {
		t.Errorf("Count() = %d, want 1", registry.Count())
	}

	got := registry.GetVault(addr)
	if got == nil {
		t.Fatal("GetVault returned nil for registered vault")
	}
	if got.Name != "Test Vault" {
		t.Errorf("Name = %q, want %q", got.Name, "Test Vault")
	}

	unknown := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if registry.GetVault(unknown) != nil {
		t.Error("GetVault should return nil for unknown vault")
	}
}

func TestVaultRegistry_LoadFromDB(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	tests := []struct {
		name       string
		vaults     map[common.Address]*entity.MorphoVault
		err        error
		wantCount  int
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "loads multiple vaults",
			vaults: map[common.Address]*entity.MorphoVault{
				addr1: {ID: 1, Name: "Vault A", VaultVersion: entity.MorphoVaultV1},
				addr2: {ID: 2, Name: "Vault B", VaultVersion: entity.MorphoVaultV2},
			},
			wantCount: 2,
		},
		{
			name:      "empty database",
			vaults:    map[common.Address]*entity.MorphoVault{},
			wantCount: 0,
		},
		{
			name:       "repo error propagates",
			err:        fmt.Errorf("database connection lost"),
			wantErr:    true,
			wantErrMsg: "loading vaults",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := NewVaultRegistry(slog.Default())
			repo := &mockMorphoRepo{
				GetAllVaultsFn: func(_ context.Context) (map[common.Address]*entity.MorphoVault, error) {
					if tt.err != nil {
						return nil, tt.err
					}
					return tt.vaults, nil
				},
			}

			err := registry.LoadFromDB(context.Background(), repo)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.wantErrMsg != "" && !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Errorf("error %q should contain %q", err.Error(), tt.wantErrMsg)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if registry.Count() != tt.wantCount {
				t.Errorf("Count() = %d, want %d", registry.Count(), tt.wantCount)
			}

			for addr, want := range tt.vaults {
				got := registry.GetVault(addr)
				if got == nil {
					t.Errorf("vault %s not found", addr.Hex())
					continue
				}
				if got.Name != want.Name {
					t.Errorf("vault %s Name = %q, want %q", addr.Hex(), got.Name, want.Name)
				}
			}
		})
	}
}
