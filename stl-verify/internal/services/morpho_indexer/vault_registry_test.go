package morpho_indexer

import (
	"log/slog"
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

func TestDetectVaultVersion(t *testing.T) {
	tests := []struct {
		name    string
		dataLen int
		want    entity.MorphoVaultVersion
	}{
		{"V1.1 - 64 bytes", 64, entity.MorphoVaultV1},
		{"V2 - 128 bytes", 128, entity.MorphoVaultV2},
		{"V2 - more than 128", 256, entity.MorphoVaultV2},
		{"empty data", 0, entity.MorphoVaultV1},
		{"32 bytes", 32, entity.MorphoVaultV1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectVaultVersion(tt.dataLen)
			if got != tt.want {
				t.Errorf("DetectVaultVersion(%d) = %d, want %d", tt.dataLen, got, tt.want)
			}
		})
	}
}
