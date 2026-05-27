package entity

import (
	"bytes"
	"testing"
)

func TestNewMapleVault_Valid(t *testing.T) {
	addr := bytes.Repeat([]byte{0xab}, 20)
	pool := bytes.Repeat([]byte{0xcd}, 20)
	v, err := NewMapleVault(1, 7, 9, addr, "Syrup USDC", "syrupUSDC", pool, 1, 20231245)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if v.ChainID != 1 || v.ProtocolID != 7 || v.AssetTokenID != 9 || v.VaultVersion != 1 {
		t.Fatalf("fields mis-set: %+v", v)
	}
	if v.Name != "Syrup USDC" || v.Symbol != "syrupUSDC" {
		t.Fatalf("name/symbol mis-set: %+v", v)
	}
	if v.CreatedAtBlock != 20231245 {
		t.Fatalf("createdAtBlock mis-set: %d", v.CreatedAtBlock)
	}
}

func TestNewMapleVault_RejectsBadAddress(t *testing.T) {
	if _, err := NewMapleVault(1, 7, 9, []byte{0x01}, "n", "s",
		bytes.Repeat([]byte{0xcd}, 20), 1, 0); err == nil {
		t.Fatal("expected error on 1-byte vault address")
	}
}

func TestNewMapleVault_RejectsBadPool(t *testing.T) {
	if _, err := NewMapleVault(1, 7, 9, bytes.Repeat([]byte{0xab}, 20),
		"n", "s", []byte{0x01}, 1, 0); err == nil {
		t.Fatal("expected error on 1-byte pool address")
	}
}

func TestNewMapleVault_RejectsZeroChain(t *testing.T) {
	if _, err := NewMapleVault(0, 7, 9, bytes.Repeat([]byte{0xab}, 20),
		"n", "s", bytes.Repeat([]byte{0xcd}, 20), 1, 0); err == nil {
		t.Fatal("expected error on zero chain id")
	}
}

func TestNewMapleVault_RejectsZeroProtocol(t *testing.T) {
	if _, err := NewMapleVault(1, 0, 9, bytes.Repeat([]byte{0xab}, 20),
		"n", "s", bytes.Repeat([]byte{0xcd}, 20), 1, 0); err == nil {
		t.Fatal("expected error on zero protocol id")
	}
}

func TestNewMapleVault_RejectsZeroAssetToken(t *testing.T) {
	if _, err := NewMapleVault(1, 7, 0, bytes.Repeat([]byte{0xab}, 20),
		"n", "s", bytes.Repeat([]byte{0xcd}, 20), 1, 0); err == nil {
		t.Fatal("expected error on zero asset token id")
	}
}

func TestNewMapleVault_RejectsZeroVaultVersion(t *testing.T) {
	if _, err := NewMapleVault(1, 7, 9, bytes.Repeat([]byte{0xab}, 20),
		"n", "s", bytes.Repeat([]byte{0xcd}, 20), 0, 0); err == nil {
		t.Fatal("expected error on zero vault version")
	}
}

func TestNewMapleVault_RejectsNegativeCreatedAtBlock(t *testing.T) {
	if _, err := NewMapleVault(1, 7, 9, bytes.Repeat([]byte{0xab}, 20),
		"n", "s", bytes.Repeat([]byte{0xcd}, 20), 1, -1); err == nil {
		t.Fatal("expected error on negative createdAtBlock")
	}
}
