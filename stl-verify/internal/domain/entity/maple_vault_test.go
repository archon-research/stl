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

// mapleVaultArgs is a complete set of valid NewMapleVault arguments. Each
// rejection case clones this baseline and corrupts exactly one field so a
// failure isolates the single invariant under test (single-fault).
type mapleVaultArgs struct {
	chainID, protocolID, assetTokenID int64
	address                           []byte
	name, symbol                      string
	poolAddress                       []byte
	vaultVersion                      int16
	createdAtBlock                    int64
}

func validMapleVaultArgs() mapleVaultArgs {
	return mapleVaultArgs{
		chainID:        1,
		protocolID:     7,
		assetTokenID:   9,
		address:        bytes.Repeat([]byte{0xab}, 20),
		name:           "Syrup USDC",
		symbol:         "syrupUSDC",
		poolAddress:    bytes.Repeat([]byte{0xcd}, 20),
		vaultVersion:   1,
		createdAtBlock: 20231245,
	}
}

func TestNewMapleVault_Rejects(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*mapleVaultArgs)
	}{
		{"bad vault address", func(a *mapleVaultArgs) { a.address = []byte{0x01} }},
		{"bad pool address", func(a *mapleVaultArgs) { a.poolAddress = []byte{0x01} }},
		{"zero chain id", func(a *mapleVaultArgs) { a.chainID = 0 }},
		{"zero protocol id", func(a *mapleVaultArgs) { a.protocolID = 0 }},
		{"zero asset token id", func(a *mapleVaultArgs) { a.assetTokenID = 0 }},
		{"zero vault version", func(a *mapleVaultArgs) { a.vaultVersion = 0 }},
		{"negative created at block", func(a *mapleVaultArgs) { a.createdAtBlock = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := validMapleVaultArgs()
			tc.corrupt(&a)
			if _, err := NewMapleVault(a.chainID, a.protocolID, a.assetTokenID, a.address,
				a.name, a.symbol, a.poolAddress, a.vaultVersion, a.createdAtBlock); err == nil {
				t.Fatalf("expected error on %s", tc.name)
			}
		})
	}
}
