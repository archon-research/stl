package main

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/uniswap_v3_dex"
)

// Shared CLI/env parsing (queue, db, redis, alchemy, S3, chain ID, deploy env)
// lives in cmd/workers/internal/dexbootstrap and is covered by
// dexbootstrap/parseconfig_test.go. This file is the smaller sibling: it only
// covers the UV3-specific NFPM_ADDRESS resolution.

func TestResolveNFPMAddress_DefaultsToMainnet(t *testing.T) {
	t.Setenv("NFPM_ADDRESS", "")
	addr, err := resolveNFPMAddress()
	if err != nil {
		t.Fatalf("resolveNFPMAddress: %v", err)
	}
	if addr != uniswap_v3_dex.DefaultNFPMAddress {
		t.Errorf("addr = %s, want default %s", addr.Hex(), uniswap_v3_dex.DefaultNFPMAddress.Hex())
	}
}

func TestResolveNFPMAddress_HonoursOverride(t *testing.T) {
	override := "0xAaAAAaAaaAaAaaaAaAAAAAaaAAaAaAaAaAaAaaAa"
	t.Setenv("NFPM_ADDRESS", override)
	addr, err := resolveNFPMAddress()
	if err != nil {
		t.Fatalf("resolveNFPMAddress: %v", err)
	}
	if addr != common.HexToAddress(override) {
		t.Errorf("addr = %s, want %s (env override)", addr.Hex(), override)
	}
}

func TestResolveNFPMAddress_RejectsMalformed(t *testing.T) {
	t.Setenv("NFPM_ADDRESS", "notahex")
	_, err := resolveNFPMAddress()
	if err == nil {
		t.Fatal("expected error for non-hex NFPM_ADDRESS")
	}
	if !strings.Contains(err.Error(), "NFPM_ADDRESS") {
		t.Errorf("error %q must reference NFPM_ADDRESS for operator clarity", err)
	}
}
