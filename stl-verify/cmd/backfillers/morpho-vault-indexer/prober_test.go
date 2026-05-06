package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
)

// TestCollectProbeConfirmed exercises every reachable disposition of
// collectProbeConfirmed: confirm a valid V1, confirm a valid V2, skip the
// *ErrNotVault path silently, skip a foreign Morpho deployment, and skip a
// zero-address asset.
//
// Every error path inside the production ParseProbeResults wraps in
// *ErrNotVault, so the structural-error propagation branch added by the bug
// fix is not reachable through the real parser today. The fix nonetheless
// stands as defense-in-depth: any future change to ParseProbeResults that
// returns a non-*ErrNotVault error will now bubble up to probeBatchWithRetry
// rather than being silently dropped.
func TestCollectProbeConfirmed(t *testing.T) {
	t.Parallel()

	addrV1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addrNotVault := common.HexToAddress("0x2222222222222222222222222222222222222222")
	addrForeignMorpho := common.HexToAddress("0x3333333333333333333333333333333333333333")
	addrV2 := common.HexToAddress("0x4444444444444444444444444444444444444444")
	addrAssetZero := common.HexToAddress("0x5555555555555555555555555555555555555555")

	asset := common.HexToAddress("0xaaaa000000000000000000000000000000000000")
	curator := common.HexToAddress("0xcccc000000000000000000000000000000000000")
	liquidityAdapter := common.HexToAddress("0xdddd000000000000000000000000000000000000")
	foreignMorpho := common.HexToAddress("0xeeee000000000000000000000000000000000000")

	prober := newTestVaultProber(t)

	batch := []common.Address{addrV1, addrNotVault, addrForeignMorpho, addrV2, addrAssetZero}

	results := concatResults(
		v1ProbeResults(t, morpho_indexer.MorphoBlueAddress, asset),
		notVaultProbeResults(),
		v1ProbeResults(t, foreignMorpho, asset),
		v2ProbeResults(t, asset, curator, liquidityAdapter),
		v1ProbeResults(t, morpho_indexer.MorphoBlueAddress, common.Address{}),
	)

	confirmed, err := prober.collectProbeConfirmed(batch, results)
	if err != nil {
		t.Fatalf("collectProbeConfirmed: unexpected error: %v", err)
	}

	if len(confirmed) != 2 {
		t.Fatalf("expected 2 confirmed vaults, got %d: %+v", len(confirmed), confirmed)
	}
	if confirmed[0].address != addrV1 || confirmed[0].version != entity.MorphoVaultV1 {
		t.Errorf("expected first confirmed to be V1 at %s, got %+v", addrV1.Hex(), confirmed[0])
	}
	if confirmed[1].address != addrV2 || confirmed[1].version != entity.MorphoVaultV2 {
		t.Errorf("expected second confirmed to be V2 at %s, got %+v", addrV2.Hex(), confirmed[1])
	}

	// Locked-in contract: skipped addresses must not appear in confirmed.
	for _, c := range confirmed {
		switch c.address {
		case addrNotVault, addrForeignMorpho, addrAssetZero:
			t.Errorf("address %s should have been skipped, got %+v", c.address.Hex(), c)
		}
	}
}

// newTestVaultProber builds a *vaultProber suitable for collectProbeConfirmed
// tests. The multicaller and erc20ABI fields are unused because
// collectProbeConfirmed only consumes already-fetched probe results.
func newTestVaultProber(t *testing.T) *vaultProber {
	t.Helper()
	shared, err := morpho_indexer.NewVaultProber()
	if err != nil {
		t.Fatalf("NewVaultProber: %v", err)
	}
	return &vaultProber{
		multicaller:  nil,
		sharedProber: shared,
		erc20ABI:     nil,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// v1ProbeResults builds the 4-result MetaMorpho probe response for a vault
// whose MORPHO() returns morphoAddr and whose asset() returns asset.
func v1ProbeResults(t *testing.T, morphoAddr, asset common.Address) []outbound.Result {
	t.Helper()
	return []outbound.Result{
		{Success: true, ReturnData: packAddress(t, morphoAddr)},
		{Success: true, ReturnData: packAddress(t, asset)},
		{Success: false, ReturnData: nil}, // curator reverts on MetaMorpho
		{Success: false, ReturnData: nil}, // liquidityAdapter reverts on MetaMorpho
	}
}

// v2ProbeResults builds the 4-result VaultV2 probe response: MORPHO reverts,
// asset, curator, liquidityAdapter all return values.
func v2ProbeResults(t *testing.T, asset, curator, liquidityAdapter common.Address) []outbound.Result {
	t.Helper()
	return []outbound.Result{
		{Success: false, ReturnData: nil},
		{Success: true, ReturnData: packAddress(t, asset)},
		{Success: true, ReturnData: packAddress(t, curator)},
		{Success: true, ReturnData: packAddress(t, liquidityAdapter)},
	}
}

// notVaultProbeResults returns 4 reverted results — the address is not a vault.
func notVaultProbeResults() []outbound.Result {
	return []outbound.Result{
		{Success: false, ReturnData: nil},
		{Success: false, ReturnData: nil},
		{Success: false, ReturnData: nil},
		{Success: false, ReturnData: nil},
	}
}

// packAddress ABI-encodes an address into 32-byte multicall ReturnData form.
func packAddress(t *testing.T, addr common.Address) []byte {
	t.Helper()
	addrType, err := abi.NewType("address", "", nil)
	if err != nil {
		t.Fatalf("abi.NewType(address): %v", err)
	}
	data, err := abi.Arguments{{Type: addrType}}.Pack(addr)
	if err != nil {
		t.Fatalf("packing address %s: %v", addr.Hex(), err)
	}
	return data
}

// concatResults flattens a list of result slices in order.
func concatResults(slices ...[]outbound.Result) []outbound.Result {
	var total int
	for _, s := range slices {
		total += len(s)
	}
	out := make([]outbound.Result, 0, total)
	for _, s := range slices {
		out = append(out, s...)
	}
	return out
}
