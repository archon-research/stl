package main

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
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

// TestFetchVaultMetadata exercises the asset-decimals branch added by VEC-198
// across three dispositions:
//
//   - happy path: every sub-call succeeds, vault lands with the asset's
//     decimals (NOT the vault share's decimals).
//   - decimals call reverts: vault is dropped to avoid persisting an
//     AssetDecimals=0 row that would block the live indexer's later
//     correction (token_repository UPSERT preserves existing decimals on
//     conflict).
//   - decimals returns malformed bytes: same skip-on-failure outcome.
//
// The fix is load-bearing: swapping the skip back to "persist with
// AssetDecimals=0" makes the second and third cases produce a vault in the
// returned slice, which is what these tests catch.
func TestFetchVaultMetadata(t *testing.T) {
	t.Parallel()

	vaultAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	assetAddr := common.HexToAddress("0xaaaa000000000000000000000000000000000000")

	tests := []struct {
		name                string
		assetSymbolResult   outbound.Result
		assetDecimalsResult outbound.Result
		wantConfirmed       bool
		wantAssetSymbol     string
		wantAssetDecimals   uint8
	}{
		{
			name:                "happy path — asset symbol and decimals both decode",
			assetSymbolResult:   okStringResult(t, "USDT"),
			assetDecimalsResult: okUint8Result(t, 6),
			wantConfirmed:       true,
			wantAssetSymbol:     "USDT",
			wantAssetDecimals:   6,
		},
		{
			name:                "decimals call reverts → skip vault",
			assetSymbolResult:   okStringResult(t, "USDT"),
			assetDecimalsResult: outbound.Result{Success: false, ReturnData: nil},
			wantConfirmed:       false,
		},
		{
			name:                "decimals unpack fails on malformed bytes → skip vault",
			assetSymbolResult:   okStringResult(t, "USDT"),
			assetDecimalsResult: outbound.Result{Success: true, ReturnData: []byte{0x01, 0x02, 0x03, 0x04}},
			wantConfirmed:       false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			prober, mc := newTestVaultProberWithMock(t)
			mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				// fetchVaultMetadata appends NumDetailsCalls + 2 calls per
				// probed vault: name, symbol, decimals, skimRecipient,
				// then asset.symbol(), asset.decimals().
				if got, want := len(calls), prober.sharedProber.NumDetailsCalls()+2; got != want {
					t.Fatalf("expected %d calls, got %d", want, got)
				}
				return concatResults(
					vaultDetailsResults(t, "Vault Name", "vSYM", 18, false),
					[]outbound.Result{tc.assetSymbolResult, tc.assetDecimalsResult},
				), nil
			}

			confirmed := []confirmedProbe{{
				address: vaultAddr,
				asset:   assetAddr,
				version: entity.MorphoVaultV1,
			}}
			firstBlocks := map[common.Address]int64{vaultAddr: 12345}

			vaults, err := prober.fetchVaultMetadata(context.Background(), confirmed, firstBlocks, big.NewInt(100))
			if err != nil {
				t.Fatalf("fetchVaultMetadata: unexpected error: %v", err)
			}

			if tc.wantConfirmed {
				if len(vaults) != 1 {
					t.Fatalf("expected 1 confirmed vault, got %d", len(vaults))
				}
				v := vaults[0]
				if v.Address != vaultAddr {
					t.Errorf("address: want %s, got %s", vaultAddr.Hex(), v.Address.Hex())
				}
				if v.AssetSymbol != tc.wantAssetSymbol {
					t.Errorf("AssetSymbol: want %q, got %q", tc.wantAssetSymbol, v.AssetSymbol)
				}
				if v.AssetDecimals != tc.wantAssetDecimals {
					t.Errorf("AssetDecimals: want %d, got %d", tc.wantAssetDecimals, v.AssetDecimals)
				}
				return
			}

			if len(vaults) != 0 {
				t.Fatalf("expected vault to be skipped, got %d vaults: %+v", len(vaults), vaults)
			}
		})
	}
}

// newTestVaultProberWithMock builds a *vaultProber wired to a MockMulticaller
// and a real ERC20 ABI — both required for fetchVaultMetadata to operate.
// Returns the prober and the mock so tests can wire ExecuteFn.
func newTestVaultProberWithMock(t *testing.T) (*vaultProber, *testutil.MockMulticaller) {
	t.Helper()
	shared, err := morpho_indexer.NewVaultProber()
	if err != nil {
		t.Fatalf("NewVaultProber: %v", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}
	mc := testutil.NewMockMulticaller()
	return &vaultProber{
		multicaller:  mc,
		sharedProber: shared,
		erc20ABI:     erc20ABI,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, mc
}

// vaultDetailsResults builds the 4-result MetaMorpho details response
// (name, symbol, decimals, skimRecipient). isV1_1 controls whether
// skimRecipient succeeds — V1 reverts, V1.1 returns an address, V2 reverts.
func vaultDetailsResults(t *testing.T, name, symbol string, decimals uint8, isV1_1 bool) []outbound.Result {
	t.Helper()
	skim := outbound.Result{Success: false, ReturnData: nil}
	if isV1_1 {
		skim = outbound.Result{Success: true, ReturnData: packAddress(t, common.HexToAddress("0x1"))}
	}
	return []outbound.Result{
		{Success: true, ReturnData: packString(t, name)},
		{Success: true, ReturnData: packString(t, symbol)},
		{Success: true, ReturnData: packUint8(t, decimals)},
		skim,
	}
}

// okStringResult returns a successful result whose ReturnData is the ABI
// encoding of a single string (used for ERC20 symbol() / name()).
func okStringResult(t *testing.T, s string) outbound.Result {
	t.Helper()
	return outbound.Result{Success: true, ReturnData: packString(t, s)}
}

// okUint8Result returns a successful result whose ReturnData is the ABI
// encoding of a single uint8 (used for ERC20 decimals()).
func okUint8Result(t *testing.T, v uint8) outbound.Result {
	t.Helper()
	return outbound.Result{Success: true, ReturnData: packUint8(t, v)}
}

// packString ABI-encodes a string into multicall ReturnData form.
func packString(t *testing.T, s string) []byte {
	t.Helper()
	strType, err := abi.NewType("string", "", nil)
	if err != nil {
		t.Fatalf("abi.NewType(string): %v", err)
	}
	data, err := abi.Arguments{{Type: strType}}.Pack(s)
	if err != nil {
		t.Fatalf("packing string %q: %v", s, err)
	}
	return data
}

// packUint8 ABI-encodes a uint8 into multicall ReturnData form.
func packUint8(t *testing.T, v uint8) []byte {
	t.Helper()
	u8Type, err := abi.NewType("uint8", "", nil)
	if err != nil {
		t.Fatalf("abi.NewType(uint8): %v", err)
	}
	data, err := abi.Arguments{{Type: u8Type}}.Pack(v)
	if err != nil {
		t.Fatalf("packing uint8 %d: %v", v, err)
	}
	return data
}

// TestUnpackAssetDecimals exercises every explicit failure mode of
// unpackAssetDecimals plus the happy path. The function folds four failure
// dispositions into one error return; each branch is covered here so that an
// accidental "return 0, nil" on any failure mode is caught directly rather
// than via the indirect fetchVaultMetadata harness.
//
// The strict uint8 type assertion (unpacked[0].(uint8)) is unreachable through
// the real ERC20 ABI — its decimals() output is typed uint8, so a successful
// Unpack always yields uint8. Same for the zero-length unpack guard: a
// successful ABI Unpack of a single-output method always returns at least one
// value. Both guards stand as defense against future ABI tweaks; we leave
// them uncovered rather than fake the call site to bypass the ABI.
func TestUnpackAssetDecimals(t *testing.T) {
	t.Parallel()

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}

	tests := []struct {
		name      string
		result    outbound.Result
		wantValue uint8
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "happy path — successful decimals decode",
			result:    okUint8Result(t, 6),
			wantValue: 6,
			wantErr:   false,
		},
		{
			name:      "revert — Success=false propagates as error",
			result:    outbound.Result{Success: false, ReturnData: nil},
			wantErr:   true,
			errSubstr: "reverted",
		},
		{
			name:      "empty return data — Success=true but zero bytes",
			result:    outbound.Result{Success: true, ReturnData: nil},
			wantErr:   true,
			errSubstr: "no data",
		},
		{
			name:      "malformed bytes — Unpack rejects short payload",
			result:    outbound.Result{Success: true, ReturnData: []byte{0xff, 0xff, 0xff, 0xff}},
			wantErr:   true,
			errSubstr: "unpacking decimals",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := unpackAssetDecimals(erc20ABI, tc.result)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (returned value %d)", got)
				}
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("error: want substring %q, got %q", tc.errSubstr, err.Error())
				}
				if got != 0 {
					t.Errorf("on error, expected returned value to be 0, got %d", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.wantValue {
				t.Errorf("value: want %d, got %d", tc.wantValue, got)
			}
		})
	}
}

// TestCollectProbeConfirmed_CardinalityMismatch verifies the guard at the top
// of collectProbeConfirmed: when len(results) != len(batch) * callsPerProbe,
// the function returns an error rather than indexing out of bounds.
//
// Why this matters: an inverted comparison (e.g. `>` instead of `!=`) would
// surface only in production. The shorter-than-expected case is tested here;
// the longer-than-expected case is symmetric and exercised below.
func TestCollectProbeConfirmed_CardinalityMismatch(t *testing.T) {
	t.Parallel()

	prober := newTestVaultProber(t)
	callsPerProbe := prober.sharedProber.NumProbeCalls()

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	batch := []common.Address{addr1, addr2}
	expected := len(batch) * callsPerProbe

	t.Run("too few results", func(t *testing.T) {
		t.Parallel()
		// 1 result vs expected 8 (2 addrs * 4 calls).
		short := []outbound.Result{{Success: false, ReturnData: nil}}

		confirmed, err := prober.collectProbeConfirmed(batch, short)
		if err == nil {
			t.Fatalf("expected cardinality error, got nil (confirmed=%+v)", confirmed)
		}
		if !strings.Contains(err.Error(), "expected") || !strings.Contains(err.Error(), "probe results") {
			t.Errorf("error message: want it to mention expected probe results, got %q", err.Error())
		}
		// Sanity: the message should embed the right counts.
		for _, s := range []string{
			"expected 8 probe results",
			"batch of 2",
			"got 1",
		} {
			if !strings.Contains(err.Error(), s) {
				t.Errorf("error message: want substring %q, got %q", s, err.Error())
			}
		}
	})

	t.Run("too many results", func(t *testing.T) {
		t.Parallel()
		// 9 results vs expected 8 (2 addrs * 4 calls). One extra at the end.
		long := make([]outbound.Result, expected+1)
		for i := range long {
			long[i] = outbound.Result{Success: false, ReturnData: nil}
		}

		confirmed, err := prober.collectProbeConfirmed(batch, long)
		if err == nil {
			t.Fatalf("expected cardinality error, got nil (confirmed=%+v)", confirmed)
		}
		if !strings.Contains(err.Error(), "expected 8 probe results") {
			t.Errorf("error message: want substring %q, got %q", "expected 8 probe results", err.Error())
		}
		if !strings.Contains(err.Error(), "got 9") {
			t.Errorf("error message: want substring %q, got %q", "got 9", err.Error())
		}
	})
}

// TestFetchVaultMetadata_CardinalityMismatch verifies the guard immediately
// after Execute in fetchVaultMetadata. The multicaller is mocked to return a
// result slice of the wrong length; the function must surface the error
// rather than panic on out-of-bounds slice access downstream.
//
// Covers both shorter-than-expected and longer-than-expected, mirroring the
// collectProbeConfirmed test so an inverted comparison in either direction
// fails the test.
func TestFetchVaultMetadata_CardinalityMismatch(t *testing.T) {
	t.Parallel()

	vaultAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	assetAddr := common.HexToAddress("0xaaaa000000000000000000000000000000000000")

	confirmed := []confirmedProbe{{
		address: vaultAddr,
		asset:   assetAddr,
		version: entity.MorphoVaultV1,
	}}
	firstBlocks := map[common.Address]int64{vaultAddr: 12345}

	t.Run("too few results", func(t *testing.T) {
		t.Parallel()

		prober, mc := newTestVaultProberWithMock(t)
		expected := prober.sharedProber.NumDetailsCalls() + numAssetExtensionCalls
		mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			// Return one fewer than expected.
			out := make([]outbound.Result, expected-1)
			for i := range out {
				out[i] = outbound.Result{Success: false, ReturnData: nil}
			}
			return out, nil
		}

		vaults, err := prober.fetchVaultMetadata(context.Background(), confirmed, firstBlocks, big.NewInt(100))
		if err == nil {
			t.Fatalf("expected cardinality error, got nil (vaults=%+v)", vaults)
		}
		if !strings.Contains(err.Error(), "expected") || !strings.Contains(err.Error(), "metadata results") {
			t.Errorf("error message: want it to mention expected metadata results, got %q", err.Error())
		}
		if !strings.Contains(err.Error(), "for 1 confirmed vaults") {
			t.Errorf("error message: want substring %q, got %q", "for 1 confirmed vaults", err.Error())
		}
	})

	t.Run("too many results", func(t *testing.T) {
		t.Parallel()

		prober, mc := newTestVaultProberWithMock(t)
		expected := prober.sharedProber.NumDetailsCalls() + numAssetExtensionCalls
		mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			// Return one more than expected.
			out := make([]outbound.Result, expected+1)
			for i := range out {
				out[i] = outbound.Result{Success: false, ReturnData: nil}
			}
			return out, nil
		}

		vaults, err := prober.fetchVaultMetadata(context.Background(), confirmed, firstBlocks, big.NewInt(100))
		if err == nil {
			t.Fatalf("expected cardinality error, got nil (vaults=%+v)", vaults)
		}
		if !strings.Contains(err.Error(), "expected") || !strings.Contains(err.Error(), "metadata results") {
			t.Errorf("error message: want it to mention expected metadata results, got %q", err.Error())
		}
	})
}

// TestFetchVaultMetadata_MultiVault locks in the per-vault offset arithmetic
// when len(probeConfirmed) > 1. The single-vault TestFetchVaultMetadata cases
// always run with base == 0, which would mask a bug that uses a fixed offset
// instead of i * callsPerMetadata for the second vault's reads.
//
// Two vaults are run through fetchVaultMetadata with distinct asset symbols
// and decimals (USDT/6 then USDC/8). If the loop arithmetic regresses to a
// constant offset, vault B's AssetSymbol/AssetDecimals will be wrong (or vault
// A's reads will spill into vault B's, etc.), and the assertions below fire.
func TestFetchVaultMetadata_MultiVault(t *testing.T) {
	t.Parallel()

	vaultA := common.HexToAddress("0x1111111111111111111111111111111111111111")
	vaultB := common.HexToAddress("0x2222222222222222222222222222222222222222")
	assetA := common.HexToAddress("0xaaaa000000000000000000000000000000000000")
	assetB := common.HexToAddress("0xbbbb000000000000000000000000000000000000")

	prober, mc := newTestVaultProberWithMock(t)
	callsPerMetadata := prober.sharedProber.NumDetailsCalls() + numAssetExtensionCalls

	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		// Two vaults × callsPerMetadata each.
		if got, want := len(calls), 2*callsPerMetadata; got != want {
			t.Fatalf("expected %d calls, got %d", want, got)
		}
		return concatResults(
			// Vault A window: details (Vault A / vSYMA / 18 / no skim) then USDT/6.
			vaultDetailsResults(t, "Vault A", "vSYMA", 18, false),
			[]outbound.Result{okStringResult(t, "USDT"), okUint8Result(t, 6)},
			// Vault B window: details (Vault B / vSYMB / 18 / no skim) then USDC/8.
			vaultDetailsResults(t, "Vault B", "vSYMB", 18, false),
			[]outbound.Result{okStringResult(t, "USDC"), okUint8Result(t, 8)},
		), nil
	}

	confirmed := []confirmedProbe{
		{address: vaultA, asset: assetA, version: entity.MorphoVaultV1},
		{address: vaultB, asset: assetB, version: entity.MorphoVaultV1},
	}
	firstBlocks := map[common.Address]int64{
		vaultA: 12345,
		vaultB: 67890,
	}

	vaults, err := prober.fetchVaultMetadata(context.Background(), confirmed, firstBlocks, big.NewInt(100))
	if err != nil {
		t.Fatalf("fetchVaultMetadata: unexpected error: %v", err)
	}
	if len(vaults) != 2 {
		t.Fatalf("expected 2 confirmed vaults, got %d: %+v", len(vaults), vaults)
	}

	// Locked-in: order preserved and per-vault windows distinct.
	if got, want := vaults[0].Address, vaultA; got != want {
		t.Errorf("vaults[0].Address: want %s, got %s", want.Hex(), got.Hex())
	}
	if got, want := vaults[0].Name, "Vault A"; got != want {
		t.Errorf("vaults[0].Name: want %q, got %q", want, got)
	}
	if got, want := vaults[0].Asset, assetA; got != want {
		t.Errorf("vaults[0].Asset: want %s, got %s", want.Hex(), got.Hex())
	}
	if got, want := vaults[0].AssetSymbol, "USDT"; got != want {
		t.Errorf("vaults[0].AssetSymbol: want %q, got %q", want, got)
	}
	if got, want := vaults[0].AssetDecimals, uint8(6); got != want {
		t.Errorf("vaults[0].AssetDecimals: want %d, got %d", want, got)
	}
	if got, want := vaults[0].FirstBlock, int64(12345); got != want {
		t.Errorf("vaults[0].FirstBlock: want %d, got %d", want, got)
	}

	if got, want := vaults[1].Address, vaultB; got != want {
		t.Errorf("vaults[1].Address: want %s, got %s", want.Hex(), got.Hex())
	}
	if got, want := vaults[1].Name, "Vault B"; got != want {
		t.Errorf("vaults[1].Name: want %q, got %q", want, got)
	}
	if got, want := vaults[1].Asset, assetB; got != want {
		t.Errorf("vaults[1].Asset: want %s, got %s", want.Hex(), got.Hex())
	}
	if got, want := vaults[1].AssetSymbol, "USDC"; got != want {
		t.Errorf("vaults[1].AssetSymbol: want %q, got %q", want, got)
	}
	if got, want := vaults[1].AssetDecimals, uint8(8); got != want {
		t.Errorf("vaults[1].AssetDecimals: want %d, got %d", want, got)
	}
	if got, want := vaults[1].FirstBlock, int64(67890); got != want {
		t.Errorf("vaults[1].FirstBlock: want %d, got %d", want, got)
	}
}
