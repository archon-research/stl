package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

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

// TestProbeBatchWithRetry_SingleAddressTransportErrorFailsRun locks in the
// house invariant that a transient probe failure at the single-address floor
// fails the run rather than silently black-holing the candidate.
//
// By the time the batch has been split down to one address, the only errors
// reaching this branch are transport failures (429 / timeout / 5xx, already
// retried to exhaustion by the rpchttp client) or a structural-transport error
// out of the multicall. ErrNotVault is consumed as a per-result Success:false
// inside collectProbeConfirmed, so it never surfaces here as an error. Swallowing
// this into (nil, nil) would drop a real vault while the run exits 0.
func TestProbeBatchWithRetry_SingleAddressTransportErrorFailsRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		transportErr error
	}{
		{name: "transport error carrying no JSON-RPC shape", transportErr: errors.New("429 Too Many Requests (retries exhausted)")},
		{name: "JSON-RPC rate limit", transportErr: &rateLimitedRPCError{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			prober, mc := newTestVaultProberWithMock(t)
			mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				return nil, tc.transportErr
			}

			addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
			firstBlocks := map[common.Address]int64{addr: 100}

			vaults, err := prober.probeBatchWithRetry(context.Background(), []common.Address{addr}, firstBlocks, big.NewInt(100))
			if err == nil {
				t.Fatalf("expected single-address transport error to fail the run, got nil (vaults=%+v)", vaults)
			}
			if !errors.Is(err, tc.transportErr) {
				t.Errorf("expected wrapped transport error, got %v", err)
			}
			if vaults != nil {
				t.Errorf("expected no vaults on error, got %+v", vaults)
			}
		})
	}
}

type rateLimitedRPCError struct{}

func (rateLimitedRPCError) Error() string {
	return "Your app has exceeded its compute units per second capacity"
}
func (rateLimitedRPCError) ErrorCode() int { return 429 }

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

// TestFetchVaultMetadata exercises fetchVaultMetadata's per-vault dispositions:
//
//   - happy path: every sub-call succeeds, vault lands with the asset's
//     decimals (NOT the vault share's decimals) and its decoded symbol.
//   - decimals call reverts: vault is dropped to avoid persisting an
//     AssetDecimals=0 row that would block the live indexer's later
//     correction (token_repository UPSERT preserves existing decimals on
//     conflict).
//   - decimals returns malformed bytes: same skip-on-failure outcome.
//   - bytes32 asset symbol (MKR-style legacy ERC20): decoded via
//     erc20meta.DecodeStringOrBytes32, so the vault is confirmed with the
//     resolved symbol instead of being dropped by a string-only type assertion.
//   - empty asset symbol (reverted symbol() call): the vault is still confirmed
//     and persisted with an empty AssetSymbol, mirroring the live indexer's
//     getTokenMetadata — the per-block reconciliation sweep fills the symbol in
//     later. The asset symbol is display-only and never blocks persistence.
//
// The fixes are load-bearing: swapping the decimals skip back to "persist with
// AssetDecimals=0" makes the revert/malformed cases produce a vault; reverting
// the symbol decode to string-only-plus-drop-on-empty makes the bytes32 and
// empty-symbol cases drop the vault. Both regressions are caught here.
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
			name:                "bytes32 asset symbol (MKR-style) decodes → confirmed",
			assetSymbolResult:   bytes32SymbolResult(t, "MKR"),
			assetDecimalsResult: okUint8Result(t, 18),
			wantConfirmed:       true,
			wantAssetSymbol:     "MKR",
			wantAssetDecimals:   18,
		},
		{
			name:                "empty asset symbol (revert) → confirmed with empty symbol",
			assetSymbolResult:   outbound.Result{Success: false, ReturnData: nil},
			assetDecimalsResult: okUint8Result(t, 6),
			wantConfirmed:       true,
			wantAssetSymbol:     "",
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

// bytes32SymbolResult returns a successful result whose ReturnData is a raw
// bytes32 (left-aligned ASCII, null-padded) — the legacy MKR-style symbol()
// encoding that a string-only decode cannot parse.
func bytes32SymbolResult(t *testing.T, s string) outbound.Result {
	t.Helper()
	if len(s) > 32 {
		t.Fatalf("bytes32 symbol %q exceeds 32 bytes", s)
	}
	var b [32]byte
	copy(b[:], s)
	return outbound.Result{Success: true, ReturnData: b[:]}
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

// A real mainnet address: its dispatcher jumps into invalid bytecode.
const unprobeableCandidate = "0x4ECeF7bd1eD0c9f64a3a5c1a785A3Bb39DC5dF6A"

type gasExhaustedRPCError struct{}

func (gasExhaustedRPCError) Error() string  { return "out of gas: gas required exceeds: 550000000" }
func (gasExhaustedRPCError) ErrorCode() int { return -32000 }

// The boundary follows the node's eth_call gas cap, so it varies by provider.
const trapsExhaustingOneMulticall = 3

type trapping struct {
	selects    func(outbound.Call) bool
	exhaustsAt int
}

func vaultProbeResponder(t *testing.T, p *vaultProber, node trapping, asset common.Address) func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error) {
	t.Helper()
	probeAnswers := v1ProbeAnswers(t, p, asset)
	return func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		trapped := 0
		for _, call := range calls {
			if node.selects(call) {
				trapped++
			}
		}
		if trapped >= node.exhaustsAt {
			return nil, gasExhaustedRPCError{}
		}
		if _, probing := probeAnswers[string(calls[0].CallData)]; probing {
			out := make([]outbound.Result, 0, len(calls))
			for _, call := range calls {
				if node.selects(call) {
					out = append(out, outbound.Result{})
					continue
				}
				out = append(out, probeAnswers[string(call.CallData)])
			}
			return out, nil
		}
		return repeatResults(len(calls)/(p.sharedProber.NumDetailsCalls()+numAssetExtensionCalls), func() []outbound.Result {
			return concatResults(
				vaultDetailsResults(t, "Vault", "vSYM", 18, false),
				[]outbound.Result{okStringResult(t, "USDC"), okUint8Result(t, 6)},
			)
		}), nil
	}
}

func v1ProbeAnswers(t *testing.T, p *vaultProber, asset common.Address) map[string]outbound.Result {
	t.Helper()
	calls := p.sharedProber.ProbeCalls(common.Address{})
	results := v1ProbeResults(t, morpho_indexer.MorphoBlueAddress, asset)
	answers := make(map[string]outbound.Result, len(calls))
	for i, call := range calls {
		answers[string(call.CallData)] = results[i]
	}
	return answers
}

func trapsEverySelector(addr common.Address) trapping {
	return trapping{
		selects:    func(call outbound.Call) bool { return call.Target == addr },
		exhaustsAt: trapsExhaustingOneMulticall,
	}
}

func trapsNothing() trapping {
	return trapping{selects: func(outbound.Call) bool { return false }, exhaustsAt: trapsExhaustingOneMulticall}
}

func trapsUnderATightGasCap(addr common.Address, callData ...[]byte) trapping {
	return trapping{
		selects: func(call outbound.Call) bool {
			return call.Target == addr && slices.ContainsFunc(callData, func(data []byte) bool {
				return bytes.Equal(call.CallData, data)
			})
		},
		exhaustsAt: 1,
	}
}

func repeatResults(n int, window func() []outbound.Result) []outbound.Result {
	var out []outbound.Result
	for range n {
		out = append(out, window()...)
	}
	return out
}

func TestProbeAllCandidates_IsolatesGasExhaustedCandidate(t *testing.T) {
	t.Parallel()

	poison := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	mc.ExecuteFn = vaultProbeResponder(t, prober, trapsEverySelector(poison), common.HexToAddress("0xaaaa000000000000000000000000000000000000"))

	candidates := map[common.Address]int64{
		poison: 100,
		common.HexToAddress("0x1111111111111111111111111111111111111111"): 101,
		common.HexToAddress("0x2222222222222222222222222222222222222222"): 102,
		common.HexToAddress("0x3333333333333333333333333333333333333333"): 103,
	}

	vaults, err := prober.probeAllCandidates(context.Background(), candidates, 100, len(candidates))
	if err != nil {
		t.Fatalf("probeAllCandidates: unexpected error: %v", err)
	}
	if len(vaults) != 3 {
		t.Fatalf("expected the 3 probeable candidates to confirm, got %d: %+v", len(vaults), vaults)
	}
	for _, v := range vaults {
		if v.Address == poison {
			t.Errorf("the unprobeable candidate must not confirm as a vault: %+v", v)
		}
	}
}

func TestProbeAllCandidates_ProbesCandidatesInAddressOrder(t *testing.T) {
	t.Parallel()

	prober, mc := newTestVaultProberWithMock(t)
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return repeatResults(len(calls)/prober.sharedProber.NumProbeCalls(), notVaultProbeResults), nil
	}

	// Descending: map iteration rotates insertion order at this size, so ascending
	// would reproduce the sorted order with no sort at all.
	candidates := make(map[common.Address]int64)
	want := make([]common.Address, 0, 8)
	for i := int64(8); i >= 1; i-- {
		candidates[common.BigToAddress(big.NewInt(i))] = i
	}
	for i := int64(1); i <= 8; i++ {
		want = append(want, common.BigToAddress(big.NewInt(i)))
	}

	if _, err := prober.probeAllCandidates(context.Background(), candidates, 100, 1); err != nil {
		t.Fatalf("probeAllCandidates: unexpected error: %v", err)
	}

	got := make([]common.Address, 0, len(mc.Invocations))
	for _, inv := range mc.Invocations {
		got = append(got, inv.Calls[0].Target)
	}
	if !slices.Equal(got, want) {
		t.Errorf("probe order = %v, want ascending address order %v", got, want)
	}
}

func TestProbeAllCandidates_CountsDiscardedUnprobeableCandidate(t *testing.T) {
	t.Parallel()

	poison := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	mc.ExecuteFn = vaultProbeResponder(t, prober, trapsEverySelector(poison), common.HexToAddress("0xaaaa000000000000000000000000000000000000"))

	reader := sdkmetric.NewManualReader()
	prober.telemetry = newProbeTelemetry(t, reader)

	if _, err := prober.probeAllCandidates(context.Background(), map[common.Address]int64{poison: 100}, 100, 1); err != nil {
		t.Fatalf("probeAllCandidates: unexpected error: %v", err)
	}

	want := map[string]string{"reason": string(morpho_indexer.UnprobeableGasExhausted), "chain": "mainnet"}
	if got := counterValue(t, reader, "morpho.vault.candidates.unprobeable", want); got != 1 {
		t.Errorf("morpho.vault.candidates.unprobeable%v = %d, want 1", want, got)
	}
}

func TestProbeAllCandidates_LogsDiscardedUnprobeableCandidate(t *testing.T) {
	t.Parallel()

	poison := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	mc.ExecuteFn = vaultProbeResponder(t, prober, trapsEverySelector(poison), common.HexToAddress("0xaaaa000000000000000000000000000000000000"))

	var logged bytes.Buffer
	prober.logger = slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))

	if _, err := prober.probeAllCandidates(context.Background(), map[common.Address]int64{poison: 100}, 100, 1); err != nil {
		t.Fatalf("probeAllCandidates: unexpected error: %v", err)
	}

	for _, want := range []string{"level=WARN", "discarding unprobeable candidate", poison.Hex(), string(morpho_indexer.UnprobeableGasExhausted)} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("discard log: want substring %q, got %q", want, logged.String())
		}
	}
}

func TestProbeAllCandidates_SkipsCandidateClassifiedInAnEarlierSubRange(t *testing.T) {
	t.Parallel()

	poison := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	mc.ExecuteFn = vaultProbeResponder(t, prober, trapsEverySelector(poison), common.HexToAddress("0xaaaa000000000000000000000000000000000000"))
	candidates := map[common.Address]int64{
		poison: 100,
		common.HexToAddress("0x1111111111111111111111111111111111111111"): 101,
	}

	if _, err := prober.probeAllCandidates(context.Background(), candidates, 100, len(candidates)); err != nil {
		t.Fatalf("first sub-range: unexpected error: %v", err)
	}
	mc.Invocations = nil

	vaults, err := prober.probeAllCandidates(context.Background(), candidates, 100, len(candidates))
	if err != nil {
		t.Fatalf("second sub-range: unexpected error: %v", err)
	}
	if len(vaults) != 1 {
		t.Fatalf("expected the probeable candidate to confirm, got %d: %+v", len(vaults), vaults)
	}
	for _, inv := range mc.Invocations {
		for _, call := range inv.Calls {
			if call.Target == poison {
				t.Fatalf("the classified candidate was probed again: %v", inv.Calls)
			}
		}
	}
}

func TestProbeBatchWithRetry_MetadataGasExhaustionFailsRun(t *testing.T) {
	t.Parallel()

	addr := common.HexToAddress("0x7777777777777777777777777777777777777777")
	prober, mc := newTestVaultProberWithMock(t)
	probeSelector := prober.sharedProber.ProbeCalls(addr)[0].CallData
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if !bytes.Equal(calls[0].CallData, probeSelector) {
			return nil, gasExhaustedRPCError{}
		}
		return v1ProbeResults(t, morpho_indexer.MorphoBlueAddress, common.HexToAddress("0xaaaa000000000000000000000000000000000000")), nil
	}

	firstBlocks := map[common.Address]int64{addr: 100}
	vaults, err := prober.probeBatchWithRetry(context.Background(), []common.Address{addr}, firstBlocks, big.NewInt(100))
	if err == nil {
		t.Fatalf("expected a gas-exhausted metadata read to fail the run, got nil (vaults=%+v)", vaults)
	}
	if !strings.Contains(err.Error(), "multicall metadata") {
		t.Errorf("error: want it to name the metadata phase, got %q", err.Error())
	}
}

func newProbeTelemetry(t *testing.T, reader sdkmetric.Reader) *morpho_indexer.Telemetry {
	t.Helper()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tel, err := morpho_indexer.NewTelemetryWithProviders(tracenoop.NewTracerProvider(), provider, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}
	return tel
}

func TestProbeBatchWithRetry_ConfirmsVaultWhoseIsolatedSelectorsStillExhaust(t *testing.T) {
	t.Parallel()

	addr := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	probeCalls := prober.sharedProber.ProbeCalls(addr)
	tightCap := trapsUnderATightGasCap(addr, probeCalls[2].CallData, probeCalls[3].CallData)
	mc.ExecuteFn = vaultProbeResponder(t, prober, tightCap,
		common.HexToAddress("0xaaaa000000000000000000000000000000000000"))

	vaults, err := prober.probeBatchWithRetry(context.Background(), []common.Address{addr},
		map[common.Address]int64{addr: 100}, big.NewInt(100))
	if err != nil {
		t.Fatalf("probeBatchWithRetry: unexpected error: %v", err)
	}
	if len(vaults) != 1 {
		t.Fatalf("expected the vault to confirm off its answering selectors, got %d: %+v", len(vaults), vaults)
	}
	if vaults[0].Address != addr {
		t.Errorf("confirmed address: want %s, got %s", addr.Hex(), vaults[0].Address.Hex())
	}
	if _, classified := prober.unprobeable.lookup(addr, 100); classified {
		t.Errorf("a candidate that answers MORPHO() and asset() must not be classified unprobeable")
	}
}

func TestProbeBatchWithRetry_DiscardsCandidateAnsweringNoIsolatedSelector(t *testing.T) {
	t.Parallel()

	addr := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	mc.ExecuteFn = vaultProbeResponder(t, prober, trapsEverySelector(addr),
		common.HexToAddress("0xaaaa000000000000000000000000000000000000"))
	var logged bytes.Buffer
	prober.logger = slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))

	vaults, err := prober.probeBatchWithRetry(context.Background(), []common.Address{addr},
		map[common.Address]int64{addr: 100}, big.NewInt(100))
	if err != nil {
		t.Fatalf("probeBatchWithRetry: unexpected error: %v", err)
	}
	if len(vaults) != 0 {
		t.Fatalf("expected no vault, got %+v", vaults)
	}
	exhausted, classified := prober.unprobeable.lookup(addr, 100)
	if !classified {
		t.Fatalf("a candidate answering no isolated selector must be classified unprobeable")
	}
	if exhausted != 0 {
		t.Errorf("exhaustedSelectors = %d, want 0: at a mainnet-sized cap an isolated trap answers instead of exhausting", exhausted)
	}
	if !strings.Contains(logged.String(), "exhaustedSelectors=0") {
		t.Errorf("discard WARN must report the evidence it decided without; got %q", logged.String())
	}
}

func TestProbeAllCandidates_CountsMemoHitsLikeFreshDiscards(t *testing.T) {
	t.Parallel()

	poison := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	mc.ExecuteFn = vaultProbeResponder(t, prober, trapsEverySelector(poison),
		common.HexToAddress("0xaaaa000000000000000000000000000000000000"))

	reader := sdkmetric.NewManualReader()
	prober.telemetry = newProbeTelemetry(t, reader)
	var logged bytes.Buffer
	prober.logger = slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))

	candidates := map[common.Address]int64{poison: 100}
	for subRange := range 2 {
		if _, err := prober.probeAllCandidates(context.Background(), candidates, 100, 1); err != nil {
			t.Fatalf("sub-range %d: unexpected error: %v", subRange, err)
		}
	}

	want := map[string]string{"reason": string(morpho_indexer.UnprobeableGasExhausted), "chain": "mainnet"}
	if got := counterValue(t, reader, "morpho.vault.candidates.unprobeable", want); got != 2 {
		t.Errorf("morpho.vault.candidates.unprobeable%v = %d, want 2: the memo hit must count too", want, got)
	}
	if got := strings.Count(logged.String(), "discarding unprobeable candidate"); got != 2 {
		t.Errorf("discard WARNs = %d, want 2: the memo hit must log too; got %q", got, logged.String())
	}
	for source, want := range map[string]int{"source=probe": 1, "source=memo": 1} {
		if got := strings.Count(logged.String(), source); got != want {
			t.Errorf("%q WARNs = %d, want %d: the two discards must be distinguishable; got %q", source, got, want, logged.String())
		}
	}
}

func TestProbeAllCandidates_ReprobesAClassifiedCandidateAtADifferentBlock(t *testing.T) {
	t.Parallel()

	poison := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	mc.ExecuteFn = vaultProbeResponder(t, prober, trapsEverySelector(poison),
		common.HexToAddress("0xaaaa000000000000000000000000000000000000"))
	candidates := map[common.Address]int64{poison: 100}

	if _, err := prober.probeAllCandidates(context.Background(), candidates, 100, 1); err != nil {
		t.Fatalf("first run: unexpected error: %v", err)
	}
	mc.Invocations = nil

	if _, err := prober.probeAllCandidates(context.Background(), candidates, 200, 1); err != nil {
		t.Fatalf("second run: unexpected error: %v", err)
	}

	if len(mc.Invocations) == 0 {
		t.Fatalf("a run at another block must re-probe rather than reuse the earlier block's verdict")
	}
	if _, classified := prober.unprobeable.lookup(poison, 200); !classified {
		t.Errorf("the second block's verdict must be remembered under its own block")
	}
}

func TestProbeBatchWithRetry_TransientErrorBubblesWithoutNarrowing(t *testing.T) {
	t.Parallel()

	prober, mc := newTestVaultProberWithMock(t)
	transportErr := &rateLimitedRPCError{}
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, transportErr
	}

	batch := make([]common.Address, 0, 50)
	firstBlocks := make(map[common.Address]int64, 50)
	for i := int64(1); i <= 50; i++ {
		addr := common.BigToAddress(big.NewInt(i))
		batch = append(batch, addr)
		firstBlocks[addr] = i
	}

	vaults, err := prober.probeBatchWithRetry(context.Background(), batch, firstBlocks, big.NewInt(100))
	if err == nil {
		t.Fatalf("expected the transient error to fail the run, got nil (vaults=%+v)", vaults)
	}
	if !errors.Is(err, transportErr) {
		t.Errorf("expected the wrapped transport error, got %v", err)
	}
	if mc.CallCount != 1 {
		t.Errorf("multicalls issued = %d, want 1: a transient must bubble, not be narrowed", mc.CallCount)
	}
}

func TestProbeCandidateSelectorwise_TransientErrorDuringFanOutFailsRun(t *testing.T) {
	t.Parallel()

	addr := common.HexToAddress(unprobeableCandidate)
	prober, mc := newTestVaultProberWithMock(t)
	transportErr := &rateLimitedRPCError{}
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 1 {
			return nil, transportErr
		}
		return nil, gasExhaustedRPCError{}
	}

	vaults, err := prober.probeBatchWithRetry(context.Background(), []common.Address{addr},
		map[common.Address]int64{addr: 100}, big.NewInt(100))
	if err == nil {
		t.Fatalf("expected a throttled isolated probe call to fail the run, got nil (vaults=%+v)", vaults)
	}
	if !errors.Is(err, transportErr) {
		t.Errorf("expected the wrapped transport error, got %v", err)
	}
	if _, classified := prober.unprobeable.lookup(addr, 100); classified {
		t.Errorf("a transient failure must not classify the candidate unprobeable")
	}
}

func TestProbeBatchWithRetry_OversizedRequestNarrowsInsteadOfFailing(t *testing.T) {
	t.Parallel()

	const maxCallsPerRequest = 8

	prober, mc := newTestVaultProberWithMock(t)
	answer := vaultProbeResponder(t, prober, trapsNothing(), common.HexToAddress("0xaaaa000000000000000000000000000000000000"))
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNum *big.Int) ([]outbound.Result, error) {
		if len(calls) > maxCallsPerRequest {
			return nil, rpc.HTTPError{StatusCode: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		return answer(ctx, calls, blockNum)
	}

	batch := make([]common.Address, 0, 4)
	firstBlocks := make(map[common.Address]int64, 4)
	for i := int64(1); i <= 4; i++ {
		addr := common.BigToAddress(big.NewInt(i))
		batch = append(batch, addr)
		firstBlocks[addr] = i
	}

	vaults, err := prober.probeBatchWithRetry(context.Background(), batch, firstBlocks, big.NewInt(100))
	if err != nil {
		t.Fatalf("probeBatchWithRetry: unexpected error: %v", err)
	}
	if len(vaults) != len(batch) {
		t.Fatalf("expected every candidate to confirm once the batch fits, got %d: %+v", len(vaults), vaults)
	}
}
