package blockchain

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var testRateProvider = common.HexToAddress("0x2722C8f8A5F880401Fa5b01eD548d657F5Cd6175")

func baseConfig(t *testing.T) PSM3Config {
	t.Helper()
	cfg, err := PSM3ConfigForChain(8453)
	if err != nil {
		t.Fatalf("config for base: %v", err)
	}
	return cfg
}

func newTestCaller(t *testing.T, mc outbound.Multicaller) *PSM3Caller {
	t.Helper()
	caller, err := NewPSM3Caller(mc, baseConfig(t))
	if err != nil {
		t.Fatalf("create caller: %v", err)
	}
	return caller
}

func packAddressOutput(t *testing.T, c *PSM3Caller, method string, addr common.Address) []byte {
	t.Helper()
	data, err := c.psm3ABI.Methods[method].Outputs.Pack(addr)
	if err != nil {
		t.Fatalf("pack %s output: %v", method, err)
	}
	return data
}

func packUintOutput(t *testing.T, v *big.Int) []byte {
	t.Helper()
	return common.BigToHash(v).Bytes()
}

// immutableResults returns a happy-path ResolveImmutables result set
// [rateProvider, usds, susds, usdc] for the given config.
func immutableResults(t *testing.T, c *PSM3Caller, cfg PSM3Config) []outbound.Result {
	t.Helper()
	return []outbound.Result{
		{Success: true, ReturnData: packAddressOutput(t, c, "rateProvider", testRateProvider)},
		{Success: true, ReturnData: packAddressOutput(t, c, "usds", cfg.USDS)},
		{Success: true, ReturnData: packAddressOutput(t, c, "susds", cfg.SUSDS)},
		{Success: true, ReturnData: packAddressOutput(t, c, "usdc", cfg.USDC)},
	}
}

// resolvedCaller returns a caller whose immutables are already resolved.
func resolvedCaller(t *testing.T, mc *testutil.MockMulticaller) *PSM3Caller {
	t.Helper()
	caller := newTestCaller(t, mc)
	cfg := baseConfig(t)

	prevFn := mc.ExecuteFn
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return immutableResults(t, caller, cfg), nil
	}
	if err := caller.ResolveImmutables(context.Background(), big.NewInt(30_999_000)); err != nil {
		t.Fatalf("resolve immutables: %v", err)
	}
	mc.CallCount = 0
	mc.ExecuteFn = prevFn
	return caller
}

// ---------------------------------------------------------------------------
// Config tests
// ---------------------------------------------------------------------------

func TestPSM3ConfigForChain(t *testing.T) {
	tests := []struct {
		chainID  int64
		wantPSM3 string
		wantALM  string
	}{
		{8453, "0x1601843c5E9bC251A3272907010AFa41Fa18347E", "0x2917956eFF0B5eaF030abDB4EF4296DF775009cA"},
		{10, "0xe0F9978b907853F354d79188A3dEfbD41978af62", "0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB"},
		{42161, "0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266", "0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709"},
		{130, "0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f", "0x345E368fcCd62266B3f5F37C9a131FD1c39f5869"},
	}
	for _, tc := range tests {
		cfg, err := PSM3ConfigForChain(tc.chainID)
		if err != nil {
			t.Fatalf("chain %d: unexpected error: %v", tc.chainID, err)
		}
		if cfg.PSM3 != common.HexToAddress(tc.wantPSM3) {
			t.Errorf("chain %d: PSM3 = %s, want %s", tc.chainID, cfg.PSM3.Hex(), tc.wantPSM3)
		}
		if cfg.ALM != common.HexToAddress(tc.wantALM) {
			t.Errorf("chain %d: ALM = %s, want %s", tc.chainID, cfg.ALM.Hex(), tc.wantALM)
		}
	}
}

func TestPSM3ConfigForChain_Unknown(t *testing.T) {
	if _, err := PSM3ConfigForChain(1); err == nil {
		t.Fatal("expected error for chain without a PSM3 deployment")
	}
}

func synomeContract(
	entries map[string][]axis_synome_contract.TokenEntry,
	proxies map[string]map[string][]axis_synome_contract.ProxyConfig,
) *axis_synome_contract.Contract {
	var c axis_synome_contract.Contract
	c.AxisSynome.Spec.Entities.AssetsByPrime.ASSETSByPrime = entries
	c.AxisSynome.Spec.Entities.AlmProxies.AlmProxy = proxies
	return &c
}

func TestValidateAgainstAxisSynome(t *testing.T) {
	cfg := baseConfig(t)
	psm3Entry := func(chain, address string) axis_synome_contract.TokenEntry {
		return axis_synome_contract.TokenEntry{ContractAddress: address, Chain: chain, Protocol: "psm3"}
	}
	sparkOnBase := map[string][]axis_synome_contract.TokenEntry{
		"spark": {psm3Entry("base", "0x1601843c5e9bc251a3272907010afa41fa18347e")},
	}
	almProxies := func(chain, address, role string) map[string]map[string][]axis_synome_contract.ProxyConfig {
		return map[string]map[string][]axis_synome_contract.ProxyConfig{
			"spark": {chain: {{Star: "spark", Chain: chain, Address: address, Role: role}}},
		}
	}
	sparkALMOnBase := almProxies("base", "0x2917956eff0b5eaf030abdb4ef4296df775009ca", "alm")

	tests := []struct {
		name    string
		chainID int64
		entries map[string][]axis_synome_contract.TokenEntry
		proxies map[string]map[string][]axis_synome_contract.ProxyConfig
		wantErr string
	}{
		{
			"match", 8453,
			sparkOnBase,
			sparkALMOnBase,
			"",
		},
		{
			"address mismatch", 8453,
			map[string][]axis_synome_contract.TokenEntry{"spark": {psm3Entry("base", "0x2b05f8e1cacc6974fd79a673a341fe1f58d27266")}},
			sparkALMOnBase,
			"config has",
		},
		{
			"no entry for chain", 8453,
			map[string][]axis_synome_contract.TokenEntry{"spark": {psm3Entry("arbitrum", "0x2b05f8e1cacc6974fd79a673a341fe1f58d27266")}},
			sparkALMOnBase,
			"no psm3 entry",
		},
		{
			"unknown chain id", 999,
			map[string][]axis_synome_contract.TokenEntry{},
			nil,
			"unknown chain ID",
		},
		{
			"alm proxy mismatch", 8453,
			sparkOnBase,
			almProxies("base", "0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709", "alm"),
			"config has",
		},
		{
			"no alm proxy for chain", 8453,
			sparkOnBase,
			almProxies("arbitrum", "0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709", "alm"),
			"no alm proxy",
		},
		{
			"subproxy is not the alm proxy", 8453,
			sparkOnBase,
			almProxies("base", "0x2917956eff0b5eaf030abdb4ef4296df775009ca", "subproxy"),
			"no alm proxy",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := cfg.ValidateAgainstAxisSynome(synomeContract(tc.entries, tc.proxies), tc.chainID)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %v does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestValidateAgainstAxisSynome_RealContract guards against drift between the
// static config and the real axis-synome data shipped in this repo.
func TestValidateAgainstAxisSynome_RealContract(t *testing.T) {
	contract, err := axis_synome_contract.LoadDefaultContract()
	if err != nil {
		t.Fatalf("load axis-synome contract: %v", err)
	}
	for chainID := range psm3Configs {
		cfg, err := PSM3ConfigForChain(chainID)
		if err != nil {
			t.Fatalf("config for chain %d: %v", chainID, err)
		}
		if err := cfg.ValidateAgainstAxisSynome(contract, chainID); err != nil {
			t.Errorf("chain %d: %v", chainID, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

func TestNewPSM3Caller_NilMulticaller(t *testing.T) {
	if _, err := NewPSM3Caller(nil, baseConfig(t)); err == nil {
		t.Fatal("expected error for nil multicaller")
	}
}

// ---------------------------------------------------------------------------
// ResolveImmutables
// ---------------------------------------------------------------------------

func TestResolveImmutables_HappyPath(t *testing.T) {
	cfg := baseConfig(t)
	block := big.NewInt(30_999_000)
	mc := testutil.NewMockMulticaller()
	caller := newTestCaller(t, mc)

	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if blockNumber == nil || blockNumber.Cmp(block) != 0 {
			t.Errorf("blockNumber = %v, want %v", blockNumber, block)
		}
		if len(calls) != 4 {
			t.Fatalf("expected 4 calls, got %d", len(calls))
		}
		for i, c := range calls {
			if c.Target != cfg.PSM3 {
				t.Errorf("call %d target = %s, want PSM3 %s", i, c.Target.Hex(), cfg.PSM3.Hex())
			}
			if c.AllowFailure {
				t.Errorf("call %d has AllowFailure=true, want false", i)
			}
		}
		return immutableResults(t, caller, cfg), nil
	}

	if err := caller.ResolveImmutables(context.Background(), big.NewInt(30_999_000)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// That the resolved rate provider is the one used downstream is covered by
	// the ReadState call-targets test.
}

func TestResolveImmutables_TokenMismatch(t *testing.T) {
	cfg := baseConfig(t)
	mc := testutil.NewMockMulticaller()
	caller := newTestCaller(t, mc)

	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		results := immutableResults(t, caller, cfg)
		// On-chain usds() disagrees with config.
		results[1].ReturnData = packAddressOutput(t, caller, "usds", common.HexToAddress("0xdead"))
		return results, nil
	}

	err := caller.ResolveImmutables(context.Background(), big.NewInt(30_999_000))
	if err == nil || !strings.Contains(err.Error(), "usds()") {
		t.Fatalf("expected usds mismatch error, got: %v", err)
	}
}

func TestResolveImmutables_ZeroRateProvider(t *testing.T) {
	cfg := baseConfig(t)
	mc := testutil.NewMockMulticaller()
	caller := newTestCaller(t, mc)

	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		results := immutableResults(t, caller, cfg)
		results[0].ReturnData = packAddressOutput(t, caller, "rateProvider", common.Address{})
		return results, nil
	}

	err := caller.ResolveImmutables(context.Background(), big.NewInt(30_999_000))
	if err == nil || !strings.Contains(err.Error(), "zero address") {
		t.Fatalf("expected zero rate provider error, got: %v", err)
	}
}

func TestResolveImmutables_MulticallError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	caller := newTestCaller(t, mc)

	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("RPC unavailable")
	}

	err := caller.ResolveImmutables(context.Background(), big.NewInt(30_999_000))
	if err == nil || !strings.Contains(err.Error(), "RPC unavailable") {
		t.Fatalf("expected wrapped multicall error, got: %v", err)
	}
}

func TestResolveImmutables_TruncatedResults(t *testing.T) {
	cfg := baseConfig(t)
	mc := testutil.NewMockMulticaller()
	caller := newTestCaller(t, mc)

	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return immutableResults(t, caller, cfg)[:2], nil
	}

	err := caller.ResolveImmutables(context.Background(), big.NewInt(30_999_000))
	if err == nil || !strings.Contains(err.Error(), "expected 4 multicall results") {
		t.Fatalf("expected truncated results error, got: %v", err)
	}
}

func TestResolveImmutables_DecodeFailure(t *testing.T) {
	cfg := baseConfig(t)
	mc := testutil.NewMockMulticaller()
	caller := newTestCaller(t, mc)

	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		results := immutableResults(t, caller, cfg)
		results[0].ReturnData = []byte{0x01, 0x02} // not a valid ABI address word
		return results, nil
	}

	err := caller.ResolveImmutables(context.Background(), big.NewInt(30_999_000))
	if err == nil || !strings.Contains(err.Error(), "unpack rateProvider") {
		t.Fatalf("expected unpack error, got: %v", err)
	}
}

func TestResolveImmutables_FailedCall(t *testing.T) {
	cfg := baseConfig(t)
	mc := testutil.NewMockMulticaller()
	caller := newTestCaller(t, mc)

	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		results := immutableResults(t, caller, cfg)
		results[2].Success = false
		return results, nil
	}

	err := caller.ResolveImmutables(context.Background(), big.NewInt(30_999_000))
	if err == nil || !strings.Contains(err.Error(), "susds call failed") {
		t.Fatalf("expected failed call error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ReadState
// ---------------------------------------------------------------------------

func TestReadState_BeforeResolve(t *testing.T) {
	caller := newTestCaller(t, testutil.NewMockMulticaller())
	_, err := caller.ReadState(context.Background(), common.HexToHash("0xdead"))
	if err == nil || !strings.Contains(err.Error(), "ResolveImmutables") {
		t.Fatalf("expected not-resolved error, got: %v", err)
	}
}

func TestReadState_HappyPath_PocketRedirect(t *testing.T) {
	cfg := baseConfig(t)
	pocket := common.HexToAddress("0x9999999999999999999999999999999999999999") // distinct from PSM3
	blockHash := common.HexToHash("0xabc123abc123abc123abc123abc123abc123abc123abc123abc123abc123ab")

	usdsBal := big.NewInt(111)
	susdsBal := big.NewInt(222)
	usdcBal := big.NewInt(333)
	totalAssets := big.NewInt(666)
	rate := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	almShares := big.NewInt(444)
	totalShares := big.NewInt(555)
	almAssetValue := big.NewInt(532)

	mc := testutil.NewMockMulticaller()
	caller := resolvedCaller(t, mc)

	// State reads must pin to the block hash, not the number: after a reorg an
	// archive node answers eth_call-by-number with the new fork's reserves,
	// which can silently disagree with the reorged block being processed (VEC-471).
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("ReadState must call ExecuteAtHash, not Execute")
		return nil, nil
	}
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, gotHash common.Hash) ([]outbound.Result, error) {
		if gotHash != blockHash {
			t.Errorf("blockHash = %s, want %s (both rounds pinned to the same block hash)", gotHash, blockHash)
		}
		switch mc.CallCount {
		case 1: // round 1
			if len(calls) != 7 {
				t.Fatalf("round 1: expected 7 calls, got %d", len(calls))
			}
			wantTargets := []common.Address{cfg.PSM3, cfg.USDS, cfg.SUSDS, cfg.PSM3, testRateProvider, cfg.PSM3, cfg.PSM3}
			for i, want := range wantTargets {
				if calls[i].Target != want {
					t.Errorf("round 1 call %d target = %s, want %s", i, calls[i].Target.Hex(), want.Hex())
				}
			}
			sharesArgs, err := caller.psm3ABI.Methods["shares"].Inputs.Unpack(calls[5].CallData[4:])
			if err != nil {
				t.Fatalf("unpack shares calldata: %v", err)
			}
			if got := sharesArgs[0].(common.Address); got != cfg.ALM {
				t.Errorf("shares arg = %s, want ALM proxy %s", got.Hex(), cfg.ALM.Hex())
			}
			return []outbound.Result{
				{Success: true, ReturnData: packAddressOutput(t, caller, "pocket", pocket)},
				{Success: true, ReturnData: packUintOutput(t, usdsBal)},
				{Success: true, ReturnData: packUintOutput(t, susdsBal)},
				{Success: true, ReturnData: packUintOutput(t, totalAssets)},
				{Success: true, ReturnData: packUintOutput(t, rate)},
				{Success: true, ReturnData: packUintOutput(t, almShares)},
				{Success: true, ReturnData: packUintOutput(t, totalShares)},
			}, nil
		case 2: // round 2: USDC.balanceOf(pocket) + convertToAssetValue(almShares)
			if len(calls) != 2 {
				t.Fatalf("round 2: expected 2 calls, got %d", len(calls))
			}
			if calls[0].Target != cfg.USDC {
				t.Errorf("round 2 target = %s, want USDC %s", calls[0].Target.Hex(), cfg.USDC.Hex())
			}
			args, err := caller.erc20ABI.Methods["balanceOf"].Inputs.Unpack(calls[0].CallData[4:])
			if err != nil {
				t.Fatalf("unpack round 2 calldata: %v", err)
			}
			if got := args[0].(common.Address); got != pocket {
				t.Errorf("round 2 balanceOf arg = %s, want pocket %s (must follow the redirect)", got.Hex(), pocket.Hex())
			}
			if calls[1].Target != cfg.PSM3 {
				t.Errorf("round 2 call 1 target = %s, want PSM3 %s", calls[1].Target.Hex(), cfg.PSM3.Hex())
			}
			convArgs, err := caller.psm3ABI.Methods["convertToAssetValue"].Inputs.Unpack(calls[1].CallData[4:])
			if err != nil {
				t.Fatalf("unpack convertToAssetValue calldata: %v", err)
			}
			if got := convArgs[0].(*big.Int); got.Cmp(almShares) != 0 {
				t.Errorf("convertToAssetValue arg = %s, want the ALM's share count %s", got, almShares)
			}
			return []outbound.Result{
				{Success: true, ReturnData: packUintOutput(t, usdcBal)},
				{Success: true, ReturnData: packUintOutput(t, almAssetValue)},
			}, nil
		default:
			t.Fatalf("unexpected multicall round %d", mc.CallCount)
			return nil, nil
		}
	}

	state, err := caller.ReadState(context.Background(), blockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.USDSBalance.Cmp(usdsBal) != 0 ||
		state.SUSDSBalance.Cmp(susdsBal) != 0 ||
		state.USDCBalance.Cmp(usdcBal) != 0 ||
		state.TotalAssets.Cmp(totalAssets) != 0 ||
		state.ConversionRate.Cmp(rate) != 0 ||
		state.ALMShares.Cmp(almShares) != 0 ||
		state.TotalShares.Cmp(totalShares) != 0 ||
		state.ALMAssetValue.Cmp(almAssetValue) != 0 {
		t.Errorf("unexpected state: %+v", state)
	}
}

// round1Results returns a happy-path ReadState round-1 result set
// [pocket, usds, susds, totalAssets, rate, shares(alm), totalShares].
func round1Results(t *testing.T, c *PSM3Caller, pocket common.Address) []outbound.Result {
	t.Helper()
	results := []outbound.Result{{Success: true, ReturnData: packAddressOutput(t, c, "pocket", pocket)}}
	for i := int64(1); i <= 6; i++ {
		results = append(results, outbound.Result{Success: true, ReturnData: packUintOutput(t, big.NewInt(i))})
	}
	return results
}

func TestReadState_Round1Failures(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(results []outbound.Result)
		wantErr string
	}{
		{"failed call", func(r []outbound.Result) { r[1].Success = false }, "balanceOf call failed"},
		{"garbage return data", func(r []outbound.Result) { r[3].ReturnData = []byte{0xff} }, "unpack totalAssets"},
		{"failed shares call", func(r []outbound.Result) { r[5].Success = false }, "shares call failed"},
		{"garbage total shares", func(r []outbound.Result) { r[6].ReturnData = []byte{0xff} }, "unpack totalShares"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			caller := resolvedCaller(t, mc)

			mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				results := round1Results(t, caller, baseConfig(t).PSM3)
				tc.mutate(results)
				return results, nil
			}

			_, err := caller.ReadState(context.Background(), common.HexToHash("0xdead"))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %v does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestReadState_Round2Failures(t *testing.T) {
	tests := []struct {
		name    string
		round2  func() ([]outbound.Result, error)
		wantErr string
	}{
		{
			"multicall error",
			func() ([]outbound.Result, error) { return nil, errors.New("RPC unavailable") },
			"usdc balance at pocket",
		},
		{
			"failed convertToAssetValue call",
			func() ([]outbound.Result, error) {
				return []outbound.Result{
					{Success: true, ReturnData: common.BigToHash(big.NewInt(7)).Bytes()},
					{Success: false},
				}, nil
			},
			"convertToAssetValue call failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			caller := resolvedCaller(t, mc)

			mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if mc.CallCount == 1 {
					return round1Results(t, caller, baseConfig(t).PSM3), nil
				}
				return tc.round2()
			}

			_, err := caller.ReadState(context.Background(), common.HexToHash("0xdead"))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %v does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestReadState_ZeroPocket(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	caller := resolvedCaller(t, mc)

	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return round1Results(t, caller, common.Address{}), nil
	}

	// A zero pocket must hard-fail before reading USDC.balanceOf(0x0).
	_, err := caller.ReadState(context.Background(), common.HexToHash("0xdead"))
	if err == nil || !strings.Contains(err.Error(), "pocket() returned the zero address") {
		t.Fatalf("expected zero-pocket error, got: %v", err)
	}
}
