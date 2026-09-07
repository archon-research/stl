package morpho_indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// --- Vault discovery ---

func TestProcessBlockEvent_VaultDiscovery_Success(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// vault state + balance (after discovery, process the event)
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var vaultCreated bool
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		vaultCreated = true
		return 99, nil
	}

	// Emit a Deposit event from the unknown vault address.
	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !vaultCreated {
		t.Error("vault was not created in DB")
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("vault should be registered in vault registry")
	}
}

func TestProcessBlockEvent_VaultDiscovery_V1_1(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho V1.1 Vault", "mV1.1", 18, true), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedVault == nil {
		t.Fatal("vault not created")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV1_1 {
		t.Errorf("VaultVersion = %d, want V1.1 (%d)", savedVault.VaultVersion, entity.MorphoVaultV1_1)
	}
}

func TestProcessBlockEvent_VaultDiscovery_WrongMorphoAddress(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	wrongMorpho := common.HexToAddress("0x0000000000000000000000000000000000000001")
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			// Probe returns wrong MORPHO address (and curator/liquidityAdapter revert).
			return h.vaultProbeResults(wrongMorpho, testLoanToken), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	// Should not error (vault discovery failures are non-fatal, just marks as not-vault).
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault")
	}
}

func TestProcessBlockEvent_VaultDiscovery_AllProbeSelectorsRevert(t *testing.T) {
	// Previously named *_MorphoCallReverts. After VEC-198 a MORPHO() revert is
	// no longer sufficient on its own — the address still needs curator() and
	// liquidityAdapter() to fail to be classified as not-a-vault. (If those
	// succeed, it's a Morpho VaultV2.)
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			return h.notAVaultProbeResults(), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault when all probe selectors revert")
	}
}

func TestProcessBlockEvent_VaultDiscovery_AssetZero(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if h.isProbeMulticall(calls) {
			// Probe: MORPHO succeeds but asset returns zero address.
			return h.vaultProbeResults(MorphoBlueAddress, common.Address{}), nil
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault when asset is zero")
	}
}

// TestProcessBlockEvent_VaultDiscovery_VaultV2 covers the new probe fallback:
// MORPHO() reverts but curator() and liquidityAdapter() succeed, so the
// address is recognised as a Morpho VaultV2 (e.g. sparkUSDTbc) rather than
// silently rejected as a non-vault. See VEC-198.
func TestProcessBlockEvent_VaultDiscovery_VaultV2(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22") // sparkUSDTbc address from VEC-198
	curator := common.HexToAddress("0x0f96000000000000000000000000000000000046A3")
	liquidityAdapter := common.HexToAddress("0x7481000000000000000000000000000000007dC2")

	// Number-pinned reads (identity): the vault probe/details batches and the asset
	// token metadata.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch {
		case h.isProbeMulticall(calls):
			return h.vaultV2ProbeResults(testLoanToken, curator, liquidityAdapter), nil
		case h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 18, false), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDT", 6), nil
		default:
			return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
		}
	}
	// Hash-pinned reads (versioned state): the adapter-set enumeration, the fee
	// config seed, and the vault-state read of the triggering AccrueInterest.
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		switch {
		case len(calls) == 1 && calls[0].Target == unknownVault && hasSameSelector(calls[0].CallData, adaptersLengthSelector):
			// This vault has no adapters, so discovery-time enumeration is a no-op
			// and no adapter probe/seed calls follow.
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(0))}}, nil
		case len(calls) == 4 && calls[0].Target == unknownVault:
			return h.feeGetterResults(big.NewInt(0), big.NewInt(0), common.Address{}, common.Address{}), nil
		case len(calls) == 2 && calls[0].Target == unknownVault:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedVault == nil {
		t.Fatal("vault not created")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV2 {
		t.Errorf("VaultVersion = %d, want VaultV2 (%d)", savedVault.VaultVersion, entity.MorphoVaultV2)
	}
	if savedVault.Symbol != "sparkUSDTbc" {
		t.Errorf("Symbol = %q, want sparkUSDTbc", savedVault.Symbol)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("VaultV2 should be registered in vault registry")
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2_UnclassifiedAdapterWithoutRealAssets is the
// discovery-path mirror of TestProcessBlockEvent_AddAdapter_RealAssetsSeedTolerance:
// enumerating a vault's existing adapter set must not be poison-pilled by one
// unmodelled adapter that does not serve realAssets(). It is registered as Unknown
// with no state seed, so VEC-219's composition probe reports it as
// adapter_data_missing — honest, since an adapter we cannot classify is one we cannot
// price either.
func TestProcessBlockEvent_VaultDiscovery_V2_UnclassifiedAdapterWithoutRealAssets(t *testing.T) {
	h := newTestHarness(t)
	logs := h.captureLogs()
	unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22")
	curator := common.HexToAddress("0x00000000000000000000000000000000000000A3")
	adapter := common.HexToAddress("0xAaAa000000000000000000000000000000000001")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch {
		case len(calls) == 4 && h.isProbeMulticall(calls):
			return h.vaultV2ProbeResults(testLoanToken, curator, adapter), nil
		case len(calls) == 4 && h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 6, false), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDT", 6), nil
		case len(calls) == adapterProbeCallsPerAdapter && calls[0].Target == adapter:
			return h.adapterProbeResults(entity.MorphoAdapterTypeUnknown), nil
		default:
			return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
		}
	}
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		switch {
		case len(calls) == 1 && hasSameSelector(calls[0].CallData, adaptersLengthSelector):
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
		case len(calls) == 1 && hasSameSelector(calls[0].CallData, adaptersSelector):
			return []outbound.Result{{Success: true, ReturnData: h.packAddress(adapter)}}, nil
		case len(calls) == 1 && calls[0].Target == adapter:
			return []outbound.Result{{Success: false, ReturnData: nil}}, nil // realAssets() reverts
		case len(calls) == 4 && calls[0].Target == unknownVault:
			return h.feeGetterResults(big.NewInt(0), big.NewInt(0), common.Address{}, common.Address{}), nil
		case len(calls) == 2 && calls[0].Target == unknownVault:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
		}
	}

	var registered []*entity.MorphoAdapterObservation
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		registered = append(registered, obs)
		return int64(len(registered)), true, nil
	}
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) (bool, error) {
		t.Fatal("no state row may be seeded for an adapter that served no realAssets() reading")
		return true, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	if err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("discovery must not be poisoned by an unmodelled adapter: %v", err)
	}

	if len(registered) != 1 {
		t.Fatalf("registered %d adapters, want 1", len(registered))
	}
	if got := registered[0].Membership.AdapterType; got == nil || *got != entity.MorphoAdapterTypeUnknown {
		t.Errorf("AdapterType = %v, want Unknown", got)
	}
	if !logs.hasWarnContaining("does not serve realAssets()") {
		t.Error("expected a WARN naming the adapter that served no realAssets() reading")
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2_FeeSurface covers a vault-shaped contract
// the V2 probe accepts — curator() and liquidityAdapter() answer while MORPHO()
// reverts — that does NOT serve the four VaultV2 fee getters. The probe never
// verified those exist, so hard-requiring them at seeding time poisoned discovery
// forever: the triggering AccrueInterest was retried and the vault never registered.
//
// All four reverting means the address has no VaultV2 fee surface: skip the seed
// behind a WARN, leaving the vault honestly fee-row-less (VEC-219's consumers then
// see no fee row rather than a fabricated one). A partial answer is genuine drift on
// a contract that does have the surface, and must still stop the event. A real
// factory-deployed VaultV2 always answers all four, so its seeding is unchanged.
func TestProcessBlockEvent_VaultDiscovery_V2_FeeSurface(t *testing.T) {
	tests := []struct {
		name       string
		feeSuccess [4]bool
		wantErr    string
		wantSeeded bool
		wantWarn   bool
	}{
		{
			name:       "all four getters answer, so the fee config is seeded",
			feeSuccess: [4]bool{true, true, true, true},
			wantSeeded: true,
		},
		{
			name:       "all four revert, so there is no fee surface to seed",
			feeSuccess: [4]bool{false, false, false, false},
			wantWarn:   true,
		},
		{
			name:       "a partially served fee surface is drift and stops the event",
			feeSuccess: [4]bool{true, true, false, true},
			wantErr:    "3 of 4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			logs := h.captureLogs()
			unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22")
			curator := common.HexToAddress("0x00000000000000000000000000000000000000A3")
			liquidityAdapter := common.HexToAddress("0x00000000000000000000000000000000000000A4")

			h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				switch {
				case h.isProbeMulticall(calls):
					return h.vaultV2ProbeResults(testLoanToken, curator, liquidityAdapter), nil
				case h.isVaultDetailsMulticall(calls):
					return h.vaultDetailResults("Partial V2 Vault", "pV2", 18, false), nil
				case len(calls) == 2 && calls[0].Target == testLoanToken:
					return h.tokenMetadataResults("USDT", 6), nil
				default:
					return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
				}
			}
			h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				switch {
				case len(calls) == 1 && hasSameSelector(calls[0].CallData, adaptersLengthSelector):
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(0))}}, nil
				case len(calls) == 4 && calls[0].Target == unknownVault:
					return h.partialFeeGetterResults(tt.feeSuccess), nil
				case len(calls) == 2 && calls[0].Target == unknownVault:
					return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
				default:
					return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
				}
			}

			var savedVault *entity.MorphoVault
			h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
				savedVault = v
				return 99, nil
			}
			seeded := false
			h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultFee) (bool, error) {
				seeded = true
				return true, nil
			}

			log := h.makeDiscoveryTriggerLog(unknownVault)
			err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)})
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("a partially served fee surface must stop the event")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q should say how many getters answered (%q)", err.Error(), tt.wantErr)
				}
				if h.svc.vaultRegistry.IsKnownVault(unknownVault) {
					t.Error("a failed discovery must not register the vault")
				}
				return
			}
			if err != nil {
				t.Fatalf("discovery must not be poisoned by a missing fee surface: %v", err)
			}

			if savedVault == nil {
				t.Fatal("vault not created")
			}
			if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
				t.Error("the discovered VaultV2 should be registered")
			}
			if seeded != tt.wantSeeded {
				t.Errorf("fee config seeded = %v, want %v", seeded, tt.wantSeeded)
			}
			if got := logs.hasWarnContaining("no VaultV2 fee surface"); got != tt.wantWarn {
				t.Errorf("WARN(no VaultV2 fee surface) = %v, want %v", got, tt.wantWarn)
			}
		})
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2_EnumeratesAndSeedsAdapters verifies
// that discovering a VaultV2 mid-life enumerates its existing on-chain adapters
// (adaptersLength()/adapters(i)), classifies each, registers them at the
// discovery block, AND seeds one adapter_state row per adapter from a hash-pinned
// realAssets() read. Without this, a discovered-but-quiet vault would carry
// adapter rows with no state rows until its first allocation, which the VEC-219
// composition-completeness probe would flag as adapter_data_missing.
// v2DiscoveryFixture names the addresses and hash-pinned seed values
// setupV2DiscoveryWithTwoAdapters wired the mocks to serve.
type v2DiscoveryFixture struct {
	vault       common.Address
	adapterA    common.Address
	adapterB    common.Address
	realAssetsA *big.Int
	realAssetsB *big.Int
}

// setupV2DiscoveryWithTwoAdapters wires every chain read a mid-life VaultV2
// discovery makes for a vault holding two adapters (one MarketV1, one VaultV1),
// plus the vault-id write. Repository capture stubs are the caller's, so each test
// decides what the transaction does with what discovery hands it.
func (h *serviceTestHarness) setupV2DiscoveryWithTwoAdapters() v2DiscoveryFixture {
	fx := v2DiscoveryFixture{
		vault:       common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22"),
		adapterA:    common.HexToAddress("0xAaAa000000000000000000000000000000000001"),
		adapterB:    common.HexToAddress("0xbBbB000000000000000000000000000000000002"),
		realAssetsA: big.NewInt(41_300_000),
		realAssetsB: big.NewInt(7_654_321),
	}
	curator := common.HexToAddress("0x00000000000000000000000000000000000000A3")

	// Number-pinned reads (identity): vault probe/details, token metadata, and the
	// per-adapter type probe.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch {
		case len(calls) == 4 && h.isProbeMulticall(calls):
			return h.vaultV2ProbeResults(testLoanToken, curator, fx.adapterA), nil
		case len(calls) == 4 && h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 6, false), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDT", 6), nil
		case len(calls) == adapterProbeCallsPerAdapter && calls[0].Target == fx.adapterA:
			return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
		case len(calls) == adapterProbeCallsPerAdapter && calls[0].Target == fx.adapterB:
			return h.adapterProbeResults(entity.MorphoAdapterTypeVaultV1), nil
		default:
			return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
		}
	}
	// Hash-pinned reads (versioned state): the adaptersLength()/adapters(i)
	// enumeration, the per-adapter realAssets() seeds, and the vault-state read of
	// the triggering AccrueInterest. The adapters(i) batch and the vault-state batch
	// are both 2 calls to the vault, so discriminate by selector.
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		switch {
		case len(calls) == 1 && calls[0].Target == fx.vault && hasSameSelector(calls[0].CallData, adaptersLengthSelector):
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(2))}}, nil
		case len(calls) == 2 && calls[0].Target == fx.vault && hasSameSelector(calls[0].CallData, adaptersSelector):
			return []outbound.Result{
				{Success: true, ReturnData: h.packAddress(fx.adapterA)},
				{Success: true, ReturnData: h.packAddress(fx.adapterB)},
			}, nil
		case len(calls) == 1 && calls[0].Target == fx.adapterA:
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(fx.realAssetsA)}}, nil
		case len(calls) == 1 && calls[0].Target == fx.adapterB:
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(fx.realAssetsB)}}, nil
		case len(calls) == 4 && calls[0].Target == fx.vault:
			return h.feeGetterResults(big.NewInt(0), big.NewInt(0), common.Address{}, common.Address{}), nil
		case len(calls) == 2 && calls[0].Target == fx.vault:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
		}
	}
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) { return 99, nil }
	return fx
}

// TestProcessBlockEvent_VaultDiscovery_V2_AdapterObservationsNotRecordedWhenTheCommitFails
// pins the same honesty the event handlers owe to discovery, which appends a whole
// adapter set in one transaction: a commit that fails must leave the registration
// counter untouched for every adapter it enumerated, not just the last one.
func TestProcessBlockEvent_VaultDiscovery_V2_AdapterObservationsNotRecordedWhenTheCommitFails(t *testing.T) {
	h := newTestHarness(t)
	reader := h.recordMetrics(t)
	fx := h.setupV2DiscoveryWithTwoAdapters()
	h.failCommitAfterMembershipAppend()

	log := h.makeDiscoveryTriggerLog(fx.vault)
	if err := h.processBlock(t, 1, 24481834, 2, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
		t.Fatal("expected the block to fail so SQS redelivers")
	}

	want := map[string]string{"observed_via": string(entity.MembershipFromDiscovery)}
	if got := counterValue(t, reader, "morpho.v2.adapter.registrations", want); got != 0 {
		t.Errorf("morpho.v2.adapter.registrations%v = %d, want 0 (the discovery transaction rolled back)", want, got)
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2_CountsObservationsByTypeAndProvenance
// pins the labels the enumeration's observations carry. observed_via
// "vault_discovery" is what lets VectorMorphoV2LazyAdapterRegistrations treat the
// allocation_event path as a defect signal rather than normal new-vault traffic.
func TestProcessBlockEvent_VaultDiscovery_V2_CountsObservationsByTypeAndProvenance(t *testing.T) {
	h := newTestHarness(t)
	reader := h.recordMetrics(t)
	fx := h.setupV2DiscoveryWithTwoAdapters()

	adapterIDByAddr := map[common.Address]int64{fx.adapterA: 101, fx.adapterB: 102}
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		return adapterIDByAddr[common.BytesToAddress(obs.Identity.Address)], true, nil
	}

	log := h.makeDiscoveryTriggerLog(fx.vault)
	if err := h.processBlock(t, 1, 24481834, 2, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	for _, wantType := range []string{"market_v1", "vault_v1"} {
		want := map[string]string{"adapter.type": wantType, "observed_via": string(entity.MembershipFromDiscovery)}
		if got := counterValue(t, reader, "morpho.v2.adapter.registrations", want); got != 1 {
			t.Errorf("morpho.v2.adapter.registrations%v = %d, want 1", want, got)
		}
	}
}

func TestProcessBlockEvent_VaultDiscovery_V2_EnumeratesAndSeedsAdapters(t *testing.T) {
	h := newTestHarness(t)
	fx := h.setupV2DiscoveryWithTwoAdapters()
	unknownVault, adapterA, adapterB := fx.vault, fx.adapterA, fx.adapterB
	realAssetsA, realAssetsB := fx.realAssetsA, fx.realAssetsB

	adapterIDByAddr := map[common.Address]int64{adapterA: 101, adapterB: 102}
	var registered []*entity.MorphoAdapterObservation
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		registered = append(registered, obs)
		return adapterIDByAddr[common.BytesToAddress(obs.Identity.Address)], true, nil
	}
	var seeded []*entity.MorphoAdapterState
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoAdapterState) (bool, error) {
		seeded = append(seeded, s)
		return true, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	if err := h.processBlock(t, 1, 24481834, 2, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("V2 vault should be registered after enumeration")
	}
	if len(registered) != 2 {
		t.Fatalf("want 2 adapters registered, got %d", len(registered))
	}
	byAddr := map[common.Address]*entity.MorphoAdapterObservation{}
	for _, obs := range registered {
		byAddr[common.BytesToAddress(obs.Identity.Address)] = obs
		if obs.Identity.MorphoVaultID != 99 {
			t.Errorf("adapter %x MorphoVaultID = %d, want 99", obs.Identity.Address, obs.Identity.MorphoVaultID)
		}
		if obs.Membership.BlockNumber != 24481834 {
			t.Errorf("adapter %x observed at block %d, want 24481834 (discovery block)", obs.Identity.Address, obs.Membership.BlockNumber)
		}
		if obs.Identity.AssetTokenID != 1 {
			t.Errorf("adapter %x AssetTokenID = %d, want 1 (vault asset)", obs.Identity.Address, obs.Identity.AssetTokenID)
		}
		// An enumeration reads END-OF-BLOCK state, so it must order above every log in
		// its block and record itself as an assertion, not as an add it never witnessed.
		if obs.Membership.LogIndex != entity.EndOfBlockLogIndex {
			t.Errorf("adapter %x LogIndex = %d, want EndOfBlockLogIndex", obs.Identity.Address, obs.Membership.LogIndex)
		}
		if obs.Membership.ObservedVia != entity.MembershipFromDiscovery {
			t.Errorf("adapter %x ObservedVia = %q, want vault_discovery", obs.Identity.Address, obs.Membership.ObservedVia)
		}
		if !obs.Membership.IsMember {
			t.Errorf("adapter %x must be recorded as a member of the enumerated set", obs.Identity.Address)
		}
	}
	if got := byAddr[adapterA].Membership.AdapterType; got == nil || *got != entity.MorphoAdapterTypeMarketV1 {
		t.Errorf("adapterA type = %v, want MarketV1", got)
	}
	if got := byAddr[adapterB].Membership.AdapterType; got == nil || *got != entity.MorphoAdapterTypeVaultV1 {
		t.Errorf("adapterB type = %v, want VaultV1", got)
	}

	if len(seeded) != 2 {
		t.Fatalf("want 2 seeded adapter_state rows, got %d", len(seeded))
	}
	seededByID := map[int64]*entity.MorphoAdapterState{}
	for _, s := range seeded {
		seededByID[s.MorphoAdapterID] = s
		if s.BlockNumber != 24481834 {
			t.Errorf("seed BlockNumber = %d, want 24481834", s.BlockNumber)
		}
		if s.BlockVersion != 2 {
			t.Errorf("seed BlockVersion = %d, want 2 (discovery event version)", s.BlockVersion)
		}
		if s.Timestamp.IsZero() {
			t.Error("seed Timestamp must be set")
		}
	}
	if got := seededByID[101]; got == nil || got.RealAssets.Cmp(realAssetsA) != 0 {
		t.Errorf("adapterA seed realAssets = %v, want %s (hash-pinned)", got, realAssetsA)
	}
	if got := seededByID[102]; got == nil || got.RealAssets.Cmp(realAssetsB) != 0 {
		t.Errorf("adapterB seed realAssets = %v, want %s (hash-pinned)", got, realAssetsB)
	}

	// The adapter SET is versioned state, so its enumeration must be hash-pinned to
	// the discovery block (VEC-218 review) — consistent with the realAssets seeds.
	h.assertMulticallPinnedViaHash(t, testBlockHash, "adaptersLength()", func(c outbound.Call) bool {
		return c.Target == unknownVault && hasSameSelector(c.CallData, adaptersLengthSelector)
	})
	h.assertMulticallPinnedViaHash(t, testBlockHash, "adapters(i)", func(c outbound.Call) bool {
		return c.Target == unknownVault && hasSameSelector(c.CallData, adaptersSelector)
	})
}

// TestProcessBlockEvent_VaultDiscovery_V2_SeedsFeeConfig verifies discovery
// snapshots the vault's full on-chain fee config, the same way it seeds each
// adapter's realAssets. Without the seed, a mid-life-discovered VaultV2 has ZERO
// morpho_vault_fee rows until a Set* fee event that may never fire again —
// sparkUSDTbc set its fees once, at blocks 24765788/24765805 — so every
// fee-dependent read of that vault falls off a cliff for its entire indexed
// history. Caps are deliberately NOT seeded: cap ids are opaque keccak hashes with
// no on-chain enumeration getter, so there is nothing to enumerate.
func TestProcessBlockEvent_VaultDiscovery_V2_SeedsFeeConfig(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22")
	curator := common.HexToAddress("0x00000000000000000000000000000000000000A3")
	liquidityAdapter := common.HexToAddress("0x0000000000000000000000000000000000000B4C")

	perfFee := big.NewInt(100_000_000_000_000_000) // 0.1 WAD
	mgmtFee := big.NewInt(3170979198)              // a WAD per-second rate
	perfRecip := common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E")
	mgmtRecip := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch {
		case len(calls) == 4 && h.isProbeMulticall(calls):
			return h.vaultV2ProbeResults(testLoanToken, curator, liquidityAdapter), nil
		case len(calls) == 4 && h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 6, false), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDT", 6), nil
		default:
			return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
		}
	}
	var gotFeeHash common.Hash
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		switch {
		case len(calls) == 1 && calls[0].Target == unknownVault && hasSameSelector(calls[0].CallData, adaptersLengthSelector):
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(0))}}, nil
		case len(calls) == 4 && calls[0].Target == unknownVault:
			gotFeeHash = blockHash
			return h.feeGetterResults(perfFee, mgmtFee, perfRecip, mgmtRecip), nil
		case len(calls) == 2 && calls[0].Target == unknownVault:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
		}
	}
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) { return 99, nil }
	var savedFee *entity.MorphoVaultFee
	h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, f *entity.MorphoVaultFee) (bool, error) {
		savedFee = f
		return true, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	if err := h.processBlock(t, 1, 24481834, 2, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedFee == nil {
		t.Fatal("discovery must seed a morpho_vault_fee snapshot for a VaultV2")
	}
	if gotFeeHash != testBlockHash {
		t.Errorf("fee seed pinned to %s, want %s", gotFeeHash, testBlockHash)
	}
	if savedFee.MorphoVaultID != 99 {
		t.Errorf("MorphoVaultID = %d, want 99", savedFee.MorphoVaultID)
	}
	if savedFee.PerformanceFee.Cmp(perfFee) != 0 || savedFee.ManagementFee.Cmp(mgmtFee) != 0 {
		t.Errorf("fees = (%s, %s), want (%s, %s)", savedFee.PerformanceFee, savedFee.ManagementFee, perfFee, mgmtFee)
	}
	if !bytes.Equal(savedFee.PerformanceFeeRecipient, perfRecip.Bytes()) {
		t.Errorf("PerformanceFeeRecipient = %x, want %s", savedFee.PerformanceFeeRecipient, perfRecip.Hex())
	}
	if !bytes.Equal(savedFee.ManagementFeeRecipient, mgmtRecip.Bytes()) {
		t.Errorf("ManagementFeeRecipient = %x, want %s", savedFee.ManagementFeeRecipient, mgmtRecip.Hex())
	}
	if savedFee.BlockNumber != 24481834 || savedFee.BlockVersion != 2 {
		t.Errorf("fee seed at (block %d, version %d), want (24481834, 2)", savedFee.BlockNumber, savedFee.BlockVersion)
	}
	if savedFee.Timestamp.IsZero() {
		t.Error("fee seed Timestamp must be set")
	}
}

// TestProcessBlockEvent_VaultDiscovery_V1NeverSeedsFeeConfig pins the version
// gate: morpho_vault_fee is a VaultV2-only table (V1/V1.1 fees live on a different
// surface), so discovering a V1 vault must not attempt the V2 fee getters.
func TestProcessBlockEvent_VaultDiscovery_V1NeverSeedsFeeConfig(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch {
		case len(calls) == 4 && h.isProbeMulticall(calls):
			return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
		case len(calls) == 4 && h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Gauntlet USDC Core", "gtUSDCcore", 18, true), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDC", 6), nil
		default:
			return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
		}
	}
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) == 2 && calls[0].Target == unknownVault {
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		}
		return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
	}
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) { return 99, nil }
	h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultFee) (bool, error) {
		t.Fatal("a V1 vault must not get a VaultV2 fee snapshot")
		return true, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	if err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2_ZeroAdaptersRegistersCleanly verifies a
// V2 vault with no adapters yet is discovered and registered without any adapter
// or state write.
func TestProcessBlockEvent_VaultDiscovery_V2_ZeroAdaptersRegistersCleanly(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22")
	curator := common.HexToAddress("0x00000000000000000000000000000000000000A3")
	liquidityAdapter := common.HexToAddress("0x0000000000000000000000000000000000000B4C")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch {
		case len(calls) == 4 && h.isProbeMulticall(calls):
			return h.vaultV2ProbeResults(testLoanToken, curator, liquidityAdapter), nil
		case len(calls) == 4 && h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 6, false), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDT", 6), nil
		default:
			return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
		}
	}
	// adaptersLength() is hash-pinned (versioned adapter set); the vault-state read
	// of the triggering AccrueInterest is too.
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		switch {
		case len(calls) == 1 && calls[0].Target == unknownVault && hasSameSelector(calls[0].CallData, adaptersLengthSelector):
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(0))}}, nil
		case len(calls) == 4 && calls[0].Target == unknownVault:
			return h.feeGetterResults(big.NewInt(0), big.NewInt(0), common.Address{}, common.Address{}), nil
		case len(calls) == 2 && calls[0].Target == unknownVault:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
		}
	}
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) { return 99, nil }
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
		t.Fatal("no adapter membership must be recorded for a zero-adapter vault")
		return 0, false, nil
	}
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) (bool, error) {
		t.Fatal("no adapter state must be seeded for a zero-adapter vault")
		return true, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	if err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("zero-adapter V2 vault should still be registered")
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2_EnumerationTransportErrorRetries
// verifies a transient enumeration failure bubbles (so SQS redelivers) and
// leaves the vault UNregistered in-memory, so the redelivery re-runs discovery +
// enumeration rather than skipping straight to the event handlers. It must also
// not poison the address as not-a-vault.
func TestProcessBlockEvent_VaultDiscovery_V2_EnumerationTransportErrorRetries(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22")
	curator := common.HexToAddress("0x00000000000000000000000000000000000000A3")
	liquidityAdapter := common.HexToAddress("0x0000000000000000000000000000000000000B4C")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch {
		case len(calls) == 4 && h.isProbeMulticall(calls):
			return h.vaultV2ProbeResults(testLoanToken, curator, liquidityAdapter), nil
		case len(calls) == 4 && h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 6, false), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDT", 6), nil
		default:
			return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
		}
	}
	// The enumeration is hash-pinned, so the failure must be injected on the hash
	// entry point — injecting it on Execute would only reach the code through the
	// mock's number-to-hash fallback.
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) == 1 && calls[0].Target == unknownVault && hasSameSelector(calls[0].CallData, adaptersLengthSelector) {
			return nil, errors.New("adaptersLength rpc down")
		}
		return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
	}
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) { return 99, nil }

	log := h.makeDiscoveryTriggerLog(unknownVault)
	err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)})
	if err == nil {
		t.Fatal("expected the block to fail so SQS redelivers")
	}
	if !strings.Contains(err.Error(), "adaptersLength rpc down") {
		t.Errorf("the block must fail on the injected enumeration error, got: %v", err)
	}
	if h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("vault must stay unregistered so discovery re-runs on retry")
	}
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("a transient enumeration error must not mark the address not-a-vault")
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2_EnumerationFailureCommitsNothingAndRediscoversAfterRestart
// is the atomicity guarantee: a V2 vault's row must never be committed without its
// enumerated adapters. When enumeration fails, the discovery transaction must
// commit NOTHING (GetOrCreateVault never called), so a worker restart that reloads
// the registry from the DB does not see a discovered-but-adapterless vault and
// skip its enumeration forever — the redelivered block re-runs full discovery.
//
// The mock morpho repo persists committed vaults so the simulated restart's
// LoadVaultRegistry sees exactly what the discovery transaction committed. The
// multicall dispatch is shape-based and wired to both entry points so the read
// pattern is independent of number- vs hash-pinning.
func TestProcessBlockEvent_VaultDiscovery_V2_EnumerationFailureCommitsNothingAndRediscoversAfterRestart(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22")
	curator := common.HexToAddress("0x00000000000000000000000000000000000000A3")
	adapterA := common.HexToAddress("0xAaAa000000000000000000000000000000000001")
	realAssetsA := big.NewInt(41_300_000)

	committed := map[common.Address]*entity.MorphoVault{}
	var vaultCreates int
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		vaultCreates++
		v.ID = 99
		committed[common.BytesToAddress(v.Address)] = v
		return 99, nil
	}
	h.morphoRepo.GetAllVaultsFn = func(_ context.Context, _ int64) (map[common.Address]*entity.MorphoVault, error) {
		out := make(map[common.Address]*entity.MorphoVault, len(committed))
		maps.Copy(out, committed)
		return out, nil
	}
	var adapterSeeded bool
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) (bool, error) {
		adapterSeeded = true
		return true, nil
	}
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
		return 101, true, nil
	}

	enumerationShouldFail := true
	dispatch := func(calls []outbound.Call) ([]outbound.Result, error) {
		switch {
		case len(calls) == 4 && h.isProbeMulticall(calls):
			return h.vaultV2ProbeResults(testLoanToken, curator, adapterA), nil
		case len(calls) == 4 && h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 6, false), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDT", 6), nil
		case len(calls) == 1 && calls[0].Target == unknownVault && hasSameSelector(calls[0].CallData, adaptersLengthSelector):
			if enumerationShouldFail {
				return nil, errors.New("adaptersLength rpc down")
			}
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
		case len(calls) == 1 && calls[0].Target == unknownVault && hasSameSelector(calls[0].CallData, adaptersSelector):
			return []outbound.Result{{Success: true, ReturnData: h.packAddress(adapterA)}}, nil
		case len(calls) == adapterProbeCallsPerAdapter && calls[0].Target == adapterA:
			return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
		case len(calls) == 1 && calls[0].Target == adapterA:
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(realAssetsA)}}, nil
		case len(calls) == 4 && calls[0].Target == unknownVault:
			return h.feeGetterResults(big.NewInt(0), big.NewInt(0), common.Address{}, common.Address{}), nil
		case len(calls) == 2 && calls[0].Target == unknownVault:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
		default:
			return nil, fmt.Errorf("unexpected multicall shape (%d calls)", len(calls))
		}
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return dispatch(calls)
	}
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return dispatch(calls)
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)

	// First delivery: enumeration fails, so the discovery transaction must commit
	// nothing.
	if err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
		t.Fatal("expected the enumeration failure to fail the block so SQS redelivers")
	}
	if vaultCreates != 0 {
		t.Fatalf("vault must NOT be persisted when enumeration fails (got %d GetOrCreateVault call(s)) — the vault row cannot exist without its adapters", vaultCreates)
	}
	if len(committed) != 0 {
		t.Fatalf("no vault may be committed on an enumeration failure, got %d", len(committed))
	}
	if h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("vault must stay unregistered in-memory so discovery re-runs on retry")
	}
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("a transient enumeration failure must not mark the address not-a-vault")
	}

	// Simulate a worker restart before SQS redelivery: a fresh registry loads only
	// what discovery actually committed. Because it committed nothing, the vault is
	// still undiscovered.
	h.svc.vaultRegistry = NewVaultRegistry(h.svc.config.Logger)
	if err := h.svc.LoadVaultRegistry(context.Background()); err != nil {
		t.Fatalf("LoadVaultRegistry after restart: %v", err)
	}
	if h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("a restart must not resurrect an undiscovered vault — nothing was committed")
	}

	// Redelivery after restart with enumeration healthy: discovery re-runs fully.
	enumerationShouldFail = false
	if err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("redelivery after restart must re-run discovery cleanly: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("redelivery must discover and register the vault")
	}
	if vaultCreates == 0 {
		t.Fatal("redelivery must persist the vault")
	}
	if !adapterSeeded {
		t.Fatal("redelivery must seed the enumerated adapter's state")
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2_SeedRealAssetsErrorRetries verifies a
// transport failure on the per-adapter realAssets() seed read bubbles (transient,
// retried) and writes no adapter/state rows.
func TestProcessBlockEvent_VaultDiscovery_V2_SeedRealAssetsErrorRetries(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22")
	curator := common.HexToAddress("0x00000000000000000000000000000000000000A3")
	adapterA := common.HexToAddress("0xAaAa000000000000000000000000000000000001")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch {
		case len(calls) == 4 && h.isProbeMulticall(calls):
			return h.vaultV2ProbeResults(testLoanToken, curator, adapterA), nil
		case len(calls) == 4 && h.isVaultDetailsMulticall(calls):
			return h.vaultDetailResults("Spark Blue Chip USDT Vault", "sparkUSDTbc", 6, false), nil
		case len(calls) == 2 && calls[0].Target == testLoanToken:
			return h.tokenMetadataResults("USDT", 6), nil
		case len(calls) == adapterProbeCallsPerAdapter && calls[0].Target == adapterA:
			return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
		default:
			return nil, fmt.Errorf("unexpected Execute shape (%d calls)", len(calls))
		}
	}
	// Enumeration is hash-pinned; the per-adapter realAssets() seed fails here.
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		switch {
		case len(calls) == 1 && calls[0].Target == unknownVault && hasSameSelector(calls[0].CallData, adaptersLengthSelector):
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
		case len(calls) == 1 && calls[0].Target == unknownVault && hasSameSelector(calls[0].CallData, adaptersSelector):
			return []outbound.Result{{Success: true, ReturnData: h.packAddress(adapterA)}}, nil
		case len(calls) == 1 && calls[0].Target == adapterA:
			return nil, errors.New("realAssets rpc down")
		default:
			return nil, fmt.Errorf("unexpected ExecuteAtHash shape (%d calls)", len(calls))
		}
	}
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) { return 99, nil }
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) (bool, error) {
		t.Fatal("no adapter state must be seeded when the realAssets read fails")
		return true, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	if err := h.processBlock(t, 1, 24481834, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
		t.Fatal("expected the block to fail so SQS redelivers")
	}
	if h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("vault must stay unregistered so discovery re-runs on retry")
	}
}

// TestProcessBlockEvent_VaultDiscovery_TransferOnlyDoesNotProbe verifies that
// a plain ERC20 Transfer log from an unknown address does NOT trigger
// tryDiscoverVault.
//
// Without this gate, every tx that touches a popular ERC20 (BAT, STORJ, …)
// would route into the 4-call probe path. Some legacy ERC20s terminate
// unrecognised selector calls with `INVALID` (0xfe) instead of `REVERT`,
// which consumes all available gas and pushes Multicall3's aggregate3 past
// Alchemy's 550M eth_call cap. The discovery layer would then treat that as
// a transient transport error (not ErrNotVault) and retry the SQS message
// forever. See docs/vec-198-morpho-v2-multicall-gas-cap-fix-plan.md.
func TestProcessBlockEvent_VaultDiscovery_TransferOnlyDoesNotProbe(t *testing.T) {
	h := newTestHarness(t)
	unknownAddr := common.HexToAddress("0x0D8775F648430679A709E98d2b0Cb6250d2887EF") // BAT — the original symptom

	// Fail fast if any multicall fires: the gate should short-circuit before
	// we ever reach the prober.
	probeAttempted := false
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		probeAttempted = true
		return nil, fmt.Errorf("multicall must not be called for Transfer-from-unknown")
	}

	log := h.makeVaultTransferLog(unknownAddr, testCaller, testReceiver, big.NewInt(1234))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if probeAttempted {
		t.Error("probe must not be attempted for Transfer-from-unknown")
	}
	// And the address must NOT be persisted in the negative cache — that would
	// hide a future legitimate Deposit/Withdraw discovery for the same address.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownAddr) {
		t.Error("address must not be marked as not-vault when only a Transfer was seen")
	}
	if h.svc.vaultRegistry.IsKnownVault(unknownAddr) {
		t.Error("address must not be registered as a vault from a Transfer-only receipt")
	}
}

// TestProcessBlockEvent_VaultDiscovery_V2AccrueInterestTriggersProbe is the
// positive counterpart to *_TransferOnlyDoesNotProbe: a VaultV2 4-field
// AccrueInterest from an unknown address must trigger discovery (and succeed,
// in this case). See IsVaultActivityEvent for why this is the only triggering
// topic.
func TestProcessBlockEvent_VaultDiscovery_V2AccrueInterestTriggersProbe(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Vault", "VLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var vaultCreated bool
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		vaultCreated = true
		return 99, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !vaultCreated {
		t.Error("V2 4-field AccrueInterest from unknown address must trigger discovery")
	}
}

func TestProcessBlockEvent_VaultDiscovery_DBError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Vault", "VLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	dbErr := errors.New("db connection failed")
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 0, dbErr
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	// DB errors are transient — processBlock should fail so the event can be reprocessed.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("processBlock should fail on transient DB error so event can be reprocessed")
	}

	// Transient failure must NOT permanently mark the address as non-vault.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient DB error")
	}
}

func TestProcessBlockEvent_VaultDiscovery_RPCTransientError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// Simulate a transient RPC failure during vault probe.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 {
			return nil, fmt.Errorf("connection timeout")
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	// Transient RPC failure should fail processBlock so the event can be reprocessed.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("processBlock should fail on transient RPC error so event can be reprocessed")
	}

	// Transient RPC failure must NOT permanently mark the address as non-vault.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient RPC error")
	}
}

// TestProcessBlockEvent_VaultDiscovery_TransientErrorThenSuccess verifies that
// when the first log's vault discovery fails transiently and the second log's
// discovery succeeds, the vault is still registered in memory AND processBlock
// returns an error.
//
// VEC-188: a later success does NOT retroactively process the earlier log —
// that log's event was never saved. Returning an error forces SQS to redeliver
// so both logs are retried on the next message. On redelivery, both logs see
// the vault as already-known (registered in memory) and both are processed
// normally.
func TestProcessBlockEvent_VaultDiscovery_TransientErrorThenSuccess(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// First probe call fails (transient), second succeeds.
	probeCallCount := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				probeCallCount++
				if probeCallCount == 1 {
					return nil, fmt.Errorf("connection timeout")
				}
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 99, nil
	}

	// Two discovery-trigger logs from the same vault in one receipt.
	// First triggers discovery (fails transiently), second retries (succeeds).
	log1 := h.makeDiscoveryTriggerLog(unknownVault)
	log2 := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log1, log2)

	// VEC-188: processBlock must FAIL even though the 2nd log's discovery
	// succeeded — the 1st log's event was never persisted and must be retried
	// via SQS redelivery.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err == nil {
		t.Fatal("processBlock must fail so SQS redelivers and both logs are retried (VEC-188)")
	}

	// Even though processBlock fails, the vault was registered in memory by
	// the 2nd log's successful discovery. This is correct: on SQS redelivery,
	// both logs will see the vault as already-known and process normally.
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Error("vault should be registered after eventual success")
	}
	// Transient RPC failure must NOT permanently mark the address as non-vault.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient RPC error")
	}
}

func TestProcessBlockEvent_VaultDiscovery_AlreadyKnownNotVault(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")
	h.svc.vaultRegistry.MarkNotVault(unknownVault)

	// No multicall should be made.
	var multicallCalled atomic.Int32
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		multicallCalled.Add(1)
		return nil, errors.New("should not be called")
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if multicallCalled.Load() != 0 {
		t.Error("multicall should not be called for known non-vaults")
	}
}

// --- V1/V1.1 vault discovery via Morpho Blue caller/onBehalf path ---
//
// V1/V1.1 vaults call into Morpho Blue when allocating; their address appears
// as `caller` and/or `onBehalf` in the singleton's Supply/Withdraw/Borrow/
// Repay/SupplyCollateral/WithdrawCollateral/Liquidate events. With
// IsVaultActivityEvent narrowed to the V2 4-field AccrueInterest topic only
// (VEC-198 PR feedback), V1/V1.1 vaults can no longer be discovered via
// their own Deposit / Withdraw / V1 AccrueInterest logs.
//
// The morpho-vault-backfill already handles this via
// emitMorphoBlueCandidates, but the backfiller is recovery-only — operators
// run it when they realise something was missed, not on a schedule. The
// live indexer therefore has to cover V1/V1.1 discovery itself by mirroring
// the same Morpho Blue caller/onBehalf probe path. Otherwise a brand-new
// V1.x vault is invisible to live indexing until somebody manually triggers
// a backfill.
//
// On first discovery via this path, processBlock returns an error to force
// SQS redelivery. The redelivery lets the second pass process any
// earlier-in-receipt vault logs (Deposit / Transfer / V1 AccrueInterest)
// that were skipped while the vault was unknown. Handlers are idempotent
// (`ON CONFLICT` on protocol_event, state-snapshot keys), so reprocessing
// the Morpho Blue Supply on the second pass is safe. The V2 path doesn't
// need this retry because V2 emits its 4-field AccrueInterest first in any
// state-changing transaction — the discovery trigger always precedes
// vault-state logs in log_index order.

// TestProcessBlockEvent_VaultDiscovery_V1ViaMorphoBlueCaller verifies that
// a V1 vault is discovered when it appears as `caller` in a Morpho Blue
// Supply event. The probe identifies V1 (skimRecipient reverts on V1; V1.1
// would succeed; V2's MORPHO() reverts).
func TestProcessBlockEvent_VaultDiscovery_V1ViaMorphoBlueCaller(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// Count probe-multicall invocations. unknownVault appears as both
	// caller and onBehalf in the Supply log below; the dedup at
	// service.go's `seen` map must keep this at exactly 1. Without the
	// dedup, two probes would fire (and the test would still pass at the
	// vault-registered assertion below — wasted RPC).
	var probeCount atomic.Int32
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				probeCount.Add(1)
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("V1 Vault", "v1V", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeSupplyLog(testMarketID, unknownVault, unknownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	// Single pass: pre-walk discovers the vault before the main loop reaches
	// any vault-emitted log. No SQS redelivery needed for ordinary
	// first-activity-for-a-brand-new-vault.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("V1 vault must be registered in registry after Morpho Blue path discovery")
	}
	if savedVault == nil {
		t.Fatal("V1 vault was not persisted to DB")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV1 {
		t.Errorf("VaultVersion = %d, want V1 (%d)", savedVault.VaultVersion, entity.MorphoVaultV1)
	}
	// Dedup verification: caller == onBehalf in the Supply log, so the
	// `seen` map in discoverV1V11VaultsInReceipt must collapse the two
	// candidates into a single probe. Two probes would mean wasted RPC +
	// duplicate DB inserts (idempotent, but still wrong).
	if got := probeCount.Load(); got != 1 {
		t.Errorf("probe fired %d times for caller==onBehalf; want exactly 1 (caller/onBehalf must dedupe via seen[])", got)
	}

	// Replay: vault is now known, no further discovery, no error.
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// Replay must also not re-probe — registry is consulted first.
	if got := probeCount.Load(); got != 1 {
		t.Errorf("probe fired %d times after replay; want still 1 (registry cache must short-circuit known vault)", got)
	}
}

// TestProcessBlockEvent_VaultDiscovery_V11ViaMorphoBlueOnBehalf verifies V1.1
// discovery when the vault appears as `onBehalf` (not just `caller`) — covers
// the case where the vault is the position owner but a separate router/
// integrator routed the call (their address is `caller`). Probe identifies
// V1.1 because skimRecipient succeeds.
func TestProcessBlockEvent_VaultDiscovery_V11ViaMorphoBlueOnBehalf(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	router := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				if calls[0].Target == unknownVault {
					return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
				}
				// router probe → not a vault
				return h.notAVaultProbeResults(), nil
			}
			return h.vaultDetailResults("V1.1 Vault", "v11V", 18, true), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeSupplyLog(testMarketID, router, unknownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("V1.1 vault must be registered after onBehalf-path discovery")
	}
	if savedVault == nil {
		t.Fatal("V1.1 vault was not persisted")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV1_1 {
		t.Errorf("VaultVersion = %d, want V1.1 (%d)", savedVault.VaultVersion, entity.MorphoVaultV1_1)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(router) {
		t.Error("router (caller, not a vault) must be marked known-not-vault after probe")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_EOA_MarkedNotVault
// verifies that an EOA appearing in a Morpho Blue event is probed once,
// fails the probe, and is cached as known-not-vault so subsequent events
// from the same EOA short-circuit before incurring another multicall.
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_EOA_MarkedNotVault(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	eoa := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.notAVaultProbeResults(), nil
			}
			return nil, fmt.Errorf("unexpected non-probe 4-call")
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	log := h.makeSupplyLog(testMarketID, eoa, eoa, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock should not error when probe definitively rejects an EOA: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(eoa) {
		t.Error("EOA should be marked known-not-vault after probe rejection")
	}
	// Asymmetry guard: a sign-flip that swaps vault / not-vault classification
	// would mark the EOA as a vault. Pin both sides.
	if h.svc.vaultRegistry.IsKnownVault(eoa) {
		t.Error("EOA must NOT be registered as a vault — probe definitively rejected it")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_KnownNotVault_SkipsProbe
// verifies that an address already in the known-not-vault cache short-circuits
// without firing a multicall — the cache is the only thing keeping the live
// indexer from re-probing every Morpho Blue event's user addresses.
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_KnownNotVault_SkipsProbe(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	eoa := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	h.svc.vaultRegistry.MarkNotVault(eoa)

	probeAttempted := false
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 && h.isProbeMulticall(calls) {
			probeAttempted = true
			return nil, fmt.Errorf("probe must not fire for known-not-vault address")
		}
		// 2-call market+position state for the Morpho Blue Supply.
		return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
	}

	log := h.makeSupplyLog(testMarketID, eoa, eoa, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if probeAttempted {
		t.Error("probe must short-circuit for already-known-not-vault addresses")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_TransientError_RetriesViaSQS
// verifies that a transient probe failure during Morpho Blue path discovery
// surfaces as an error so SQS redelivers the receipt — the address must NOT
// be marked known-not-vault on transient failure (that would be a permanent
// black-hole for a real V1/V1.1 vault that's just temporarily unreachable).
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_TransientError_RetriesViaSQS(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return nil, fmt.Errorf("connection timeout")
			}
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	log := h.makeSupplyLog(testMarketID, unknownVault, unknownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock must fail on transient probe error so SQS redelivers")
	}
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("address must NOT be marked known-not-vault on a transient error — that would black-hole a real vault that's just temporarily unreachable")
	}
	// Asymmetry guard: a swap of caching semantics (cache transient,
	// surface ErrNotVault) would only fail one of the two existing tests.
	// Assert the surfaced error is *not* an *ErrNotVault* so a future
	// reclassification has to update this test alongside the EOA test.
	var nv *ErrNotVault
	if errors.As(err, &nv) {
		t.Errorf("transient probe failure must surface as a plain error, not *ErrNotVault — wrapping it as ErrNotVault would silently mark the address as known-not-vault on the next retry: %v", err)
	}
}

// TestProcessReceipt_VaultDiscovery_MorphoBluePath_DepositPlusSupplyInOnePass
// is the contract test for the pre-walk: a typical user-deposit receipt for a
// brand-new V1.1 vault has the vault's own Deposit log AT log[0] and its
// allocation Morpho Blue Supply (vault as caller/onBehalf) at log[1]. The
// pre-walk in processReceipt runs FIRST and registers the vault from the
// Supply, so by the time the main loop reaches log[0] the vault is already
// in the registry and the Deposit is processed via the IsKnownVault branch.
// No SQS redelivery, no whole-block reprocessing.
func TestProcessReceipt_VaultDiscovery_MorphoBluePath_DepositPlusSupplyInOnePass(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			if calls[0].Target == unknownVault {
				// vault state (totalAssets + totalSupply) on Deposit handling.
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			// asset token metadata
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// vault state + balance for Deposit handling
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("V1.1 Vault", "v11V", 18, true), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 99, nil
	}

	depositLog := h.makeVaultDepositLog(unknownVault, testCaller, testOnBehalf, big.NewInt(5000), big.NewInt(4500))
	supplyLog := h.makeSupplyLog(testMarketID, unknownVault, unknownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, depositLog, supplyLog)

	var depositPositionSaved bool
	h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
		depositPositionSaved = true
		return nil
	}

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("vault must be registered by the pre-walk before the main loop reaches log[0]")
	}
	if !depositPositionSaved {
		t.Error("Deposit at log[0] was not processed in the same pass — the pre-walk should have registered the vault BEFORE the main loop reached log[0], so the Deposit hits the IsKnownVault branch in processReceipt's switch")
	}
}

// TestMorphoBlueVaultCandidates_TableDriven pins the contract that the live
// indexer and the morpho-vault-backfill share via
// MorphoBlueVaultCandidates. A future PR that, say, switches Liquidate to
// return only {Caller} or replaces the type switch with reflection on field
// names "Caller"+"OnBehalf" must fail this test rather than silently drift
// the live/backfill discovery contracts apart.
func TestMorphoBlueVaultCandidates_TableDriven(t *testing.T) {
	caller := common.HexToAddress("0x1111111111111111111111111111111111111111")
	onBehalf := common.HexToAddress("0x2222222222222222222222222222222222222222")
	receiver := common.HexToAddress("0x3333333333333333333333333333333333333333")
	borrower := common.HexToAddress("0x4444444444444444444444444444444444444444")

	tests := []struct {
		name  string
		event MorphoBlueEvent
		want  []common.Address
	}{
		{"Supply", &SupplyEvent{Caller: caller, OnBehalf: onBehalf}, []common.Address{caller, onBehalf}},
		{"Withdraw", &WithdrawEvent{Caller: caller, OnBehalf: onBehalf, Receiver: receiver}, []common.Address{caller, onBehalf}},
		{"Borrow", &BorrowEvent{Caller: caller, OnBehalf: onBehalf, Receiver: receiver}, []common.Address{caller, onBehalf}},
		{"Repay", &RepayEvent{Caller: caller, OnBehalf: onBehalf}, []common.Address{caller, onBehalf}},
		{"SupplyCollateral", &SupplyCollateralEvent{Caller: caller, OnBehalf: onBehalf}, []common.Address{caller, onBehalf}},
		{"WithdrawCollateral", &WithdrawCollateralEvent{Caller: caller, OnBehalf: onBehalf, Receiver: receiver}, []common.Address{caller, onBehalf}},
		{"Liquidate", &LiquidateEvent{Caller: caller, Borrower: borrower}, []common.Address{caller, borrower}},
		// Events without user candidates fall through to nil.
		{"CreateMarket (no candidates)", &CreateMarketEvent{}, nil},
		{"AccrueInterest (no candidates)", &AccrueInterestEvent{}, nil},
		{"SetFee (no candidates)", &SetFeeEvent{}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MorphoBlueVaultCandidates(tt.event)
			if !slices.Equal(got, tt.want) {
				t.Errorf("MorphoBlueVaultCandidates() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestProcessBlockEvent_VaultDiscovery_V1ViaMorphoBlueLiquidateBorrower
// covers the Liquidate branch of MorphoBlueVaultCandidates, which uses
// {Caller, Borrower} (not {Caller, OnBehalf} like the position events).
// MetaMorpho V1/V1.1 vaults don't borrow on Morpho Blue in practice, but
// the slot is included for symmetry — a regression that drops Liquidate
// from the switch (or swaps borrower for receiver, etc.) would be invisible
// without this test.
func TestProcessBlockEvent_VaultDiscovery_V1ViaMorphoBlueLiquidateBorrower(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")
	liquidator := common.HexToAddress("0x5555555555555555555555555555555555555555")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == MorphoBlueAddress {
				// market+position state for handleLiquidateEvent. Two positions.
				return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// market state + borrower position + liquidator position
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult(), h.defaultPositionStateResult()}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				if calls[0].Target == unknownVault {
					return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
				}
				return h.notAVaultProbeResults(), nil
			}
			return h.vaultDetailResults("V1 Vault", "v1V", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var savedVault *entity.MorphoVault
	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, v *entity.MorphoVault) (int64, error) {
		savedVault = v
		return 99, nil
	}

	log := h.makeLiquidateLog(testMarketID, liquidator, unknownVault,
		big.NewInt(1000), big.NewInt(900), big.NewInt(1100), big.NewInt(0), big.NewInt(0))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownVault(unknownVault) {
		t.Fatal("V1 vault as Liquidate borrower must be registered after probe")
	}
	if savedVault == nil {
		t.Fatal("V1 vault was not persisted")
	}
	if savedVault.VaultVersion != entity.MorphoVaultV1 {
		t.Errorf("VaultVersion = %d, want V1 (%d)", savedVault.VaultVersion, entity.MorphoVaultV1)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(liquidator) {
		t.Error("liquidator (caller, not a vault) must be marked known-not-vault after probe rejection")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_KnownVault_SkipsProbe
// verifies that an already-registered vault appearing as caller/onBehalf
// short-circuits without firing the probe. The IsKnownVault check at line
// 752 of service.go guards the hot path — a regression that removes it
// would re-probe every Morpho Blue Supply for already-discovered vaults
// (silent perf regression).
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_KnownVault_SkipsProbe(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	knownVault := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	h.registerTestVault(knownVault, 77, entity.MorphoVaultV1)

	probeAttempted := false
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 && h.isProbeMulticall(calls) {
			probeAttempted = true
			return nil, fmt.Errorf("probe must not fire for already-known vault address")
		}
		return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
	}

	log := h.makeSupplyLog(testMarketID, knownVault, knownVault, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if probeAttempted {
		t.Error("probe must short-circuit for already-known vault addresses")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_FreshUserCallerProbedExactlyOnce
// is a canary against the harness's pre-marking of testCaller / testOnBehalf
// / etc. as known-not-vault. Those pre-marks are an isolation convenience —
// they keep existing Supply/Withdraw/Borrow tests from incidentally hitting
// the V1/V1.1 probe path. But they also make those tests blind to one
// regression: a change that stops calling discoverV1V11VaultsInReceipt
// from processReceipt's Morpho Blue case would break NO existing test
// because the pre-marked addresses short-circuit before the probe anyway.
//
// This test uses a fresh, un-pre-marked EOA address as the caller and
// asserts the probe fires exactly once and resolves to known-not-vault.
// If the wiring at service.go's Morpho Blue case is ever removed, this
// test will fail with probe never fires AND eoa not cached.
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_FreshUserCallerProbedExactlyOnce(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	freshUser := common.HexToAddress("0xc0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0")

	var probeCount atomic.Int32
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				probeCount.Add(1)
				return h.notAVaultProbeResults(), nil
			}
		}
		return nil, fmt.Errorf("unexpected %d calls", len(calls))
	}

	// caller == onBehalf so dedup ensures exactly one probe.
	log := h.makeSupplyLog(testMarketID, freshUser, freshUser, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if got := probeCount.Load(); got != 1 {
		t.Fatalf("probe fired %d times for fresh caller; want exactly 1 — if 0, the discoverV1V11VaultsInReceipt wiring is broken; if >1, the seen-map dedup is broken", got)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(freshUser) {
		t.Error("freshUser must end up in not-vault cache after probe rejection — if missing, the cache write at service.go's ErrNotVault branch is broken")
	}
}

// TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_ZeroAddress_SkipsProbe
// verifies the zero-address / MorphoBlueAddress filter at service.go:749.
// Without it, a Borrow event with onBehalf=0x0 would route into the probe,
// resolve to *ErrNotVault, and pollute the negative-cache with the zero
// address — noise that survives across the process lifetime.
func TestProcessBlockEvent_VaultDiscovery_MorphoBluePath_ZeroAddress_SkipsProbe(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	caller := common.HexToAddress("0x6666666666666666666666666666666666666666")

	probeAttempted := false
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 4 && h.isProbeMulticall(calls) {
			if calls[0].Target == (common.Address{}) {
				probeAttempted = true
				return nil, fmt.Errorf("probe must never fire on zero address")
			}
			// caller probe → not a vault
			return h.notAVaultProbeResults(), nil
		}
		return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
	}

	// Borrow with onBehalf == 0x0 — caller is a real address, onBehalf is zero.
	log := h.makeBorrowLog(testMarketID, caller, common.Address{}, caller, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if probeAttempted {
		t.Error("zero address must never reach the probe path")
	}
	if h.svc.vaultRegistry.IsKnownNotVault(common.Address{}) {
		t.Error("zero address must never end up in the not-vault cache")
	}
}

// --- tryDiscoverVault error paths ---

func TestProcessBlockEvent_VaultDiscovery_EventDecodeError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// Use a V2 4-field AccrueInterest topic (the only discovery trigger) with
	// empty data so the non-indexed argument unpack fails and the event-decode
	// guard inside tryDiscoverVault marks the address as not-vault.
	v2AccrueEvent := h.metaMorphoV2AccrueABI.Events["AccrueInterest"]
	log := shared.Log{
		Address: unknownVault.Hex(),
		Topics: []string{
			v2AccrueEvent.ID.Hex(),
		},
		Data:            "",
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	// Should not error (vault discovery failure is non-fatal)
	if err != nil {
		t.Fatalf("processBlock should not error on vault discovery failure: %v", err)
	}
	if !h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should be marked as not-vault on event decode error")
	}
}

func TestProcessBlockEvent_VaultDiscovery_GetTokenMetadataError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			// getTokenMetadata call fails
			return nil, errors.New("token metadata rpc error")
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock should fail on transient token metadata RPC error so event can be reprocessed")
	}
	// Token metadata RPC error is transient — vault should NOT be permanently marked as non-vault
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient token metadata error")
	}
}

func TestProcessBlockEvent_VaultDiscovery_GetOrCreateTokenError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ int, _ *int64) (int64, error) {
		return 0, errors.New("token creation failed")
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock should fail on transient token creation error so event can be reprocessed")
	}
	// Token creation DB error is transient — vault should NOT be permanently marked as non-vault
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient token creation error")
	}
}

// --- tryDiscoverVault receipt token tests ---

func TestProcessBlockEvent_VaultDiscovery_ReceiptTokenCreated(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	var capturedToken entity.ReceiptToken
	receiptTokenCalled := false
	h.receiptTokenRepo.GetOrCreateReceiptTokenFn = func(_ context.Context, _ pgx.Tx, token entity.ReceiptToken) (int64, error) {
		receiptTokenCalled = true
		capturedToken = token
		return 1, nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("processBlock should succeed, got: %v", err)
	}

	if !receiptTokenCalled {
		t.Fatal("expected receiptTokenRepo.GetOrCreateReceiptToken to be called")
	}
	if capturedToken.ChainID != 1 {
		t.Errorf("receipt token ChainID = %d, want 1", capturedToken.ChainID)
	}
	if capturedToken.ReceiptTokenAddress != unknownVault {
		t.Errorf("receipt token address = %s, want %s", capturedToken.ReceiptTokenAddress.Hex(), unknownVault.Hex())
	}
	if capturedToken.Symbol != "mVLT" {
		t.Errorf("receipt token symbol = %q, want %q", capturedToken.Symbol, "mVLT")
	}
}

func TestProcessBlockEvent_VaultDiscovery_ReceiptTokenRepoError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Morpho Vault", "mVLT", 18, false), nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.receiptTokenRepo.GetOrCreateReceiptTokenFn = func(_ context.Context, _ pgx.Tx, _ entity.ReceiptToken) (int64, error) {
		return 0, errors.New("receipt token db error")
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock should fail on receipt token repo error")
	}
	if !strings.Contains(err.Error(), "receipt token") {
		t.Errorf("error should mention receipt token, got: %s", err.Error())
	}
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient receipt token error")
	}
}

// TestVaultDiscovery_AssetSymbolRevert_StoresEmptySymbol verifies that when a
// vault is discovered via the V2 AccrueInterest path and the asset token's
// symbol() reverts while decimals() succeeds, the asset token is persisted
// with an empty symbol (the sweep picks it up later).
func TestVaultDiscovery_AssetSymbolRevert_StoresEmptySymbol(t *testing.T) {
	const blockNumber = int64(20000005)
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9898989898989898989898989898989898989898")

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 4:
			if h.isProbeMulticall(calls) {
				return h.vaultProbeResults(MorphoBlueAddress, testLoanToken), nil
			}
			return h.vaultDetailResults("Asset Revert Vault", "aRV", 18, false), nil
		case 2:
			if calls[0].Target == unknownVault {
				return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult()}, nil
			}
			// getTokenMetadata for the asset: symbol() reverts, decimals() succeeds.
			return []outbound.Result{
				{Success: false, ReturnData: nil},           // symbol() reverts
				{Success: true, ReturnData: h.packUint8(6)}, // decimals() OK
			}, nil
		default:
			return nil, fmt.Errorf("unexpected %d calls", len(calls))
		}
	}

	h.morphoRepo.GetOrCreateVaultFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVault) (int64, error) {
		return 77, nil
	}

	// Capture GetOrCreateToken calls to assert the asset is stored with empty symbol.
	type tokenCall struct {
		address common.Address
		symbol  string
	}
	var tokenCalls []tokenCall
	h.tokenRepo.GetOrCreateTokenFn = func(_ context.Context, _ pgx.Tx, _ int64, addr common.Address, sym string, _ int, _ *int64) (int64, error) {
		tokenCalls = append(tokenCalls, tokenCall{addr, sym})
		return int64(len(tokenCalls)), nil
	}

	log := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, blockNumber, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	// Find the asset token call (testLoanToken) and assert it has an empty symbol.
	var assetCall *tokenCall
	for i := range tokenCalls {
		if tokenCalls[i].address == testLoanToken {
			assetCall = &tokenCalls[i]
			break
		}
	}
	if assetCall == nil {
		t.Fatalf("GetOrCreateToken never called for asset token %s; calls: %v", testLoanToken.Hex(), tokenCalls)
	}
	if assetCall.symbol != "" {
		t.Errorf("asset token symbol = %q, want empty (pending marker for sweep)", assetCall.symbol)
	}
}
