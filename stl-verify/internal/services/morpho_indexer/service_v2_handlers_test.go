package morpho_indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

func errTestUnexpectedCall(calls []outbound.Call) error {
	return fmt.Errorf("unexpected multicall with %d calls", len(calls))
}

// --- log-capture helper ---

type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) hasWarnContaining(sub string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, sub) {
			return true
		}
	}
	return false
}

// captureLogs replaces the service logger with a records-capturing one.
func (h *serviceTestHarness) captureLogs() *capturingHandler {
	handler := &capturingHandler{}
	h.svc.logger = slog.New(handler)
	return handler
}

// --- probe / read result helpers ---

// adapterProbeResults returns the 2-call adapter probe response
// (morpho, morphoVaultV1) that classifies to adapterType.
func (h *serviceTestHarness) adapterProbeResults(adapterType entity.MorphoAdapterType) []outbound.Result {
	ok := func(succeed bool) outbound.Result {
		if succeed {
			return outbound.Result{Success: true, ReturnData: h.packAddress(common.HexToAddress("0x1"))}
		}
		return outbound.Result{Success: false, ReturnData: nil}
	}
	switch adapterType {
	case entity.MorphoAdapterTypeMarketV1:
		return []outbound.Result{ok(true), ok(false)}
	case entity.MorphoAdapterTypeVaultV1:
		return []outbound.Result{ok(false), ok(true)}
	default:
		return []outbound.Result{ok(false), ok(false)}
	}
}

var testAdapterAddr = common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

// --- AddAdapter ---

func TestProcessBlockEvent_AddAdapter(t *testing.T) {
	tests := []struct {
		name        string
		adapterType entity.MorphoAdapterType
		wantWarn    bool
	}{
		{"MarketV1", entity.MorphoAdapterTypeMarketV1, false},
		{"VaultV1", entity.MorphoAdapterTypeVaultV1, false},
		{"Unknown", entity.MorphoAdapterTypeUnknown, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			logs := h.captureLogs()

			h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) == 2 && calls[0].Target == testAdapterAddr {
					return h.adapterProbeResults(tt.adapterType), nil
				}
				return nil, errTestUnexpectedCall(calls)
			}

			var saved *entity.MorphoAdapter
			h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, a *entity.MorphoAdapter) (int64, error) {
				saved = a
				return 42, nil
			}

			ev := h.vaultV2EventsABI.Events["AddAdapter"]
			log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if saved == nil {
				t.Fatal("GetOrCreateAdapter not called")
			}
			if saved.MorphoVaultID != 7 {
				t.Errorf("MorphoVaultID = %d, want 7", saved.MorphoVaultID)
			}
			if !bytes.Equal(saved.Address, testAdapterAddr.Bytes()) {
				t.Errorf("Address = %x, want %s", saved.Address, testAdapterAddr.Hex())
			}
			if saved.AssetTokenID != 1 {
				t.Errorf("AssetTokenID = %d, want 1 (vault asset)", saved.AssetTokenID)
			}
			if saved.AdapterType != tt.adapterType {
				t.Errorf("AdapterType = %d, want %d", saved.AdapterType, tt.adapterType)
			}
			if saved.AddedAtBlock != 20000000 {
				t.Errorf("AddedAtBlock = %d, want 20000000", saved.AddedAtBlock)
			}
			if saved.RemovedAtBlock != nil {
				t.Errorf("RemovedAtBlock = %v, want nil", *saved.RemovedAtBlock)
			}
			if got := logs.hasWarnContaining("unknown type"); got != tt.wantWarn {
				t.Errorf("WARN(unknown type) = %v, want %v", got, tt.wantWarn)
			}
		})
	}
}

func TestProcessBlockEvent_AddAdapter_NonV2VaultErrors(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	adapterProbed := false
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		adapterProbed = true
		return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
	}
	h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapter) (int64, error) {
		t.Fatal("GetOrCreateAdapter must not be called for a non-V2 vault")
		return 0, nil
	}

	ev := h.vaultV2EventsABI.Events["AddAdapter"]
	log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)})
	if err == nil {
		t.Fatal("expected error for VaultV2 event on non-V2 vault")
	}
	if adapterProbed {
		t.Error("adapter must not be probed once the vault-version guard fails")
	}
}

// --- RemoveAdapter ---

func TestProcessBlockEvent_RemoveAdapter(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	var (
		gotVaultID int64
		gotAddr    []byte
		gotBlock   int64
		called     bool
	)
	h.morphoRepo.MarkAdapterRemovedFn = func(_ context.Context, _ pgx.Tx, vaultID int64, address []byte, removedAtBlock int64) error {
		called = true
		gotVaultID, gotAddr, gotBlock = vaultID, address, removedAtBlock
		return nil
	}

	ev := h.vaultV2EventsABI.Events["RemoveAdapter"]
	log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !called {
		t.Fatal("MarkAdapterRemoved not called")
	}
	if gotVaultID != 7 {
		t.Errorf("vaultID = %d, want 7", gotVaultID)
	}
	if !bytes.Equal(gotAddr, testAdapterAddr.Bytes()) {
		t.Errorf("address = %x, want %s", gotAddr, testAdapterAddr.Hex())
	}
	if gotBlock != 20000000 {
		t.Errorf("removedAtBlock = %d, want 20000000", gotBlock)
	}
}

// --- Allocate / Deallocate ---

func TestProcessBlockEvent_Allocation(t *testing.T) {
	tests := []struct {
		name  string
		event string
	}{
		{"Allocate", "Allocate"},
		{"Deallocate", "Deallocate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

			realAssets := big.NewInt(123456789)
			var gotHash common.Hash
			viaHash := false
			h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
				if len(calls) != 1 || calls[0].Target != testAdapterAddr {
					return nil, errTestUnexpectedCall(calls)
				}
				viaHash = true
				gotHash = blockHash
				return []outbound.Result{{Success: true, ReturnData: h.packUint256(realAssets)}}, nil
			}

			var (
				gotVaultID int64
				gotAddr    []byte
			)
			h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, vaultID int64, address []byte) (*entity.MorphoAdapter, error) {
				gotVaultID, gotAddr = vaultID, address
				return &entity.MorphoAdapter{ID: 55, MorphoVaultID: 7, Address: testAdapterAddr.Bytes(), AssetTokenID: 1, AdapterType: entity.MorphoAdapterTypeMarketV1, AddedAtBlock: 19000000}, nil
			}
			var savedState *entity.MorphoAdapterState
			h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoAdapterState) error {
				savedState = s
				return nil
			}

			ev := h.vaultV2EventsABI.Events[tt.event]
			log := h.makeV2VaultLog(ev, testVaultAddr,
				[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
				big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
			if err := h.processBlock(t, 1, 20000000, 3, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if !viaHash {
				t.Fatal("realAssets() must be read via ExecuteAtHash (state read)")
			}
			if gotHash != testBlockHash {
				t.Errorf("realAssets pinned to %s, want %s", gotHash, testBlockHash)
			}
			if gotVaultID != 7 || !bytes.Equal(gotAddr, testAdapterAddr.Bytes()) {
				t.Errorf("GetActiveAdapter(%d,%x), want (7,%s)", gotVaultID, gotAddr, testAdapterAddr.Hex())
			}
			if savedState == nil {
				t.Fatal("SaveAdapterState not called")
			}
			if savedState.MorphoAdapterID != 55 {
				t.Errorf("MorphoAdapterID = %d, want 55", savedState.MorphoAdapterID)
			}
			if savedState.RealAssets.Cmp(realAssets) != 0 {
				t.Errorf("RealAssets = %s, want %s", savedState.RealAssets, realAssets)
			}
			if savedState.BlockNumber != 20000000 {
				t.Errorf("BlockNumber = %d, want 20000000", savedState.BlockNumber)
			}
			if savedState.BlockVersion != 3 {
				t.Errorf("BlockVersion = %d, want 3", savedState.BlockVersion)
			}
			if savedState.Timestamp.IsZero() {
				t.Error("Timestamp must be set")
			}
		})
	}
}

func TestProcessBlockEvent_Allocation_UnknownAdapterErrors(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
	}
	h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ int64, _ []byte) (*entity.MorphoAdapter, error) {
		return nil, nil // adapter was never AddAdapter'd — missed data
	}
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
		t.Fatal("SaveAdapterState must not be called when the adapter is unknown")
		return nil
	}

	ev := h.vaultV2EventsABI.Events["Allocate"]
	log := h.makeV2VaultLog(ev, testVaultAddr,
		[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
		big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)})
	if err == nil {
		t.Fatal("expected error for allocation on an unknown adapter")
	}
}

// --- ForceDeallocate ---

func TestProcessBlockEvent_ForceDeallocate_WarnsWritesNothing(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
	logs := h.captureLogs()

	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
		t.Fatal("ForceDeallocate must not write adapter state (Deallocate companion already does)")
		return nil
	}

	ev := h.vaultV2EventsABI.Events["ForceDeallocate"]
	// indexed(sender, onBehalf); non-indexed(adapter, assets, ids, penaltyAssets)
	log := h.makeV2VaultLog(ev, testVaultAddr,
		[]common.Hash{addrTopic(testCaller), addrTopic(testOnBehalf)},
		testAdapterAddr, big.NewInt(9000), hashSlice(common.HexToHash("0xaa")), big.NewInt(42))
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if !logs.hasWarnContaining("forceDeallocate") {
		t.Error("expected a WARN mentioning forceDeallocate")
	}
}

// --- cap changes ---

func TestProcessBlockEvent_CapChange(t *testing.T) {
	capID := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ab")
	idData := []byte{0x01, 0x02, 0x03, 0x04}

	tests := []struct {
		name        string
		event       string
		indexed     []common.Hash
		valueKey    string
		prior       *entity.MorphoVaultCap
		newValue    *big.Int
		wantAbsolus *big.Int
		wantRelativ *big.Int
	}{
		{
			name:        "IncreaseAbsoluteCap no prior row defaults relative to 0",
			event:       "IncreaseAbsoluteCap",
			indexed:     []common.Hash{capID},
			valueKey:    "newAbsoluteCap",
			prior:       nil,
			newValue:    big.NewInt(1_000_000),
			wantAbsolus: big.NewInt(1_000_000),
			wantRelativ: big.NewInt(0),
		},
		{
			name:        "IncreaseAbsoluteCap preserves prior relative",
			event:       "IncreaseAbsoluteCap",
			indexed:     []common.Hash{capID},
			valueKey:    "newAbsoluteCap",
			prior:       &entity.MorphoVaultCap{AbsoluteCap: big.NewInt(500), RelativeCap: big.NewInt(700)},
			newValue:    big.NewInt(1_000_000),
			wantAbsolus: big.NewInt(1_000_000),
			wantRelativ: big.NewInt(700),
		},
		{
			name:        "IncreaseRelativeCap preserves prior absolute",
			event:       "IncreaseRelativeCap",
			indexed:     []common.Hash{capID},
			valueKey:    "newRelativeCap",
			prior:       &entity.MorphoVaultCap{AbsoluteCap: big.NewInt(500), RelativeCap: big.NewInt(700)},
			newValue:    big.NewInt(900_000_000_000_000_000),
			wantAbsolus: big.NewInt(500),
			wantRelativ: big.NewInt(900_000_000_000_000_000),
		},
		{
			name:        "DecreaseAbsoluteCap (with sender) no prior row",
			event:       "DecreaseAbsoluteCap",
			indexed:     []common.Hash{addrTopic(testCaller), capID},
			valueKey:    "newAbsoluteCap",
			prior:       nil,
			newValue:    big.NewInt(250),
			wantAbsolus: big.NewInt(250),
			wantRelativ: big.NewInt(0),
		},
		{
			name:        "DecreaseRelativeCap (with sender) preserves prior absolute",
			event:       "DecreaseRelativeCap",
			indexed:     []common.Hash{addrTopic(testCaller), capID},
			valueKey:    "newRelativeCap",
			prior:       &entity.MorphoVaultCap{AbsoluteCap: big.NewInt(500), RelativeCap: big.NewInt(700)},
			newValue:    big.NewInt(123),
			wantAbsolus: big.NewInt(500),
			wantRelativ: big.NewInt(123),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

			var gotCapVaultID int64
			var gotCapID []byte
			h.morphoRepo.GetLatestVaultCapFn = func(_ context.Context, _ pgx.Tx, vaultID int64, id []byte) (*entity.MorphoVaultCap, error) {
				gotCapVaultID, gotCapID = vaultID, id
				return tt.prior, nil
			}
			var saved *entity.MorphoVaultCap
			h.morphoRepo.SaveVaultCapFn = func(_ context.Context, _ pgx.Tx, c *entity.MorphoVaultCap) error {
				saved = c
				return nil
			}

			ev := h.vaultV2EventsABI.Events[tt.event]
			log := h.makeV2VaultLog(ev, testVaultAddr, tt.indexed, idData, tt.newValue)
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if gotCapVaultID != 7 || !bytes.Equal(gotCapID, capID.Bytes()) {
				t.Errorf("GetLatestVaultCap(%d,%x), want (7,%s)", gotCapVaultID, gotCapID, capID.Hex())
			}
			if saved == nil {
				t.Fatal("SaveVaultCap not called")
			}
			if saved.MorphoVaultID != 7 {
				t.Errorf("MorphoVaultID = %d, want 7", saved.MorphoVaultID)
			}
			if !bytes.Equal(saved.CapID, capID.Bytes()) {
				t.Errorf("CapID = %x, want %s", saved.CapID, capID.Hex())
			}
			if !bytes.Equal(saved.IDData, idData) {
				t.Errorf("IDData = %x, want %x", saved.IDData, idData)
			}
			if saved.AbsoluteCap.Cmp(tt.wantAbsolus) != 0 {
				t.Errorf("AbsoluteCap = %s, want %s", saved.AbsoluteCap, tt.wantAbsolus)
			}
			if saved.RelativeCap.Cmp(tt.wantRelativ) != 0 {
				t.Errorf("RelativeCap = %s, want %s", saved.RelativeCap, tt.wantRelativ)
			}
			if saved.BlockNumber != 20000000 {
				t.Errorf("BlockNumber = %d, want 20000000", saved.BlockNumber)
			}
		})
	}
}

// --- fee updates ---

func TestProcessBlockEvent_FeeUpdates(t *testing.T) {
	fee := big.NewInt(100_000_000_000_000_000) // 0.1 WAD
	recipient := common.HexToAddress("0x5555555555555555555555555555555555555555")

	tests := []struct {
		name    string
		event   string
		indexed []common.Hash
		data    []any
		check   func(t *testing.T, u entity.MorphoVaultFeeUpdate)
	}{
		{
			name:  "SetPerformanceFee",
			event: "SetPerformanceFee",
			data:  []any{fee},
			check: func(t *testing.T, u entity.MorphoVaultFeeUpdate) {
				if u.PerformanceFee == nil || u.PerformanceFee.Cmp(fee) != 0 {
					t.Errorf("PerformanceFee = %v, want %s", u.PerformanceFee, fee)
				}
				if u.ManagementFee != nil || u.PerformanceFeeRecipient != nil || u.ManagementFeeRecipient != nil {
					t.Error("only PerformanceFee must be set")
				}
			},
		},
		{
			name:  "SetManagementFee",
			event: "SetManagementFee",
			data:  []any{fee},
			check: func(t *testing.T, u entity.MorphoVaultFeeUpdate) {
				if u.ManagementFee == nil || u.ManagementFee.Cmp(fee) != 0 {
					t.Errorf("ManagementFee = %v, want %s", u.ManagementFee, fee)
				}
				if u.PerformanceFee != nil || u.PerformanceFeeRecipient != nil || u.ManagementFeeRecipient != nil {
					t.Error("only ManagementFee must be set")
				}
			},
		},
		{
			name:    "SetPerformanceFeeRecipient",
			event:   "SetPerformanceFeeRecipient",
			indexed: []common.Hash{addrTopic(recipient)},
			check: func(t *testing.T, u entity.MorphoVaultFeeUpdate) {
				if !bytes.Equal(u.PerformanceFeeRecipient, recipient.Bytes()) {
					t.Errorf("PerformanceFeeRecipient = %x, want %s", u.PerformanceFeeRecipient, recipient.Hex())
				}
				if u.PerformanceFee != nil || u.ManagementFee != nil || u.ManagementFeeRecipient != nil {
					t.Error("only PerformanceFeeRecipient must be set")
				}
			},
		},
		{
			name:    "SetManagementFeeRecipient",
			event:   "SetManagementFeeRecipient",
			indexed: []common.Hash{addrTopic(recipient)},
			check: func(t *testing.T, u entity.MorphoVaultFeeUpdate) {
				if !bytes.Equal(u.ManagementFeeRecipient, recipient.Bytes()) {
					t.Errorf("ManagementFeeRecipient = %x, want %s", u.ManagementFeeRecipient, recipient.Hex())
				}
				if u.PerformanceFee != nil || u.ManagementFee != nil || u.PerformanceFeeRecipient != nil {
					t.Error("only ManagementFeeRecipient must be set")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

			var gotVaultID int64
			var gotUpdate entity.MorphoVaultFeeUpdate
			called := false
			h.morphoRepo.UpdateVaultFeeConfigFn = func(_ context.Context, _ pgx.Tx, vaultID int64, u entity.MorphoVaultFeeUpdate) error {
				called = true
				gotVaultID, gotUpdate = vaultID, u
				return nil
			}

			ev := h.vaultV2EventsABI.Events[tt.event]
			log := h.makeV2VaultLog(ev, testVaultAddr, tt.indexed, tt.data...)
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if !called {
				t.Fatal("UpdateVaultFeeConfig not called")
			}
			if gotVaultID != 7 {
				t.Errorf("vaultID = %d, want 7", gotVaultID)
			}
			tt.check(t, gotUpdate)
		})
	}
}

// TestProcessBlockEvent_V2Handlers_ErrorsPropagate verifies each structured V2
// handler fails the whole event (so SQS redelivers) rather than swallowing a
// transient dependency failure into partial success. One row per handler's
// distinct failing dependency.
func TestProcessBlockEvent_V2Handlers_ErrorsPropagate(t *testing.T) {
	adapterIdx := []common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)}
	capID := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ab")

	tests := []struct {
		name  string
		setup func(h *serviceTestHarness) shared.Log
	}{
		{
			name: "AddAdapter: adapter probe RPC error",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("probe rpc down")
				}
				h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapter) (int64, error) {
					t.Fatal("adapter must not be persisted when the probe fails")
					return 0, nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			},
		},
		{
			name: "Allocation: realAssets RPC error",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return nil, errors.New("realAssets rpc down")
				}
				h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
					t.Fatal("adapter state must not be persisted when realAssets fails")
					return nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr, adapterIdx, big.NewInt(1), hashSlice(common.HexToHash("0xaa")), big.NewInt(1))
			},
		},
		{
			name: "Allocation: GetActiveAdapter DB error",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
				}
				h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ int64, _ []byte) (*entity.MorphoAdapter, error) {
					return nil, errors.New("db down")
				}
				h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
					t.Fatal("adapter state must not be persisted on a DB lookup error")
					return nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr, adapterIdx, big.NewInt(1), hashSlice(common.HexToHash("0xaa")), big.NewInt(1))
			},
		},
		{
			name: "CapChange: GetLatestVaultCap DB error",
			setup: func(h *serviceTestHarness) shared.Log {
				h.morphoRepo.GetLatestVaultCapFn = func(_ context.Context, _ pgx.Tx, _ int64, _ []byte) (*entity.MorphoVaultCap, error) {
					return nil, errors.New("db down")
				}
				h.morphoRepo.SaveVaultCapFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultCap) error {
					t.Fatal("cap must not be persisted on a lookup error")
					return nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["IncreaseAbsoluteCap"], testVaultAddr, []common.Hash{capID}, []byte{0x01}, big.NewInt(1))
			},
		},
		{
			name: "Fee: UpdateVaultFeeConfig DB error",
			setup: func(h *serviceTestHarness) shared.Log {
				h.morphoRepo.UpdateVaultFeeConfigFn = func(_ context.Context, _ pgx.Tx, _ int64, _ entity.MorphoVaultFeeUpdate) error {
					return errors.New("db down")
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["SetPerformanceFee"], testVaultAddr, nil, big.NewInt(1))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			log := tt.setup(h)
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
				t.Fatal("expected the block to fail so SQS redelivers")
			}
		})
	}
}

// TestProcessBlockEvent_RemoveAdapter_NonV2VaultErrors exercises resolveV2Vault's
// version guard through a handler other than AddAdapter.
func TestProcessBlockEvent_RemoveAdapter_NonV2VaultErrors(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)
	h.morphoRepo.MarkAdapterRemovedFn = func(_ context.Context, _ pgx.Tx, _ int64, _ []byte, _ int64) error {
		t.Fatal("MarkAdapterRemoved must not run for a non-V2 vault")
		return nil
	}
	log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["RemoveAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
		t.Fatal("expected error for VaultV2 event on non-V2 vault")
	}
}
