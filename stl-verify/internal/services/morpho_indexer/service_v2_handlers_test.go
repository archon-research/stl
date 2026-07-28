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
	"github.com/ethereum/go-ethereum/crypto"
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

// --- transaction / probe observer ---

// txProbeObserver watches whether the adapter type probe (a chain RPC round-trip)
// ever runs while a DB transaction is open — the pool-pressure hazard the fix
// removes. A handler legitimately opens a short audit-log transaction that closes
// before the probe, so the invariant is not "probe before any transaction" but
// "probe never runs while a transaction is open".
type txProbeObserver struct {
	mu             sync.Mutex
	openTx         int
	probeCount     int
	probedInsideTx bool
}

func (o *txProbeObserver) enterTx() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.openTx++
}

func (o *txProbeObserver) exitTx() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.openTx--
}

func (o *txProbeObserver) recordProbe() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.probeCount++
	if o.openTx > 0 {
		o.probedInsideTx = true
	}
}

func (o *txProbeObserver) requireProbedOutsideTx(t *testing.T) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.probeCount != 1 {
		t.Fatalf("adapter probe count = %d, want exactly 1 (so the outside-tx check is meaningful)", o.probeCount)
	}
	if o.probedInsideTx {
		t.Error("adapter type probe ran while a DB transaction was open; it must complete before the write transaction opens")
	}
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
			h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				if len(calls) == 1 && calls[0].Target == testAdapterAddr {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
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

// TestProcessBlockEvent_AddAdapter_SeedsAdapterState pins the composition-
// completeness guarantee at the moment of registration: an AddAdapter must leave
// behind BOTH the registry row and a realAssets snapshot for it, seeded from a
// hash-pinned read of the same block. Registering without a state row leaves the
// adapter looking like adapter_data_missing to VEC-219's composition probe for as
// long as the vault stays quiet — sparkUSDTbc went 5,517 blocks (~18h) between its
// AddAdapter and its first Allocate.
func TestProcessBlockEvent_AddAdapter_SeedsAdapterState(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	realAssets := big.NewInt(41_300_000)
	var gotHash common.Hash
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 && calls[0].Target == testAdapterAddr {
			return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
		}
		return nil, errTestUnexpectedCall(calls)
	}
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		if len(calls) == 1 && calls[0].Target == testAdapterAddr {
			gotHash = blockHash
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(realAssets)}}, nil
		}
		return nil, errTestUnexpectedCall(calls)
	}
	h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapter) (int64, error) {
		return 42, nil
	}
	var savedState *entity.MorphoAdapterState
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoAdapterState) error {
		savedState = s
		return nil
	}

	ev := h.vaultV2EventsABI.Events["AddAdapter"]
	log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
	if err := h.processBlock(t, 1, 20000000, 3, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedState == nil {
		t.Fatal("AddAdapter must seed an adapter_state row for the freshly registered adapter")
	}
	if gotHash != testBlockHash {
		t.Errorf("realAssets seed pinned to %s, want %s", gotHash, testBlockHash)
	}
	if savedState.MorphoAdapterID != 42 {
		t.Errorf("MorphoAdapterID = %d, want 42 (the row GetOrCreateAdapter returned)", savedState.MorphoAdapterID)
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

// TestProcessBlockEvent_AdapterProbeRunsBeforeTransaction pins the pool-pressure
// fix: an adapter's on-chain type probe (a chain RPC round-trip) must complete
// BEFORE the write transaction opens, so no pooled DB connection is held idle
// across the probe. Covers both probe-bearing paths — the live AddAdapter handler
// (always classifies) and the Allocate lazy self-heal (classifies only when the
// adapter is unregistered).
func TestProcessBlockEvent_AdapterProbeRunsBeforeTransaction(t *testing.T) {
	tests := []struct {
		name  string
		setup func(h *serviceTestHarness) shared.Log
	}{
		{
			name: "AddAdapter classifies before opening the transaction",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			},
		},
		{
			name: "Allocate lazy-register classifies before opening the transaction",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				// Adapter predates discovery, so the pre-transaction membership check
				// misses and the heal path probes before opening the write tx.
				h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ pgx.Tx, _ int64, _ []byte) (*entity.MorphoAdapter, error) {
					return nil, nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr,
					[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
					big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

			obs := &txProbeObserver{}
			h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) == 2 && calls[0].Target == testAdapterAddr {
					obs.recordProbe()
					return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
				}
				return nil, errTestUnexpectedCall(calls)
			}
			h.txManager.WithTransactionFn = func(_ context.Context, fn func(tx pgx.Tx) error) error {
				obs.enterTx()
				defer obs.exitTx()
				return fn(nil)
			}

			log := tt.setup(h)
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}
			obs.requireProbedOutsideTx(t)
		})
	}
}

// --- RemoveAdapter ---

func TestProcessBlockEvent_RemoveAdapter(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	// Known adapter: the pre-transaction membership check finds it (so no probe
	// fires) and the in-tx GetActiveAdapter returns the active row, so removal
	// proceeds directly (no lazy registration).
	h.morphoRepo.GetActiveAdaptersByVaultFn = func(_ context.Context, _ int64) ([]*entity.MorphoAdapter, error) {
		return []*entity.MorphoAdapter{{ID: 55, MorphoVaultID: 7, Address: testAdapterAddr.Bytes(), AssetTokenID: 1, AdapterType: entity.MorphoAdapterTypeMarketV1, AddedAtBlock: 19000000}}, nil
	}
	h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ pgx.Tx, vaultID int64, address []byte) (*entity.MorphoAdapter, error) {
		return &entity.MorphoAdapter{ID: 55, MorphoVaultID: vaultID, Address: address, AssetTokenID: 1, AdapterType: entity.MorphoAdapterTypeMarketV1, AddedAtBlock: 19000000}, nil
	}

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

			// Known adapter: the pre-transaction membership check finds it, so no
			// classification probe fires; the in-tx GetActiveAdapter is the decisive
			// read that yields the id for the state snapshot.
			h.morphoRepo.GetActiveAdaptersByVaultFn = func(_ context.Context, _ int64) ([]*entity.MorphoAdapter, error) {
				return []*entity.MorphoAdapter{{ID: 55, MorphoVaultID: 7, Address: testAdapterAddr.Bytes(), AssetTokenID: 1, AdapterType: entity.MorphoAdapterTypeMarketV1, AddedAtBlock: 19000000}}, nil
			}
			var (
				gotVaultID int64
				gotAddr    []byte
			)
			h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ pgx.Tx, vaultID int64, address []byte) (*entity.MorphoAdapter, error) {
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

// TestProcessBlockEvent_Allocation_UnknownAdapterHeals verifies the self-heal:
// an Allocate/Deallocate for an adapter we never saw AddAdapter for (it predates
// the vault's discovery) must NOT hard-fail the event and poison the FIFO queue.
// Instead the adapter is classified on-chain, registered at the event block, and
// its state row saved — behind a "registered lazily" WARN. The adapter address
// comes from the vault's own event and is verified by the probe, so this is a
// heal, not a phantom write.
func TestProcessBlockEvent_Allocation_UnknownAdapterHeals(t *testing.T) {
	tests := []struct {
		name            string
		adapterType     entity.MorphoAdapterType
		wantUnknownWarn bool
	}{
		{"MarketV1 probe", entity.MorphoAdapterTypeMarketV1, false},
		{"both-revert probe records Unknown", entity.MorphoAdapterTypeUnknown, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			logs := h.captureLogs()

			realAssets := big.NewInt(41_300_000)
			h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				if len(calls) == 1 && calls[0].Target == testAdapterAddr {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(realAssets)}}, nil
				}
				return nil, errTestUnexpectedCall(calls)
			}
			// getAdapterType classification (2 number-pinned calls to the adapter).
			h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) == 2 && calls[0].Target == testAdapterAddr {
					return h.adapterProbeResults(tt.adapterType), nil
				}
				return nil, errTestUnexpectedCall(calls)
			}

			h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ pgx.Tx, _ int64, _ []byte) (*entity.MorphoAdapter, error) {
				return nil, nil // adapter predates discovery — never AddAdapter'd
			}
			var registered *entity.MorphoAdapter
			h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, a *entity.MorphoAdapter) (int64, error) {
				registered = a
				return 77, nil
			}
			var savedState *entity.MorphoAdapterState
			h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoAdapterState) error {
				savedState = s
				return nil
			}

			ev := h.vaultV2EventsABI.Events["Allocate"]
			log := h.makeV2VaultLog(ev, testVaultAddr,
				[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
				big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock must self-heal, not fail: %v", err)
			}

			if registered == nil {
				t.Fatal("adapter was not lazily registered")
			}
			if registered.MorphoVaultID != 7 {
				t.Errorf("MorphoVaultID = %d, want 7", registered.MorphoVaultID)
			}
			if !bytes.Equal(registered.Address, testAdapterAddr.Bytes()) {
				t.Errorf("Address = %x, want %s", registered.Address, testAdapterAddr.Hex())
			}
			if registered.AdapterType != tt.adapterType {
				t.Errorf("AdapterType = %d, want %d (probed on-chain)", registered.AdapterType, tt.adapterType)
			}
			if registered.AddedAtBlock != 20000000 {
				t.Errorf("AddedAtBlock = %d, want 20000000 (event block)", registered.AddedAtBlock)
			}
			if registered.AssetTokenID != 1 {
				t.Errorf("AssetTokenID = %d, want 1 (vault asset)", registered.AssetTokenID)
			}
			if savedState == nil {
				t.Fatal("adapter state not saved after heal")
			}
			if savedState.MorphoAdapterID != 77 {
				t.Errorf("MorphoAdapterID = %d, want 77 (the lazily-registered row)", savedState.MorphoAdapterID)
			}
			if savedState.RealAssets.Cmp(realAssets) != 0 {
				t.Errorf("RealAssets = %s, want %s", savedState.RealAssets, realAssets)
			}
			if !logs.hasWarnContaining("registered lazily") {
				t.Error("expected a WARN that the adapter was registered lazily")
			}
			if got := logs.hasWarnContaining("unknown type"); got != tt.wantUnknownWarn {
				t.Errorf("WARN(unknown type) = %v, want %v", got, tt.wantUnknownWarn)
			}
		})
	}
}

// TestProcessBlockEvent_RemoveAdapter_UnknownAdapterHeals verifies a RemoveAdapter
// for an adapter that predates discovery lazily registers an audit-consistent row
// (probed type, event block) and then closes it in the same transaction, rather
// than failing MarkAdapterRemoved with a 0-rows error and poisoning the queue.
func TestProcessBlockEvent_RemoveAdapter_UnknownAdapterHeals(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
	logs := h.captureLogs()

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 && calls[0].Target == testAdapterAddr {
			return h.adapterProbeResults(entity.MorphoAdapterTypeVaultV1), nil
		}
		return nil, errTestUnexpectedCall(calls)
	}
	h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ pgx.Tx, _ int64, _ []byte) (*entity.MorphoAdapter, error) {
		return nil, nil // unknown adapter
	}
	var registered *entity.MorphoAdapter
	h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, a *entity.MorphoAdapter) (int64, error) {
		registered = a
		return 88, nil
	}
	var (
		removedVaultID int64
		removedAddr    []byte
		removedBlock   int64
		marked         bool
	)
	h.morphoRepo.MarkAdapterRemovedFn = func(_ context.Context, _ pgx.Tx, vaultID int64, address []byte, removedAtBlock int64) error {
		marked = true
		removedVaultID, removedAddr, removedBlock = vaultID, address, removedAtBlock
		return nil
	}

	ev := h.vaultV2EventsABI.Events["RemoveAdapter"]
	log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock must self-heal, not fail: %v", err)
	}

	if registered == nil {
		t.Fatal("unknown adapter was not lazily registered before removal")
	}
	if registered.AddedAtBlock != 20000000 {
		t.Errorf("AddedAtBlock = %d, want 20000000 (event block)", registered.AddedAtBlock)
	}
	if registered.AdapterType != entity.MorphoAdapterTypeVaultV1 {
		t.Errorf("AdapterType = %d, want VaultV1 (probed)", registered.AdapterType)
	}
	if !marked {
		t.Fatal("MarkAdapterRemoved not called after lazy registration")
	}
	if removedVaultID != 7 || !bytes.Equal(removedAddr, testAdapterAddr.Bytes()) || removedBlock != 20000000 {
		t.Errorf("MarkAdapterRemoved(%d,%x,%d), want (7,%s,20000000)", removedVaultID, removedAddr, removedBlock, testAdapterAddr.Hex())
	}
	if !logs.hasWarnContaining("registered lazily") {
		t.Error("expected a WARN that the adapter was registered lazily")
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

// maxUint128 is the on-chain "unlimited" absolute cap sentinel (2^128 - 1); it
// also exercises the full uint128 width of the on-chain read.
var maxUint128 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

// TestProcessBlockEvent_CapChange verifies that any of the 4 cap events snapshots
// the vault's FULL current cap state — read on-chain (absoluteCap, relativeCap)
// pinned to the event's block hash — rather than carrying a value forward. The
// event's own value is irrelevant to what is persisted; the on-chain pair is
// authoritative.
func TestProcessBlockEvent_CapChange(t *testing.T) {
	idData := []byte{0x01, 0x02, 0x03, 0x04}
	// capID must equal keccak256(idData): the entity enforces it, mirroring how
	// the contract derives the id.
	capID := crypto.Keccak256Hash(idData)

	tests := []struct {
		name     string
		event    string
		indexed  []common.Hash
		absolute *big.Int
		relative *big.Int
	}{
		{
			name:     "IncreaseAbsoluteCap",
			event:    "IncreaseAbsoluteCap",
			indexed:  []common.Hash{capID},
			absolute: big.NewInt(1_000_000),
			relative: big.NewInt(1_000_000_000_000_000_000),
		},
		{
			name:     "IncreaseRelativeCap",
			event:    "IncreaseRelativeCap",
			indexed:  []common.Hash{capID},
			absolute: maxUint128,
			relative: big.NewInt(500_000_000_000_000_000),
		},
		{
			name:     "DecreaseAbsoluteCap (with sender)",
			event:    "DecreaseAbsoluteCap",
			indexed:  []common.Hash{addrTopic(testCaller), capID},
			absolute: big.NewInt(250),
			relative: big.NewInt(0),
		},
		{
			name:     "DecreaseRelativeCap (with sender)",
			event:    "DecreaseRelativeCap",
			indexed:  []common.Hash{addrTopic(testCaller), capID},
			absolute: big.NewInt(500),
			relative: big.NewInt(123),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

			var gotHash common.Hash
			viaHash := false
			h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
				if len(calls) != 2 || calls[0].Target != testVaultAddr || calls[1].Target != testVaultAddr ||
					calls[0].AllowFailure || calls[1].AllowFailure {
					return nil, errTestUnexpectedCall(calls)
				}
				viaHash = true
				gotHash = blockHash
				return []outbound.Result{
					{Success: true, ReturnData: h.packUint256(tt.absolute)},
					{Success: true, ReturnData: h.packUint256(tt.relative)},
				}, nil
			}

			var saved *entity.MorphoVaultCap
			h.morphoRepo.SaveVaultCapFn = func(_ context.Context, _ pgx.Tx, c *entity.MorphoVaultCap) error {
				saved = c
				return nil
			}

			ev := h.vaultV2EventsABI.Events[tt.event]
			// The non-indexed (idData, newValue) payload is what the log carries;
			// newValue is deliberately NOT what gets persisted.
			log := h.makeV2VaultLog(ev, testVaultAddr, tt.indexed, idData, big.NewInt(999))
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if !viaHash {
				t.Fatal("caps must be read via ExecuteAtHash (state read)")
			}
			if gotHash != testBlockHash {
				t.Errorf("caps pinned to %s, want %s", gotHash, testBlockHash)
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
			if saved.AbsoluteCap.Cmp(tt.absolute) != 0 {
				t.Errorf("AbsoluteCap = %s, want %s (on-chain read, not the event value)", saved.AbsoluteCap, tt.absolute)
			}
			if saved.RelativeCap.Cmp(tt.relative) != 0 {
				t.Errorf("RelativeCap = %s, want %s (on-chain read, not the event value)", saved.RelativeCap, tt.relative)
			}
			if saved.BlockNumber != 20000000 {
				t.Errorf("BlockNumber = %d, want 20000000", saved.BlockNumber)
			}
		})
	}
}

// TestProcessBlockEvent_CapChange_ReadErrors verifies the cap snapshot aborts the
// event when the on-chain read fails — both a transport error and a Success=false
// sub-result — rather than persisting a partial/defaulted row.
func TestProcessBlockEvent_CapChange_ReadErrors(t *testing.T) {
	idData := []byte{0x01, 0x02, 0x03, 0x04}
	capID := crypto.Keccak256Hash(idData)

	tests := []struct {
		name    string
		execute func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error)
	}{
		{
			name: "transport error",
			execute: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				return nil, errors.New("cap rpc down")
			},
		},
		{
			name: "Success=false sub-result",
			execute: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				return []outbound.Result{
					{Success: false, ReturnData: nil},
					{Success: true, ReturnData: nil},
				}, nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			h.multicaller.ExecuteAtHashFn = tt.execute
			h.morphoRepo.SaveVaultCapFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultCap) error {
				t.Fatal("cap must not be persisted when the on-chain read fails")
				return nil
			}
			log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["IncreaseAbsoluteCap"], testVaultAddr, []common.Hash{capID}, idData, big.NewInt(1))
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
				t.Fatal("expected the block to fail so SQS redelivers")
			}
		})
	}
}

// --- fee changes ---

// feeGetterResults returns the 4-call getVaultFees response in the exact order
// getVaultFees packs: performanceFee, managementFee, performanceFeeRecipient,
// managementFeeRecipient.
func (h *serviceTestHarness) feeGetterResults(perfFee, mgmtFee *big.Int, perfRecip, mgmtRecip common.Address) []outbound.Result {
	return []outbound.Result{
		{Success: true, ReturnData: h.packUint256(perfFee)},
		{Success: true, ReturnData: h.packUint256(mgmtFee)},
		{Success: true, ReturnData: h.packAddress(perfRecip)},
		{Success: true, ReturnData: h.packAddress(mgmtRecip)},
	}
}

// TestProcessBlockEvent_FeeChange verifies that any of the 4 Set* fee events
// snapshots the vault's FULL current fee config — read on-chain (performanceFee,
// managementFee, and both recipients) pinned to the event's block hash — rather
// than persisting the single field the event carried. The event's own value is
// deliberately different from the on-chain read; the on-chain config is
// authoritative.
func TestProcessBlockEvent_FeeChange(t *testing.T) {
	// The authoritative on-chain fee config, identical across all 4 events.
	perfFee := big.NewInt(100_000_000_000_000_000) // 0.1 WAD
	mgmtFee := big.NewInt(3170979198)              // a WAD per-second rate
	perfRecip := common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E")
	mgmtRecip := common.Address{} // zero-address recipient is the contract default

	// A value carried on the event that must NOT be what gets persisted.
	eventFee := big.NewInt(999)
	eventRecip := common.HexToAddress("0x5555555555555555555555555555555555555555")

	tests := []struct {
		name    string
		event   string
		indexed []common.Hash
		data    []any
	}{
		{name: "SetPerformanceFee", event: "SetPerformanceFee", data: []any{eventFee}},
		{name: "SetManagementFee", event: "SetManagementFee", data: []any{eventFee}},
		{name: "SetPerformanceFeeRecipient", event: "SetPerformanceFeeRecipient", indexed: []common.Hash{addrTopic(eventRecip)}},
		{name: "SetManagementFeeRecipient", event: "SetManagementFeeRecipient", indexed: []common.Hash{addrTopic(eventRecip)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

			var gotHash common.Hash
			viaHash := false
			h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
				if len(calls) != 4 {
					return nil, errTestUnexpectedCall(calls)
				}
				for _, c := range calls {
					if c.Target != testVaultAddr || c.AllowFailure {
						return nil, errTestUnexpectedCall(calls)
					}
				}
				viaHash = true
				gotHash = blockHash
				return h.feeGetterResults(perfFee, mgmtFee, perfRecip, mgmtRecip), nil
			}

			var saved *entity.MorphoVaultFee
			h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, f *entity.MorphoVaultFee) error {
				saved = f
				return nil
			}

			ev := h.vaultV2EventsABI.Events[tt.event]
			log := h.makeV2VaultLog(ev, testVaultAddr, tt.indexed, tt.data...)
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if !viaHash {
				t.Fatal("fees must be read via ExecuteAtHash (state read)")
			}
			if gotHash != testBlockHash {
				t.Errorf("fees pinned to %s, want %s", gotHash, testBlockHash)
			}
			if saved == nil {
				t.Fatal("SaveVaultFee not called")
			}
			if saved.MorphoVaultID != 7 {
				t.Errorf("MorphoVaultID = %d, want 7", saved.MorphoVaultID)
			}
			if saved.PerformanceFee.Cmp(perfFee) != 0 {
				t.Errorf("PerformanceFee = %s, want %s (on-chain read, not the event value)", saved.PerformanceFee, perfFee)
			}
			if saved.ManagementFee.Cmp(mgmtFee) != 0 {
				t.Errorf("ManagementFee = %s, want %s (on-chain read, not the event value)", saved.ManagementFee, mgmtFee)
			}
			if !bytes.Equal(saved.PerformanceFeeRecipient, perfRecip.Bytes()) {
				t.Errorf("PerformanceFeeRecipient = %x, want %s (on-chain read)", saved.PerformanceFeeRecipient, perfRecip.Hex())
			}
			if !bytes.Equal(saved.ManagementFeeRecipient, mgmtRecip.Bytes()) {
				t.Errorf("ManagementFeeRecipient = %x, want %s (on-chain read)", saved.ManagementFeeRecipient, mgmtRecip.Hex())
			}
			if saved.BlockNumber != 20000000 {
				t.Errorf("BlockNumber = %d, want 20000000", saved.BlockNumber)
			}
			if saved.BlockVersion != 0 {
				t.Errorf("BlockVersion = %d, want 0", saved.BlockVersion)
			}
		})
	}
}

// TestProcessBlockEvent_FeeChange_ReadErrors verifies the fee snapshot aborts the
// event when the on-chain read fails — both a transport error and a Success=false
// sub-result — rather than persisting a partial/defaulted row.
func TestProcessBlockEvent_FeeChange_ReadErrors(t *testing.T) {
	tests := []struct {
		name    string
		execute func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error)
	}{
		{
			name: "transport error",
			execute: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				return nil, errors.New("fee rpc down")
			},
		},
		{
			name: "Success=false sub-result",
			execute: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				return []outbound.Result{
					{Success: true, ReturnData: nil},
					{Success: false, ReturnData: nil},
					{Success: true, ReturnData: nil},
					{Success: true, ReturnData: nil},
				}, nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			h.multicaller.ExecuteAtHashFn = tt.execute
			h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultFee) error {
				t.Fatal("fee must not be persisted when the on-chain read fails")
				return nil
			}
			log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["SetPerformanceFee"], testVaultAddr, nil, big.NewInt(1))
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
				t.Fatal("expected the block to fail so SQS redelivers")
			}
		})
	}
}

// TestProcessBlockEvent_FeeChange_SameBlockSnapshotsIdentical verifies the
// snapshot dedup contract at the handler level: two different fee events in the
// same block (SetPerformanceFee + SetPerformanceFeeRecipient) each read the same
// on-chain config at the same block hash and build byte-identical MorphoVaultFee
// snapshots — so the mvf trigger + ON CONFLICT collapse them to one row.
func TestProcessBlockEvent_FeeChange_SameBlockSnapshotsIdentical(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	perfFee := big.NewInt(100_000_000_000_000_000)
	mgmtFee := big.NewInt(0)
	perfRecip := common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E")
	mgmtRecip := common.Address{}

	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) != 4 {
			return nil, errTestUnexpectedCall(calls)
		}
		return h.feeGetterResults(perfFee, mgmtFee, perfRecip, mgmtRecip), nil
	}

	var saved []*entity.MorphoVaultFee
	h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, f *entity.MorphoVaultFee) error {
		saved = append(saved, f)
		return nil
	}

	perfLog := h.makeV2VaultLog(h.vaultV2EventsABI.Events["SetPerformanceFee"], testVaultAddr, nil, big.NewInt(1))
	recipLog := h.makeV2VaultLog(h.vaultV2EventsABI.Events["SetPerformanceFeeRecipient"], testVaultAddr, []common.Hash{addrTopic(perfRecip)})
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, perfLog, recipLog)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if len(saved) != 2 {
		t.Fatalf("expected SaveVaultFee called twice (once per event), got %d", len(saved))
	}
	a, b := saved[0], saved[1]
	if a.PerformanceFee.Cmp(b.PerformanceFee) != 0 || a.ManagementFee.Cmp(b.ManagementFee) != 0 ||
		!bytes.Equal(a.PerformanceFeeRecipient, b.PerformanceFeeRecipient) ||
		!bytes.Equal(a.ManagementFeeRecipient, b.ManagementFeeRecipient) ||
		a.BlockNumber != b.BlockNumber || a.BlockVersion != b.BlockVersion || !a.Timestamp.Equal(b.Timestamp) {
		t.Errorf("same-block fee events produced differing snapshots:\n  %+v\n  %+v", a, b)
	}
}

// TestProcessBlockEvent_V2Handlers_ErrorsPropagate verifies each structured V2
// handler fails the whole event (so SQS redelivers) rather than swallowing a
// transient dependency failure into partial success. One row per handler's
// distinct failing dependency.
func TestProcessBlockEvent_V2Handlers_ErrorsPropagate(t *testing.T) {
	adapterIdx := []common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)}

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
				// Adapter is already registered (pre-tx check finds it), so the
				// failure under test is the decisive in-tx GetActiveAdapter read.
				h.morphoRepo.GetActiveAdaptersByVaultFn = func(_ context.Context, _ int64) ([]*entity.MorphoAdapter, error) {
					return []*entity.MorphoAdapter{{ID: 55, MorphoVaultID: 7, Address: testAdapterAddr.Bytes(), AssetTokenID: 1, AdapterType: entity.MorphoAdapterTypeMarketV1, AddedAtBlock: 19000000}}, nil
				}
				h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ pgx.Tx, _ int64, _ []byte) (*entity.MorphoAdapter, error) {
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
			// The heal path must still fail the event on a TRANSPORT probe error:
			// a momentarily-unreachable adapter is transient and must retry, never
			// be recorded with a defaulted type.
			name: "Allocation: lazy-register probe transport error",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
				}
				h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("adapter probe rpc down")
				}
				h.morphoRepo.GetActiveAdapterFn = func(_ context.Context, _ pgx.Tx, _ int64, _ []byte) (*entity.MorphoAdapter, error) {
					return nil, nil // unknown adapter → heal path fires, then the probe fails
				}
				h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapter) (int64, error) {
					t.Fatal("adapter must not be persisted when the classification probe fails")
					return 0, nil
				}
				h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
					t.Fatal("adapter state must not be persisted when the probe fails")
					return nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr, adapterIdx, big.NewInt(1), hashSlice(common.HexToHash("0xaa")), big.NewInt(1))
			},
		},
		{
			name: "CapChange: SaveVaultCap DB error",
			setup: func(h *serviceTestHarness) shared.Log {
				capIDData := []byte{0x01}
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: h.packUint256(big.NewInt(1))},
						{Success: true, ReturnData: h.packUint256(big.NewInt(1))},
					}, nil
				}
				h.morphoRepo.SaveVaultCapFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultCap) error {
					return errors.New("db down")
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["IncreaseAbsoluteCap"], testVaultAddr, []common.Hash{crypto.Keccak256Hash(capIDData)}, capIDData, big.NewInt(1))
			},
		},
		{
			name: "Fee: SaveVaultFee DB error",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return h.feeGetterResults(big.NewInt(1), big.NewInt(0), common.Address{}, common.Address{}), nil
				}
				h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultFee) error {
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
