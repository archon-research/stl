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

// testAdapterMember is the answer a pre-transaction membership read gives for an
// adapter that is already in the vault's set at the position being processed.
func testAdapterMember() *entity.MorphoAdapterMember {
	return &entity.MorphoAdapterMember{
		MorphoAdapterIdentity: entity.MorphoAdapterIdentity{
			ID: 55, MorphoVaultID: 7, Address: testAdapterAddr.Bytes(), AssetTokenID: 1,
		},
		AdapterType: entity.MorphoAdapterTypeMarketV1,
		AsOfBlock:   19000000,
		ObservedVia: entity.MembershipFromAddAdapter,
	}
}

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

			var saved *entity.MorphoAdapterObservation
			h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
				saved = obs
				return 42, true, nil
			}

			ev := h.vaultV2EventsABI.Events["AddAdapter"]
			log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if saved == nil {
				t.Fatal("ObserveAdapterMembership not called")
			}
			if saved.Identity.MorphoVaultID != 7 {
				t.Errorf("MorphoVaultID = %d, want 7", saved.Identity.MorphoVaultID)
			}
			if !bytes.Equal(saved.Identity.Address, testAdapterAddr.Bytes()) {
				t.Errorf("Address = %x, want %s", saved.Identity.Address, testAdapterAddr.Hex())
			}
			if saved.Identity.AssetTokenID != 1 {
				t.Errorf("AssetTokenID = %d, want 1 (vault asset)", saved.Identity.AssetTokenID)
			}
			if got := saved.Membership.AdapterType; got == nil || *got != tt.adapterType {
				t.Errorf("AdapterType = %v, want %d", got, tt.adapterType)
			}
			if saved.Membership.BlockNumber != 20000000 {
				t.Errorf("BlockNumber = %d, want 20000000", saved.Membership.BlockNumber)
			}
			if !saved.Membership.IsMember {
				t.Error("an AddAdapter records membership")
			}
			if saved.Membership.ObservedVia != entity.MembershipFromAddAdapter {
				t.Errorf("ObservedVia = %q, want add_adapter_event", saved.Membership.ObservedVia)
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
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
		return 42, true, nil
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
		t.Errorf("MorphoAdapterID = %d, want 42 (the id ObserveAdapterMembership returned)", savedState.MorphoAdapterID)
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

// TestProcessBlockEvent_AddAdapter_RealAssetsSeedTolerance pins which adapters may
// be registered without a realAssets() seed.
//
// The Unknown sentinel exists to record an adapter kind we do not model behind a
// WARN rather than drop it. Hard-requiring realAssets() for such an adapter defeated
// that: an unmodelled adapter that does not serve the getter poison-pilled the block
// forever. setIsAdapter never calls realAssets(), so an added adapter genuinely need
// not serve it — while for a MODELLED kind the vault itself calls it while
// allocating, so a revert there is drift. A multicall TRANSPORT error is transient
// and must fail the block for every type.
func TestProcessBlockEvent_AddAdapter_RealAssetsSeedTolerance(t *testing.T) {
	tests := []struct {
		name         string
		adapterType  entity.MorphoAdapterType
		reverts      bool
		transportErr bool
		wantErr      bool
		wantSeeded   bool
		wantWarn     bool
	}{
		{
			name: "a modelled adapter is seeded", adapterType: entity.MorphoAdapterTypeMarketV1,
			wantSeeded: true,
		},
		{
			name: "an unclassified adapter that serves realAssets is seeded too", adapterType: entity.MorphoAdapterTypeUnknown,
			wantSeeded: true,
		},
		{
			name:        "an unclassified adapter that reverts is registered without a seed",
			adapterType: entity.MorphoAdapterTypeUnknown, reverts: true, wantWarn: true,
		},
		{
			name:        "a modelled adapter that reverts is drift and fails the block",
			adapterType: entity.MorphoAdapterTypeMarketV1, reverts: true, wantErr: true,
		},
		{
			name:        "a transport failure fails the block even for an unclassified adapter",
			adapterType: entity.MorphoAdapterTypeUnknown, transportErr: true, wantErr: true,
		},
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
				if len(calls) != 1 || calls[0].Target != testAdapterAddr {
					return nil, errTestUnexpectedCall(calls)
				}
				switch {
				case tt.transportErr:
					return nil, errors.New("rpc down")
				case tt.reverts:
					return []outbound.Result{{Success: false, ReturnData: nil}}, nil
				default:
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
				}
			}

			registered := false
			h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
				registered = true
				return 42, true, nil
			}
			seeded := false
			h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
				seeded = true
				return nil
			}

			log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)})
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected the block to fail so SQS redelivers")
				}
				if registered {
					t.Error("no adapter may be registered when the seed read fails hard")
				}
				return
			}
			if err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if !registered {
				t.Error("the adapter must be registered even when it serves no realAssets()")
			}
			if seeded != tt.wantSeeded {
				t.Errorf("adapter state seeded = %v, want %v", seeded, tt.wantSeeded)
			}
			if got := logs.hasWarnContaining("does not serve realAssets()"); got != tt.wantWarn {
				t.Errorf("WARN(does not serve realAssets()) = %v, want %v", got, tt.wantWarn)
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
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
		t.Fatal("no membership may be recorded for a non-V2 vault")
		return 0, false, nil
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
				// Adapter predates discovery, so the pre-transaction membership read
				// misses and the assertion path probes before opening the write tx.
				h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
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

	// The removal path makes no decision, so it reads nothing and probes nothing.
	// Both guards are the point of the test: the type probe and the membership read
	// existed only to classify the heal row a removal used to have to register.
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("a removal must not probe the adapter type: an observation of NON-membership carries none")
		return nil, errTestUnexpectedCall(calls)
	}
	h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
		t.Fatal("a removal must not read membership: it is an unconditional append")
		return nil, nil
	}

	var saved *entity.MorphoAdapterObservation
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		saved = obs
		return 55, true, nil
	}

	ev := h.vaultV2EventsABI.Events["RemoveAdapter"]
	log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
	if err := h.processBlock(t, 1, 20000000, 3, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if saved == nil {
		t.Fatal("ObserveAdapterMembership not called")
	}
	if saved.Identity.MorphoVaultID != 7 {
		t.Errorf("MorphoVaultID = %d, want 7", saved.Identity.MorphoVaultID)
	}
	if !bytes.Equal(saved.Identity.Address, testAdapterAddr.Bytes()) {
		t.Errorf("address = %x, want %s", saved.Identity.Address, testAdapterAddr.Hex())
	}
	if saved.Membership.IsMember {
		t.Error("a RemoveAdapter records NON-membership")
	}
	if saved.Membership.AdapterType != nil {
		t.Errorf("AdapterType = %v, want nil: nothing classified this adapter", *saved.Membership.AdapterType)
	}
	if saved.Membership.ObservedVia != entity.MembershipFromRemoveAdapter {
		t.Errorf("ObservedVia = %q, want remove_adapter_event", saved.Membership.ObservedVia)
	}
	if saved.Membership.BlockNumber != 20000000 {
		t.Errorf("BlockNumber = %d, want 20000000", saved.Membership.BlockNumber)
	}
	if saved.Membership.BlockVersion != 3 {
		t.Errorf("BlockVersion = %d, want 3", saved.Membership.BlockVersion)
	}
	if saved.Membership.Timestamp.IsZero() {
		t.Error("Timestamp must be set")
	}
}

// TestProcessBlockEvent_Allocation_WarnsOnlyWhenTheObservationWasRecorded pins the ops
// signal to the thing it is supposed to mean. An Allocate asserts membership on every
// single allocation, thousands per day; the WARN is worth an operator's attention only
// when the assertion actually added something the log did not already hold — i.e. when
// we are learning about an adapter whose AddAdapter we never saw. Warning on every
// allocation would bury the signal it exists to raise, and the alert built on the
// matching counter (VectorMorphoV2LazyAdapterRegistrations) would fire constantly.
func TestProcessBlockEvent_Allocation_WarnsOnlyWhenTheObservationWasRecorded(t *testing.T) {
	tests := []struct {
		name     string
		appended bool
	}{
		{"nothing appended: the log already said member", false},
		{"appended: the log had no answer here", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			logs := h.captureLogs()

			h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				if len(calls) == 1 && calls[0].Target == testAdapterAddr {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
				}
				return nil, errTestUnexpectedCall(calls)
			}
			h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) == 2 && calls[0].Target == testAdapterAddr {
					return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
				}
				return nil, errTestUnexpectedCall(calls)
			}
			h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
				if tt.appended {
					return nil, nil
				}
				return testAdapterMember(), nil
			}
			h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
				return 55, tt.appended, nil
			}

			log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr,
				[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
				big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			if got := logs.hasWarnContaining("membership inferred from an Allocate"); got != tt.appended {
				t.Errorf("WARN(membership inferred) = %v, want %v", got, tt.appended)
			}
		})
	}
}

// TestProcessBlockEvent_RemoveAdapter_UnknownAdapterIsRecordedNotHealed replaces the
// old "unknown adapter heals" behaviour. A removal for an adapter no AddAdapter was
// ever seen for used to probe the chain and register a zero-length [R,R] incarnation
// so MarkAdapterRemoved had something to close. There is nothing to close now: the
// removal is one untyped observation, recorded with no probe and no lookup, and the
// identity row is created by the repository on first sight.
func TestProcessBlockEvent_RemoveAdapter_UnknownAdapterIsRecordedNotHealed(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
	logs := h.captureLogs()

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("an unknown adapter is not probed on the removal path either")
		return nil, errTestUnexpectedCall(calls)
	}
	var saved *entity.MorphoAdapterObservation
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		saved = obs
		return 91, true, nil
	}
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
		t.Fatal("a removal seeds no state")
		return nil
	}

	log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["RemoveAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
		t.Fatalf("a removal for an unseen adapter must be recorded, not fail: %v", err)
	}
	if saved == nil {
		t.Fatal("ObserveAdapterMembership not called")
	}
	if saved.Membership.AdapterType != nil || saved.Membership.IsMember {
		t.Errorf("want an untyped non-membership observation, got type=%v member=%t",
			saved.Membership.AdapterType, saved.Membership.IsMember)
	}
	if logs.hasWarnContaining("registered lazily") {
		t.Error("nothing is registered lazily on the removal path any more")
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

			// Known adapter: the pre-transaction membership read finds it at this
			// position, so no classification probe fires and the in-transaction
			// assertion appends nothing — it only resolves the id for the snapshot.
			h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
				return testAdapterMember(), nil
			}
			var (
				gotVaultID int64
				gotAddr    []byte
			)
			h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
				gotVaultID, gotAddr = obs.Identity.MorphoVaultID, obs.Identity.Address
				if obs.Membership.ObservedVia != entity.MembershipFromAllocation {
					t.Errorf("ObservedVia = %q, want allocation_event", obs.Membership.ObservedVia)
				}
				return 55, false, nil
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
				t.Errorf("ObserveAdapterMembership(%d,%x), want (7,%s)", gotVaultID, gotAddr, testAdapterAddr.Hex())
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
// Instead the adapter is classified on-chain, the membership the log implies is
// recorded at the event position, and its state row saved — behind a WARN saying the
// membership was inferred. The adapter address comes from the vault's own event and is
// verified by the probe, so this is evidence, not a phantom write.
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

			h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
				return nil, nil // adapter predates discovery — never AddAdapter'd
			}
			var registered *entity.MorphoAdapterObservation
			h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
				registered = obs
				return 77, true, nil
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
				t.Fatal("the membership the Allocate implies was not recorded")
			}
			if registered.Identity.MorphoVaultID != 7 {
				t.Errorf("MorphoVaultID = %d, want 7", registered.Identity.MorphoVaultID)
			}
			if !bytes.Equal(registered.Identity.Address, testAdapterAddr.Bytes()) {
				t.Errorf("Address = %x, want %s", registered.Identity.Address, testAdapterAddr.Hex())
			}
			if got := registered.Membership.AdapterType; got == nil || *got != tt.adapterType {
				t.Errorf("AdapterType = %v, want %d (probed on-chain)", got, tt.adapterType)
			}
			if registered.Membership.BlockNumber != 20000000 {
				t.Errorf("BlockNumber = %d, want 20000000 (event block)", registered.Membership.BlockNumber)
			}
			if registered.Membership.ObservedVia != entity.MembershipFromAllocation {
				t.Errorf("ObservedVia = %q, want allocation_event", registered.Membership.ObservedVia)
			}
			if registered.Identity.AssetTokenID != 1 {
				t.Errorf("AssetTokenID = %d, want 1 (vault asset)", registered.Identity.AssetTokenID)
			}
			if savedState == nil {
				t.Fatal("adapter state not saved after heal")
			}
			if savedState.MorphoAdapterID != 77 {
				t.Errorf("MorphoAdapterID = %d, want 77 (the id the observation resolved)", savedState.MorphoAdapterID)
			}
			if savedState.RealAssets.Cmp(realAssets) != 0 {
				t.Errorf("RealAssets = %s, want %s", savedState.RealAssets, realAssets)
			}
			if !logs.hasWarnContaining("membership inferred from an Allocate") {
				t.Error("expected a WARN that the membership was inferred rather than witnessed")
			}
			if got := logs.hasWarnContaining("unknown type"); got != tt.wantUnknownWarn {
				t.Errorf("WARN(unknown type) = %v, want %v", got, tt.wantUnknownWarn)
			}
		})
	}
}

// --- ForceDeallocate ---

// --- registration / snapshot telemetry ---

// TestProcessBlockEvent_AdapterRegistration_CountsOnlyAppendedObservations pins the
// counter's meaning to "observations the log gained", not "write attempts". The
// registry reports that per write, and both live shapes of "wrote nothing" must leave
// the counter alone: an Allocate whose membership the log already answers (thousands
// per day), and an SQS redelivery of a transition the primary key already holds.
// Counting attempts instead would put VectorMorphoV2LazyAdapterRegistrations (>3 in 6h
// under observed_via="allocation_event") permanently in alarm, and would inflate
// add_adapter_event by one per redelivery.
func TestProcessBlockEvent_AdapterRegistration_CountsOnlyAppendedObservations(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(h *serviceTestHarness)
		makeLog func(h *serviceTestHarness) shared.Log
		wantVia entity.MembershipSource
	}{
		{
			name: "Allocate whose membership the log already answers",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				// Already a member at this position, so nothing is probed.
				h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
					return testAdapterMember(), nil
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr,
					[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
					big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
			},
			wantVia: entity.MembershipFromAllocation,
		},
		{
			name: "redelivered AddAdapter the primary key already holds",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) == 2 && calls[0].Target == testAdapterAddr {
						return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			},
			wantVia: entity.MembershipFromAddAdapter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			reader := h.recordMetrics(t)
			tt.setup(h)
			h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
				return 55, false, nil
			}

			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, tt.makeLog(h))}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			want := map[string]string{"observed_via": string(tt.wantVia)}
			if got := counterValue(t, reader, "morpho.v2.adapter.registrations", want); got != 0 {
				t.Errorf("morpho.v2.adapter.registrations%v = %d, want 0: nothing was appended, so the log gained no observation", want, got)
			}
		})
	}
}

// TestProcessBlockEvent_AdapterRegistration_RecordsProvenanceAndType verifies each
// live write path labels morpho.v2.adapter.registrations with how the membership
// was observed. observed_via is what separates an expected AddAdapter transition
// from a membership inferred from an Allocate, which is a discovery gap: discovery
// enumerates a vault's whole adapter set, so post-discovery every active adapter is
// already on record and allocation_event should stay at zero. Both cases probe to
// Unknown so the adapter_type label the unknown-adapter alert selects on is
// exercised too.
func TestProcessBlockEvent_AdapterRegistration_RecordsProvenanceAndType(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(h *serviceTestHarness)
		makeLog func(h *serviceTestHarness) shared.Log
		wantVia entity.MembershipSource
	}{
		{
			name: "AddAdapter event",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) == 2 && calls[0].Target == testAdapterAddr {
						return h.adapterProbeResults(entity.MorphoAdapterTypeUnknown), nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(0))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			},
			wantVia: entity.MembershipFromAddAdapter,
		},
		{
			name: "Allocate for an adapter that predates discovery",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) == 2 && calls[0].Target == testAdapterAddr {
						return h.adapterProbeResults(entity.MorphoAdapterTypeUnknown), nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
					return nil, nil
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr,
					[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
					big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
			},
			wantVia: entity.MembershipFromAllocation,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			reader := h.recordMetrics(t)
			tt.setup(h)
			h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
				return 42, true, nil
			}

			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, tt.makeLog(h))}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			want := map[string]string{"adapter.type": "unknown", "observed_via": string(tt.wantVia)}
			if got := counterValue(t, reader, "morpho.v2.adapter.registrations", want); got != 1 {
				t.Errorf("morpho.v2.adapter.registrations%v = %d, want 1", want, got)
			}
		})
	}
}

// TestProcessBlockEvent_V2Snapshots_RecordSnapshotType verifies every
// event-driven structured write increments morpho.v2.snapshots.written under its
// own snapshot.type. VectorMorphoV2NoSnapshotsWritten compares this counter
// against the V2 events that should have produced it, so a handler that stops
// writing without erroring (a dispatch case falling through to the audit-log-only
// default) is otherwise invisible.
func TestProcessBlockEvent_V2Snapshots_RecordSnapshotType(t *testing.T) {
	capIDData := []byte{0x01, 0x02, 0x03, 0x04}
	capID := crypto.Keccak256Hash(capIDData)

	tests := []struct {
		name     string
		setup    func(h *serviceTestHarness)
		makeLog  func(h *serviceTestHarness) shared.Log
		wantType v2SnapshotType
	}{
		{
			name: "Allocate snapshots adapter realAssets",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(123456789))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
					return testAdapterMember(), nil
				}
				h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
					return 55, false, nil
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr,
					[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
					big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
			},
			wantType: v2SnapshotAdapterState,
		},
		{
			name: "IncreaseAbsoluteCap snapshots the cap pair",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) != 2 {
						return nil, errTestUnexpectedCall(calls)
					}
					return []outbound.Result{
						{Success: true, ReturnData: h.packUint256(big.NewInt(1_000_000))},
						{Success: true, ReturnData: h.packUint256(big.NewInt(500))},
					}, nil
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["IncreaseAbsoluteCap"], testVaultAddr, []common.Hash{capID}, capIDData, big.NewInt(999))
			},
			wantType: v2SnapshotVaultCap,
		},
		{
			name: "SetPerformanceFee snapshots the fee config",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) != 4 {
						return nil, errTestUnexpectedCall(calls)
					}
					return h.feeGetterResults(big.NewInt(100_000_000_000_000_000), big.NewInt(0),
						common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E"), common.Address{}), nil
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["SetPerformanceFee"], testVaultAddr, nil, big.NewInt(1))
			},
			wantType: v2SnapshotVaultFee,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			reader := h.recordMetrics(t)
			tt.setup(h)

			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, tt.makeLog(h))}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			want := map[string]string{"snapshot.type": string(tt.wantType)}
			if got := counterValue(t, reader, "morpho.v2.snapshots.written", want); got != 1 {
				t.Errorf("morpho.v2.snapshots.written%v = %d, want 1", want, got)
			}
		})
	}
}

// TestProcessBlockEvent_AdapterMembership_NotRecordedWhenTheCommitFails keeps the
// registration counter honest about rolled-back appends. Recorded from inside the
// write transaction it counted observations no reader can ever see, once per adapter
// per SQS redelivery of a stuck block — 12 an hour on the 300s visibility timeout,
// which alone holds VectorMorphoV2LazyAdapterRegistrations (>3 in 6h) in alarm over a
// table with zero rows.
func TestProcessBlockEvent_AdapterMembership_NotRecordedWhenTheCommitFails(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(h *serviceTestHarness)
		makeLog func(h *serviceTestHarness) shared.Log
		wantVia entity.MembershipSource
	}{
		{
			name: "AddAdapter",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) == 2 && calls[0].Target == testAdapterAddr {
						return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			},
			wantVia: entity.MembershipFromAddAdapter,
		},
		{
			name:  "RemoveAdapter",
			setup: func(_ *serviceTestHarness) {},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["RemoveAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			},
			wantVia: entity.MembershipFromRemoveAdapter,
		},
		{
			name: "Allocate",
			setup: func(h *serviceTestHarness) {
				h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) == 2 && calls[0].Target == testAdapterAddr {
						return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					if len(calls) == 1 && calls[0].Target == testAdapterAddr {
						return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(5000))}}, nil
					}
					return nil, errTestUnexpectedCall(calls)
				}
				h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
					return nil, nil
				}
			},
			makeLog: func(h *serviceTestHarness) shared.Log {
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr,
					[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
					big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
			},
			wantVia: entity.MembershipFromAllocation,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			reader := h.recordMetrics(t)
			tt.setup(h)
			h.failCommitAfterMembershipAppend()

			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, tt.makeLog(h))}); err == nil {
				t.Fatal("expected the block to fail so SQS redelivers")
			}

			want := map[string]string{"observed_via": string(tt.wantVia)}
			if got := counterValue(t, reader, "morpho.v2.adapter.registrations", want); got != 0 {
				t.Errorf("morpho.v2.adapter.registrations%v = %d, want 0 (the transaction rolled back)", want, got)
			}
		})
	}
}

// TestProcessBlockEvent_AddAdapter_CountsTheCommittedSeedAsASnapshot closes the
// registration path's hole in the snapshot counter. The seed row an AddAdapter
// commits is an event-driven adapter_state write like the one an Allocate makes, so
// VectorMorphoV2NoSnapshotsWritten must see it; a vault whose only V2 traffic is
// governance registering adapters would otherwise look like a dead write path.
func TestProcessBlockEvent_AddAdapter_CountsTheCommittedSeedAsASnapshot(t *testing.T) {
	tests := []struct {
		name          string
		adapterType   entity.MorphoAdapterType
		seedReverts   bool
		wantSnapshots int64
	}{
		{name: "a seeded adapter counts its committed state row", adapterType: entity.MorphoAdapterTypeMarketV1, wantSnapshots: 1},
		{
			name:        "an unclassified adapter that serves no realAssets writes no row to count",
			adapterType: entity.MorphoAdapterTypeUnknown, seedReverts: true, wantSnapshots: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
			reader := h.recordMetrics(t)

			h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) == 2 && calls[0].Target == testAdapterAddr {
					return h.adapterProbeResults(tt.adapterType), nil
				}
				return nil, errTestUnexpectedCall(calls)
			}
			h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				if len(calls) != 1 || calls[0].Target != testAdapterAddr {
					return nil, errTestUnexpectedCall(calls)
				}
				if tt.seedReverts {
					return []outbound.Result{{Success: false, ReturnData: nil}}, nil
				}
				return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
			}
			h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
				return 42, true, nil
			}

			log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err != nil {
				t.Fatalf("processBlock: %v", err)
			}

			want := map[string]string{"snapshot.type": string(v2SnapshotAdapterState)}
			if got := counterValue(t, reader, "morpho.v2.snapshots.written", want); got != tt.wantSnapshots {
				t.Errorf("morpho.v2.snapshots.written%v = %d, want %d", want, got, tt.wantSnapshots)
			}
		})
	}
}

// TestProcessBlockEvent_V2Snapshot_NotRecordedWhenWriteFails keeps the counter
// honest: it counts committed snapshots, so a failed write must leave it at zero
// rather than reporting a row that never landed.
func TestProcessBlockEvent_V2Snapshot_NotRecordedWhenWriteFails(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
	reader := h.recordMetrics(t)

	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) != 4 {
			return nil, errTestUnexpectedCall(calls)
		}
		return h.feeGetterResults(big.NewInt(1), big.NewInt(0), common.Address{}, common.Address{}), nil
	}
	h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultFee) error {
		return errors.New("fee write failed")
	}

	log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["SetPerformanceFee"], testVaultAddr, nil, big.NewInt(1))
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
		t.Fatal("expected the block to fail so SQS redelivers")
	}

	want := map[string]string{"snapshot.type": string(v2SnapshotVaultFee)}
	if got := counterValue(t, reader, "morpho.v2.snapshots.written", want); got != 0 {
		t.Errorf("morpho.v2.snapshots.written%v = %d, want 0 (the write failed)", want, got)
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

// partialFeeGetterResults returns the same 4-call getVaultFees response with a
// per-getter success flag, so a test can serve a contract that answers only some of
// the fee surface (or none of it).
func (h *serviceTestHarness) partialFeeGetterResults(success [4]bool) []outbound.Result {
	full := h.feeGetterResults(big.NewInt(1), big.NewInt(2), common.HexToAddress("0x9"), common.HexToAddress("0xa"))
	for i, ok := range success {
		if !ok {
			full[i] = outbound.Result{Success: false, ReturnData: nil}
		}
	}
	return full
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
				// AllowFailure is required, not tolerated: it is the only way the
				// batch can report "this contract serves NONE of the fee getters"
				// instead of the whole multicall reverting. The all-or-nothing
				// requirement is enforced on the results (assertFeeSurfaceComplete).
				for _, c := range calls {
					if c.Target != testVaultAddr || !c.AllowFailure {
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

// TestProcessBlockEvent_FeeChange_NoFeeSurfaceIsAHardError is the other arm of the
// fee-surface tolerance: discovery may skip seeding a vault-shaped contract that
// serves none of the four getters, but a Set* fee EVENT proves the surface exists, so
// all four reverting there is drift that must stop the block rather than be skipped.
func TestProcessBlockEvent_FeeChange_NoFeeSurfaceIsAHardError(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) != 4 {
			return nil, errTestUnexpectedCall(calls)
		}
		return h.partialFeeGetterResults([4]bool{}), nil
	}
	h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultFee) error {
		t.Fatal("no fee row may be written when every fee getter reverts")
		return nil
	}

	log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["SetPerformanceFee"], testVaultAddr, nil, big.NewInt(1))
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)})
	if err == nil {
		t.Fatal("expected the block to fail so SQS redelivers")
	}
	if !strings.Contains(err.Error(), "serves none of the VaultV2 fee getters") {
		t.Errorf("error should name the absent fee surface, got: %v", err)
	}
}

// TestProcessBlockEvent_V2Handlers_ErrorsPropagate verifies each structured V2
// handler fails the whole event (so SQS redelivers) rather than swallowing a
// transient dependency failure into partial success. One row per handler's
// distinct failing dependency.
func TestProcessBlockEvent_V2Handlers_ErrorsPropagate(t *testing.T) {
	adapterIdx := []common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)}

	tests := []struct {
		name string
		// wantErr pins WHICH dependency failed the event. It is required on every
		// row: without it a row passes as long as something fails, which hides a
		// swallowed error that merely trips a later step.
		wantErr string
		setup   func(h *serviceTestHarness) shared.Log
	}{
		{
			name:    "AddAdapter: adapter probe RPC error",
			wantErr: "classifying adapter",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("probe rpc down")
				}
				h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
					t.Fatal("no membership may be recorded when the probe fails")
					return 0, false, nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			},
		},
		{
			name:    "Allocation: realAssets RPC error",
			wantErr: "fetching realAssets",
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
			name:    "AddAdapter: ObserveAdapterMembership DB error",
			wantErr: "recording adapter",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
				}
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
				}
				h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
					return 0, false, errors.New("db down")
				}
				h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
					t.Fatal("adapter state must not be persisted when the registry write fails")
					return nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["AddAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
			},
		},
		{
			// The pre-transaction membership read decides whether the assertion path
			// needs to probe at all; a DB failure there must stop the event, not be
			// read as "adapter not a member".
			name:    "Allocation: GetActiveAdapterAt DB error",
			wantErr: "looking up adapter",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
				}
				// The probe would succeed: the ONLY thing failing this event is the
				// membership read.
				h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
				}
				h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
					return nil, errors.New("db down")
				}
				h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
					t.Fatal("adapter state must not be persisted on a membership lookup error")
					return nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr, adapterIdx, big.NewInt(1), hashSlice(common.HexToHash("0xaa")), big.NewInt(1))
			},
		},
		{
			name:    "Allocation: ObserveAdapterMembership DB error",
			wantErr: "recording adapter",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
				}
				// Adapter is already a member (the pre-tx read finds it), so the
				// failure under test is the in-transaction observation write.
				h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
					return testAdapterMember(), nil
				}
				h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
					return 0, false, errors.New("db down")
				}
				h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
					t.Fatal("adapter state must not be persisted on a DB write error")
					return nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr, adapterIdx, big.NewInt(1), hashSlice(common.HexToHash("0xaa")), big.NewInt(1))
			},
		},
		{
			// The heal path must still fail the event on a TRANSPORT probe error:
			// a momentarily-unreachable adapter is transient and must retry, never
			// be recorded with a defaulted type.
			name:    "Allocation: lazy-register probe transport error",
			wantErr: "classifying adapter",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
				}
				h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("adapter probe rpc down")
				}
				h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
					return nil, nil // not a member here → the probe runs, then fails
				}
				h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
					t.Fatal("no membership may be recorded when the classification probe fails")
					return 0, false, nil
				}
				h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
					t.Fatal("adapter state must not be persisted when the probe fails")
					return nil
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr, adapterIdx, big.NewInt(1), hashSlice(common.HexToHash("0xaa")), big.NewInt(1))
			},
		},
		{
			name:    "CapChange: SaveVaultCap DB error",
			wantErr: "SaveVaultCap db down",
			setup: func(h *serviceTestHarness) shared.Log {
				capIDData := []byte{0x01}
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: h.packUint256(big.NewInt(1))},
						{Success: true, ReturnData: h.packUint256(big.NewInt(1))},
					}, nil
				}
				h.morphoRepo.SaveVaultCapFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultCap) error {
					return errors.New("SaveVaultCap db down")
				}
				return h.makeV2VaultLog(h.vaultV2EventsABI.Events["IncreaseAbsoluteCap"], testVaultAddr, []common.Hash{crypto.Keccak256Hash(capIDData)}, capIDData, big.NewInt(1))
			},
		},
		{
			name:    "Fee: SaveVaultFee DB error",
			wantErr: "SaveVaultFee db down",
			setup: func(h *serviceTestHarness) shared.Log {
				h.multicaller.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return h.feeGetterResults(big.NewInt(1), big.NewInt(0), common.Address{}, common.Address{}), nil
				}
				h.morphoRepo.SaveVaultFeeFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultFee) error {
					return errors.New("SaveVaultFee db down")
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
			err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)})
			if err == nil {
				t.Fatal("expected the block to fail so SQS redelivers")
			}
			if tt.wantErr == "" {
				t.Fatal("every row must pin its own dependency's error via wantErr")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should come from the failing dependency (%q)", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestProcessBlockEvent_Allocation_VanishedAdapterFailsHard covers the disagreement
// between the two adapter reads: the pre-transaction membership check finds the
// adapter (so nothing is probed and no type is carried into the transaction), but
// the decisive in-transaction append then reports it unclassified. There is no
// live single-consumer path that can remove an adapter in between, so this is
// unexplained drift: with no probed type the only alternatives are recording a
// defaulted classification or failing. It must fail, and SQS redelivery re-probes.
func TestProcessBlockEvent_Allocation_VanishedAdapterFailsHard(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) == 1 && calls[0].Target == testAdapterAddr {
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(1))}}, nil
		}
		return nil, errTestUnexpectedCall(calls)
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("no probe may run: the pre-transaction membership check found the adapter")
		return nil, errTestUnexpectedCall(calls)
	}
	h.morphoRepo.GetActiveAdapterAtFn = func(_ context.Context, _ int64, _ []byte, _ entity.BlockPosition) (*entity.MorphoAdapterMember, error) {
		return testAdapterMember(), nil
	}
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		if obs.Membership.AdapterType != nil {
			t.Errorf("AdapterType = %v, want nil: nothing was probed, so nothing may be invented", *obs.Membership.AdapterType)
		}
		return 0, false, fmt.Errorf("wrapped: %w", outbound.ErrAdapterUnclassified)
	}
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterState) error {
		t.Fatal("no adapter state may be written for an adapter that vanished mid-transaction")
		return nil
	}

	log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["Allocate"], testVaultAddr,
		[]common.Hash{addrTopic(testCaller), addrTopic(testAdapterAddr)},
		big.NewInt(5000), hashSlice(common.HexToHash("0xaa")), big.NewInt(5000))
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)})
	if err == nil {
		t.Fatal("expected the block to fail so SQS redelivers and the pre-tx check re-probes")
	}
	if !strings.Contains(err.Error(), "no type was probed") {
		t.Errorf("error should name the missing probed type, got: %v", err)
	}
}

// TestProcessBlockEvent_RemoveAdapter_NonV2VaultErrors exercises resolveV2Vault's
// version guard through a handler other than AddAdapter.
func TestProcessBlockEvent_RemoveAdapter_NonV2VaultErrors(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
		t.Fatal("no membership may be recorded for a non-V2 vault")
		return 0, false, nil
	}
	log := h.makeV2VaultLog(h.vaultV2EventsABI.Events["RemoveAdapter"], testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{makeReceipt(testTxHash, log)}); err == nil {
		t.Fatal("expected error for VaultV2 event on non-V2 vault")
	}
}
