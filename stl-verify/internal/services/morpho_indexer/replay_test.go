package morpho_indexer

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// TestVaultV2StructuredEventTopics asserts the derived topic set is exactly the
// 13 adapter / cap / fee events with structured handlers — derived from the ABI,
// and excluding the shared ERC4626/ERC20 surface (Deposit/Withdraw/Transfer/
// AccrueInterest) that is not part of the backfiller's V2 replay.
func TestVaultV2StructuredEventTopics(t *testing.T) {
	topics, err := VaultV2StructuredEventTopics()
	if err != nil {
		t.Fatalf("VaultV2StructuredEventTopics: %v", err)
	}
	if len(topics) != 13 {
		t.Fatalf("want 13 structured topics, got %d", len(topics))
	}

	abiV2, err := abis.GetVaultV2EventsABI()
	if err != nil {
		t.Fatalf("GetVaultV2EventsABI: %v", err)
	}
	for _, name := range []string{
		"AddAdapter", "RemoveAdapter",
		"Allocate", "Deallocate", "ForceDeallocate",
		"IncreaseAbsoluteCap", "DecreaseAbsoluteCap",
		"IncreaseRelativeCap", "DecreaseRelativeCap",
		"SetPerformanceFee", "SetManagementFee",
		"SetPerformanceFeeRecipient", "SetManagementFeeRecipient",
	} {
		if _, ok := topics[abiV2.Events[name].ID]; !ok {
			t.Errorf("structured topic set missing %s", name)
		}
	}

	// The shared ERC4626/ERC20 surface must NOT be in the structured V2 set.
	metaMorphoABI, err := abis.GetMetaMorphoV1EventsABI()
	if err != nil {
		t.Fatalf("GetMetaMorphoV1EventsABI: %v", err)
	}
	for _, name := range []string{"Deposit", "Withdraw", "Transfer"} {
		if _, ok := topics[metaMorphoABI.Events[name].ID]; ok {
			t.Errorf("structured topic set unexpectedly includes shared event %s", name)
		}
	}
}

// TestV2VaultAddresses asserts only VaultV2 vaults are returned, not V1/V1.1.
func TestV2VaultAddresses(t *testing.T) {
	h := newTestHarness(t)

	v2a := common.HexToAddress("0xa000000000000000000000000000000000000001")
	v2b := common.HexToAddress("0xa000000000000000000000000000000000000002")
	v1 := common.HexToAddress("0xa000000000000000000000000000000000000003")
	h.registerTestVault(v2a, 1, entity.MorphoVaultV2)
	h.registerTestVault(v2b, 2, entity.MorphoVaultV2)
	h.registerTestVault(v1, 3, entity.MorphoVaultV1)

	got := h.svc.V2VaultAddresses()
	if len(got) != 2 {
		t.Fatalf("want 2 V2 vaults, got %d: %v", len(got), got)
	}
	if _, ok := got[v2a]; !ok {
		t.Errorf("missing V2 vault %s", v2a.Hex())
	}
	if _, ok := got[v2b]; !ok {
		t.Errorf("missing V2 vault %s", v2b.Hex())
	}
	if _, ok := got[v1]; ok {
		t.Errorf("V1 vault %s must not appear", v1.Hex())
	}
}

// TestReplayMetaMorphoLog_RoutesToHandler verifies ReplayMetaMorphoLog drives a
// V2 structured log into the same typed handler the live SQS path uses (here
// AddAdapter → GetOrCreateAdapter), rather than only audit-logging it. Reuses
// the existing mock harness from service_v2_handlers_test.go.
func TestReplayMetaMorphoLog_RoutesToHandler(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 && calls[0].Target == testAdapterAddr {
			return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
		}
		return nil, errTestUnexpectedCall(calls)
	}

	var saved *entity.MorphoAdapter
	h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, a *entity.MorphoAdapter) (int64, error) {
		saved = a
		return 42, nil
	}
	var auditSaved bool
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		auditSaved = true
		return nil
	}

	ev := h.vaultV2EventsABI.Events["AddAdapter"]
	log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})

	blockTS := time.Unix(1700000000, 0).UTC()
	if err := h.svc.ReplayMetaMorphoLog(context.Background(), log, 20000000, testBlockHash, 3, blockTS); err != nil {
		t.Fatalf("ReplayMetaMorphoLog: %v", err)
	}

	if saved == nil {
		t.Fatal("GetOrCreateAdapter not called — log was not routed to the AddAdapter handler")
	}
	if saved.MorphoVaultID != 7 {
		t.Errorf("MorphoVaultID = %d, want 7", saved.MorphoVaultID)
	}
	if !bytes.Equal(saved.Address, testAdapterAddr.Bytes()) {
		t.Errorf("Address = %x, want %s", saved.Address, testAdapterAddr.Hex())
	}
	if saved.AddedAtBlock != 20000000 {
		t.Errorf("AddedAtBlock = %d, want 20000000", saved.AddedAtBlock)
	}
	if !auditSaved {
		t.Error("audit-log protocol_event not saved during replay")
	}
}

// TestReplayMetaMorphoLog_UnknownVaultErrors verifies replay fails loudly when a
// log's emitter is not a known V2 vault, rather than silently dropping it.
func TestReplayMetaMorphoLog_UnknownVaultErrors(t *testing.T) {
	h := newTestHarness(t)
	// No vault registered for testVaultAddr.

	ev := h.vaultV2EventsABI.Events["RemoveAdapter"]
	log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})

	err := h.svc.ReplayMetaMorphoLog(context.Background(), log, 20000000, testBlockHash, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Fatal("expected an error replaying a log from an unregistered vault")
	}
}
