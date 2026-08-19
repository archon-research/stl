package morpho_indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
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

// TestVaultV2ConfigEventTopics asserts the config subset is exactly the 10
// governance events — the structured set minus the three allocation events. The
// bootstrap sweeps eth_getLogs on this set; a drift here silently changes what a
// bootstrap run heals.
func TestVaultV2ConfigEventTopics(t *testing.T) {
	topics, err := VaultV2ConfigEventTopics()
	if err != nil {
		t.Fatalf("VaultV2ConfigEventTopics: %v", err)
	}
	if len(topics) != 10 {
		t.Fatalf("want 10 config topics, got %d", len(topics))
	}

	abiV2, err := abis.GetVaultV2EventsABI()
	if err != nil {
		t.Fatalf("GetVaultV2EventsABI: %v", err)
	}
	for _, name := range []string{
		"AddAdapter", "RemoveAdapter",
		"IncreaseAbsoluteCap", "DecreaseAbsoluteCap",
		"IncreaseRelativeCap", "DecreaseRelativeCap",
		"SetPerformanceFee", "SetManagementFee",
		"SetPerformanceFeeRecipient", "SetManagementFeeRecipient",
	} {
		if _, ok := topics[abiV2.Events[name].ID]; !ok {
			t.Errorf("config topic set missing %s", name)
		}
	}
	for _, name := range []string{"Allocate", "Deallocate", "ForceDeallocate"} {
		if _, ok := topics[abiV2.Events[name].ID]; ok {
			t.Errorf("config topic set unexpectedly includes allocation event %s", name)
		}
	}
}

// TestVaultV2ConfigEventTopics_SubsetOfStructured pins the invariant
// ReplayMetaMorphoLog depends on: every config topic must also be a structured
// topic, or the replay guard would reject the very logs the bootstrap feeds it.
func TestVaultV2ConfigEventTopics_SubsetOfStructured(t *testing.T) {
	config, err := VaultV2ConfigEventTopics()
	if err != nil {
		t.Fatalf("VaultV2ConfigEventTopics: %v", err)
	}
	structured, err := VaultV2StructuredEventTopics()
	if err != nil {
		t.Fatalf("VaultV2StructuredEventTopics: %v", err)
	}
	for topic := range config {
		if _, ok := structured[topic]; !ok {
			t.Errorf("config topic %s is not in the structured set; ReplayMetaMorphoLog would reject it", topic.Hex())
		}
	}
}

// TestSeedV2VaultAdapters_EnumeratesAndSeedsState verifies the bootstrap's
// adapter-seed entry point drives the same enumerate → classify → upsert → seed
// path discovery uses, for a vault that is ALREADY persisted (so it never goes
// through discovery again).
func TestSeedV2VaultAdapters_EnumeratesAndSeedsState(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	const headBlock = int64(24_000_000)
	realAssets := big.NewInt(4242)
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		if blockHash != testBlockHash {
			return nil, fmt.Errorf("state read pinned to %s, want the bootstrap head hash %s", blockHash.Hex(), testBlockHash.Hex())
		}
		return h.vaultAdapterEnumerationResults(calls, testVaultAddr, testAdapterAddr, realAssets)
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == adapterProbeCallsPerAdapter && calls[0].Target == testAdapterAddr {
			return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
		}
		return nil, errTestUnexpectedCall(calls)
	}

	var savedAdapter *entity.MorphoAdapterObservation
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		savedAdapter = obs
		return 99, true, nil
	}
	var savedState *entity.MorphoAdapterState
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoAdapterState) error {
		savedState = s
		return nil
	}

	blockTS := time.Unix(1_760_000_000, 0).UTC()
	if err := h.svc.SeedV2VaultAdapters(context.Background(), testVaultAddr, headBlock, testBlockHash, 0, blockTS); err != nil {
		t.Fatalf("SeedV2VaultAdapters: %v", err)
	}

	if savedAdapter == nil {
		t.Fatal("no adapter recorded — enumeration did not reach ObserveAdapterMembership")
	}
	if savedAdapter.Identity.MorphoVaultID != 7 {
		t.Errorf("MorphoVaultID = %d, want 7", savedAdapter.Identity.MorphoVaultID)
	}
	if !bytes.Equal(savedAdapter.Identity.Address, testAdapterAddr.Bytes()) {
		t.Errorf("Address = %x, want %s", savedAdapter.Identity.Address, testAdapterAddr.Hex())
	}
	if got := savedAdapter.Membership.AdapterType; got == nil || *got != entity.MorphoAdapterTypeMarketV1 {
		t.Errorf("AdapterType = %v, want MarketV1", got)
	}
	// The head enumeration is an end-of-block state read, and it must say so: a
	// bootstrap seed asserts what the set CONTAINS at the pinned head, it never
	// witnesses an AddAdapter.
	if savedAdapter.Membership.ObservedVia != entity.MembershipFromBootstrapSeed {
		t.Errorf("ObservedVia = %q, want bootstrap_seed", savedAdapter.Membership.ObservedVia)
	}
	if savedAdapter.Membership.LogIndex != entity.EndOfBlockLogIndex {
		t.Errorf("LogIndex = %d, want EndOfBlockLogIndex", savedAdapter.Membership.LogIndex)
	}
	if !savedAdapter.Membership.IsMember {
		t.Error("an enumerated adapter is recorded as a member")
	}
	if savedState == nil {
		t.Fatal("no adapter_state seeded — VEC-219's composition probe would report adapter_data_missing")
	}
	if savedState.MorphoAdapterID != 99 || savedState.BlockNumber != headBlock {
		t.Errorf("state = {adapter %d, block %d}, want {99, %d}", savedState.MorphoAdapterID, savedState.BlockNumber, headBlock)
	}
	if savedState.RealAssets.Cmp(realAssets) != 0 {
		t.Errorf("RealAssets = %s, want %s", savedState.RealAssets, realAssets)
	}
}

// TestSeedV2VaultAdapters_RejectsNonV2Vault: the seed reads VaultV2-only
// selectors, so a V1 vault reaching it is a caller bug that must fail loudly
// rather than issue reverting calls.
func TestSeedV2VaultAdapters_RejectsNonV2Vault(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	err := h.svc.SeedV2VaultAdapters(context.Background(), testVaultAddr, 24_000_000, testBlockHash, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Fatal("expected an error seeding adapters for a non-V2 vault")
	}
}

// TestSeedV2VaultAdapters_EnumerationFailureWritesNothing pins the
// reads-then-persist ordering: a transport error during enumeration must leave
// the transaction unopened, so a re-triggered run starts from a clean slate
// instead of a half-seeded vault.
func TestSeedV2VaultAdapters_EnumerationFailureWritesNothing(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	h.multicaller.ExecuteAtHashFn = func(context.Context, []outbound.Call, common.Hash) ([]outbound.Result, error) {
		return nil, errors.New("rpc unavailable")
	}
	h.txManager.WithTransactionFn = func(context.Context, func(pgx.Tx) error) error {
		t.Fatal("a write transaction was opened despite the enumeration read failing")
		return nil
	}

	if err := h.svc.SeedV2VaultAdapters(context.Background(), testVaultAddr, 24_000_000, testBlockHash, 0, time.Unix(1, 0).UTC()); err == nil {
		t.Fatal("expected the enumeration transport error to propagate")
	}
}

// vaultAdapterEnumerationResults answers the three hash-pinned reads the adapter
// seed issues — adaptersLength(), adapters(0), and the adapter's realAssets() —
// for a vault holding exactly one adapter.
func (h *serviceTestHarness) vaultAdapterEnumerationResults(calls []outbound.Call, vault, adapter common.Address, realAssets *big.Int) ([]outbound.Result, error) {
	h.t.Helper()
	vaultV2ABI, err := abis.GetVaultV2ReadABI()
	if err != nil {
		return nil, err
	}
	adapterABI, err := abis.GetVaultV2AdapterReadABI()
	if err != nil {
		return nil, err
	}

	switch {
	case len(calls) == 1 && calls[0].Target == vault && isPackedCall(calls[0], vaultV2ABI, "adaptersLength"):
		data, err := vaultV2ABI.Methods["adaptersLength"].Outputs.Pack(big.NewInt(1))
		if err != nil {
			return nil, err
		}
		return []outbound.Result{{Success: true, ReturnData: data}}, nil
	case len(calls) == 1 && calls[0].Target == vault:
		data, err := vaultV2ABI.Methods["adapters"].Outputs.Pack(adapter)
		if err != nil {
			return nil, err
		}
		return []outbound.Result{{Success: true, ReturnData: data}}, nil
	case len(calls) == 1 && calls[0].Target == adapter:
		data, err := adapterABI.Methods["realAssets"].Outputs.Pack(realAssets)
		if err != nil {
			return nil, err
		}
		return []outbound.Result{{Success: true, ReturnData: data}}, nil
	}
	return nil, errTestUnexpectedCall(calls)
}

// isPackedCall reports whether call carries the 4-byte selector of method.
func isPackedCall(call outbound.Call, contractABI *abi.ABI, method string) bool {
	return len(call.CallData) >= 4 && bytes.Equal(call.CallData[:4], contractABI.Methods[method].ID)
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
// AddAdapter → ObserveAdapterMembership), rather than only audit-logging it. Reuses
// the existing mock harness from vault_v2_handler_test.go.
func TestReplayMetaMorphoLog_RoutesToHandler(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

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

	var saved *entity.MorphoAdapterObservation
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		saved = obs
		return 42, true, nil
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
		t.Fatal("ObserveAdapterMembership not called — log was not routed to the AddAdapter handler")
	}
	if saved.Identity.MorphoVaultID != 7 {
		t.Errorf("MorphoVaultID = %d, want 7", saved.Identity.MorphoVaultID)
	}
	if !bytes.Equal(saved.Identity.Address, testAdapterAddr.Bytes()) {
		t.Errorf("Address = %x, want %s", saved.Identity.Address, testAdapterAddr.Hex())
	}
	if saved.Membership.BlockNumber != 20000000 {
		t.Errorf("BlockNumber = %d, want 20000000", saved.Membership.BlockNumber)
	}
	if saved.Membership.ObservedVia != entity.MembershipFromAddAdapter {
		t.Errorf("ObservedVia = %q, want add_adapter_event", saved.Membership.ObservedVia)
	}
	if !auditSaved {
		t.Error("audit-log protocol_event not saved during replay")
	}
}

// TestReplayMetaMorphoLog_StampsArchivingBlockContext verifies replay stamps the
// replayed log's block coordinates on the context the way the live SQS path
// does. Without them the archiving decorator keys every hash-pinned batch at
// block 0, so each replayed block overwrites the previous one's archive.
func TestReplayMetaMorphoLog_StampsArchivingBlockContext(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 && calls[0].Target == testAdapterAddr {
			return h.adapterProbeResults(entity.MorphoAdapterTypeMarketV1), nil
		}
		return nil, errTestUnexpectedCall(calls)
	}
	var seenNumber int64
	var seenNumberOK bool
	var seenVersion int
	var seenVersionOK bool
	h.multicaller.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		seenNumber, seenNumberOK = archiving.BlockNumberFromContext(ctx)
		seenVersion, seenVersionOK = archiving.BlockVersionFromContext(ctx)
		if len(calls) == 1 && calls[0].Target == testAdapterAddr {
			return []outbound.Result{{Success: true, ReturnData: h.packUint256(big.NewInt(41_300_000))}}, nil
		}
		return nil, errTestUnexpectedCall(calls)
	}
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoAdapterObservation) (int64, bool, error) {
		return 42, true, nil
	}

	ev := h.vaultV2EventsABI.Events["AddAdapter"]
	log := h.makeV2VaultLog(ev, testVaultAddr, []common.Hash{addrTopic(testAdapterAddr)})

	if err := h.svc.ReplayMetaMorphoLog(context.Background(), log, 20000000, testBlockHash, 3, time.Unix(1700000000, 0).UTC()); err != nil {
		t.Fatalf("ReplayMetaMorphoLog: %v", err)
	}

	if !seenNumberOK {
		t.Error("hash-pinned read saw no archiving block number; the archive would key it at block 0")
	}
	if seenNumber != 20000000 {
		t.Errorf("archiving block number = %d, want 20000000", seenNumber)
	}
	if !seenVersionOK {
		t.Error("hash-pinned read saw no archiving block version")
	}
	if seenVersion != 3 {
		t.Errorf("archiving block version = %d, want 3", seenVersion)
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

// TestReplayMetaMorphoLog_NonStructuredTopicErrors verifies the replay path
// rejects a share-accounting log (here a V1 Deposit) before dispatch: the replay
// constructor nils the user/token/cache ports that the deposit handler would
// dereference, so an unguarded route would panic. The guard must return a clean
// error and never reach the position-snapshot path.
func TestReplayMetaMorphoLog_NonStructuredTopicErrors(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)
	h.morphoRepo.SaveVaultPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoVaultPosition) error {
		t.Fatal("a non-structured (Deposit) log must not reach the position-snapshot path")
		return nil
	}

	log := h.makeVaultDepositLog(testVaultAddr, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	err := h.svc.ReplayMetaMorphoLog(context.Background(), log, 20000000, testBlockHash, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Fatal("expected an error replaying a non-structured (Deposit) log")
	}
	if !strings.Contains(err.Error(), "not a VaultV2 structured event") {
		t.Errorf("error %q should explain the topic is not a structured V2 event", err.Error())
	}
}
