package morpho_indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestConfigDefaults(t *testing.T) {
	defaults := ConfigDefaults()

	if defaults.MaxMessages != 1 {
		t.Errorf("MaxMessages = %d, want one message per receive so the visibility clock covers it", defaults.MaxMessages)
	}
	if defaults.PollInterval == 0 {
		t.Error("PollInterval should not be zero")
	}
	if defaults.Logger == nil {
		t.Error("Logger should not be nil")
	}
}

// TestNewReplayConfig_CarriesTelemetry guards the one thing a replay composition
// root cannot notice it forgot: every recorder is nil-safe, so a nil
// Config.Telemetry mutes the whole replay path in silence — no
// morpho_v2_adapter_registrations_total series at all, which is what the
// morpho-v2-bootstrap runbook's first check reads.
func TestNewReplayConfig_CarriesTelemetry(t *testing.T) {
	config, err := NewReplayConfig(1, testutil.DiscardLogger())
	if err != nil {
		t.Fatalf("NewReplayConfig: %v", err)
	}
	if config.Telemetry == nil {
		t.Error("Telemetry is nil: the replay path would record no metrics at all")
	}
	if config.ChainID != 1 {
		t.Errorf("ChainID = %d, want 1", config.ChainID)
	}
	if config.Logger == nil {
		t.Error("Logger should not be nil")
	}
}

// A chain with no name has no `chain` metric label either, which is what left the
// Vector indexer alerts rendering an empty chain — so the replay config refuses
// to build rather than metering a run into an unselectable series.
func TestNewReplayConfig_RejectsAnUnnamedChain(t *testing.T) {
	if _, err := NewReplayConfig(999_999, testutil.DiscardLogger()); err == nil {
		t.Fatal("expected an unknown chain ID to be rejected")
	}
}

func TestMorphoBlueAddress(t *testing.T) {
	expected := "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"
	if MorphoBlueAddress.Hex() != expected {
		t.Errorf("MorphoBlueAddress = %s, want %s", MorphoBlueAddress.Hex(), expected)
	}
}

func TestMorphoBlueDeployBlock(t *testing.T) {
	tests := []struct {
		name    string
		chainID int64
		want    int64
		wantErr bool
	}{
		{"ethereum mainnet", 1, 18883124, false},
		{"base", 8453, 18925795, false},
		{"arbitrum", 42161, 226833208, false},
		{"unknown chain", 999, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MorphoBlueDeployBlock(tt.chainID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MorphoBlueDeployBlock(%d) error = %v, wantErr %v", tt.chainID, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("MorphoBlueDeployBlock(%d) = %d, want %d", tt.chainID, got, tt.want)
			}
		})
	}
}

// --- NewService validation ---

func TestNewService_ValidateDependencies(t *testing.T) {
	rc := testutil.NewMockBlockCache()
	mc := testutil.NewMockMulticaller()
	tm := &testutil.MockTxManager{}
	ur := &testutil.MockUserRepository{}
	pr := &testutil.MockProtocolRepository{}
	tr := &testutil.MockTokenRepository{}
	mRepo := &testutil.MockMorphoRepository{}
	er := &testutil.MockEventRepository{}
	rtr := &testutil.MockReceiptTokenRepository{}
	cons := &testutil.MockSQSConsumer{}

	sqsCfg := shared.SQSConsumerConfigDefaults()
	sqsCfg.ChainID = 1
	config := Config{SQSConsumerConfig: sqsCfg}

	tests := []struct {
		name             string
		consumer         outbound.SQSConsumer
		cache            outbound.BlockCache
		multicall        outbound.Multicaller
		txMgr            outbound.TxManager
		userRepo         outbound.UserRepository
		protoRepo        outbound.ProtocolRepository
		tokenRepo        outbound.TokenRepository
		morphoRepo       outbound.MorphoRepository
		eventRepo        outbound.EventRepository
		receiptTokenRepo outbound.ReceiptTokenRepository
		errContains      string
	}{
		{"nil consumer", nil, rc, mc, tm, ur, pr, tr, mRepo, er, rtr, "consumer is required"},
		{"nil cache", cons, nil, mc, tm, ur, pr, tr, mRepo, er, rtr, "cache is required"},
		{"nil multicall", cons, rc, nil, tm, ur, pr, tr, mRepo, er, rtr, "multicallClient is required"},
		{"nil txManager", cons, rc, mc, nil, ur, pr, tr, mRepo, er, rtr, "txManager is required"},
		{"nil userRepo", cons, rc, mc, tm, nil, pr, tr, mRepo, er, rtr, "userRepo is required"},
		{"nil protocolRepo", cons, rc, mc, tm, ur, nil, tr, mRepo, er, rtr, "protocolRepo is required"},
		{"nil tokenRepo", cons, rc, mc, tm, ur, pr, nil, mRepo, er, rtr, "tokenRepo is required"},
		{"nil morphoRepo", cons, rc, mc, tm, ur, pr, tr, nil, er, rtr, "morphoRepo is required"},
		{"nil eventRepo", cons, rc, mc, tm, ur, pr, tr, mRepo, nil, rtr, "eventRepo is required"},
		{"nil receiptTokenRepo", cons, rc, mc, tm, ur, pr, tr, mRepo, er, nil, "receiptTokenRepo is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(config, tt.consumer, tt.cache, tt.multicall, tt.txMgr, tt.userRepo, tt.protoRepo, tt.tokenRepo, tt.morphoRepo, tt.eventRepo, tt.receiptTokenRepo)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
			}
		})
	}

	// All deps provided: should succeed.
	svc, err := NewService(config, cons, rc, mc, tm, ur, pr, tr, mRepo, er, rtr)
	if err != nil {
		t.Fatalf("NewService with all deps: %v", err)
	}
	if svc == nil {
		t.Fatal("NewService returned nil service")
	}
}

func TestProcessBlockEvent_SetFee(t *testing.T) {
	h := newTestHarness(t)
	// SetFee only saves protocol event — no market state or position updates.
	var eventSaved bool
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		eventSaved = true
		return nil
	}

	log := h.makeSetFeeLog(testMarketID, big.NewInt(100000000000000000))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !eventSaved {
		t.Error("SaveEvent not called for SetFee")
	}
}

// TestProcessBlockEvent_V2GovernanceEvent_AuditLogged verifies that a V2 event
// without a typed handler (SetCurator) emitted by a known vault still produces a
// protocol_event audit-log row labelled with the correct event name. Adapter /
// cap / fee events now have structured handlers; the remaining governance /
// timelock / gate surface stays audit-log-only, but operators must still see
// those events landing.
func TestProcessBlockEvent_V2GovernanceEvent_AuditLogged(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV2)

	// Chain-verified topic for SetCurator on sparkUSDTbc (2026-05-06); no typed
	// handler, so this exercises the audit-log-only path.
	const setCuratorTopic = "0xbd0a63c12948fbc9194a5839019f99c9d71db924e5c70018265bc778b8f1a506"
	newCurator := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

	log := shared.Log{
		Address: testVaultAddr.Hex(),
		Topics: []string{
			setCuratorTopic,
			common.BytesToHash(newCurator.Bytes()).Hex(),
		},
		Data:            "0x",
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}

	var savedEvent *entity.ProtocolEvent
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
		savedEvent = e
		return nil
	}

	receipt := makeReceipt(testTxHash, log)
	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedEvent == nil {
		t.Fatal("SaveEvent not called for V2 governance event")
	}
	if savedEvent.EventName != "SetCurator" {
		t.Errorf("EventName = %q, want SetCurator", savedEvent.EventName)
	}
	if !bytes.Equal(savedEvent.ContractAddress, testVaultAddr.Bytes()) {
		t.Errorf("ContractAddress = %x, want %s", savedEvent.ContractAddress, testVaultAddr.Hex())
	}
	if !json.Valid(savedEvent.EventData) {
		t.Errorf("EventData is not valid JSON: %s", savedEvent.EventData)
	}
}

// TestProcessReceipt_VaultDiscoveryRace_KeepsFirstError verifies the VEC-188
// invariant that a transient failure on the first log for a vault address must
// NOT be wiped by a later success for the same address within the same receipt.
//
// Scenario: two MetaMorpho Deposit logs in the same receipt target the same
// newly-discovered vault. The first tryDiscoverVault call hits a transient
// (non-ErrNotVault) error. The second call succeeds. Under the old behavior,
// the success's delete(discoveryErrs, logAddress) wiped the error and
// processReceipt returned nil — SQS would ACK, permanently losing the first
// log's event. The fix keeps the first failure so the error propagates and SQS
// redelivers, allowing BOTH logs to be retried.
func TestProcessReceipt_VaultDiscoveryRace_KeepsFirstError(t *testing.T) {
	h := newTestHarness(t)
	unknownVault := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// First probe call fails transiently (simulating 429 / timeout from Alchemy).
	// Second probe call succeeds, allowing vault registration on the 2nd log.
	probeCallCount := 0
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		switch len(calls) {
		case 2:
			return h.tokenMetadataResults("WETH", 18), nil
		case 3:
			// vault state + balance (after discovery, process the event)
			return []outbound.Result{h.defaultVaultTotalAssetsResult(), h.defaultVaultTotalSupplyResult(), h.defaultBalanceOfResult(big.NewInt(100000))}, nil
		case 4:
			if h.isProbeMulticall(calls) {
				probeCallCount++
				if probeCallCount == 1 {
					// Transient RPC error on the FIRST log's discovery attempt.
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
	// Log 1: triggers discovery → transient failure (event lost).
	// Log 2: retries discovery → succeeds, vault registered.
	// Per VEC-188, the first log's event was never saved, so processReceipt
	// MUST return a non-nil error to force SQS redelivery.
	log1 := h.makeDiscoveryTriggerLog(unknownVault)
	log2 := h.makeDiscoveryTriggerLog(unknownVault)
	receipt := makeReceipt(testTxHash, log1, log2)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("processBlock must return error so SQS redelivers and BOTH logs are retried; " +
			"a later success for the same address does NOT retroactively save the earlier lost log")
	}
	if !strings.Contains(err.Error(), "connection timeout") {
		t.Errorf("error should surface the first log's transient failure, got: %s", err.Error())
	}

	// Transient RPC failure must NOT permanently mark the address as non-vault.
	if h.svc.vaultRegistry.IsKnownNotVault(unknownVault) {
		t.Error("vault should NOT be marked as not-vault on transient RPC error")
	}
}

// --- Error handling & edge cases ---

func TestProcessBlockEvent_CacheMiss_ReturnsError(t *testing.T) {
	h := newTestHarness(t)
	// Don't store anything in cache — GetReceipts returns nil, nil.
	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 99999, Version: 0,
	})
	if err == nil {
		t.Fatal("expected error for cache miss, got nil")
	}
}

func TestProcessBlockEvent_CacheConnectionError(t *testing.T) {
	h := newTestHarness(t)
	// Set an error on the mock cache to simulate a connection failure.
	h.cache.SetError(testutil.ErrCacheClosed)

	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 99999, Version: 0,
	})
	if err == nil {
		t.Fatal("expected error for cache connection failure")
	}
	if !strings.Contains(err.Error(), "fetching receipts from cache") {
		t.Errorf("error should mention cache, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_EmptyReceipt(t *testing.T) {
	h := newTestHarness(t)
	receipt := makeReceipt(testTxHash) // no logs
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("expected nil for empty receipt, got: %v", err)
	}
}

func TestProcessBlockEvent_IrrelevantLogs(t *testing.T) {
	h := newTestHarness(t)
	// Log from random address with random topic — not Morpho Blue or MetaMorpho.
	irrelevantLog := shared.Log{
		Address: "0x0000000000000000000000000000000000000001",
		Topics:  []string{"0x0000000000000000000000000000000000000000000000000000000000000001"},
		Data:    "",
	}
	receipt := makeReceipt(testTxHash, irrelevantLog)
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("expected nil for irrelevant logs, got: %v", err)
	}
}

func TestProcessReceipt_NoRelevantEvents_SkipsSpan(t *testing.T) {
	h := newTestHarness(t)

	// Wire a real tracer so we can verify no processReceipt span is created.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})

	telemetry, err := NewTelemetryWithProviders(tp, noop.NewMeterProvider(), "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}
	h.svc.telemetry = telemetry

	// Ensure no downstream work happens.
	var multicallCalled atomic.Int32
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		multicallCalled.Add(1)
		return nil, errors.New("should not be called")
	}

	// Receipt with only irrelevant logs — no Morpho Blue or MetaMorpho events.
	irrelevantLog := shared.Log{
		Address: "0x0000000000000000000000000000000000000001",
		Topics:  []string{"0x0000000000000000000000000000000000000000000000000000000000000001"},
		Data:    "",
	}
	receipt := makeReceipt(testTxHash, irrelevantLog)

	if err := h.svc.processReceipt(context.Background(), receipt, 1, 20000000, testBlockHash, 0, time.Now()); err != nil {
		t.Fatalf("processReceipt: %v", err)
	}

	// Force flush spans.
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Errorf("force flush spans: %v", err)
	}

	// Verify no morpho.processReceipt span was created.
	spans := exporter.GetSpans()
	for _, s := range spans {
		if s.Name == "morpho.processReceipt" {
			t.Error("morpho.processReceipt span should not be created for receipts with no relevant events")
		}
	}

	if multicallCalled.Load() != 0 {
		t.Error("multicall should not be called when receipt has no relevant events")
	}
}

func TestProcessReceipt_KnownNotVault_SkipsSpan(t *testing.T) {
	h := newTestHarness(t)

	// Wire a real tracer so we can verify no processReceipt span is created.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})

	telemetry, err := NewTelemetryWithProviders(tp, noop.NewMeterProvider(), "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}
	h.svc.telemetry = telemetry

	// Mark the address as a known non-vault.
	knownNotVault := common.HexToAddress("0x0000000000000000000000000000000000000099")
	h.svc.vaultRegistry.MarkNotVault(knownNotVault)

	// Build a receipt with a Transfer event (matches MetaMorpho ABI) from a
	// known-not-vault address. This used to create an empty span.
	transferTopic := h.svc.eventExtractor.metaMorphoABI.Events["Transfer"].ID.Hex()
	receipt := makeReceipt(testTxHash, shared.Log{
		Address: knownNotVault.Hex(),
		Topics: []string{
			transferTopic,
			"0x0000000000000000000000000000000000000000000000000000000000000001",
			"0x0000000000000000000000000000000000000000000000000000000000000002",
		},
		Data: "0x0000000000000000000000000000000000000000000000000000000000000064",
	})

	if err := h.svc.processReceipt(context.Background(), receipt, 1, 20000000, testBlockHash, 0, time.Now()); err != nil {
		t.Fatalf("processReceipt: %v", err)
	}

	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Errorf("force flush spans: %v", err)
	}

	spans := exporter.GetSpans()
	for _, s := range spans {
		if s.Name == "morpho.processReceipt" {
			t.Error("morpho.processReceipt span should not be created for receipts from known-not-vault addresses")
		}
	}
}

func TestProcessBlockEvent_MultipleReceipts_OneError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)

	// First call succeeds, subsequent calls fail.
	var callCount atomic.Int32
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		n := callCount.Add(1)
		if n == 1 {
			return []outbound.Result{h.defaultMarketStateResult(), h.defaultPositionStateResult()}, nil
		}
		return nil, errors.New("rpc failure")
	}

	log1 := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	log2 := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(2000), big.NewInt(1800))
	receipt1 := makeReceipt("0xaaa", log1)
	receipt2 := makeReceipt("0xbbb", log2)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt1, receipt2})
	if err == nil {
		t.Fatal("expected error when one receipt fails")
	}
	// Should contain the RPC error.
	if !strings.Contains(err.Error(), "rpc failure") {
		t.Errorf("error should contain 'rpc failure', got: %s", err.Error())
	}
}

func TestProcessBlockEvent_LogAtMorphoBlueAddress_UnknownTopic(t *testing.T) {
	h := newTestHarness(t)

	// A log at MorphoBlue address but with an unknown topic that still matches
	// a MetaMorpho event signature. This should be skipped (line 246-248).
	transferEvent := h.metaMorphoEventsABI.Events["Transfer"]
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	data, _ := transferEvent.Inputs.NonIndexed().Pack(big.NewInt(5000))

	log := shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			transferEvent.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	receipt := makeReceipt(testTxHash, log)

	// Should not error — just skip.
	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err != nil {
		t.Fatalf("expected nil for skipped log, got: %v", err)
	}
}

// --- Start/Stop lifecycle ---

func TestStartStop(t *testing.T) {
	h := newTestHarness(t)

	// Override GetAllVaults to return a vault.
	h.morphoRepo.GetAllVaultsFn = func(_ context.Context, _ int64) (map[common.Address]*entity.MorphoVault, error) {
		return map[common.Address]*entity.MorphoVault{
			testVaultAddr: {
				ID:             1,
				ChainID:        1,
				ProtocolID:     1,
				Address:        testVaultAddr.Bytes(),
				Name:           "Test",
				Symbol:         "TST",
				AssetTokenID:   1,
				VaultVersion:   entity.MorphoVaultV1,
				CreatedAtBlock: 18000000,
			},
		}, nil
	}

	ctx := context.Background()
	if err := h.svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if h.svc.vaultRegistry.Count() != 1 {
		t.Errorf("vaultRegistry.Count() = %d, want 1", h.svc.vaultRegistry.Count())
	}

	if err := h.svc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStart_EmptyRegistry(t *testing.T) {
	h := newTestHarness(t)
	h.morphoRepo.GetAllVaultsFn = func(_ context.Context, _ int64) (map[common.Address]*entity.MorphoVault, error) {
		return nil, nil
	}

	ctx := context.Background()
	if err := h.svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if h.svc.vaultRegistry.Count() != 0 {
		t.Errorf("vaultRegistry.Count() = %d, want 0", h.svc.vaultRegistry.Count())
	}

	_ = h.svc.Stop()
}

func TestStart_RegistryLoadFailure(t *testing.T) {
	h := newTestHarness(t)
	h.morphoRepo.GetAllVaultsFn = func(_ context.Context, _ int64) (map[common.Address]*entity.MorphoVault, error) {
		return nil, errors.New("db unavailable")
	}

	err := h.svc.Start(context.Background())
	if err == nil {
		_ = h.svc.Stop()
		t.Fatal("Start should fail when vault registry cannot be loaded")
	}
	if !strings.Contains(err.Error(), "loading vault registry") {
		t.Errorf("expected 'loading vault registry' error, got: %v", err)
	}
}

func TestStart_RefusesAVisibilityTimeoutAReceiveCanOutrun(t *testing.T) {
	h := newTestHarness(t)
	h.consumer.VisibilityTimeoutFn = func() time.Duration { return 30 * time.Second }

	err := h.svc.Start(context.Background())
	if err == nil {
		_ = h.svc.Stop()
		t.Fatal("Start accepted a 30s visibility timeout; a booted worker never crashloops on it, because " +
			"ProcessMessages revalidates on every poll and RunLoop only logs what it returns, so the pod reports " +
			"Ready and spins logging forever while the queue never drains")
	}
	if !strings.Contains(err.Error(), "visibility timeout") {
		t.Errorf("Start error = %q, want it to name the visibility timeout", err)
	}
}

func TestStop_NilCancel(t *testing.T) {
	h := newTestHarness(t)
	h.svc.cancel = nil
	if err := h.svc.Stop(); err != nil {
		t.Fatalf("Stop with nil cancel: %v", err)
	}
}

// --- Protocol event saving ---

func TestProcessBlockEvent_SavesProtocolEvent(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	var savedEvent *entity.ProtocolEvent
	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
		savedEvent = e
		return nil
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	if err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if savedEvent == nil {
		t.Fatal("SaveEvent not called")
	}
	if savedEvent.EventName != "Supply" {
		t.Errorf("EventName = %s, want Supply", savedEvent.EventName)
	}
	if savedEvent.BlockNumber != 20000000 {
		t.Errorf("BlockNumber = %d, want 20000000", savedEvent.BlockNumber)
	}
}

// --- saveProtocolEvent error paths ---

func TestProcessBlockEvent_SaveProtocolEvent_SaveEventError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	h.eventRepo.SaveEventFn = func(_ context.Context, _ pgx.Tx, _ *entity.ProtocolEvent) error {
		return errors.New("event repo save failed")
	}

	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "saving protocol event") {
		t.Errorf("error should mention saving protocol event, got: %s", err.Error())
	}
}

func TestProcessBlockEvent_SaveProtocolEvent_GetOrCreateProtocolError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	h.protocolRepo.GetOrCreateProtocolFn = func(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _ string, _ string, _ int64) (int64, error) {
		return 0, errors.New("protocol repo error")
	}

	log := h.makeSetFeeLog(testMarketID, big.NewInt(100))
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "protocol repo error") {
		t.Errorf("error should propagate, got: %s", err.Error())
	}
}

// --- processMetaMorphoLog extraction error ---

func TestProcessBlockEvent_MetaMorphoLog_ExtractionError(t *testing.T) {
	h := newTestHarness(t)
	h.registerTestVault(testVaultAddr, 7, entity.MorphoVaultV1)

	// Create a log with valid MetaMorpho topic but invalid data.
	depositEvent := h.metaMorphoEventsABI.Events["Deposit"]
	log := shared.Log{
		Address: testVaultAddr.Hex(),
		Topics: []string{
			depositEvent.ID.Hex(),
			common.BytesToHash(testCaller.Bytes()).Hex(),
			common.BytesToHash(testOnBehalf.Bytes()).Hex(),
		},
		Data:            "invalid_hex_data",
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for extraction failure")
	}
	if !strings.Contains(err.Error(), "extracting MetaMorpho event") {
		t.Errorf("error should mention extraction, got: %s", err.Error())
	}
}

// --- processMorphoBlueLog extraction error ---

func TestProcessBlockEvent_MorphoBlueLog_ExtractionError(t *testing.T) {
	h := newTestHarness(t)

	// Create a log with valid Morpho Blue Supply topic but invalid data.
	supplyEvent := h.morphoBlueEventsABI.Events["Supply"]
	log := shared.Log{
		Address: MorphoBlueAddress.Hex(),
		Topics: []string{
			supplyEvent.ID.Hex(),
			common.Hash(testMarketID).Hex(),
			common.BytesToHash(testCaller.Bytes()).Hex(),
			common.BytesToHash(testOnBehalf.Bytes()).Hex(),
		},
		Data:            "invalid_hex_data",
		TransactionHash: testTxHash,
		LogIndex:        "0x0",
	}
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for extraction failure")
	}
	if !strings.Contains(err.Error(), "extracting Morpho Blue event") {
		t.Errorf("error should mention extraction, got: %s", err.Error())
	}
}

// --- Log index parsing error ---

func TestProcessBlockEvent_InvalidLogIndex(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.setupPositionEventMulticall()

	// Create a supply log with an unparseable LogIndex.
	log := h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))
	log.LogIndex = "not_a_number"
	receipt := makeReceipt(testTxHash, log)

	err := h.processBlock(t, 1, 20000000, 0, []shared.TransactionReceipt{receipt})
	if err == nil {
		t.Fatal("expected error for invalid log index")
	}
	if !strings.Contains(err.Error(), "parsing log index") {
		t.Errorf("error should mention parsing log index, got: %s", err.Error())
	}
}

func TestReconcilePendingSymbols_ResolvesAtCurrentBlock(t *testing.T) {
	h := newTestHarness(t)

	pending := common.HexToAddress("0x2f010444C6a61feaEBCDd4040fA8B30F519e6c31")
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		return []common.Address{pending}, nil
	}
	resolved := map[common.Address]string{}
	h.tokenRepo.ResolveTokenSymbolFn = func(_ context.Context, _ int64, address common.Address, symbol string) error {
		resolved[address] = symbol
		return nil
	}
	var sawBlock *big.Int
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		sawBlock = blockNumber
		return []outbound.Result{{Success: true, ReturnData: h.packString("stakedao-frxUsDOLA")}}, nil
	}

	// Block 25252160 is a sweep block (multiple of 10).
	h.svc.reconcilePendingSymbols(context.Background(), 1, 25252160)

	if resolved[pending] != "stakedao-frxUsDOLA" {
		t.Fatalf("resolved = %v, want symbol persisted for %s", resolved, pending.Hex())
	}
	if sawBlock == nil || sawBlock.Int64() != 25252160 {
		t.Fatalf("symbol read at block %v, want 25252160 (the block being processed)", sawBlock)
	}
}

// TestReconcilePendingSymbols_FullBatchStillSweeps covers the batch-full branch:
// a full batch (potential truncation) warns but still resolves what it fetched.
func TestReconcilePendingSymbols_FullBatchStillSweeps(t *testing.T) {
	h := newTestHarness(t)

	full := make([]common.Address, symbolSweepBatchSize)
	for i := range full {
		full[i] = common.BigToAddress(big.NewInt(int64(i + 1)))
	}
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		return full, nil
	}
	h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range results {
			results[i] = outbound.Result{Success: false}
		}
		// Only the first token resolves; the rest still revert.
		results[0] = outbound.Result{Success: true, ReturnData: h.packString("FIRST")}
		return results, nil
	}
	var resolved []common.Address
	h.tokenRepo.ResolveTokenSymbolFn = func(_ context.Context, _ int64, address common.Address, _ string) error {
		resolved = append(resolved, address)
		return nil
	}

	h.svc.reconcilePendingSymbols(context.Background(), 1, 100)

	if len(resolved) != 1 || resolved[0] != full[0] {
		t.Errorf("resolved = %v, want exactly the first token despite the full batch", resolved)
	}
}

func TestReconcilePendingSymbols_NonSweepBlockIsNoop(t *testing.T) {
	h := newTestHarness(t)
	listed := false
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		listed = true
		return nil, nil
	}
	h.svc.reconcilePendingSymbols(context.Background(), 1, 25252161) // not a multiple of 10
	if listed {
		t.Error("non-sweep block must not query missing-symbol tokens")
	}
}

func TestReconcilePendingSymbols_ListErrorDoesNotPanicOrPropagate(t *testing.T) {
	h := newTestHarness(t)
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		return nil, fmt.Errorf("db down")
	}
	// Best-effort: must not panic. It is a void method, so nothing to assert beyond no panic.
	h.svc.reconcilePendingSymbols(context.Background(), 1, 100)
}

// --- processBlockEvent / reconcilePendingSymbols integration ---

// TestProcessBlockEvent_FetchError_SkipsReconcile asserts that when
// fetchAndProcessReceipts fails (cache miss), reconcilePendingSymbols is never
// reached. Reconcile is best-effort and runs ONLY after a successful block
// fetch; a failed block must propagate its error without touching the token
// repo.
func TestProcessBlockEvent_FetchError_SkipsReconcile(t *testing.T) {
	h := newTestHarness(t)

	listCalled := false
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
		listCalled = true
		return nil, nil
	}

	// Do NOT store receipts in cache: GetReceipts returns nil, nil which
	// fetchAndProcessReceipts converts to a "receipts not found" error.
	err := h.svc.processBlockEvent(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 20, Version: 0,
	})
	if err == nil {
		t.Fatal("expected error for cache miss, got nil")
	}
	if listCalled {
		t.Error("ListTokensMissingSymbol must not be called when fetchAndProcessReceipts fails")
	}
}

// TestProcessBlockEvent_Success_RunsReconcileOnSweepBlock asserts that on a
// successful block that falls on a sweep-eligible block number (multiple of
// symbolSweepIntervalBlocks), reconcilePendingSymbols is called with the
// correct chainID. Empty receipts are used so no Morpho-specific processing
// is needed.
func TestProcessBlockEvent_Success_RunsReconcileOnSweepBlock(t *testing.T) {
	h := newTestHarness(t)

	var listCalledWithChainID int64
	listCalled := false
	h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, chainID int64, _ int) ([]common.Address, error) {
		listCalled = true
		listCalledWithChainID = chainID
		return nil, nil // empty list, nothing to reconcile
	}

	// Block 20 is a multiple of symbolSweepIntervalBlocks (10).
	if err := h.processBlock(t, 1, 20, 0, []shared.TransactionReceipt{}); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if !listCalled {
		t.Error("ListTokensMissingSymbol must be called on a sweep-eligible block after successful processing")
	}
	if listCalledWithChainID != 1 {
		t.Errorf("ListTokensMissingSymbol called with chainID %d, want 1", listCalledWithChainID)
	}
}

// TestReconcilePendingSymbols_SwallowedErrors covers the two best-effort
// error-swallow branches inside reconcilePendingSymbols. None of them must
// panic or propagate an error (the method is void).
func TestReconcilePendingSymbols_SwallowedErrors(t *testing.T) {
	addr1 := common.HexToAddress("0xAAAA000000000000000000000000000000001111")
	addr2 := common.HexToAddress("0xBBBB000000000000000000000000000000002222")

	t.Run("ResolveSymbolsAt_error_does_not_propagate", func(t *testing.T) {
		// ResolveSymbolsAt errors (multicaller returns error). reconcilePendingSymbols
		// must swallow it. ResolveTokenSymbolFn must NOT be called.
		h := newTestHarness(t)

		h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
			return []common.Address{addr1}, nil
		}
		h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			return nil, errors.New("rpc timeout")
		}
		resolveCalled := false
		h.tokenRepo.ResolveTokenSymbolFn = func(_ context.Context, _ int64, _ common.Address, _ string) error {
			resolveCalled = true
			return nil
		}

		h.svc.reconcilePendingSymbols(context.Background(), 1, 2000)

		if resolveCalled {
			t.Error("ResolveTokenSymbol must not be called when ResolveSymbolsAt errors")
		}
	})

	t.Run("ResolveTokenSymbol_error_continues_to_next_token", func(t *testing.T) {
		// ResolveTokenSymbolFn returns an error for ONE of TWO resolved tokens.
		// The other token's persist must still be attempted — one failing persist
		// must not drop the rest.
		h := newTestHarness(t)

		h.tokenRepo.ListTokensMissingSymbolFn = func(_ context.Context, _ int64, _ int) ([]common.Address, error) {
			return []common.Address{addr1, addr2}, nil
		}
		// Both symbols resolve successfully via multicall.
		h.multicaller.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			results := make([]outbound.Result, len(calls))
			for i := range calls {
				results[i] = outbound.Result{Success: true, ReturnData: h.packString("SYM")}
			}
			return results, nil
		}

		// ResolveTokenSymbol fails for the first address it sees; succeeds for the second.
		var resolveAttempted []common.Address
		h.tokenRepo.ResolveTokenSymbolFn = func(_ context.Context, _ int64, address common.Address, _ string) error {
			resolveAttempted = append(resolveAttempted, address)
			if len(resolveAttempted) == 1 {
				return errors.New("persist failed")
			}
			return nil
		}

		h.svc.reconcilePendingSymbols(context.Background(), 1, 2000)

		// Both addresses must have been attempted regardless of the first failure.
		if len(resolveAttempted) != 2 {
			t.Errorf("ResolveTokenSymbol called %d times, want 2 (one failure must not skip the other)", len(resolveAttempted))
		}
	})
}

// TestProcessBlockEvent_MissingBlockHash_ReturnsError: an event with an empty
// BlockHash must fail loud before ever reaching the multicaller, instead of
// silently defaulting to the zero hash (common.HexToHash never errors).
func TestProcessBlockEvent_MissingBlockHash_ReturnsError(t *testing.T) {
	h := newTestHarness(t)
	h.setupMarketExistsInDB(testMarketID, 42)
	h.storeReceipts(t, 1, 20000000, 0, []shared.TransactionReceipt{
		makeReceipt(testTxHash, h.makeSupplyLog(testMarketID, testCaller, testOnBehalf, big.NewInt(1000), big.NewInt(900))),
	})

	var multicallCalled atomic.Int32
	h.multicaller.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		multicallCalled.Add(1)
		return nil, fmt.Errorf("multicaller must not be called")
	}
	var stateSaved, positionSaved int32
	h.morphoRepo.SaveMarketStateFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketState) error {
		atomic.AddInt32(&stateSaved, 1)
		return nil
	}
	h.morphoRepo.SaveMarketPositionFn = func(_ context.Context, _ pgx.Tx, _ *entity.MorphoMarketPosition) error {
		atomic.AddInt32(&positionSaved, 1)
		return nil
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 20000000, Version: 0, BlockHash: ""}
	if err := h.svc.processBlockEvent(context.Background(), event); err == nil {
		t.Fatal("expected non-nil error from processBlockEvent when event.BlockHash is empty")
	}

	if multicallCalled.Load() != 0 {
		t.Error("multicaller invoked, want it never called")
	}
	if atomic.LoadInt32(&stateSaved) != 0 {
		t.Error("SaveMarketState invoked, want it never called (block must not be persisted)")
	}
	if atomic.LoadInt32(&positionSaved) != 0 {
		t.Error("SaveMarketPosition invoked, want it never called (block must not be persisted)")
	}
}

// Suppress unused import warnings.
var (
	_ = testutil.DiscardLogger
)
