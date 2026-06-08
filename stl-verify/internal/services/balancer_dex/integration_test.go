//go:build integration

package balancer_dex

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// TestIntegration_SwapWritesAllRows verifies that processing a Vault.Swap log
// on the seeded wstETH-WETH-CSP pool produces the expected protocol_event +
// balancer_pool_swap + balancer_pool_state rows in one transaction. The
// migration seed is the source of truth — pool_id is already populated.
func TestIntegration_SwapWritesAllRows(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	t.Setenv("BUILD_GIT_HASH", "integration-test-swap-writes-all-rows")

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry.New: %v", err)
	}

	txm, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	protoRepo, err := postgres.NewProtocolRepository(pool, nil, buildReg.BuildID(), 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	eventRepo := postgres.NewEventRepository(nil, buildReg.BuildID())
	balancerRepo, err := postgres.NewBalancerPoolRepository(pool, nil, buildReg.BuildID())
	if err != nil {
		t.Fatalf("NewBalancerPoolRepository: %v", err)
	}

	mc := testutil.NewMockMulticaller()
	cache := testutil.NewMockBlockCache()
	consumer := &testutil.MockSQSConsumer{}

	cfg := Config{SQSConsumerConfig: shared.SQSConsumerConfigDefaults()}
	cfg.ChainID = 1
	svc, err := NewService(cfg, consumer, cache, mc, txm, balancerRepo, tokenRepo, protoRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// Start loads the seeded wstETH-WETH-CSP pool from the migration.
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = svc.Stop() }()

	if svc.registry.poolCount() != 1 {
		t.Fatalf("expected 1 seeded pool, got %d", svc.registry.poolCount())
	}

	poolAddr := common.HexToAddress("0x93d199263632a4EF4Bb438F1feB99e57b4b5f0BD")
	poolID := common.HexToHash("0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd0000000000000000000005c2")
	wstETHAddr := common.HexToAddress("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0")
	wethAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")

	// First multicall: populatePoolTokens (registry has no token slots yet).
	// Second multicall: readPoolState during Swap handling.
	addrSliceT, _ := abi.NewType("address[]", "", nil)
	uintSliceT, _ := abi.NewType("uint256[]", "", nil)
	uintT, _ := abi.NewType("uint256", "", nil)
	boolT, _ := abi.NewType("bool", "", nil)

	getPoolArgs := abi.Arguments{{Type: addrSliceT}, {Type: uintSliceT}, {Type: uintT}}
	gptData, _ := getPoolArgs.Pack(
		[]common.Address{wstETHAddr, poolAddr, wethAddr},
		[]*big.Int{big.NewInt(1000), big.NewInt(0), big.NewInt(2000)},
		big.NewInt(42),
	)
	ampArgs := abi.Arguments{{Type: uintT}, {Type: boolT}, {Type: uintT}}
	ampData, _ := ampArgs.Pack(big.NewInt(50000), false, big.NewInt(1000))
	pausedArgs := abi.Arguments{{Type: boolT}, {Type: uintT}, {Type: uintT}}
	pausedData, _ := pausedArgs.Pack(false, big.NewInt(0), big.NewInt(0))
	sfArgs := abi.Arguments{{Type: uintSliceT}}
	sfData, _ := sfArgs.Pack([]*big.Int{big.NewInt(1e18), big.NewInt(1e18), big.NewInt(1e18)})
	uintArgs := abi.Arguments{{Type: uintT}}
	mkUint := func(v int64) []byte {
		out, _ := uintArgs.Pack(big.NewInt(v))
		return out
	}

	// ERC20 metadata batch response: 3 tokens × (symbol+decimals) = 6 results.
	strT, _ := abi.NewType("string", "", nil)
	uint8T, _ := abi.NewType("uint8", "", nil)
	symOut, _ := (abi.Arguments{{Type: strT}}).Pack("TKN")
	decOut, _ := (abi.Arguments{{Type: uint8T}}).Pack(uint8(18))
	metaBatchResp := []outbound.Result{
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
		{Success: true, ReturnData: symOut}, {Success: true, ReturnData: decOut},
	}

	callIdx := 0
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		callIdx++
		switch callIdx {
		case 1:
			// vault.getPoolTokens
			return []outbound.Result{{Success: true, ReturnData: gptData}}, nil
		case 2:
			// readERC20MetadataBatch — 3 tokens × 2 sub-calls
			return metaBatchResp, nil
		default:
			// readPoolState — 10 sub-calls
			return []outbound.Result{
				{Success: true, ReturnData: gptData},
				{Success: true, ReturnData: ampData},
				{Success: true, ReturnData: mkUint(int64(1.05e18))},
				{Success: true, ReturnData: mkUint(int64(1.0e18))},
				{Success: true, ReturnData: mkUint(int64(2.5e18))},
				{Success: true, ReturnData: sfData},
				{Success: true, ReturnData: mkUint(int64(4e14))},
				{Success: true, ReturnData: pausedData},
				{Success: true, ReturnData: mkUint(int64(1.18e18))},
				{Success: true, ReturnData: mkUint(int64(1.0e18))},
			}, nil
		}
	}

	// Build a Vault.Swap log.
	vaultABI, err := loadVaultABI()
	if err != nil {
		t.Fatalf("loadVaultABI: %v", err)
	}
	swapEv := vaultABI.Events["Swap"]
	data, err := swapEv.Inputs.NonIndexed().Pack(big.NewInt(100), big.NewInt(99))
	if err != nil {
		t.Fatalf("packing Swap: %v", err)
	}
	log := shared.Log{
		Address: BalancerVaultAddress.Hex(),
		Topics: []string{
			swapEv.ID.Hex(),
			poolID.Hex(),
			common.BytesToHash(wstETHAddr.Bytes()).Hex(),
			common.BytesToHash(wethAddr.Bytes()).Hex(),
		},
		Data:            "0x" + common.Bytes2Hex(data),
		TransactionHash: "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		LogIndex:        "0x1",
	}
	receipt := shared.TransactionReceipt{TransactionHash: log.TransactionHash, Logs: []shared.Log{log}}
	body, _ := json.Marshal([]shared.TransactionReceipt{receipt})
	cache.SetReceipts(1, 19000000, 0, body)

	if err := svc.processBlockEvent(ctx, outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    19000000,
		Version:        0,
		BlockTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	// Assert protocol_event row landed.
	var peCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM protocol_event WHERE event_name = 'Swap'`).Scan(&peCount); err != nil {
		t.Fatalf("count protocol_event: %v", err)
	}
	if peCount != 1 {
		t.Errorf("protocol_event Swap count = %d, want 1", peCount)
	}

	// Assert balancer_pool_swap row landed.
	var swapCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM balancer_pool_swap`).Scan(&swapCount); err != nil {
		t.Fatalf("count balancer_pool_swap: %v", err)
	}
	if swapCount != 1 {
		t.Errorf("balancer_pool_swap count = %d, want 1", swapCount)
	}

	// Assert balancer_pool_state row landed.
	var stateCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM balancer_pool_state`).Scan(&stateCount); err != nil {
		t.Fatalf("count balancer_pool_state: %v", err)
	}
	if stateCount != 1 {
		t.Errorf("balancer_pool_state count = %d, want 1", stateCount)
	}

	// Assert balancer_pool_token has 3 slots, with slot 1 marked is_phantom.
	var bptCount, phantomCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM balancer_pool_token`).Scan(&bptCount); err != nil {
		t.Fatalf("count balancer_pool_token: %v", err)
	}
	if bptCount != 3 {
		t.Errorf("balancer_pool_token count = %d, want 3 (incl. phantom)", bptCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM balancer_pool_token WHERE is_phantom = true`).Scan(&phantomCount); err != nil {
		t.Fatalf("count phantom: %v", err)
	}
	if phantomCount != 1 {
		t.Errorf("phantom slot count = %d, want 1", phantomCount)
	}

	// Ensure registry view is hydrated.
	if entry := svc.registry.poolByAddress(poolAddr); entry == nil || entry.tokenCount() != 3 {
		t.Errorf("registry not hydrated after populate: %+v", entry)
	}

	_ = entity.BalancerPool{} // keep import non-trivial
}

func loadVaultABI() (*abi.ABI, error) {
	return abis.GetBalancerV2VaultEventsABI()
}
