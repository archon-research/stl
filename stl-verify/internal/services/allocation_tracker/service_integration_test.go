//go:build integration

package allocation_tracker

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestIntegration_SubProxyAndAlmProxy_AreIndexedAndQueryable feeds the service
// a block whose receipts contain two USDS Transfer logs — one to the Spark ALM
// proxy, one to the Spark SubProxy — and verifies that:
//
//  1. Both transfers produce allocation_position rows.
//  2. A query scoped to the SubProxy returns exactly one row.
//
// This exercises the full Service → BalanceOfSource → PrimePositionHandler →
// AllocationRepository → Postgres path with a real DB; only the Alchemy RPC
// (multicaller) and the block-cache reader are mocked, in line with the
// "mock only what we cannot control" rule from CLAUDE.md.
func TestIntegration_SubProxyAndAlmProxy_AreIndexedAndQueryable(t *testing.T) {
	ctx := context.Background()

	t.Setenv("BUILD_GIT_HASH", "test-integration-subproxy")

	pool, _, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	// The prime table is seeded by db/migrations/20260305_120000_create_prime_debts.sql.
	var sparkID int64
	if err := pool.QueryRow(ctx, "SELECT id FROM prime WHERE name = 'spark'").Scan(&sparkID); err != nil {
		t.Fatalf("read spark prime id: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry: %v", err)
	}

	txm, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 1)
	if err != nil {
		t.Fatalf("token repo: %v", err)
	}
	allocRepo := postgres.NewAllocationRepository(pool, txm, tokenRepo, logger, buildReg.BuildID())
	supplyRepo := postgres.NewTokenTotalSupplyRepository(pool, txm, tokenRepo, logger, buildReg.BuildID())

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("erc20 abi: %v", err)
	}
	atokenABI, err := abis.GetATokenReadABI()
	if err != nil {
		t.Fatalf("atoken abi: %v", err)
	}

	// Real addresses for the two proxies and USDS, matching DefaultProxies /
	// mainnetEntries. Using real values makes the test failure messages
	// directly traceable to the entries that produced them.
	almProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	subProxy := common.HexToAddress("0x3300f198988e4c9c63f75df86de36421f06af8c4")
	usds := common.HexToAddress("0xdc035d45d973e3ec169d2276ddab16f1e407384f")

	// Balances the on-chain reads would return at this block.
	wad := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	almBalance := new(big.Int).Mul(big.NewInt(50_000_000), wad)
	subBalance := new(big.Int).Mul(big.NewInt(25_000_000), wad)

	mc := newERC20BalanceMockMulticaller(t, erc20ABI, usds, "USDS", 18, map[common.Address]*big.Int{
		almProxy: almBalance,
		subProxy: subBalance,
	})

	primeLookup := map[string]int64{"spark": sparkID}
	pgHandler := NewPrimePositionHandler(allocRepo, supplyRepo, txm, mc, erc20ABI, primeLookup, logger, nil)

	registry := NewSourceRegistry(logger)
	registry.Register(NewBalanceOfSource(mc, erc20ABI, atokenABI, logger))

	entries := []*TokenEntry{
		{ContractAddress: usds, WalletAddress: almProxy, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
		{ContractAddress: usds, WalletAddress: subProxy, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "risk_capital", TokenType: "erc20"},
	}
	proxies := []ProxyConfig{
		{Star: "spark", Chain: "mainnet", Address: almProxy},
		{Star: "spark", Chain: "mainnet", Address: subProxy},
	}

	// Two USDS Transfer logs in one receipt — one inbound to each proxy.
	const blockNumber = int64(19_000_000)
	externalSender := common.HexToAddress("0x9999999999999999999999999999999999999999")
	receiptsJSON := mustMarshalReceipts(t, []TransactionReceipt{{
		Logs: []gethtypes.Log{
			makeTransferLog(usds, externalSender, almProxy, big.NewInt(1_000_000), 0),
			makeTransferLog(usds, externalSender, subProxy, big.NewInt(2_000_000), 1),
		},
	}})
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, blockNumber, 0, receiptsJSON)

	svc, err := NewService(
		// SweepEveryNBlocks is set high so the single block we drive does not
		// trigger a sweep and confuse the row count.
		Config{ChainID: 1, SweepEveryNBlocks: 1000, Logger: logger},
		nil, // no SQS consumer; we drive processBlock directly
		cache,
		registry,
		entries,
		pgHandler,
		proxies,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        0,
		BlockTimestamp: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC).Unix(),
	}
	if err := svc.processBlock(ctx, event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	var total int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM allocation_position").Scan(&total); err != nil {
		t.Fatalf("count total rows: %v", err)
	}
	if total != 2 {
		t.Fatalf("total allocation_position rows: got %d, want 2 (one per matched entry)", total)
	}

	var subCount int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM allocation_position WHERE proxy_address = $1",
		subProxy.Bytes(),
	).Scan(&subCount); err != nil {
		t.Fatalf("count subproxy rows: %v", err)
	}
	if subCount != 1 {
		t.Fatalf("subproxy allocation_position rows: got %d, want 1", subCount)
	}

	var balance string
	if err := pool.QueryRow(ctx,
		"SELECT balance::text FROM allocation_position WHERE proxy_address = $1",
		subProxy.Bytes(),
	).Scan(&balance); err != nil {
		t.Fatalf("query subproxy balance: %v", err)
	}
	// 25M USDS persisted as NUMERIC with 18 decimals.
	const wantBalance = "25000000.000000000000000000"
	if balance != wantBalance {
		t.Errorf("subproxy balance: got %q, want %q", balance, wantBalance)
	}
}

// TestIntegration_SweepPosition_UsesZeroTxHash drives a periodic sweep
// (SweepEveryNBlocks=1) against a real Postgres and asserts that the persisted
// sweep rows carry the zero-hash tx_hash sentinel rather than a fabricated
// synthetic hash (VEC-340). This exercises the full sweep → AllocationRepository
// → Postgres path, confirming the sentinel satisfies the NOT-NULL primary key
// and round-trips, and that repeated sweeps for the same observation dedup to a
// single row.
func TestIntegration_SweepPosition_UsesZeroTxHash(t *testing.T) {
	ctx := context.Background()

	t.Setenv("BUILD_GIT_HASH", "test-integration-sweep-zero-hash")

	pool, _, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	var sparkID int64
	if err := pool.QueryRow(ctx, "SELECT id FROM prime WHERE name = 'spark'").Scan(&sparkID); err != nil {
		t.Fatalf("read spark prime id: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry: %v", err)
	}
	txm, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 1)
	if err != nil {
		t.Fatalf("token repo: %v", err)
	}
	allocRepo := postgres.NewAllocationRepository(pool, txm, tokenRepo, logger, buildReg.BuildID())
	supplyRepo := postgres.NewTokenTotalSupplyRepository(pool, txm, tokenRepo, logger, buildReg.BuildID())

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("erc20 abi: %v", err)
	}
	atokenABI, err := abis.GetATokenReadABI()
	if err != nil {
		t.Fatalf("atoken abi: %v", err)
	}

	almProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	subProxy := common.HexToAddress("0x3300f198988e4c9c63f75df86de36421f06af8c4")
	usds := common.HexToAddress("0xdc035d45d973e3ec169d2276ddab16f1e407384f")

	wad := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	almBalance := new(big.Int).Mul(big.NewInt(50_000_000), wad)
	subBalance := new(big.Int).Mul(big.NewInt(25_000_000), wad)

	mc := newERC20BalanceMockMulticaller(t, erc20ABI, usds, "USDS", 18, map[common.Address]*big.Int{
		almProxy: almBalance,
		subProxy: subBalance,
	})

	primeLookup := map[string]int64{"spark": sparkID}
	pgHandler := NewPrimePositionHandler(allocRepo, supplyRepo, txm, mc, erc20ABI, primeLookup, logger, nil)

	registry := NewSourceRegistry(logger)
	registry.Register(NewBalanceOfSource(mc, erc20ABI, atokenABI, logger))

	entries := []*TokenEntry{
		{ContractAddress: usds, WalletAddress: almProxy, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
		{ContractAddress: usds, WalletAddress: subProxy, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "risk_capital", TokenType: "erc20"},
	}
	proxies := []ProxyConfig{
		{Star: "spark", Chain: "mainnet", Address: almProxy},
		{Star: "spark", Chain: "mainnet", Address: subProxy},
	}

	// Empty receipts: no Transfer logs, so the only rows are sweep observations.
	const blockNumber = int64(19_000_000)
	receiptsJSON := mustMarshalReceipts(t, []TransactionReceipt{})
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, blockNumber, 0, receiptsJSON)

	svc, err := NewService(
		// SweepEveryNBlocks=1 so the single driven block triggers a sweep.
		Config{ChainID: 1, SweepEveryNBlocks: 1, Logger: logger},
		nil,
		cache,
		registry,
		entries,
		pgHandler,
		proxies,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        0,
		BlockTimestamp: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC).Unix(),
	}
	// Drive the same block twice: idempotent reprocessing must not create
	// duplicate sweep rows despite the constant (zero) tx_hash.
	if err := svc.processBlock(ctx, event); err != nil {
		t.Fatalf("processBlock (1): %v", err)
	}
	if err := svc.processBlock(ctx, event); err != nil {
		t.Fatalf("processBlock (2): %v", err)
	}

	var sweepRows int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM allocation_position WHERE direction = 'sweep'",
	).Scan(&sweepRows); err != nil {
		t.Fatalf("count sweep rows: %v", err)
	}
	if sweepRows != 2 {
		t.Fatalf("sweep rows: got %d, want 2 (one per entry, deduped across reprocessing)", sweepRows)
	}

	// Every sweep row must carry the all-zero sentinel, not a synthetic hash.
	var nonZero int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM allocation_position WHERE direction = 'sweep' AND tx_hash <> $1",
		make([]byte, 32),
	).Scan(&nonZero); err != nil {
		t.Fatalf("count non-zero sweep tx_hash: %v", err)
	}
	if nonZero != 0 {
		t.Fatalf("sweep rows with non-zero tx_hash: got %d, want 0", nonZero)
	}
}

// TestIntegration_UsdcTransferToSubProxy_IsIgnored is the negative complement
// to the test above. The SubProxy is a tracked address (so the
// TransferExtractor will emit a TransferEvent for transfers touching it), but
// USDC is intentionally not registered as a TokenEntry for any proxy. The
// indexer must therefore ignore the receipt's USDC Transfer log entirely:
//
//  1. No row is written to allocation_position.
//  2. The Multicaller is never called — the filter happens at entry-lookup,
//     before any on-chain fetch.
func TestIntegration_UsdcTransferToSubProxy_IsIgnored(t *testing.T) {
	ctx := context.Background()

	t.Setenv("BUILD_GIT_HASH", "test-integration-usdc-ignored")

	pool, _, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	var sparkID int64
	if err := pool.QueryRow(ctx, "SELECT id FROM prime WHERE name = 'spark'").Scan(&sparkID); err != nil {
		t.Fatalf("read spark prime id: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry: %v", err)
	}
	txm, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		t.Fatalf("tx manager: %v", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 1)
	if err != nil {
		t.Fatalf("token repo: %v", err)
	}
	allocRepo := postgres.NewAllocationRepository(pool, txm, tokenRepo, logger, buildReg.BuildID())
	supplyRepo := postgres.NewTokenTotalSupplyRepository(pool, txm, tokenRepo, logger, buildReg.BuildID())

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("erc20 abi: %v", err)
	}
	atokenABI, err := abis.GetATokenReadABI()
	if err != nil {
		t.Fatalf("atoken abi: %v", err)
	}

	almProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	subProxy := common.HexToAddress("0x3300f198988e4c9c63f75df86de36421f06af8c4")
	usds := common.HexToAddress("0xdc035d45d973e3ec169d2276ddab16f1e407384f")
	usdc := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

	// Any call into the multicaller fails the test — proves the filter happens
	// before the on-chain read, not after.
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Errorf("multicaller invoked with %d sub-calls — no fetch should happen for an unregistered token", len(calls))
		return make([]outbound.Result, len(calls)), nil
	}

	primeLookup := map[string]int64{"spark": sparkID}
	pgHandler := NewPrimePositionHandler(allocRepo, supplyRepo, txm, mc, erc20ABI, primeLookup, logger, nil)

	registry := NewSourceRegistry(logger)
	registry.Register(NewBalanceOfSource(mc, erc20ABI, atokenABI, logger))

	// USDS is registered for both proxies (mirroring real config). USDC is
	// intentionally NOT registered for either proxy.
	entries := []*TokenEntry{
		{ContractAddress: usds, WalletAddress: almProxy, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "pol", TokenType: "erc20"},
		{ContractAddress: usds, WalletAddress: subProxy, Star: "spark", Chain: "mainnet", Protocol: "sky", AllocationType: "risk_capital", TokenType: "erc20"},
	}
	proxies := []ProxyConfig{
		{Star: "spark", Chain: "mainnet", Address: almProxy},
		{Star: "spark", Chain: "mainnet", Address: subProxy},
	}

	const blockNumber = int64(19_000_000)
	externalSender := common.HexToAddress("0x9999999999999999999999999999999999999999")
	receiptsJSON := mustMarshalReceipts(t, []TransactionReceipt{{
		Logs: []gethtypes.Log{
			makeTransferLog(usdc, externalSender, subProxy, big.NewInt(3_000_000), 0),
		},
	}})
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, blockNumber, 0, receiptsJSON)

	svc, err := NewService(
		Config{ChainID: 1, SweepEveryNBlocks: 1000, Logger: logger},
		nil,
		cache,
		registry,
		entries,
		pgHandler,
		proxies,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        0,
		BlockTimestamp: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC).Unix(),
	}
	if err := svc.processBlock(ctx, event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	var total int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM allocation_position").Scan(&total); err != nil {
		t.Fatalf("count total rows: %v", err)
	}
	if total != 0 {
		t.Fatalf("allocation_position rows: got %d, want 0 (USDC transfer must be ignored)", total)
	}

	var subCount int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM allocation_position WHERE proxy_address = $1",
		subProxy.Bytes(),
	).Scan(&subCount); err != nil {
		t.Fatalf("count subproxy rows: %v", err)
	}
	if subCount != 0 {
		t.Fatalf("subproxy allocation_position rows: got %d, want 0", subCount)
	}

	if mc.CallCount != 0 {
		t.Errorf("multicaller CallCount: got %d, want 0 (no fetch should be triggered when no entry matches)", mc.CallCount)
	}
}

// newERC20BalanceMockMulticaller returns a MockMulticaller that dispatches by
// selector: balanceOf(wallet) returns balances[wallet], decimals() returns
// `decimals`, symbol() returns `symbol`. Any other call (or balanceOf for an
// unknown wallet) fails the test loudly — silent zero-substitution would mask
// real bugs in the call-building or routing path. Calls whose Target is not
// `tokenAddress` also fail the test, catching production-side targeting bugs
// (e.g. balanceOf invoked against the wrong contract).
func newERC20BalanceMockMulticaller(
	t *testing.T,
	erc20ABI *abi.ABI,
	tokenAddress common.Address,
	symbol string,
	decimals uint8,
	balances map[common.Address]*big.Int,
) *testutil.MockMulticaller {
	t.Helper()

	balanceOfMethod := erc20ABI.Methods["balanceOf"]
	decimalsMethod := erc20ABI.Methods["decimals"]
	symbolMethod := erc20ABI.Methods["symbol"]

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			if c.Target != tokenAddress {
				t.Errorf("unexpected Call.Target %s — expected %s", c.Target.Hex(), tokenAddress.Hex())
				results[i] = outbound.Result{Success: false}
				continue
			}
			if len(c.CallData) < 4 {
				results[i] = outbound.Result{Success: false}
				continue
			}
			sel := c.CallData[:4]
			switch {
			case bytes.Equal(sel, balanceOfMethod.ID):
				args, err := balanceOfMethod.Inputs.Unpack(c.CallData[4:])
				if err != nil || len(args) == 0 {
					results[i] = outbound.Result{Success: false}
					continue
				}
				wallet, ok := args[0].(common.Address)
				if !ok {
					results[i] = outbound.Result{Success: false}
					continue
				}
				bal, ok := balances[wallet]
				if !ok {
					t.Errorf("unexpected balanceOf wallet %s — fixture missing", wallet.Hex())
					results[i] = outbound.Result{Success: false}
					continue
				}
				rd, packErr := balanceOfMethod.Outputs.Pack(bal)
				if packErr != nil {
					t.Fatalf("pack balanceOf output: %v", packErr)
				}
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			case bytes.Equal(sel, decimalsMethod.ID):
				rd, packErr := decimalsMethod.Outputs.Pack(decimals)
				if packErr != nil {
					t.Fatalf("pack decimals output: %v", packErr)
				}
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			case bytes.Equal(sel, symbolMethod.ID):
				rd, packErr := symbolMethod.Outputs.Pack(symbol)
				if packErr != nil {
					t.Fatalf("pack symbol output: %v", packErr)
				}
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			default:
				t.Errorf("unexpected selector %x in multicall", sel)
				results[i] = outbound.Result{Success: false}
			}
		}
		return results, nil
	}
	return mc
}
