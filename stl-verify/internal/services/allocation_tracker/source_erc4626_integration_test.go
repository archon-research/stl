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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// erc4626IntegrationFixture is the shared wiring both ERC4626 integration tests
// build on: a fresh migrated DB, the Service→ERC4626Source→PrimePositionHandler→
// Postgres registry, and a mock multicaller answering the single vault. The event-
// and sweep-path tests differ only in SweepEveryNBlocks and the block they feed.
type erc4626IntegrationFixture struct {
	pool            *pgxpool.Pool
	logger          *slog.Logger
	registry        *SourceRegistry
	pgHandler       *PrimePositionHandler
	entries         []*TokenEntry
	proxies         []ProxyConfig
	vault           common.Address
	almProxy        common.Address
	wantTotalSupply string
}

// setupERC4626Integration builds the full Service → ERC4626Source →
// PrimePositionHandler → TokenTotalSupplyRepository → Postgres path against a real
// DB; only the Alchemy RPC (multicaller) and block-cache reader are mocked. The
// vault is a stand-in Morpho Blue receipt token (18 decimals) held by the Spark
// ALM proxy.
func setupERC4626Integration(t *testing.T, ctx context.Context) *erc4626IntegrationFixture {
	t.Helper()

	t.Setenv("BUILD_GIT_HASH", "test-integration-erc4626-supply")

	pool, _, dbCleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(dbCleanup)

	sparkID := seedSparkPrime(t, ctx, pool)

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

	almProxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	// Stand-in Morpho Blue vault receipt token for the fixture (18 decimals).
	vault := common.HexToAddress("0x1234000000000000000000000000000000004626")

	wad := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	shares := new(big.Int).Mul(big.NewInt(1_000), wad)
	assets := new(big.Int).Mul(big.NewInt(1_050), wad)
	totalSupply := new(big.Int).Mul(big.NewInt(5_000_000), wad)

	// A throwaway source exposes the embedded vault ABI selectors for the mock;
	// the registered source below is bound to that mock.
	probeSrc, err := NewERC4626Source(nil, logger)
	if err != nil {
		t.Fatalf("erc4626 probe source: %v", err)
	}
	mc := newERC4626MockMulticaller(t, probeSrc.vaultABI, erc20ABI, vault, "mvUSDC", 18, shares, assets, totalSupply)

	src, err := NewERC4626Source(mc, logger)
	if err != nil {
		t.Fatalf("erc4626 source: %v", err)
	}

	primeLookup := map[string]int64{"spark": sparkID}
	pgHandler := NewPrimePositionHandler(allocRepo, supplyRepo, txm, mc, erc20ABI, primeLookup, logger, nil)

	registry := NewSourceRegistry(logger)
	registry.Register(src)

	entries := []*TokenEntry{
		{ContractAddress: vault, WalletAddress: almProxy, Star: "spark", Chain: "mainnet", Protocol: "morpho", AllocationType: "pol", TokenType: "erc4626"},
	}
	proxies := []ProxyConfig{
		{Star: "spark", Chain: "mainnet", Address: almProxy},
	}

	return &erc4626IntegrationFixture{
		pool:      pool,
		logger:    logger,
		registry:  registry,
		pgHandler: pgHandler,
		entries:   entries,
		proxies:   proxies,
		vault:     vault,
		almProxy:  almProxy,
		// 5,000,000 vault shares persisted as NUMERIC with 18 decimals.
		wantTotalSupply: "5000000.000000000000000000",
	}
}

// seedSparkPrime self-seeds the spark prime row (idempotent upsert) rather than
// leaning on the migration-seeded row, so a sibling integration test that wipes
// shared tables can't leave this one without its prime; it returns the row id. The
// vault_address matches what db/migrations/20260305_120000_create_prime_debts.sql
// seeds for spark, and prime's natural key is name.
func seedSparkPrime(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	sparkVault := common.HexToAddress("0x691a6c29e9e96dd897718305427ad5d534db16ba").Bytes()
	if _, err := pool.Exec(ctx,
		`INSERT INTO prime (name, vault_address) VALUES ('spark', $1) ON CONFLICT (name) DO NOTHING`,
		sparkVault,
	); err != nil {
		t.Fatalf("seed spark prime: %v", err)
	}
	var sparkID int64
	if err := pool.QueryRow(ctx, "SELECT id FROM prime WHERE name = 'spark'").Scan(&sparkID); err != nil {
		t.Fatalf("read spark prime id: %v", err)
	}
	return sparkID
}

// assertVaultSupplyRow asserts the vault has exactly one token_total_supply row
// with the fixture's total_supply, a NULL scaled_total_supply, and wantSource.
func assertVaultSupplyRow(t *testing.T, ctx context.Context, f *erc4626IntegrationFixture, wantSource string) {
	t.Helper()

	var rows int
	if err := f.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM token_total_supply tts
		 JOIN token t ON t.id = tts.token_id
		 WHERE t.address = $1`,
		f.vault.Bytes(),
	).Scan(&rows); err != nil {
		t.Fatalf("count supply rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("token_total_supply rows for vault: got %d, want 1", rows)
	}

	var (
		totalSupplyText string
		scaledIsNull    bool
		source          string
	)
	if err := f.pool.QueryRow(ctx,
		`SELECT tts.total_supply::text, tts.scaled_total_supply IS NULL, tts.source
		 FROM token_total_supply tts
		 JOIN token t ON t.id = tts.token_id
		 WHERE t.address = $1`,
		f.vault.Bytes(),
	).Scan(&totalSupplyText, &scaledIsNull, &source); err != nil {
		t.Fatalf("query supply row: %v", err)
	}

	if totalSupplyText != f.wantTotalSupply {
		t.Errorf("total_supply: got %q, want %q", totalSupplyText, f.wantTotalSupply)
	}
	if !scaledIsNull {
		t.Errorf("scaled_total_supply: got non-NULL, want NULL for an erc4626 vault")
	}
	if source != wantSource {
		t.Errorf("source: got %q, want %q", source, wantSource)
	}
}

// TestIntegration_ERC4626Vault_LandsTotalSupplyRow feeds the service a block
// whose receipts contain one Transfer of a Morpho Blue vault receipt token to
// the Spark ALM proxy, and verifies that the event path lands a row in
// token_total_supply with the vault's totalSupply, a NULL scaled_total_supply,
// and the "event" source discriminator.
func TestIntegration_ERC4626Vault_LandsTotalSupplyRow(t *testing.T) {
	ctx := context.Background()
	f := setupERC4626Integration(t, ctx)

	const blockNumber = int64(19_500_000)
	externalSender := common.HexToAddress("0x9999999999999999999999999999999999999999")
	receiptsJSON := mustMarshalReceipts(t, []TransactionReceipt{{
		Logs: []gethtypes.Log{
			makeTransferLog(f.vault, externalSender, f.almProxy, big.NewInt(500), 0),
		},
	}})
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, blockNumber, 0, receiptsJSON)

	svc, err := NewService(
		Config{ChainID: 1, SweepEveryNBlocks: 1000, Logger: f.logger},
		nil,
		cache,
		f.registry,
		f.entries,
		f.pgHandler,
		f.proxies,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        0,
		BlockTimestamp: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC).Unix(),
		BlockHash:      testBlockHash.Hex(),
	}
	if err := svc.processBlock(ctx, event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	assertVaultSupplyRow(t, ctx, f, "event")

	// Replay the same event under the same build_id: the advisory-locked
	// assign_processing_version_token_total_supply BEFORE INSERT trigger
	// (db/migrations/20260423_214929_create_token_total_supply.sql) reuses the
	// existing processing_version, so the ON CONFLICT DO NOTHING insert keeps the
	// vault at exactly one row instead of appending an SNS-replay duplicate.
	if err := svc.processBlock(ctx, event); err != nil {
		t.Fatalf("processBlock (replay): %v", err)
	}
	assertVaultSupplyRow(t, ctx, f, "event")
}

// TestIntegration_ERC4626Vault_SweepLandsTotalSupplyRow drives the sweep path,
// not the event path: a block with empty receipts emits no Transfer, so only the
// periodic sweep (SweepEveryNBlocks: 1) runs. It must still read the vault's
// totalSupply and land a token_total_supply row tagged source = 'sweep', with a
// NULL scaled_total_supply — the reconciliation path that captures yield changes
// no Transfer announces.
func TestIntegration_ERC4626Vault_SweepLandsTotalSupplyRow(t *testing.T) {
	ctx := context.Background()
	f := setupERC4626Integration(t, ctx)

	const blockNumber = int64(19_500_000)
	// Empty receipts: no Transfer to any proxy, so the event path is a no-op and
	// only the sweep runs.
	receiptsJSON := mustMarshalReceipts(t, []TransactionReceipt{})
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, blockNumber, 0, receiptsJSON)

	svc, err := NewService(
		Config{ChainID: 1, SweepEveryNBlocks: 1, Logger: f.logger},
		nil,
		cache,
		f.registry,
		f.entries,
		f.pgHandler,
		f.proxies,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        0,
		BlockTimestamp: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC).Unix(),
		BlockHash:      testBlockHash.Hex(),
	}
	if err := svc.processBlock(ctx, event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	assertVaultSupplyRow(t, ctx, f, "sweep")
}

// newERC4626MockMulticaller returns a MockMulticaller that dispatches by
// selector against the single vault: balanceOf(wallet)→shares,
// totalSupply()→totalSupply, convertToAssets(_)→assets (state reads, hash-pinned),
// and decimals()/symbol() for the metadata probe (number-pinned). Any other
// selector, or a call whose Target is not the vault, fails the test loudly —
// silent substitution would mask call-building or routing bugs.
func newERC4626MockMulticaller(
	t *testing.T,
	vaultABI abi.ABI,
	erc20ABI *abi.ABI,
	vault common.Address,
	symbol string,
	decimals uint8,
	shares, assets, totalSupply *big.Int,
) *testutil.MockMulticaller {
	t.Helper()

	balanceOf := vaultABI.Methods["balanceOf"]
	totalSupplyM := vaultABI.Methods["totalSupply"]
	convert := vaultABI.Methods["convertToAssets"]
	decimalsM := erc20ABI.Methods["decimals"]
	symbolM := erc20ABI.Methods["symbol"]

	resolve := func(calls []outbound.Call) []outbound.Result {
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			if c.Target != vault {
				t.Errorf("unexpected Call.Target %s — expected vault %s", c.Target.Hex(), vault.Hex())
				results[i] = outbound.Result{Success: false}
				continue
			}
			sel := c.CallData[:4]
			switch {
			case bytes.Equal(sel, balanceOf.ID):
				results[i] = mustPackResult(t, balanceOf.Outputs, shares)
			case bytes.Equal(sel, totalSupplyM.ID):
				results[i] = mustPackResult(t, totalSupplyM.Outputs, totalSupply)
			case bytes.Equal(sel, convert.ID):
				results[i] = mustPackResult(t, convert.Outputs, assets)
			case bytes.Equal(sel, decimalsM.ID):
				results[i] = mustPackResult(t, decimalsM.Outputs, decimals)
			case bytes.Equal(sel, symbolM.ID):
				results[i] = mustPackResult(t, symbolM.Outputs, symbol)
			default:
				t.Errorf("unexpected selector %x", sel)
				results[i] = outbound.Result{Success: false}
			}
		}
		return results
	}

	mc := testutil.NewMockMulticaller()
	// ExecuteFn backs the metadataCache static probe (number-pinned, VEC-471);
	// ExecuteAtHashFn backs the ERC4626Source state reads (hash-pinned).
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return resolve(calls), nil
	}
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return resolve(calls), nil
	}
	return mc
}

func mustPackResult(t *testing.T, args abi.Arguments, v any) outbound.Result {
	t.Helper()
	rd, err := args.Pack(v)
	if err != nil {
		t.Fatalf("pack output: %v", err)
	}
	return outbound.Result{Success: true, ReturnData: rd}
}
