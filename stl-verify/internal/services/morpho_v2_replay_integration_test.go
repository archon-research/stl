//go:build integration

package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// morphoBlueAddress is the canonical Morpho Blue singleton the indexer resolves
// its protocol row by (GetOrCreateProtocol). The seeded protocol must use this
// address so the audit-log writer reuses one protocol row rather than creating a
// second.
const morphoBlueAddress = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

// replayFixture mirrors testdata/sparkusdtbc_v2_replay.json: real recorded
// sparkUSDTbc VaultV2 events plus the block metadata and realAssets readings a
// hash-pinned replay needs. See the file's "source" header — the values are real
// mainnet chain data and must never be edited by hand.
type replayFixture struct {
	ChainID int64 `json:"chainId"`
	Vault   struct {
		Address       string `json:"address"`
		Asset         string `json:"asset"`
		AssetSymbol   string `json:"assetSymbol"`
		AssetDecimals int    `json:"assetDecimals"`
		VaultVersion  int    `json:"vaultVersion"`
		Name          string `json:"name"`
		Symbol        string `json:"symbol"`
		DeployBlock   int64  `json:"deployBlock"`
	} `json:"vault"`
	Adapter struct {
		Address              string `json:"address"`
		Morpho               string `json:"morpho"`
		MorphoVaultV1Reverts bool   `json:"morphoVaultV1Reverts"`
		AddedAtBlock         int64  `json:"addedAtBlock"`
	} `json:"adapter"`
	VaultConfigLatest struct {
		PerformanceFee          string `json:"performanceFee"`
		ManagementFee           string `json:"managementFee"`
		PerformanceFeeRecipient string `json:"performanceFeeRecipient"`
		ManagementFeeRecipient  string `json:"managementFeeRecipient"`
		Caps                    map[string]struct {
			AbsoluteCap string `json:"absoluteCap"`
			RelativeCap string `json:"relativeCap"`
			Allocation  string `json:"allocation"`
		} `json:"caps"`
	} `json:"vaultConfigLatest"`
	Blocks map[string]struct {
		Hash      string `json:"hash"`
		Timestamp int64  `json:"timestamp"`
	} `json:"blocks"`
	RealAssets map[string]string `json:"realAssets"` // block hash -> decimal realAssets
	// CapStates is the recorded end-of-block (absoluteCap, relativeCap) per cap
	// id, keyed by block HASH then cap id — the hash-pinned getVaultCaps read the
	// cap handler issues. Values are real chain data; never hand-edit.
	CapStates map[string]map[string]struct {
		AbsoluteCap string `json:"absoluteCap"`
		RelativeCap string `json:"relativeCap"`
	} `json:"capStates"`
	Events []shared.Log `json:"events"`
}

// TestReplaySparkUSDTbcV2Events replays the recorded sparkUSDTbc VaultV2 events
// through the exact backfiller replay path (NewReplayService + LoadVaultRegistry
// + ReplayMetaMorphoLog) against a real Postgres schema, then asserts the final
// DB state row-for-row against the recorded chain snapshot. The only mock is the
// multicaller (the uncontrollable chain): it serves the adapter identity probe
// and the hash-pinned realAssets() reads from the fixture, and fails the test on
// any call it does not recognise.
func TestReplaySparkUSDTbcV2Events(t *testing.T) {
	ctx := context.Background()
	fx := loadReplayFixture(t)

	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	vaultID := seedVaultRegistry(t, ctx, pool, fx)
	svc := buildReplayServiceForTest(t, ctx, pool, fx)

	replayFixtureEvents(t, ctx, svc, fx)

	assertAdapterRow(t, ctx, pool, vaultID, fx)
	assertAdapterStateRows(t, ctx, pool, vaultID, fx)
	assertVaultCapRows(t, ctx, pool, vaultID, fx)
	assertVaultFeeColumns(t, ctx, pool, vaultID, fx)
	assertProtocolEventRows(t, ctx, pool, fx)

	// Idempotency: a second replay with the same service (same build_id) must be
	// a no-op — every count above stays put.
	before := snapshotRowCounts(t, ctx, pool, vaultID)
	replayFixtureEvents(t, ctx, svc, fx)
	after := snapshotRowCounts(t, ctx, pool, vaultID)
	for table, n := range before {
		if after[table] != n {
			t.Errorf("idempotency: %s went from %d to %d on second replay", table, n, after[table])
		}
	}
}

func loadReplayFixture(t *testing.T) *replayFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/sparkusdtbc_v2_replay.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	var fx replayFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("unmarshalling fixture: %v", err)
	}
	if len(fx.Events) == 0 {
		t.Fatal("fixture carries no events")
	}
	return &fx
}

// seedVaultRegistry mirrors the morpho repository test's createTestFixtures:
// chain (migration-seeded, upserted for safety), the USDT asset token, the
// Morpho Blue protocol, and the V2 vault row the replay expects to already
// exist. Returns the vault's DB id.
func seedVaultRegistry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx *replayFixture) int64 {
	t.Helper()

	if _, err := pool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES ($1, 'Ethereum') ON CONFLICT (chain_id) DO NOTHING`,
		fx.ChainID); err != nil {
		t.Fatalf("seeding chain: %v", err)
	}

	var tokenID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol RETURNING id`,
		fx.ChainID, common.HexToAddress(fx.Vault.Asset).Bytes(), fx.Vault.AssetSymbol, fx.Vault.AssetDecimals,
	).Scan(&tokenID); err != nil {
		t.Fatalf("seeding asset token: %v", err)
	}

	var protocolID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES ($1, $2, 'Morpho Blue', 'lending', 18883124, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name RETURNING id`,
		fx.ChainID, common.HexToAddress(morphoBlueAddress).Bytes(),
	).Scan(&protocolID); err != nil {
		t.Fatalf("seeding protocol: %v", err)
	}

	var vaultID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		fx.ChainID, protocolID, common.HexToAddress(fx.Vault.Address).Bytes(),
		fx.Vault.Name, fx.Vault.Symbol, tokenID, fx.Vault.VaultVersion, fx.Vault.DeployBlock,
	).Scan(&vaultID); err != nil {
		t.Fatalf("seeding vault: %v", err)
	}
	return vaultID
}

// buildReplayServiceForTest constructs the replay service the same way the
// morpho-vault-indexer backfiller does (buildReplayService), with real Postgres
// repositories and the fixture-backed fake multicaller, then loads the vault
// registry from the seeded DB.
func buildReplayServiceForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx *replayFixture) *morpho_indexer.Service {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	buildID := buildregistry.BuildID(1)

	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	morphoRepo, err := postgres.NewMorphoRepository(pool, logger, buildID)
	if err != nil {
		t.Fatalf("NewMorphoRepository: %v", err)
	}
	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, buildID, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	eventRepo := postgres.NewEventRepository(logger, buildID)

	cfg := morpho_indexer.ConfigDefaults()
	cfg.ChainID = fx.ChainID
	cfg.Logger = logger

	svc, err := morpho_indexer.NewReplayService(cfg, newFixtureMulticaller(t, fx), txManager, protocolRepo, morphoRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewReplayService: %v", err)
	}
	if err := svc.LoadVaultRegistry(ctx); err != nil {
		t.Fatalf("LoadVaultRegistry: %v", err)
	}
	return svc
}

// newFixtureMulticaller returns a fake Multicaller that serves exactly the two
// chain reads the replay issues: the number-pinned adapter identity probe
// (morpho() succeeds returning the Morpho Blue singleton, morphoVaultV1()
// reverts ⇒ MarketV1) and the hash-pinned realAssets() read, keyed by the block
// hash the handler pins to. Any other call shape is an error, failing the event
// (and thus the test) rather than defaulting silently.
func newFixtureMulticaller(t *testing.T, fx *replayFixture) *testutil.MockMulticaller {
	t.Helper()
	mc := testutil.NewMockMulticaller()
	adapter := common.HexToAddress(fx.Adapter.Address)
	vault := common.HexToAddress(fx.Vault.Address)
	morphoSingleton := common.HexToAddress(fx.Adapter.Morpho)

	realAssetsByHash := make(map[common.Hash]*big.Int, len(fx.RealAssets))
	for hexHash, dec := range fx.RealAssets {
		v, ok := new(big.Int).SetString(dec, 10)
		if !ok {
			t.Fatalf("fixture realAssets %q is not a decimal integer", dec)
		}
		realAssetsByHash[common.HexToHash(hexHash)] = v
	}

	// capStates[blockHash][capID] = {absolute, relative}, mirroring the
	// hash-pinned (absoluteCap, relativeCap) read the cap handler issues.
	capStates := make(map[common.Hash]map[common.Hash][2]*big.Int, len(fx.CapStates))
	for hexHash, byCap := range fx.CapStates {
		inner := make(map[common.Hash][2]*big.Int, len(byCap))
		for hexCap, pair := range byCap {
			abs, ok := new(big.Int).SetString(pair.AbsoluteCap, 10)
			if !ok {
				t.Fatalf("fixture absoluteCap %q is not a decimal integer", pair.AbsoluteCap)
			}
			rel, ok := new(big.Int).SetString(pair.RelativeCap, 10)
			if !ok {
				t.Fatalf("fixture relativeCap %q is not a decimal integer", pair.RelativeCap)
			}
			inner[common.HexToHash(hexCap)] = [2]*big.Int{abs, rel}
		}
		capStates[common.HexToHash(hexHash)] = inner
	}

	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) == 2 && calls[0].Target == adapter && calls[1].Target == adapter &&
			calls[0].AllowFailure && calls[1].AllowFailure {
			return []outbound.Result{
				{Success: true, ReturnData: common.LeftPadBytes(morphoSingleton.Bytes(), 32)},
				{Success: false, ReturnData: nil},
			}, nil
		}
		return nil, fmt.Errorf("fake multicaller: unexpected Execute shape (%d calls)", len(calls))
	}

	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		// Adapter realAssets(): one non-AllowFailure call to the adapter.
		if len(calls) == 1 && calls[0].Target == adapter {
			v, ok := realAssetsByHash[blockHash]
			if !ok {
				return nil, fmt.Errorf("fake multicaller: no fixture realAssets for block hash %s", blockHash.Hex())
			}
			return []outbound.Result{{Success: true, ReturnData: common.LeftPadBytes(v.Bytes(), 32)}}, nil
		}
		// Vault caps: absoluteCap(id) + relativeCap(id), both to the vault. The
		// cap id is the 32-byte argument after the 4-byte selector.
		if len(calls) == 2 && calls[0].Target == vault && calls[1].Target == vault {
			if len(calls[0].CallData) < 36 {
				return nil, fmt.Errorf("fake multicaller: cap call data too short (%d bytes)", len(calls[0].CallData))
			}
			capID := common.BytesToHash(calls[0].CallData[4:36])
			byCap, ok := capStates[blockHash]
			if !ok {
				return nil, fmt.Errorf("fake multicaller: no fixture capStates for block hash %s", blockHash.Hex())
			}
			pair, ok := byCap[capID]
			if !ok {
				return nil, fmt.Errorf("fake multicaller: no fixture capState for block %s cap %s", blockHash.Hex(), capID.Hex())
			}
			return []outbound.Result{
				{Success: true, ReturnData: common.LeftPadBytes(pair[0].Bytes(), 32)},
				{Success: true, ReturnData: common.LeftPadBytes(pair[1].Bytes(), 32)},
			}, nil
		}
		return nil, fmt.Errorf("fake multicaller: unexpected ExecuteAtHash shape (%d calls)", len(calls))
	}
	return mc
}

// replayFixtureEvents feeds every fixture event through ReplayMetaMorphoLog in
// strict (blockNumber, logIndex) order — the ordering the backfiller enforces so
// AddAdapter lands before the adapter's first allocation.
func replayFixtureEvents(t *testing.T, ctx context.Context, svc *morpho_indexer.Service, fx *replayFixture) {
	t.Helper()

	type queued struct {
		log         shared.Log
		blockNumber int64
		logIndex    int64
		blockHash   common.Hash
		timestamp   time.Time
	}
	entries := make([]queued, 0, len(fx.Events))
	for _, log := range fx.Events {
		blockNumber := parseHexInt(t, log.BlockNumber)
		logIndex := parseHexInt(t, log.LogIndex)
		block, ok := fx.Blocks[strconv.FormatInt(blockNumber, 10)]
		if !ok {
			t.Fatalf("fixture missing block metadata for %d", blockNumber)
		}
		entries = append(entries, queued{
			log:         log,
			blockNumber: blockNumber,
			logIndex:    logIndex,
			blockHash:   common.HexToHash(log.BlockHash),
			timestamp:   time.Unix(block.Timestamp, 0).UTC(),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].blockNumber != entries[j].blockNumber {
			return entries[i].blockNumber < entries[j].blockNumber
		}
		return entries[i].logIndex < entries[j].logIndex
	})

	for _, e := range entries {
		if err := svc.ReplayMetaMorphoLog(ctx, e.log, e.blockNumber, e.blockHash, 0, e.timestamp); err != nil {
			t.Fatalf("ReplayMetaMorphoLog block=%d logIndex=%d: %v", e.blockNumber, e.logIndex, err)
		}
	}
}

func assertAdapterRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID int64, fx *replayFixture) {
	t.Helper()

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM morpho_adapter WHERE morpho_vault_id = $1`, vaultID).Scan(&count); err != nil {
		t.Fatalf("counting adapters: %v", err)
	}
	if count != 1 {
		t.Fatalf("morpho_adapter: want exactly 1 row, got %d", count)
	}

	var (
		address       []byte
		adapterType   int16
		addedAtBlock  int64
		removedAtNull *int64
	)
	if err := pool.QueryRow(ctx,
		`SELECT address, adapter_type, added_at_block, removed_at_block FROM morpho_adapter WHERE morpho_vault_id = $1`,
		vaultID).Scan(&address, &adapterType, &addedAtBlock, &removedAtNull); err != nil {
		t.Fatalf("reading adapter row: %v", err)
	}
	if got, want := common.BytesToAddress(address), common.HexToAddress(fx.Adapter.Address); got != want {
		t.Errorf("adapter address = %s, want %s", got.Hex(), want.Hex())
	}
	if adapterType != 1 {
		t.Errorf("adapter_type = %d, want 1 (MarketV1)", adapterType)
	}
	if addedAtBlock != fx.Adapter.AddedAtBlock {
		t.Errorf("added_at_block = %d, want %d", addedAtBlock, fx.Adapter.AddedAtBlock)
	}
	if removedAtNull != nil {
		t.Errorf("removed_at_block = %d, want NULL", *removedAtNull)
	}
}

func assertAdapterStateRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID int64, fx *replayFixture) {
	t.Helper()

	// One adapter_state row per distinct allocation block: same-block allocations
	// share (block_number, timestamp) and collapse to one snapshot.
	lowBlock, highBlock, distinctBlocks := allocationBlockBounds(t, fx)

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter_state s
		 JOIN morpho_adapter a ON a.id = s.morpho_adapter_id
		 WHERE a.morpho_vault_id = $1`, vaultID).Scan(&count); err != nil {
		t.Fatalf("counting adapter states: %v", err)
	}
	if count != distinctBlocks {
		t.Errorf("morpho_adapter_state: want %d rows (distinct allocation blocks), got %d", distinctBlocks, count)
	}

	lowWant := fx.RealAssets[fx.Blocks[strconv.FormatInt(lowBlock, 10)].Hash]
	highWant := fx.RealAssets[fx.Blocks[strconv.FormatInt(highBlock, 10)].Hash]

	if got := adapterStateAtBlock(t, ctx, pool, vaultID, lowBlock); got != lowWant {
		t.Errorf("earliest adapter_state (block %d) real_assets = %s, want %s", lowBlock, got, lowWant)
	}
	if got := adapterStateAtBlock(t, ctx, pool, vaultID, highBlock); got != highWant {
		t.Errorf("latest adapter_state (block %d) real_assets = %s, want %s", highBlock, got, highWant)
	}

	// The single latest row overall must equal the highest-block recorded value.
	var latest string
	if err := pool.QueryRow(ctx,
		`SELECT s.real_assets::text FROM morpho_adapter_state s
		 JOIN morpho_adapter a ON a.id = s.morpho_adapter_id
		 WHERE a.morpho_vault_id = $1
		 ORDER BY s.block_number DESC, s.block_version DESC, s.processing_version DESC
		 LIMIT 1`, vaultID).Scan(&latest); err != nil {
		t.Fatalf("reading latest adapter_state: %v", err)
	}
	if latest != highWant {
		t.Errorf("latest adapter_state real_assets = %s, want %s (block %d)", latest, highWant, highBlock)
	}
}

func adapterStateAtBlock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID, block int64) string {
	t.Helper()
	var v string
	if err := pool.QueryRow(ctx,
		`SELECT s.real_assets::text FROM morpho_adapter_state s
		 JOIN morpho_adapter a ON a.id = s.morpho_adapter_id
		 WHERE a.morpho_vault_id = $1 AND s.block_number = $2`, vaultID, block).Scan(&v); err != nil {
		t.Fatalf("reading adapter_state at block %d: %v", block, err)
	}
	return v
}

func assertVaultCapRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID int64, fx *replayFixture) {
	t.Helper()

	// Latest row per cap_id carries the full current cap state (both fields).
	rows, err := pool.Query(ctx,
		`SELECT DISTINCT ON (cap_id) '0x' || encode(cap_id, 'hex'), absolute_cap::text, relative_cap::text
		 FROM morpho_vault_cap
		 WHERE morpho_vault_id = $1
		 ORDER BY cap_id, block_number DESC, block_version DESC, processing_version DESC`, vaultID)
	if err != nil {
		t.Fatalf("querying latest vault caps: %v", err)
	}
	defer rows.Close()

	latest := make(map[string][2]string)
	for rows.Next() {
		var capID, absolute, relative string
		if err := rows.Scan(&capID, &absolute, &relative); err != nil {
			t.Fatalf("scanning vault cap: %v", err)
		}
		latest[capID] = [2]string{absolute, relative}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating vault caps: %v", err)
	}

	if len(latest) != len(fx.VaultConfigLatest.Caps) {
		t.Errorf("distinct cap ids = %d, want %d", len(latest), len(fx.VaultConfigLatest.Caps))
	}
	for capID, want := range fx.VaultConfigLatest.Caps {
		got, ok := latest[capID]
		if !ok {
			t.Errorf("cap %s: no row in DB", capID)
			continue
		}
		if got[0] != want.AbsoluteCap {
			t.Errorf("cap %s absolute_cap = %s, want %s", capID, got[0], want.AbsoluteCap)
		}
		if got[1] != want.RelativeCap {
			t.Errorf("cap %s relative_cap = %s, want %s", capID, got[1], want.RelativeCap)
		}
	}

	// Each cap id's two same-block events (IncreaseAbsoluteCap +
	// IncreaseRelativeCap) each snapshot the full (absolute, relative) pair read
	// at the same block hash, so they write byte-identical rows that dedupe to
	// exactly one row per id — one row per cap id total, not two.
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM morpho_vault_cap WHERE morpho_vault_id = $1`, vaultID).Scan(&total); err != nil {
		t.Fatalf("counting vault caps: %v", err)
	}
	if want := len(fx.VaultConfigLatest.Caps); total != want {
		t.Errorf("morpho_vault_cap total rows = %d, want %d (same-block pairs dedupe to one row per id)", total, want)
	}
}

func assertVaultFeeColumns(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID int64, fx *replayFixture) {
	t.Helper()

	var (
		perfFee   *string
		mgmtFee   *string
		perfRecip []byte
		mgmtRecip []byte
	)
	if err := pool.QueryRow(ctx,
		`SELECT performance_fee::text, management_fee::text, performance_fee_recipient, management_fee_recipient
		 FROM morpho_vault WHERE id = $1`, vaultID).Scan(&perfFee, &mgmtFee, &perfRecip, &mgmtRecip); err != nil {
		t.Fatalf("reading vault fee columns: %v", err)
	}

	if perfFee == nil || *perfFee != fx.VaultConfigLatest.PerformanceFee {
		t.Errorf("performance_fee = %v, want %s", perfFee, fx.VaultConfigLatest.PerformanceFee)
	}
	if perfRecip == nil || common.BytesToAddress(perfRecip) != common.HexToAddress(fx.VaultConfigLatest.PerformanceFeeRecipient) {
		t.Errorf("performance_fee_recipient = %x, want %s", perfRecip, fx.VaultConfigLatest.PerformanceFeeRecipient)
	}
	if mgmtFee != nil {
		t.Errorf("management_fee = %s, want NULL (no SetManagementFee event)", *mgmtFee)
	}
	if mgmtRecip != nil {
		t.Errorf("management_fee_recipient = %x, want NULL (no SetManagementFeeRecipient event)", mgmtRecip)
	}
}

func assertProtocolEventRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx *replayFixture) {
	t.Helper()
	// Every replayed event writes exactly one protocol_event audit row.
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM protocol_event WHERE contract_address = $1`,
		common.HexToAddress(fx.Vault.Address).Bytes()).Scan(&count); err != nil {
		t.Fatalf("counting protocol events: %v", err)
	}
	if count != len(fx.Events) {
		t.Errorf("protocol_event rows = %d, want %d (one per replayed event)", count, len(fx.Events))
	}
}

func snapshotRowCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID int64) map[string]int {
	t.Helper()
	counts := map[string]int{}
	counts["morpho_adapter"] = countRows(t, ctx, pool, `SELECT count(*) FROM morpho_adapter WHERE morpho_vault_id = $1`, vaultID)
	counts["morpho_adapter_state"] = countRows(t, ctx, pool,
		`SELECT count(*) FROM morpho_adapter_state s JOIN morpho_adapter a ON a.id = s.morpho_adapter_id WHERE a.morpho_vault_id = $1`, vaultID)
	counts["morpho_vault_cap"] = countRows(t, ctx, pool, `SELECT count(*) FROM morpho_vault_cap WHERE morpho_vault_id = $1`, vaultID)
	counts["protocol_event"] = countRows(t, ctx, pool,
		`SELECT count(*) FROM protocol_event pe JOIN morpho_vault v ON v.address = pe.contract_address WHERE v.id = $1`, vaultID)
	return counts
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// allocationBlockBounds returns the lowest and highest allocation-event block
// numbers and the count of distinct allocation blocks in the fixture.
func allocationBlockBounds(t *testing.T, fx *replayFixture) (low, high int64, distinct int) {
	t.Helper()
	const (
		allocateTopic   = "0x2bc7948a96a066968d2a58aaf46eb0b305aa166b1d1951d2f7ef0919746b8c2a"
		deallocateTopic = "0xd602b36fb24934aef1bc2a658de029b486fa4c664a6e45de1f48e3fd1be25dd9"
	)
	blocks := map[int64]struct{}{}
	for _, log := range fx.Events {
		if len(log.Topics) == 0 {
			continue
		}
		if log.Topics[0] != allocateTopic && log.Topics[0] != deallocateTopic {
			continue
		}
		b := parseHexInt(t, log.BlockNumber)
		blocks[b] = struct{}{}
		if low == 0 || b < low {
			low = b
		}
		if b > high {
			high = b
		}
	}
	if len(blocks) == 0 {
		t.Fatal("fixture has no allocation events")
	}
	return low, high, len(blocks)
}

func parseHexInt(t *testing.T, s string) int64 {
	t.Helper()
	v, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		t.Fatalf("parsing hex int %q: %v", s, err)
	}
	return v
}
