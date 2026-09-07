//go:build integration

package morpho_indexer

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
//
// Coverage boundary of the recorded 33-event set: six of the handled V2 event
// types are ABSENT because sparkUSDTbc never emitted them on-chain —
// RemoveAdapter, ForceDeallocate, DecreaseAbsoluteCap, DecreaseRelativeCap,
// SetManagementFee and SetManagementFeeRecipient. Their handler paths are covered
// only by unit tests; in particular the two Decrease*Cap events have never been
// decoded from a real mainnet log anywhere, only from ABI-round-tripped
// synthetics, so this fixture proves nothing about their real log encoding. Do not
// read a green run here as end-to-end evidence for those six.
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
	// FeeStates is the recorded end-of-block full fee config (both fees + both
	// recipients), keyed by block HASH — the hash-pinned getVaultFees read the
	// fee handler issues. Values are real chain data; never hand-edit.
	FeeStates map[string]struct {
		PerformanceFee          string `json:"performanceFee"`
		ManagementFee           string `json:"managementFee"`
		PerformanceFeeRecipient string `json:"performanceFeeRecipient"`
		ManagementFeeRecipient  string `json:"managementFeeRecipient"`
	} `json:"feeStates"`
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

	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	vaultID := seedVaultRegistry(t, ctx, pool, fx)
	svc := buildReplayServiceForTest(t, ctx, pool, fx)

	replayFixtureEvents(t, ctx, svc, fx)

	assertAdapterRow(t, ctx, pool, vaultID, fx)
	assertAdapterStateRows(t, ctx, pool, vaultID, fx)
	assertVaultCapRows(t, ctx, pool, vaultID, fx)
	assertVaultFeeRows(t, ctx, pool, vaultID, fx)
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

// TestReplaySparkUSDTbcV2Events_ReverseOrderReachesTheSameState is the assertion the
// previous design could not make. Ordering used to be correctness-critical and to fail
// SILENTLY: an Allocate replayed before its AddAdapter minted a registry row stamped
// with the Allocate's block, and only a replay that also covered the AddAdapter walked
// it back. With membership recorded as observations at their own positions, replaying
// the same 33 events in the exact REVERSE order reaches the same answers — current
// membership, current classification, the true add block, and every realAssets snapshot.
//
// The membership LOG legitimately differs: in reverse every allocation log lands before
// any lower-block observation exists, so each one records the allocation_event assertion
// the forward pass never needed — 12 extra rows for this fixture (6 Allocate + 6
// Deallocate above the single AddAdapter). That is why this compares answers rather than
// row counts; the extra rows are the honest record of events that really did prove
// membership before we had seen the add.
func TestReplaySparkUSDTbcV2Events_ReverseOrderReachesTheSameState(t *testing.T) {
	ctx := context.Background()
	fx := loadReplayFixture(t)

	// Each pass runs as a subtest so SetupTestDB (which keys the database on
	// t.Name()) gives it its own database, and the two replays cannot see each other.
	var forward, reverse adapterAnswers
	var forwardInferred, reverseInferred int
	replayInto := func(name string, descending bool, out *adapterAnswers, inferred *int) {
		t.Run(name, func(t *testing.T) {
			pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
			t.Cleanup(cleanup)
			vaultID := seedVaultRegistry(t, ctx, pool, fx)
			svc := buildReplayServiceForTest(t, ctx, pool, fx)
			replayFixtureEventsOrdered(t, ctx, svc, fx, descending)
			*out = readAdapterAnswers(t, ctx, pool, vaultID)
			*inferred = countRows(t, ctx, pool,
				`SELECT count(*) FROM morpho_adapter_membership m JOIN morpho_adapter a ON a.id = m.morpho_adapter_id
				 WHERE a.morpho_vault_id = $1 AND m.observed_via = 'allocation_event'`, vaultID)
		})
	}

	replayInto("forward", false, &forward, &forwardInferred)
	replayInto("reverse", true, &reverse, &reverseInferred)

	if forward != reverse {
		t.Errorf("reverse-order replay reached a different state:\n forward = %+v\n reverse = %+v", forward, reverse)
	}
	if forward.identityRows != 1 {
		t.Errorf("forward replay wrote %d identity rows, want 1", forward.identityRows)
	}
	if forward.firstAdd != fx.Adapter.AddedAtBlock {
		t.Errorf("forward first add block = %d, want %d", forward.firstAdd, fx.Adapter.AddedAtBlock)
	}
	// Guards against a vacuous pass: the two orders must really have taken
	// different paths. In block order every Allocate lands after the AddAdapter has
	// already answered its position, so nothing is inferred; in reverse the
	// Allocates arrive first and must record the membership they prove.
	if forwardInferred != 0 {
		t.Errorf("in-order replay inferred %d memberships from Allocates, want 0", forwardInferred)
	}
	if reverseInferred == 0 {
		t.Error("reverse replay inferred nothing from an Allocate — the out-of-order path was never exercised")
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
// morpho-vault-backfill does (buildReplayService), with real Postgres
// repositories and the fixture-backed fake multicaller, then loads the vault
// registry from the seeded DB.
func buildReplayServiceForTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx *replayFixture) *Service {
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

	cfg := ConfigDefaults()
	cfg.ChainID = fx.ChainID
	cfg.Logger = logger

	svc, err := NewReplayService(cfg, newFixtureMulticaller(t, fx), txManager, protocolRepo, morphoRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewReplayService: %v", err)
	}
	if err := svc.LoadVaultRegistry(ctx); err != nil {
		t.Fatalf("LoadVaultRegistry: %v", err)
	}
	return svc
}

// newFixtureMulticaller returns a fake Multicaller that serves exactly the four
// chain read shapes the replay issues, all from recorded fixture data:
//
//   - the number-pinned adapter identity probe (morpho() succeeds returning the
//     Morpho Blue singleton, every other marker reverts ⇒ MarketV1);
//   - the 1-call hash-pinned realAssets() read;
//   - the 2-call hash-pinned (absoluteCap, relativeCap) read;
//   - the 4-call hash-pinned fee-config read (both fees + both recipients).
//
// The three hash-pinned shapes are keyed by the block hash the handler pins to.
// Any other call shape is an error, failing the event (and thus the test) rather
// than defaulting silently.
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

	// feeStates[blockHash] = the 4 fee-getter return words (performanceFee,
	// managementFee, performanceFeeRecipient, managementFeeRecipient), in the
	// order getVaultFees packs them, mirroring the hash-pinned read the fee
	// handler issues.
	feeStates := make(map[common.Hash][4][]byte, len(fx.FeeStates))
	for hexHash, cfg := range fx.FeeStates {
		perfFee, ok := new(big.Int).SetString(cfg.PerformanceFee, 10)
		if !ok {
			t.Fatalf("fixture performanceFee %q is not a decimal integer", cfg.PerformanceFee)
		}
		mgmtFee, ok := new(big.Int).SetString(cfg.ManagementFee, 10)
		if !ok {
			t.Fatalf("fixture managementFee %q is not a decimal integer", cfg.ManagementFee)
		}
		feeStates[common.HexToHash(hexHash)] = [4][]byte{
			common.LeftPadBytes(perfFee.Bytes(), 32),
			common.LeftPadBytes(mgmtFee.Bytes(), 32),
			common.LeftPadBytes(common.HexToAddress(cfg.PerformanceFeeRecipient).Bytes(), 32),
			common.LeftPadBytes(common.HexToAddress(cfg.ManagementFeeRecipient).Bytes(), 32),
		}
	}

	isAdapterProbe := func(calls []outbound.Call) bool {
		if len(calls) != adapterProbeCallsPerAdapter {
			return false
		}
		for _, call := range calls {
			if call.Target != adapter || !call.AllowFailure {
				return false
			}
		}
		return true
	}

	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if isAdapterProbe(calls) {
			results := make([]outbound.Result, adapterProbeCallsPerAdapter)
			for i := range results {
				results[i] = outbound.Result{Success: false, ReturnData: nil}
			}
			results[0] = outbound.Result{Success: true, ReturnData: common.LeftPadBytes(morphoSingleton.Bytes(), 32)}
			return results, nil
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
		// Vault fees: performanceFee + managementFee + performanceFeeRecipient +
		// managementFeeRecipient, all four to the vault.
		if len(calls) == 4 && calls[0].Target == vault && calls[1].Target == vault &&
			calls[2].Target == vault && calls[3].Target == vault {
			words, ok := feeStates[blockHash]
			if !ok {
				return nil, fmt.Errorf("fake multicaller: no fixture feeStates for block hash %s", blockHash.Hex())
			}
			return []outbound.Result{
				{Success: true, ReturnData: words[0]},
				{Success: true, ReturnData: words[1]},
				{Success: true, ReturnData: words[2]},
				{Success: true, ReturnData: words[3]},
			}, nil
		}
		return nil, fmt.Errorf("fake multicaller: unexpected ExecuteAtHash shape (%d calls)", len(calls))
	}
	return mc
}

// replayFixtureEvents feeds every fixture event through ReplayMetaMorphoLog in
// strict (blockNumber, logIndex) order — the ordering the backfiller enforces so
// AddAdapter lands before the adapter's first allocation.
// replayFixtureEvents replays every recorded event in strict (blockNumber, logIndex)
// order, the order the backfiller produces.
func replayFixtureEvents(t *testing.T, ctx context.Context, svc *Service, fx *replayFixture) {
	t.Helper()
	replayFixtureEventsOrdered(t, ctx, svc, fx, false)
}

// replayFixtureEventsOrdered replays the recorded events ascending or DESCENDING by
// (blockNumber, logIndex). The descending pass is the point: with membership as an
// append-only log keyed on each observation's own position, replay order can no longer
// change the final answers, so an out-of-order replay is a supported (if noisier) input
// rather than a silent corruption.
func replayFixtureEventsOrdered(t *testing.T, ctx context.Context, svc *Service, fx *replayFixture, descending bool) {
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
		a, b := entries[i], entries[j]
		if descending {
			a, b = b, a
		}
		if a.blockNumber != b.blockNumber {
			return a.blockNumber < b.blockNumber
		}
		return a.logIndex < b.logIndex
	})

	for _, e := range entries {
		if err := svc.ReplayMetaMorphoLog(ctx, e.log, e.blockNumber, e.blockHash, 0, e.timestamp); err != nil {
			t.Fatalf("ReplayMetaMorphoLog block=%d logIndex=%d: %v", e.blockNumber, e.logIndex, err)
		}
	}
}

// assertAdapterRow pins what the append-only registry must hold after the replay:
// exactly ONE identity row for the vault's adapter (the invariant that replaces the
// deleted orphan guard — every morpho_adapter_state row hangs off an id nothing can
// move), a membership log whose latest observation says "member" with the probed type,
// and an add block that is a MIN over the log rather than a column.
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
		adapterID int64
		address   []byte
	)
	if err := pool.QueryRow(ctx,
		`SELECT id, address FROM morpho_adapter WHERE morpho_vault_id = $1`,
		vaultID).Scan(&adapterID, &address); err != nil {
		t.Fatalf("reading adapter row: %v", err)
	}
	if got, want := common.BytesToAddress(address), common.HexToAddress(fx.Adapter.Address); got != want {
		t.Errorf("adapter address = %s, want %s", got.Hex(), want.Hex())
	}

	// Latest observation: the current membership answer and the current type.
	var (
		isMember    bool
		adapterType *int16
		observedVia string
	)
	if err := pool.QueryRow(ctx,
		`SELECT is_member, adapter_type, observed_via FROM morpho_adapter_membership
		 WHERE morpho_adapter_id = $1
		 ORDER BY block_number DESC, block_version DESC, log_index DESC, processing_version DESC
		 LIMIT 1`, adapterID).Scan(&isMember, &adapterType, &observedVia); err != nil {
		t.Fatalf("reading the latest membership observation: %v", err)
	}
	if !isMember {
		t.Errorf("latest observation says the adapter is not a member (via %s)", observedVia)
	}
	if adapterType == nil || *adapterType != 1 {
		t.Errorf("adapter_type = %v, want 1 (MarketV1)", adapterType)
	}

	// "When was it added" is a MIN over the log's add_adapter_event rows.
	var addedAtBlock *int64
	if err := pool.QueryRow(ctx,
		`SELECT MIN(block_number) FILTER (WHERE is_member AND observed_via = 'add_adapter_event')
		 FROM morpho_adapter_membership WHERE morpho_adapter_id = $1`, adapterID).Scan(&addedAtBlock); err != nil {
		t.Fatalf("reading the first add block: %v", err)
	}
	if addedAtBlock == nil || *addedAtBlock != fx.Adapter.AddedAtBlock {
		t.Errorf("first add block = %v, want %d", addedAtBlock, fx.Adapter.AddedAtBlock)
	}

	// Every snapshot hangs off that one id, and the view agrees with the log.
	var strayState int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter_state WHERE morpho_adapter_id <> $1`, adapterID).Scan(&strayState); err != nil {
		t.Fatalf("counting stray adapter_state rows: %v", err)
	}
	if strayState != 0 {
		t.Errorf("%d morpho_adapter_state rows hang off some other adapter id", strayState)
	}

	var viewed int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter_current WHERE morpho_vault_id = $1 AND id = $2`,
		vaultID, adapterID).Scan(&viewed); err != nil {
		t.Fatalf("querying morpho_adapter_current: %v", err)
	}
	if viewed != 1 {
		t.Errorf("morpho_adapter_current returned %d rows for the active adapter, want 1", viewed)
	}
}

// adapterAnswers is everything an adapter-registry consumer can actually ask, which is
// what a replay must reproduce regardless of the order the events arrive in. It
// deliberately excludes the membership ROW COUNT: an out-of-order replay records one
// extra allocation_event assertion per allocation log that arrives before the AddAdapter
// that would have answered it (12 of them in the reverse pass of the fixture), and those
// rows are a faithful record of what was observed, not a discrepancy.
type adapterAnswers struct {
	identityRows int
	isMember     bool
	adapterType  int16
	firstAdd     int64
	stateRows    int
	stateSum     string
}

func readAdapterAnswers(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID int64) adapterAnswers {
	t.Helper()
	var a adapterAnswers
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter WHERE morpho_vault_id = $1`, vaultID).Scan(&a.identityRows); err != nil {
		t.Fatalf("counting adapters: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT m.is_member, m.adapter_type
		 FROM morpho_adapter a
		 JOIN LATERAL (
		     SELECT is_member, adapter_type FROM morpho_adapter_membership
		     WHERE morpho_adapter_id = a.id
		     ORDER BY block_number DESC, block_version DESC, log_index DESC, processing_version DESC
		     LIMIT 1
		 ) m ON TRUE
		 WHERE a.morpho_vault_id = $1`, vaultID).Scan(&a.isMember, &a.adapterType); err != nil {
		t.Fatalf("reading the latest membership: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT MIN(m.block_number) FILTER (WHERE m.is_member AND m.observed_via = 'add_adapter_event')
		 FROM morpho_adapter_membership m JOIN morpho_adapter a ON a.id = m.morpho_adapter_id
		 WHERE a.morpho_vault_id = $1`, vaultID).Scan(&a.firstAdd); err != nil {
		t.Fatalf("reading the first add block: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(sum(s.real_assets), 0)::TEXT
		 FROM morpho_adapter_state s JOIN morpho_adapter a ON a.id = s.morpho_adapter_id
		 WHERE a.morpho_vault_id = $1`, vaultID).Scan(&a.stateRows, &a.stateSum); err != nil {
		t.Fatalf("reading adapter state: %v", err)
	}
	return a
}

func assertAdapterStateRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID int64, fx *replayFixture) {
	t.Helper()

	// One adapter_state row per distinct snapshotting block — the AddAdapter
	// registration seed plus each allocation block. Same-block allocations share
	// (block_number, timestamp) and collapse to one snapshot.
	lowBlock, highBlock, distinctBlocks := adapterSnapshotBlockBounds(t, fx)

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_adapter_state s
		 JOIN morpho_adapter a ON a.id = s.morpho_adapter_id
		 WHERE a.morpho_vault_id = $1`, vaultID).Scan(&count); err != nil {
		t.Fatalf("counting adapter states: %v", err)
	}
	if count != distinctBlocks {
		t.Errorf("morpho_adapter_state: want %d rows (distinct snapshotting blocks), got %d", distinctBlocks, count)
	}
	// The earliest row must be the AddAdapter seed, not the first allocation: a
	// freshly registered adapter is never left without a state row.
	if lowBlock != fx.Adapter.AddedAtBlock {
		t.Errorf("earliest adapter_state block = %d, want the AddAdapter block %d", lowBlock, fx.Adapter.AddedAtBlock)
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

// assertVaultFeeRows verifies the append-only fee snapshots: sparkUSDTbc fires
// exactly two fee events (SetPerformanceFeeRecipient @24765788, SetPerformanceFee
// @24765805), so morpho_vault_fee holds exactly two rows, and the latest row
// (highest block) equals the recorded current full fee config.
func assertVaultFeeRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vaultID int64, fx *replayFixture) {
	t.Helper()

	var total int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM morpho_vault_fee WHERE morpho_vault_id = $1`, vaultID).Scan(&total); err != nil {
		t.Fatalf("counting vault fees: %v", err)
	}
	if want := len(fx.FeeStates); total != want {
		t.Errorf("morpho_vault_fee total rows = %d, want %d (one per fee event block)", total, want)
	}

	var (
		perfFee   string
		mgmtFee   string
		perfRecip []byte
		mgmtRecip []byte
	)
	if err := pool.QueryRow(ctx,
		`SELECT performance_fee::text, management_fee::text, performance_fee_recipient, management_fee_recipient
		 FROM morpho_vault_fee
		 WHERE morpho_vault_id = $1
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC
		 LIMIT 1`, vaultID).Scan(&perfFee, &mgmtFee, &perfRecip, &mgmtRecip); err != nil {
		t.Fatalf("reading latest vault fee row: %v", err)
	}

	if perfFee != fx.VaultConfigLatest.PerformanceFee {
		t.Errorf("latest performance_fee = %s, want %s", perfFee, fx.VaultConfigLatest.PerformanceFee)
	}
	if mgmtFee != fx.VaultConfigLatest.ManagementFee {
		t.Errorf("latest management_fee = %s, want %s", mgmtFee, fx.VaultConfigLatest.ManagementFee)
	}
	if common.BytesToAddress(perfRecip) != common.HexToAddress(fx.VaultConfigLatest.PerformanceFeeRecipient) {
		t.Errorf("latest performance_fee_recipient = %x, want %s", perfRecip, fx.VaultConfigLatest.PerformanceFeeRecipient)
	}
	if common.BytesToAddress(mgmtRecip) != common.HexToAddress(fx.VaultConfigLatest.ManagementFeeRecipient) {
		t.Errorf("latest management_fee_recipient = %x, want %s", mgmtRecip, fx.VaultConfigLatest.ManagementFeeRecipient)
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
	counts["morpho_adapter_membership"] = countRows(t, ctx, pool,
		`SELECT count(*) FROM morpho_adapter_membership m JOIN morpho_adapter a ON a.id = m.morpho_adapter_id WHERE a.morpho_vault_id = $1`, vaultID)
	counts["morpho_adapter_state"] = countRows(t, ctx, pool,
		`SELECT count(*) FROM morpho_adapter_state s JOIN morpho_adapter a ON a.id = s.morpho_adapter_id WHERE a.morpho_vault_id = $1`, vaultID)
	counts["morpho_vault_cap"] = countRows(t, ctx, pool, `SELECT count(*) FROM morpho_vault_cap WHERE morpho_vault_id = $1`, vaultID)
	counts["morpho_vault_fee"] = countRows(t, ctx, pool, `SELECT count(*) FROM morpho_vault_fee WHERE morpho_vault_id = $1`, vaultID)
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
func adapterSnapshotBlockBounds(t *testing.T, fx *replayFixture) (low, high int64, distinct int) {
	t.Helper()
	// Every event type that makes the replay write an adapter_state row: the
	// AddAdapter registration seed plus each allocation.
	const (
		addAdapterTopic = "0x8f125a24838c4c23e893904b255b5c672d43d4cb8af7e3d15841eaeabc1e68aa"
		allocateTopic   = "0x2bc7948a96a066968d2a58aaf46eb0b305aa166b1d1951d2f7ef0919746b8c2a"
		deallocateTopic = "0xd602b36fb24934aef1bc2a658de029b486fa4c664a6e45de1f48e3fd1be25dd9"
	)
	snapshotting := map[string]struct{}{addAdapterTopic: {}, allocateTopic: {}, deallocateTopic: {}}

	blocks := map[int64]struct{}{}
	for _, log := range fx.Events {
		if len(log.Topics) == 0 {
			continue
		}
		if _, ok := snapshotting[log.Topics[0]]; !ok {
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
		t.Fatal("fixture has no adapter-snapshotting events")
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
