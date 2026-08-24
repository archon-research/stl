//go:build integration

package morpho_v2_bootstrap

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestRun_SeedAndReplayRecordTheWholeLifecycle is the correctness test for this
// job's riskiest property, run against real SQL rather than mock repos.
//
// The seed asserts each currently-active adapter at the run's pinned HEAD block —
// far later than its true AddAdapter. The replay walks the real history from the
// factory deploy block. Under the old registry the two had to CONVERGE onto the
// incarnations the chain had, and getting it wrong left a de-registered adapter
// spuriously active or failed the run outright. There is nothing to converge now:
// each is an observation at its own position, and the questions consumers ask are
// answered by ordering them.
//
// History exercised (the hardest shape — removed then re-added):
//
//	AddAdapter@100 → RemoveAdapter@200 → AddAdapter@300, still active at head.
//
// Expected end state: ONE identity row, three observations in block order, a
// latest observation that says "member", and the seed's head-block snapshot
// hanging off that same identity.
func TestRun_SeedAndReplayRecordTheWholeLifecycle(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	const (
		addBlock    = uint64(23_400_000)
		removeBlock = uint64(23_500_000)
		reAddBlock  = uint64(23_600_000)
		headBlock   = int64(23_700_000)
	)
	vault := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

	seedVaultRow(t, ctx, pool, vault)

	chain := newFakeChainReader()
	head := chain.setFinalizedHead(headBlock, 1_770_000_000)
	for _, b := range []uint64{addBlock, removeBlock, reAddBlock} {
		chain.addBlock(b, 1_760_000_000+b)
	}
	chain.logs = []ethtypes.Log{
		adapterLifecycleLog(t, "AddAdapter", vault, adapter, addBlock, chain.hashOf(addBlock), 0),
		adapterLifecycleLog(t, "RemoveAdapter", vault, adapter, removeBlock, chain.hashOf(removeBlock), 0),
		adapterLifecycleLog(t, "AddAdapter", vault, adapter, reAddBlock, chain.hashOf(reAddBlock), 0),
	}

	multicaller := testutil.NewMockMulticaller()
	wireAdapterReads(t, multicaller, head.Hash(), vault, adapter, big.NewInt(777))

	service := buildIntegrationService(t, ctx, pool, chain, multicaller, &fakeProgressStore{})
	if err := service.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	adapterID := readSingleAdapterID(t, ctx, pool, adapter)
	observations := readAdapterMembership(t, ctx, pool, adapterID)
	want := []membershipObservation{
		{block: int64(addBlock), isMember: true, observedVia: "add_adapter_event"},
		{block: int64(removeBlock), isMember: false, observedVia: "remove_adapter_event"},
		{block: int64(reAddBlock), isMember: true, observedVia: "add_adapter_event"},
	}
	if !slices.Equal(observations, want) {
		t.Errorf("membership log = %+v, want %+v", observations, want)
	}

	// The head assertion added nothing: the replayed re-add already answers
	// "member" there, which is the property that makes a re-run quiet.
	if got := len(observations); got != 3 {
		t.Errorf("observations = %d, want 3 — the head seed must not append when the log already agrees", got)
	}

	// The seed's snapshot must exist and hang off the one identity row — that is
	// what clears VEC-219's adapter_data_missing gate, and no lifecycle
	// observation can move the row it points at.
	var stateBlock int64
	var stateAdapterID int64
	err := pool.QueryRow(ctx,
		`SELECT block_number, morpho_adapter_id FROM morpho_adapter_state ORDER BY block_number DESC LIMIT 1`,
	).Scan(&stateBlock, &stateAdapterID)
	if err != nil {
		t.Fatalf("reading seeded adapter_state: %v", err)
	}
	if stateBlock != headBlock {
		t.Errorf("adapter_state block = %d, want the pinned head %d", stateBlock, headBlock)
	}
	if stateAdapterID != adapterID {
		t.Errorf("adapter_state belongs to adapter %d, want the identity row %d", stateAdapterID, adapterID)
	}

	// And the adapter is in the current set the view exposes.
	var current int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM morpho_adapter_current WHERE id = $1`, adapterID).Scan(&current); err != nil {
		t.Fatalf("querying morpho_adapter_current: %v", err)
	}
	if current != 1 {
		t.Errorf("morpho_adapter_current holds %d rows for the adapter, want 1", current)
	}
}

// TestRun_DeregistersAnAdapterTheChainNoLongerHolds drives R2 through real SQL:
// the registry says an adapter is a member, the head enumeration does not return
// it, and the run must record that absence.
//
// This is the one correction the rest of the system cannot make. Every other
// write path asserts that an adapter IS a member — an AddAdapter, an Allocate, an
// enumeration — so a RemoveAdapter we never witnessed would otherwise stay
// invisible forever and the adapter would keep being priced. The enumeration is
// the only read that sees the whole set.
func TestRun_DeregistersAnAdapterTheChainNoLongerHolds(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	const addBlock = uint64(23_400_000)
	const headBlock = int64(23_700_000)
	vault := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	gone := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")
	kept := common.HexToAddress("0x00000000000000000000000000000000000000bb")

	seedVaultRow(t, ctx, pool, vault)

	chain := newFakeChainReader()
	head := chain.setFinalizedHead(headBlock, 1_770_000_000)
	chain.addBlock(addBlock, 1_760_000_000)
	chain.logs = []ethtypes.Log{
		adapterLifecycleLog(t, "AddAdapter", vault, gone, addBlock, chain.hashOf(addBlock), 0),
	}

	// Run 1: the chain still holds `gone`, so it is recorded as a member.
	multicaller := testutil.NewMockMulticaller()
	wireAdapterReads(t, multicaller, head.Hash(), vault, gone, big.NewInt(777))
	if err := buildIntegrationService(t, ctx, pool, chain, multicaller, &fakeProgressStore{}).Run(ctx); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	goneID := readSingleAdapterID(t, ctx, pool, gone)
	if !isCurrentAdapter(t, ctx, pool, goneID) {
		t.Fatal("the adapter must be a member after the first run")
	}

	// Run 2: the curator removed it while we were not watching — the enumeration
	// now returns a different adapter, and the RemoveAdapter log is NOT replayed.
	chain.logs = nil
	secondCaller := testutil.NewMockMulticaller()
	wireAdapterReads(t, secondCaller, head.Hash(), vault, kept, big.NewInt(555))
	if err := buildIntegrationService(t, ctx, pool, chain, secondCaller, &fakeProgressStore{}).Run(ctx); err != nil {
		t.Fatalf("second Run: %v", err)
	}

	if isCurrentAdapter(t, ctx, pool, goneID) {
		t.Errorf("adapter %s is still a member after an enumeration that did not return it: %+v",
			gone.Hex(), readAdapterMembership(t, ctx, pool, goneID))
	}
	observations := readAdapterMembership(t, ctx, pool, goneID)
	last := observations[len(observations)-1]
	if last.isMember || last.observedVia != "bootstrap_seed" || last.block != headBlock {
		t.Errorf("last observation = %+v, want a bootstrap_seed non-membership at the head block %d", last, headBlock)
	}
	// The identity row survives the de-registration — that is what keeps its
	// realAssets snapshots attached to something.
	if got := readSingleAdapterID(t, ctx, pool, gone); got != goneID {
		t.Errorf("identity id moved from %d to %d", goneID, got)
	}
	// And the adapter the chain DOES hold is a member.
	if !isCurrentAdapter(t, ctx, pool, readSingleAdapterID(t, ctx, pool, kept)) {
		t.Error("the enumerated adapter must be recorded as a member")
	}

	// Run 3: nothing changed, so the sweep must be quiet — a de-registration is
	// recorded once, not on every run.
	before := countRows(t, ctx, pool, "morpho_adapter_membership")
	thirdCaller := testutil.NewMockMulticaller()
	wireAdapterReads(t, thirdCaller, head.Hash(), vault, kept, big.NewInt(555))
	if err := buildIntegrationService(t, ctx, pool, chain, thirdCaller, &fakeProgressStore{}).Run(ctx); err != nil {
		t.Fatalf("third Run: %v", err)
	}
	if after := countRows(t, ctx, pool, "morpho_adapter_membership"); after != before {
		t.Errorf("membership rows = %d after an unchanged re-run, want the original %d", after, before)
	}
}

// TestRun_LeavesAnAdapterAddedAboveThePinnedHeadAlone covers the gap between the
// run's pinned finalized head and the chain head — roughly 13 minutes of blocks,
// plus however long the run takes.
//
// Live indexing is still writing in that gap. An AddAdapter it records there cannot
// appear in an enumeration pinned to the finalized head, so a de-registration pass
// that asked the registry "who is a member NOW" would find that adapter, miss it in
// the enumeration, and record it as removed at a block where it was on-chain. The
// ordering tuple would still answer correctly (the real add sits above the false
// removal), but the run would emit a WARN and a de-registration for an adapter that
// was never removed. Reading the registry AS OF the pinned head is what avoids it.
func TestRun_LeavesAnAdapterAddedAboveThePinnedHeadAlone(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	const headBlock = int64(23_700_000)
	vault := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	enumerated := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")
	justAdded := common.HexToAddress("0x00000000000000000000000000000000000000cc")

	seedVaultRow(t, ctx, pool, vault)
	// What live indexing recorded after the head this run will pin.
	justAddedID := seedMembershipObservation(t, ctx, pool, vault, justAdded, headBlock+50, true, "add_adapter_event")

	chain := newFakeChainReader()
	head := chain.setFinalizedHead(headBlock, 1_770_000_000)
	multicaller := testutil.NewMockMulticaller()
	wireAdapterReads(t, multicaller, head.Hash(), vault, enumerated, big.NewInt(555))

	if err := buildIntegrationService(t, ctx, pool, chain, multicaller, &fakeProgressStore{}).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	observations := readAdapterMembership(t, ctx, pool, justAddedID)
	if len(observations) != 1 {
		t.Errorf("the seed wrote %d extra observations for an adapter it could not have enumerated: %+v",
			len(observations)-1, observations)
	}
	if !isCurrentAdapter(t, ctx, pool, justAddedID) {
		t.Error("the adapter added above the pinned head must still be a member")
	}
}

// seedMembershipObservation writes an adapter identity row and one membership
// observation directly, standing in for what live indexing recorded while a run was
// in flight. Returns the adapter's identity id.
func seedMembershipObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vault, adapter common.Address, block int64, isMember bool, observedVia string) int64 {
	t.Helper()
	var adapterID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO morpho_adapter (morpho_vault_id, address, asset_token_id)
		 SELECT v.id, $2, v.asset_token_id FROM morpho_vault v WHERE v.address = $1
		 RETURNING id`,
		vault.Bytes(), adapter.Bytes()).Scan(&adapterID); err != nil {
		t.Fatalf("seeding morpho_adapter: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO morpho_adapter_membership
		     (morpho_adapter_id, block_number, block_version, log_index, timestamp, is_member, adapter_type, observed_via)
		 VALUES ($1, $2, 0, 0, NOW(), $3, 1, $4)`,
		adapterID, block, isMember, observedVia); err != nil {
		t.Fatalf("seeding morpho_adapter_membership: %v", err)
	}
	return adapterID
}

// isCurrentAdapter reports whether the adapter is in the set morpho_adapter_current
// exposes — the surface VEC-219's readers use.
func isCurrentAdapter(t *testing.T, ctx context.Context, pool *pgxpool.Pool, adapterID int64) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM morpho_adapter_current WHERE id = $1`, adapterID).Scan(&n); err != nil {
		t.Fatalf("querying morpho_adapter_current: %v", err)
	}
	return n == 1
}

// TestRun_IsIdempotent re-runs the whole bootstrap against the state the first
// run produced. A second run must converge, not duplicate: the operator is
// explicitly told a repeat run is safe.
func TestRun_IsIdempotent(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	const addBlock = uint64(23_400_000)
	const headBlock = int64(23_700_000)
	vault := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

	seedVaultRow(t, ctx, pool, vault)

	chain := newFakeChainReader()
	head := chain.setFinalizedHead(headBlock, 1_770_000_000)
	chain.addBlock(addBlock, 1_760_000_000)
	chain.logs = []ethtypes.Log{
		adapterLifecycleLog(t, "AddAdapter", vault, adapter, addBlock, chain.hashOf(addBlock), 0),
	}

	multicaller := testutil.NewMockMulticaller()
	wireAdapterReads(t, multicaller, head.Hash(), vault, adapter, big.NewInt(777))

	service := buildIntegrationService(t, ctx, pool, chain, multicaller, &fakeProgressStore{})
	if err := service.Run(ctx); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	firstAdapterID := readSingleAdapterID(t, ctx, pool, adapter)
	firstObservations := readAdapterMembership(t, ctx, pool, firstAdapterID)
	firstStates := countRows(t, ctx, pool, "morpho_adapter_state")

	// A second run is a new workflow execution, so it carries no heartbeat
	// details: it sweeps the whole range again, as in production.
	reRun := buildIntegrationService(t, ctx, pool, chain, multicaller, &fakeProgressStore{})
	if err := reRun.Run(ctx); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	secondAdapterID := readSingleAdapterID(t, ctx, pool, adapter)
	if secondAdapterID != firstAdapterID {
		t.Fatalf("re-running minted a new identity id %d, want the original %d", secondAdapterID, firstAdapterID)
	}
	secondObservations := readAdapterMembership(t, ctx, pool, secondAdapterID)
	if len(secondObservations) != len(firstObservations) {
		t.Fatalf("re-running produced %d observations, want the original %d", len(secondObservations), len(firstObservations))
	}
	if !slices.Equal(secondObservations, firstObservations) {
		t.Errorf("membership log changed on re-run: %+v then %+v", firstObservations, secondObservations)
	}
	if got := countRows(t, ctx, pool, "morpho_adapter_state"); got != firstStates {
		t.Errorf("adapter_state rows = %d after re-run, want the original %d (same build must dedupe)", got, firstStates)
	}
}

// TestRun_ResumesAfterAKilledAttemptAndFinishesTheWork drives the resume path
// through real SQL: an attempt dies mid-sweep, and the next attempt — the same
// activity, so it reads back what the first heartbeated — restarts at the next
// chunk and replays the history the first never reached.
//
// The AddAdapter deliberately sits in the LAST chunk, so a resume that quietly
// skipped the remaining blocks would leave the vault unrepaired rather than
// merely re-do work.
func TestRun_ResumesAfterAKilledAttemptAndFinishesTheWork(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	const (
		addBlock       = uint64(23_398_000)
		headBlock      = int64(23_400_000)
		firstChunkTo   = mainnetVaultV2DeployBlock + 9_999
		secondChunkTop = firstChunkTo + 1
	)
	vault := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

	seedVaultRow(t, ctx, pool, vault)

	chain := newFakeChainReader()
	head := chain.setFinalizedHead(headBlock, 1_770_000_000)
	chain.addBlock(addBlock, 1_760_000_000)
	chain.logs = []ethtypes.Log{
		adapterLifecycleLog(t, "AddAdapter", vault, adapter, addBlock, chain.hashOf(addBlock), 0),
	}

	multicaller := testutil.NewMockMulticaller()
	wireAdapterReads(t, multicaller, head.Hash(), vault, adapter, big.NewInt(777))

	progress := &fakeProgressStore{}
	service := buildIntegrationService(t, ctx, pool, chain, multicaller, progress)

	chain.failFilterAfter = 1
	if err := service.Run(ctx); err == nil {
		t.Fatal("expected the killed attempt to fail")
	}
	if got := progress.savedTo(); len(got) != 1 || got[0] != firstChunkTo {
		t.Fatalf("recorded sweep positions %v, want just the completed chunk [%d]", got, int64(firstChunkTo))
	}

	chain.failFilterAfter, chain.queries = 0, nil
	if err := service.Run(ctx); err != nil {
		t.Fatalf("resumed Run: %v", err)
	}

	if len(chain.queries) == 0 {
		t.Fatal("the resumed attempt issued no eth_getLogs request")
	}
	if got := chain.queries[0].FromBlock.Int64(); got != secondChunkTop {
		t.Errorf("resumed sweep starts at block %d, want %d", got, int64(secondChunkTop))
	}

	adapterID := readSingleAdapterID(t, ctx, pool, adapter)
	observations := readAdapterMembership(t, ctx, pool, adapterID)
	if len(observations) != 1 {
		t.Fatalf("membership log = %+v, want the one observation the resumed attempt replayed", observations)
	}
	if observations[0].block != int64(addBlock) || observations[0].observedVia != "add_adapter_event" {
		t.Errorf("observation = %+v, want the AddAdapter at block %d — the resumed attempt did not replay it",
			observations[0], addBlock)
	}
}

// --- integration helpers -----------------------------------------------------

// buildIntegrationService wires the bootstrap against the REAL postgres
// repositories and the REAL morpho-indexer replay service, exactly as
// cmd/cronjobs/morpho-v2-bootstrap does. Only the node is faked.
func buildIntegrationService(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chain ChainReader, multicaller *testutil.MockMulticaller, progress ProgressStore) *Service {
	t.Helper()
	t.Setenv("BUILD_GIT_HASH", "test")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("buildregistry.New: %v", err)
	}
	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	morphoRepo, err := postgres.NewMorphoRepository(pool, logger, buildReg.BuildID())
	if err != nil {
		t.Fatalf("NewMorphoRepository: %v", err)
	}
	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, buildReg.BuildID(), 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	eventRepo := postgres.NewEventRepository(logger, buildReg.BuildID())

	svcConfig := morpho_indexer.ConfigDefaults()
	svcConfig.ChainID = 1
	svcConfig.Logger = logger
	replay, err := morpho_indexer.NewReplayService(svcConfig, multicaller, txManager, protocolRepo, morphoRepo, eventRepo)
	if err != nil {
		t.Fatalf("NewReplayService: %v", err)
	}

	cfg := ConfigDefaults()
	cfg.ChainID = 1
	cfg.Logger = logger
	service, err := NewService(cfg, chain, replay, progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service
}

// seedVaultRow inserts the protocol, asset token, and VaultV2 row the bootstrap
// expects to already exist — the exact starting state of the vaults this job
// repairs: a vault with no adapters.
func seedVaultRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vault common.Address) {
	t.Helper()
	var protocolID int64
	err := pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (1, '\xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb'::bytea, 'Morpho Blue', 'lending', 18883124, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`).Scan(&protocolID)
	if err != nil {
		t.Fatalf("seeding protocol: %v", err)
	}

	var tokenID int64
	err = pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, $1, 'USDC', 6)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
		 RETURNING id`,
		common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").Bytes()).Scan(&tokenID)
	if err != nil {
		t.Fatalf("seeding token: %v", err)
	}

	_, err = pool.Exec(ctx,
		`INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
		 VALUES (1, $1, $2, 'Test Vault', 'tVAULT', $3, 3, 23400000)
		 ON CONFLICT DO NOTHING`,
		protocolID, vault.Bytes(), tokenID)
	if err != nil {
		t.Fatalf("seeding morpho_vault: %v", err)
	}
}

// adapterLifecycleLog builds an AddAdapter or RemoveAdapter log from the
// registered ABI, so the fixture cannot drift from the real event signature.
func adapterLifecycleLog(t *testing.T, event string, vault, adapter common.Address, blockNumber uint64, blockHash common.Hash, index uint) ethtypes.Log {
	t.Helper()
	eventsABI, err := abis.GetVaultV2EventsABI()
	if err != nil {
		t.Fatalf("GetVaultV2EventsABI: %v", err)
	}
	ev, ok := eventsABI.Events[event]
	if !ok {
		t.Fatalf("event %q not in the VaultV2 ABI", event)
	}
	return ethtypes.Log{
		Address:     vault,
		Topics:      []common.Hash{ev.ID, common.BytesToHash(common.LeftPadBytes(adapter.Bytes(), 32))},
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
		TxHash:      common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
		Index:       index,
	}
}

// membershipObservation is one morpho_adapter_membership row, reduced to what a
// bootstrap test asserts on.
type membershipObservation struct {
	block       int64
	isMember    bool
	observedVia string
}

// readSingleAdapterID returns the adapter's one identity row id, failing if the
// registry holds anything other than exactly one — the invariant that replaced
// the incarnation model.
func readSingleAdapterID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, adapter common.Address) int64 {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT id FROM morpho_adapter WHERE address = $1`, adapter.Bytes())
	if err != nil {
		t.Fatalf("querying morpho_adapter: %v", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning morpho_adapter: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating morpho_adapter: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("morpho_adapter rows for %s = %d, want exactly 1 forever", adapter.Hex(), len(ids))
	}
	return ids[0]
}

// readAdapterMembership returns an adapter's observations in selection order
// (oldest first), so a test can assert on the whole lifecycle rather than only
// the current answer.
func readAdapterMembership(t *testing.T, ctx context.Context, pool *pgxpool.Pool, adapterID int64) []membershipObservation {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT block_number, is_member, observed_via FROM morpho_adapter_membership
		 WHERE morpho_adapter_id = $1
		 ORDER BY block_number, block_version, log_index, processing_version`,
		adapterID)
	if err != nil {
		t.Fatalf("querying morpho_adapter_membership: %v", err)
	}
	defer rows.Close()

	var out []membershipObservation
	for rows.Next() {
		var o membershipObservation
		if err := rows.Scan(&o.block, &o.isMember, &o.observedVia); err != nil {
			t.Fatalf("scanning morpho_adapter_membership: %v", err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating morpho_adapter_membership: %v", err)
	}
	return out
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return n
}

// hashOf returns the hash of a previously registered block, so a test can build
// several logs at the same block without threading its hash through.
func (f *fakeChainReader) hashOf(number uint64) common.Hash {
	for hash, header := range f.headers {
		if header.Number.Uint64() == number {
			return hash
		}
	}
	return common.Hash{}
}
