//go:build integration

package morpho_v2_bootstrap

import (
	"context"
	"io"
	"log/slog"
	"math/big"
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

// TestRun_SeedThenReplayConvergesAdapterIncarnations is the correctness test for
// this job's riskiest property, run against real SQL rather than mock repos.
//
// The seed writes each currently-active adapter at the run's pinned HEAD block —
// far later than its true AddAdapter. The replay then walks the real history from
// the factory deploy block. The two must converge onto the incarnations the chain
// actually had, using only the repository's own semantics (GetOrCreateAdapter's
// closed-window-first convergence and MarkAdapterRemoved's added_at_block <= X
// scoping). Getting this wrong would either leave a de-registered adapter
// spuriously active or make MarkAdapterRemoved match 0 rows and fail the run.
//
// History exercised (the hardest shape — removed then re-added):
//
//	AddAdapter@100 → RemoveAdapter@200 → AddAdapter@300, still active at head.
//
// Expected end state: one CLOSED incarnation [100,200] plus one ACTIVE row from
// 300, and an adapter_state row seeded at the head block.
func TestRun_SeedThenReplayConvergesAdapterIncarnations(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
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

	incarnations := readAdapterIncarnations(t, ctx, pool, adapter)
	if len(incarnations) != 2 {
		t.Fatalf("adapter incarnations = %+v, want exactly 2 (one closed, one active)", incarnations)
	}
	closed, active := incarnations[0], incarnations[1]
	if closed.addedAt != int64(addBlock) || closed.removedAt == nil || *closed.removedAt != int64(removeBlock) {
		t.Errorf("closed incarnation = %+v, want [%d,%d]", closed, addBlock, removeBlock)
	}
	if active.addedAt != int64(reAddBlock) || active.removedAt != nil {
		t.Errorf("active incarnation = %+v, want added_at=%d and still open", active, reAddBlock)
	}

	// The seed's snapshot must exist and belong to the currently-active
	// incarnation — that is what clears VEC-219's adapter_data_missing gate.
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
	if stateAdapterID != active.id {
		t.Errorf("adapter_state belongs to adapter %d, want the active incarnation %d", stateAdapterID, active.id)
	}
}

// TestRun_IsIdempotent re-runs the whole bootstrap against the state the first
// run produced. Re-clicking Trigger must converge, not duplicate: the operator
// is explicitly told a repeat run is safe.
func TestRun_IsIdempotent(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
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
	firstAdapters := readAdapterIncarnations(t, ctx, pool, adapter)
	firstStates := countRows(t, ctx, pool, "morpho_adapter_state")

	// A re-trigger is a new workflow execution, so it carries no heartbeat
	// details: the second run sweeps the whole range again, as in production.
	reRun := buildIntegrationService(t, ctx, pool, chain, multicaller, &fakeProgressStore{})
	if err := reRun.Run(ctx); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	secondAdapters := readAdapterIncarnations(t, ctx, pool, adapter)
	if len(secondAdapters) != len(firstAdapters) {
		t.Fatalf("re-running produced %d adapter rows, want the original %d", len(secondAdapters), len(firstAdapters))
	}
	if secondAdapters[0] != firstAdapters[0] {
		t.Errorf("adapter row changed on re-run: %+v then %+v", firstAdapters[0], secondAdapters[0])
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
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
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

	incarnations := readAdapterIncarnations(t, ctx, pool, adapter)
	if len(incarnations) != 1 {
		t.Fatalf("adapter incarnations = %+v, want the one the resumed attempt replayed", incarnations)
	}
	if incarnations[0].addedAt != int64(addBlock) {
		t.Errorf("adapter added_at_block = %d, want %d — the resumed attempt did not replay the AddAdapter",
			incarnations[0].addedAt, addBlock)
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

// adapterIncarnation is one morpho_adapter row's lifetime window.
type adapterIncarnation struct {
	id        int64
	addedAt   int64
	removedAt *int64
}

// readAdapterIncarnations returns every row for an adapter address, oldest window
// first, so a test can assert on the full lifetime history rather than only the
// active row.
func readAdapterIncarnations(t *testing.T, ctx context.Context, pool *pgxpool.Pool, adapter common.Address) []adapterIncarnation {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT id, added_at_block, removed_at_block FROM morpho_adapter WHERE address = $1 ORDER BY added_at_block`,
		adapter.Bytes())
	if err != nil {
		t.Fatalf("querying morpho_adapter: %v", err)
	}
	defer rows.Close()

	var out []adapterIncarnation
	for rows.Next() {
		var a adapterIncarnation
		if err := rows.Scan(&a.id, &a.addedAt, &a.removedAt); err != nil {
			t.Fatalf("scanning morpho_adapter: %v", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating morpho_adapter: %v", err)
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
