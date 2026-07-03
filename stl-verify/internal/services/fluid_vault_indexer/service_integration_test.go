//go:build integration

package fluid_vault_indexer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// fluidIntegrationFixture wires the real Postgres repositories (token, protocol,
// fluid vault, tx manager) against a fresh schema, with the chain (resolver +
// ERC-20 metadata) mocked via fakeChain — the chain is the only data source we
// cannot control, per the project's integration-test rule.
type fluidIntegrationFixture struct {
	svc   *Service
	chain *fakeChain
	cache *stubCache
	pool  *pgxpool.Pool
}

func setupFluidIntegration(t *testing.T) *fluidIntegrationFixture {
	t.Helper()

	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	tokenRepo, err := postgres.NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	protocolRepo, err := postgres.NewProtocolRepository(pool, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	vaultRepo, err := postgres.NewFluidVaultRepository(pool, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewFluidVaultRepository: %v", err)
	}
	txManager, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}

	chain := newFakeChain(t)
	cache := &stubCache{receipts: map[int64]json.RawMessage{}}

	svc, err := NewService(
		Config{SQSConsumerConfig: shared.SQSConsumerConfig{ChainID: 1, Logger: testLogger()}},
		stubConsumer{}, cache, stubBlockQuerier{head: 19_500_000}, chain, txManager, vaultRepo, tokenRepo, protocolRepo,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	return &fluidIntegrationFixture{svc: svc, chain: chain, cache: cache, pool: pool}
}

// TestIntegration_ReconcileThenSnapshot exercises the full path: a startup
// reconcile registers an in-scope sUSDS vault (writing fluid_vault +
// resolving its tokens), then a BlockEvent with a vault log writes one
// fluid_vault_state row carrying the correct on-chain totals.
func TestIntegration_ReconcileThenSnapshot(t *testing.T) {
	ctx := context.Background()
	f := setupFluidIntegration(t)

	vault := common.HexToAddress(susdsVaultAddr)
	susds := common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")
	f.chain.allVaults = []common.Address{vault}
	f.chain.vaultData[vault] = readFixture(t, "vault_entire_data_single_susds.hex")
	f.chain.tokenSymbol[susds] = "sUSDS"
	f.chain.tokenDec[susds] = 18

	if err := f.svc.ReconcileVaults(ctx, 19_500_000); err != nil {
		t.Fatalf("ReconcileVaults: %v", err)
	}

	var vaultCount int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM fluid_vault WHERE chain_id = 1`).Scan(&vaultCount); err != nil {
		t.Fatalf("counting fluid_vault: %v", err)
	}
	if vaultCount != 1 {
		t.Fatalf("fluid_vault rows = %d, want 1", vaultCount)
	}

	f.cache.receipts[19_500_010] = receiptsWithLog(t, vault, logOperateTopic(t))
	event := blockEvent(19_500_010)
	if err := f.svc.processBlockEvent(ctx, event); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	var totalColl, totalDebt string
	err := f.pool.QueryRow(ctx,
		`SELECT total_collateral::text, total_debt::text FROM fluid_vault_state
		 WHERE block_number = $1`, int64(19_500_010)).Scan(&totalColl, &totalDebt)
	if err != nil {
		t.Fatalf("querying fluid_vault_state: %v", err)
	}
	if totalColl != "11328444893030209" {
		t.Errorf("total_collateral = %s, want 11328444893030209", totalColl)
	}
	if totalDebt != "8021962986715460141" {
		t.Errorf("total_debt = %s, want 8021962986715460141", totalDebt)
	}
}
