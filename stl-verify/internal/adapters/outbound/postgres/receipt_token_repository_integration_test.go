//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const receiptTokenSchemaName = "test_receipt_token"

var receiptTokenPool *pgxpool.Pool

func init() {
	registerTestFileSetup(receiptTokenSchemaName, func() {
		receiptTokenPool = testutil.SetupSchemaForMain(sharedDSN, receiptTokenSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, receiptTokenPool, receiptTokenSchemaName)
	})
}

// truncateReceiptToken clears the receipt_token table and its dependencies for test isolation.
func truncateReceiptToken(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := receiptTokenPool.Exec(ctx, `TRUNCATE receipt_token CASCADE`)
	if err != nil {
		t.Fatalf("failed to truncate receipt_token: %v", err)
	}
}

// seedReceiptTokenDeps inserts the protocol and token rows that receipt_token
// foreign keys reference, returning (protocolID, tokenID).
func seedReceiptTokenDeps(t *testing.T, ctx context.Context, chainID int64) (int64, int64) {
	t.Helper()

	protocolRepo, err := NewProtocolRepository(receiptTokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	tokenRepo, err := NewTokenRepository(receiptTokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	tx, err := receiptTokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	protocolAddr := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	protocolID, err := protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddr, "SparkLend", "lending", 100)
	if err != nil {
		t.Fatalf("GetOrCreateProtocol: %v", err)
	}

	tokenAddr := common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")
	tokenID, err := tokenRepo.GetOrCreateToken(ctx, tx, chainID, tokenAddr, "DAI", 18, 100)
	if err != nil {
		t.Fatalf("GetOrCreateToken: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	return protocolID, tokenID
}

func TestGetOrCreateReceiptToken_CreatesNewRow(t *testing.T) {
	ctx := context.Background()
	truncateReceiptToken(t, ctx)

	protocolID, tokenID := seedReceiptTokenDeps(t, ctx, 1)

	repo, err := NewReceiptTokenRepository(receiptTokenPool, nil)
	if err != nil {
		t.Fatalf("NewReceiptTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	token, err := entity.NewReceiptToken(1, protocolID, tokenID, 500, addr, "spDAI")
	if err != nil {
		t.Fatalf("NewReceiptToken: %v", err)
	}

	tx, err := receiptTokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	id, err := repo.GetOrCreateReceiptToken(ctx, tx, *token)
	if err != nil {
		t.Fatalf("GetOrCreateReceiptToken: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero id")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify the row exists with correct values.
	var symbol string
	var createdAtBlock int64
	err = receiptTokenPool.QueryRow(ctx,
		`SELECT symbol, created_at_block FROM receipt_token WHERE id = $1`, id,
	).Scan(&symbol, &createdAtBlock)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if symbol != "spDAI" {
		t.Errorf("symbol = %q, want spDAI", symbol)
	}
	if createdAtBlock != 500 {
		t.Errorf("created_at_block = %d, want 500", createdAtBlock)
	}
}

func TestGetOrCreateReceiptToken_IdempotentReturnsSameID(t *testing.T) {
	ctx := context.Background()
	truncateReceiptToken(t, ctx)

	protocolID, tokenID := seedReceiptTokenDeps(t, ctx, 1)

	repo, err := NewReceiptTokenRepository(receiptTokenPool, nil)
	if err != nil {
		t.Fatalf("NewReceiptTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	token, err := entity.NewReceiptToken(1, protocolID, tokenID, 500, addr, "spDAI")
	if err != nil {
		t.Fatalf("NewReceiptToken: %v", err)
	}

	// First insert.
	tx1, err := receiptTokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	id1, err := repo.GetOrCreateReceiptToken(ctx, tx1, *token)
	if err != nil {
		t.Fatalf("first GetOrCreateReceiptToken: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	// Second insert with same chain_id + address.
	tx2, err := receiptTokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	id2, err := repo.GetOrCreateReceiptToken(ctx, tx2, *token)
	if err != nil {
		t.Fatalf("second GetOrCreateReceiptToken: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	if id1 != id2 {
		t.Errorf("expected same id on second call, got id1=%d id2=%d", id1, id2)
	}
}

func TestGetOrCreateReceiptToken_CreatedAtBlockUsesLeast(t *testing.T) {
	ctx := context.Background()
	truncateReceiptToken(t, ctx)

	protocolID, tokenID := seedReceiptTokenDeps(t, ctx, 1)

	repo, err := NewReceiptTokenRepository(receiptTokenPool, nil)
	if err != nil {
		t.Fatalf("NewReceiptTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")

	// First call: block 500 (a later block, processed first out of order).
	token1, err := entity.NewReceiptToken(1, protocolID, tokenID, 500, addr, "spDAI")
	if err != nil {
		t.Fatalf("NewReceiptToken: %v", err)
	}
	tx1, err := receiptTokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	if _, err := repo.GetOrCreateReceiptToken(ctx, tx1, *token1); err != nil {
		t.Fatalf("first GetOrCreateReceiptToken: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	// Second call: block 100 (the true first seen block, processed out of order).
	token2, err := entity.NewReceiptToken(1, protocolID, tokenID, 100, addr, "spDAI")
	if err != nil {
		t.Fatalf("NewReceiptToken: %v", err)
	}
	tx2, err := receiptTokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	if _, err := repo.GetOrCreateReceiptToken(ctx, tx2, *token2); err != nil {
		t.Fatalf("second GetOrCreateReceiptToken: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	var createdAtBlock int64
	err = receiptTokenPool.QueryRow(ctx,
		`SELECT created_at_block FROM receipt_token WHERE chain_id = $1 AND receipt_token_address = $2`,
		1, addr.Bytes(),
	).Scan(&createdAtBlock)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if createdAtBlock != 100 {
		t.Errorf("created_at_block = %d, want 100 (LEAST of 500 and 100)", createdAtBlock)
	}
}

func TestGetOrCreateReceiptToken_DifferentChainSameAddressCreatesNewRow(t *testing.T) {
	ctx := context.Background()
	truncateReceiptToken(t, ctx)

	protocolID1, tokenID1 := seedReceiptTokenDeps(t, ctx, 1)
	protocolID2, tokenID2 := seedReceiptTokenDeps(t, ctx, 43114)

	repo, err := NewReceiptTokenRepository(receiptTokenPool, nil)
	if err != nil {
		t.Fatalf("NewReceiptTokenRepository: %v", err)
	}

	addr := common.HexToAddress("0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")

	// Chain 1
	token1, err := entity.NewReceiptToken(1, protocolID1, tokenID1, 500, addr, "spDAI")
	if err != nil {
		t.Fatalf("NewReceiptToken chain1: %v", err)
	}
	tx1, err := receiptTokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	id1, err := repo.GetOrCreateReceiptToken(ctx, tx1, *token1)
	if err != nil {
		t.Fatalf("GetOrCreateReceiptToken chain1: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	// Chain 43114 (Avalanche — different chain, same address)
	token2, err := entity.NewReceiptToken(43114, protocolID2, tokenID2, 500, addr, "spDAI")
	if err != nil {
		t.Fatalf("NewReceiptToken chain42161: %v", err)
	}
	tx2, err := receiptTokenPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	id2, err := repo.GetOrCreateReceiptToken(ctx, tx2, *token2)
	if err != nil {
		t.Fatalf("GetOrCreateReceiptToken chain42161: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	if id1 == id2 {
		t.Errorf("expected different IDs for different chains, got id1=%d id2=%d", id1, id2)
	}
}
