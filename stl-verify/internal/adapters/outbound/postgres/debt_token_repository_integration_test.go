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

const debtTokenSchemaName = "test_debt_token"

var debtTokenPool *pgxpool.Pool

func init() {
	registerTestFileSetup(debtTokenSchemaName, func() {
		debtTokenPool = testutil.SetupSchemaForMain(sharedDSN, debtTokenSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, debtTokenPool, debtTokenSchemaName)
	})
}

// truncateDebtToken clears the debt_token table and its dependencies for test isolation.
func truncateDebtToken(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := debtTokenPool.Exec(ctx, `TRUNCATE debt_token CASCADE`)
	if err != nil {
		t.Fatalf("failed to truncate debt_token: %v", err)
	}
}

// seedDebtTokenDeps inserts the protocol and token rows that debt_token
// foreign keys reference, returning (protocolID, tokenID).
func seedDebtTokenDeps(t *testing.T, ctx context.Context, chainID int64) (int64, int64) {
	t.Helper()

	protocolRepo, err := NewProtocolRepository(debtTokenPool, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewProtocolRepository: %v", err)
	}
	tokenRepo, err := NewTokenRepository(debtTokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	tx, err := debtTokenPool.Begin(ctx)
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
	tokenID, err := tokenRepo.GetOrCreateToken(ctx, tx, chainID, tokenAddr, "DAI", 18, i64(100))
	if err != nil {
		t.Fatalf("GetOrCreateToken: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	return protocolID, tokenID
}

func TestGetOrCreateDebtToken(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, ctx context.Context)
	}{
		{
			name: "creates new row with correct values",
			run: func(t *testing.T, ctx context.Context) {
				truncateDebtToken(t, ctx)
				protocolID, tokenID := seedDebtTokenDeps(t, ctx, 1)

				repo, err := NewDebtTokenRepository(debtTokenPool, nil)
				if err != nil {
					t.Fatalf("NewDebtTokenRepository: %v", err)
				}

				variableAddr := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
				stableAddr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
				token, err := entity.NewDebtToken(protocolID, tokenID, 500, variableAddr.Bytes(), stableAddr.Bytes(), "variableDebtDAI", "stableDebtDAI")
				if err != nil {
					t.Fatalf("NewDebtToken: %v", err)
				}

				tx, err := debtTokenPool.Begin(ctx)
				if err != nil {
					t.Fatalf("Begin: %v", err)
				}
				defer tx.Rollback(ctx)

				id, err := repo.GetOrCreateDebtToken(ctx, tx, *token)
				if err != nil {
					t.Fatalf("GetOrCreateDebtToken: %v", err)
				}
				if id == 0 {
					t.Fatal("expected non-zero id")
				}

				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("Commit: %v", err)
				}

				// Verify the row exists with correct values.
				var variableSymbol, stableSymbol string
				var createdAtBlock int64
				err = debtTokenPool.QueryRow(ctx,
					`SELECT variable_symbol, stable_symbol, created_at_block FROM debt_token WHERE id = $1`, id,
				).Scan(&variableSymbol, &stableSymbol, &createdAtBlock)
				if err != nil {
					t.Fatalf("query: %v", err)
				}
				if variableSymbol != "variableDebtDAI" {
					t.Errorf("variable_symbol = %q, want variableDebtDAI", variableSymbol)
				}
				if stableSymbol != "stableDebtDAI" {
					t.Errorf("stable_symbol = %q, want stableDebtDAI", stableSymbol)
				}
				if createdAtBlock != 500 {
					t.Errorf("created_at_block = %d, want 500", createdAtBlock)
				}
			},
		},
		{
			name: "idempotent returns same ID with no duplicate row",
			run: func(t *testing.T, ctx context.Context) {
				truncateDebtToken(t, ctx)
				protocolID, tokenID := seedDebtTokenDeps(t, ctx, 1)

				repo, err := NewDebtTokenRepository(debtTokenPool, nil)
				if err != nil {
					t.Fatalf("NewDebtTokenRepository: %v", err)
				}

				variableAddr := common.HexToAddress("0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")
				token, err := entity.NewDebtToken(protocolID, tokenID, 500, variableAddr.Bytes(), nil, "variableDebtDAI", "")
				if err != nil {
					t.Fatalf("NewDebtToken: %v", err)
				}

				// First insert.
				tx1, err := debtTokenPool.Begin(ctx)
				if err != nil {
					t.Fatalf("Begin tx1: %v", err)
				}
				id1, err := repo.GetOrCreateDebtToken(ctx, tx1, *token)
				if err != nil {
					t.Fatalf("first GetOrCreateDebtToken: %v", err)
				}
				if err := tx1.Commit(ctx); err != nil {
					t.Fatalf("Commit tx1: %v", err)
				}

				// Second insert with same (protocol_id, underlying_token_id).
				tx2, err := debtTokenPool.Begin(ctx)
				if err != nil {
					t.Fatalf("Begin tx2: %v", err)
				}
				id2, err := repo.GetOrCreateDebtToken(ctx, tx2, *token)
				if err != nil {
					t.Fatalf("second GetOrCreateDebtToken: %v", err)
				}
				if err := tx2.Commit(ctx); err != nil {
					t.Fatalf("Commit tx2: %v", err)
				}

				if id1 != id2 {
					t.Errorf("expected same id on second call, got id1=%d id2=%d", id1, id2)
				}

				var count int
				err = debtTokenPool.QueryRow(ctx,
					`SELECT COUNT(*) FROM debt_token WHERE protocol_id = $1 AND underlying_token_id = $2`,
					protocolID, tokenID,
				).Scan(&count)
				if err != nil {
					t.Fatalf("count query: %v", err)
				}
				if count != 1 {
					t.Errorf("expected exactly 1 row, got %d", count)
				}
			},
		},
		{
			name: "only variable address (stable nil)",
			run: func(t *testing.T, ctx context.Context) {
				truncateDebtToken(t, ctx)
				protocolID, tokenID := seedDebtTokenDeps(t, ctx, 1)

				repo, err := NewDebtTokenRepository(debtTokenPool, nil)
				if err != nil {
					t.Fatalf("NewDebtTokenRepository: %v", err)
				}

				variableAddr := common.HexToAddress("0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")
				token, err := entity.NewDebtToken(protocolID, tokenID, 500, variableAddr.Bytes(), nil, "variableDebtDAI", "")
				if err != nil {
					t.Fatalf("NewDebtToken: %v", err)
				}

				tx, err := debtTokenPool.Begin(ctx)
				if err != nil {
					t.Fatalf("Begin: %v", err)
				}
				defer tx.Rollback(ctx)

				id, err := repo.GetOrCreateDebtToken(ctx, tx, *token)
				if err != nil {
					t.Fatalf("GetOrCreateDebtToken: %v", err)
				}
				if err := tx.Commit(ctx); err != nil {
					t.Fatalf("Commit: %v", err)
				}

				var variableDebtAddress, stableDebtAddress []byte
				err = debtTokenPool.QueryRow(ctx,
					`SELECT variable_debt_address, stable_debt_address FROM debt_token WHERE id = $1`, id,
				).Scan(&variableDebtAddress, &stableDebtAddress)
				if err != nil {
					t.Fatalf("query: %v", err)
				}
				if string(variableDebtAddress) != string(variableAddr.Bytes()) {
					t.Errorf("variable_debt_address = %x, want %x", variableDebtAddress, variableAddr.Bytes())
				}
				if stableDebtAddress != nil {
					t.Errorf("stable_debt_address = %x, want nil", stableDebtAddress)
				}
			},
		},
		{
			name: "upsert preserves stored symbol, stable address, and lower created_at_block",
			run: func(t *testing.T, ctx context.Context) {
				truncateDebtToken(t, ctx)
				protocolID, tokenID := seedDebtTokenDeps(t, ctx, 1)

				repo, err := NewDebtTokenRepository(debtTokenPool, nil)
				if err != nil {
					t.Fatalf("NewDebtTokenRepository: %v", err)
				}

				variableAddr := common.HexToAddress("0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
				stableAddr := common.HexToAddress("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

				// First write at a LOWER block, with both symbols and both addresses.
				first, err := entity.NewDebtToken(protocolID, tokenID, 500, variableAddr.Bytes(), stableAddr.Bytes(), "variableDebtDAI", "stableDebtDAI")
				if err != nil {
					t.Fatalf("NewDebtToken first: %v", err)
				}
				tx1, err := debtTokenPool.Begin(ctx)
				if err != nil {
					t.Fatalf("Begin tx1: %v", err)
				}
				id1, err := repo.GetOrCreateDebtToken(ctx, tx1, *first)
				if err != nil {
					t.Fatalf("first GetOrCreateDebtToken: %v", err)
				}
				if err := tx1.Commit(ctx); err != nil {
					t.Fatalf("Commit tx1: %v", err)
				}

				// Second write at a HIGHER block, with an EMPTY variable symbol and a
				// NIL stable address. The merge must keep the previously-stored
				// variable symbol, the stored stable address, and the LOWER block.
				second, err := entity.NewDebtToken(protocolID, tokenID, 900, variableAddr.Bytes(), nil, "", "")
				if err != nil {
					t.Fatalf("NewDebtToken second: %v", err)
				}
				tx2, err := debtTokenPool.Begin(ctx)
				if err != nil {
					t.Fatalf("Begin tx2: %v", err)
				}
				id2, err := repo.GetOrCreateDebtToken(ctx, tx2, *second)
				if err != nil {
					t.Fatalf("second GetOrCreateDebtToken: %v", err)
				}
				if err := tx2.Commit(ctx); err != nil {
					t.Fatalf("Commit tx2: %v", err)
				}

				if id1 != id2 {
					t.Errorf("expected same id on upsert, got id1=%d id2=%d", id1, id2)
				}

				var variableSymbol, stableSymbol string
				var stableDebtAddress []byte
				var createdAtBlock int64
				err = debtTokenPool.QueryRow(ctx,
					`SELECT variable_symbol, stable_symbol, stable_debt_address, created_at_block FROM debt_token WHERE id = $1`, id2,
				).Scan(&variableSymbol, &stableSymbol, &stableDebtAddress, &createdAtBlock)
				if err != nil {
					t.Fatalf("query: %v", err)
				}
				if variableSymbol != "variableDebtDAI" {
					t.Errorf("variable_symbol = %q, want variableDebtDAI (previously stored value must survive empty overwrite)", variableSymbol)
				}
				if stableSymbol != "stableDebtDAI" {
					t.Errorf("stable_symbol = %q, want stableDebtDAI", stableSymbol)
				}
				if string(stableDebtAddress) != string(stableAddr.Bytes()) {
					t.Errorf("stable_debt_address = %x, want %x (nil overwrite must not clear it)", stableDebtAddress, stableAddr.Bytes())
				}
				if createdAtBlock != 500 {
					t.Errorf("created_at_block = %d, want 500 (LEAST must retain the lower block)", createdAtBlock)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tt.run(t, ctx)
		})
	}
}
