package testutil

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SeedToken inserts a token into the token table and returns its auto-generated ID.
func SeedToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int, address, symbol string, decimals int) int64 {
	t.Helper()
	addressBytes, err := HexToBytes(address)
	if err != nil {
		t.Fatalf("failed to parse address %s: %v", address, err)
	}

	var id int64
	err = pool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals, updated_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
		RETURNING id
	`, chainID, addressBytes, symbol, decimals).Scan(&id)
	if err != nil {
		t.Fatalf("failed to insert test token %s: %v", symbol, err)
	}
	return id
}

// SeedOracle inserts an oracle and returns its auto-generated ID.
func SeedOracle(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name, displayName string, chainID int, address string) int64 {
	t.Helper()
	addrBytes, err := HexToBytes(address)
	if err != nil {
		t.Fatalf("failed to parse oracle address %s: %v", address, err)
	}

	var id int64
	err = pool.QueryRow(ctx, `
		INSERT INTO oracle (name, display_name, chain_id, address, deployment_block, enabled)
		VALUES ($1, $2, $3, $4, 100, true)
		ON CONFLICT (name) DO UPDATE SET display_name = EXCLUDED.display_name
		RETURNING id
	`, name, displayName, chainID, addrBytes).Scan(&id)
	if err != nil {
		t.Fatalf("failed to insert oracle %s: %v", name, err)
	}
	return id
}

// SeedOracleAsset inserts an oracle asset link.
func SeedOracleAsset(t *testing.T, ctx context.Context, pool *pgxpool.Pool, oracleID, tokenID int64) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled)
		VALUES ($1, $2, true)
		ON CONFLICT (oracle_id, token_id) DO NOTHING
	`, oracleID, tokenID)
	if err != nil {
		t.Fatalf("failed to insert oracle asset (oracle=%d, token=%d): %v", oracleID, tokenID, err)
	}
}
