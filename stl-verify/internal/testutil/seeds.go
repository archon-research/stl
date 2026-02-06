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

// SeedOracleSource inserts an oracle source and returns its auto-generated ID.
func SeedOracleSource(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name, displayName string, chainID int, poolAddressProvider string) int64 {
	t.Helper()
	providerBytes, err := HexToBytes(poolAddressProvider)
	if err != nil {
		t.Fatalf("failed to parse pool address provider %s: %v", poolAddressProvider, err)
	}

	var id int64
	err = pool.QueryRow(ctx, `
		INSERT INTO oracle_source (name, display_name, chain_id, pool_address_provider, deployment_block, enabled)
		VALUES ($1, $2, $3, $4, 100, true)
		ON CONFLICT (name) DO UPDATE SET display_name = EXCLUDED.display_name
		RETURNING id
	`, name, displayName, chainID, providerBytes).Scan(&id)
	if err != nil {
		t.Fatalf("failed to insert oracle source %s: %v", name, err)
	}
	return id
}

// SeedOracleAsset inserts an oracle asset link.
func SeedOracleAsset(t *testing.T, ctx context.Context, pool *pgxpool.Pool, oracleSourceID, tokenID int64) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_source_id, token_id, enabled)
		VALUES ($1, $2, true)
		ON CONFLICT (oracle_source_id, token_id) DO NOTHING
	`, oracleSourceID, tokenID)
	if err != nil {
		t.Fatalf("failed to insert oracle asset (source=%d, token=%d): %v", oracleSourceID, tokenID, err)
	}
}
