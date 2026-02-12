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

// SeedOracleWithDeploymentBlock inserts an oracle with a specific deployment block and returns its ID.
func SeedOracleWithDeploymentBlock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name, displayName string, chainID int, address string, deploymentBlock int64) int64 {
	t.Helper()
	addrBytes, err := HexToBytes(address)
	if err != nil {
		t.Fatalf("failed to parse oracle address %s: %v", address, err)
	}

	var id int64
	err = pool.QueryRow(ctx, `
		INSERT INTO oracle (name, display_name, chain_id, address, deployment_block, enabled)
		VALUES ($1, $2, $3, $4, $5, true)
		ON CONFLICT (name) DO UPDATE SET display_name = EXCLUDED.display_name, deployment_block = EXCLUDED.deployment_block
		RETURNING id
	`, name, displayName, chainID, addrBytes, deploymentBlock).Scan(&id)
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

// SeedProtocol inserts a protocol and returns its auto-generated ID.
func SeedProtocol(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int, address, name, protocolType string, createdAtBlock int64, metadata string) int64 {
	t.Helper()
	addrBytes, err := HexToBytes(address)
	if err != nil {
		t.Fatalf("failed to parse protocol address %s: %v", address, err)
	}

	var metadataArg any
	if metadata != "" {
		metadataArg = []byte(metadata)
	}

	var id int64
	err = pool.QueryRow(ctx, `
		INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		VALUES ($1, $2, $3, $4, $5, NOW(), $6)
		ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		RETURNING id
	`, chainID, addrBytes, name, protocolType, createdAtBlock, metadataArg).Scan(&id)
	if err != nil {
		t.Fatalf("failed to insert protocol %s: %v", name, err)
	}
	return id
}

// SeedProtocolOracle inserts a protocol-oracle binding and returns its auto-generated ID.
func SeedProtocolOracle(t *testing.T, ctx context.Context, pool *pgxpool.Pool, protocolID, oracleID, fromBlock int64) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
		VALUES ($1, $2, $3)
		RETURNING id
	`, protocolID, oracleID, fromBlock).Scan(&id)
	if err != nil {
		t.Fatalf("failed to insert protocol oracle (protocol=%d, oracle=%d): %v", protocolID, oracleID, err)
	}
	return id
}
