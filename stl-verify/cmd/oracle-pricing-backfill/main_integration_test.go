//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

func TestRunIntegration_HappyPath(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	ctx := context.Background()

	// Set deployment_block and protocol binding below the test range so clamping doesn't skip blocks
	if _, err := pool.Exec(ctx, `UPDATE oracle SET deployment_block = 0 WHERE name = 'sparklend'`); err != nil {
		t.Fatalf("update deployment_block: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE protocol_oracle SET from_block = 0`); err != nil {
		t.Fatalf("update protocol_oracle from_block: %v", err)
	}

	// Count seeded tokens to parameterize the RPC mock
	var tokenCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM oracle_asset oa
		 JOIN oracle os ON os.id = oa.oracle_id
		 WHERE os.name = 'sparklend' AND oa.enabled = true`).Scan(&tokenCount); err != nil {
		t.Fatalf("count tokens: %v", err)
	}
	if tokenCount == 0 {
		t.Fatal("no seeded oracle assets found")
	}

	rpcServer := testutil.StartMockEthRPC(t, tokenCount)
	defer rpcServer.Close()

	args := []string{
		"-rpc-url", rpcServer.URL,
		"-from", "100",
		"-to", "105",
		"-db", dbURL,
		"-concurrency", "2",
		"-batch-size", "100",
	}

	if err := run(args); err != nil {
		t.Fatalf("run() failed: %v", err)
	}

	// Verify prices were stored — each block has unique prices so all 6 blocks
	// should have entries for every token.
	var priceCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price`).Scan(&priceCount); err != nil {
		t.Fatalf("query price count: %v", err)
	}
	expectedPrices := 6 * tokenCount // 6 blocks * tokenCount tokens
	if priceCount != expectedPrices {
		t.Errorf("expected %d prices, got %d", expectedPrices, priceCount)
	}

	var distinctBlocks int
	if err := pool.QueryRow(ctx, `SELECT COUNT(DISTINCT block_number) FROM onchain_token_price`).Scan(&distinctBlocks); err != nil {
		t.Fatalf("query distinct blocks: %v", err)
	}
	if distinctBlocks != 6 {
		t.Errorf("expected 6 distinct blocks, got %d", distinctBlocks)
	}
}

func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	// Use a mock RPC so we get past the RPC dial (which is lazy for HTTP)
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	err := run([]string{
		"-rpc-url", rpcServer.URL,
		"-from", "100",
		"-to", "105",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected database/connect error, got: %v", err)
	}
}
