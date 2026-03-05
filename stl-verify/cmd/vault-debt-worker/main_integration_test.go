//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// mockVatRPC serves sequential eth_call responses matching the call order:
//  1. vault.ilk()       → bytes32 ilk identifier
//  2. vat.ilks(ilk)     → (Art, rate, spot, line, dust) tuple
//  3. vat.urns(ilk,usr) → (ink, art) tuple
//
// Subsequent calls repeat responses 2 and 3 for each poll tick.
func mockVatRPC(t *testing.T) *httptest.Server {
	t.Helper()

	// ALLOCATOR-SPARK-A right-padded to 32 bytes
	ilkResult := "0x414c4c4f4341544f522d535041524b2d41000000000000000000000000000000"

	// ilks() tuple: Art=0, rate=1e27 (RAY), spot=0, line=0, dust=0
	// rate is the second 32-byte word
	ilksResult := "0x" +
		strings.Repeat("0", 64) + // Art
		fmt.Sprintf("%064x", int64(1e9)) + // rate (truncated 1e27 for test simplicity)
		strings.Repeat("0", 192) // spot, line, dust

	// urns() tuple: ink=0, art=1000e9
	urnsResult := "0x" +
		strings.Repeat("0", 64) + // ink
		fmt.Sprintf("%064x", int64(1000e9)) // art

	callCount := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string          `json:"method"`
			ID     json.RawMessage `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if req.Method != "eth_call" {
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x1"`))
			return
		}

		callCount++
		var result string
		switch callCount {
		case 1: // vault.ilk()
			result = ilkResult
		default:
			// Alternate between ilks() and urns() for each poll tick
			if callCount%2 == 0 {
				result = ilksResult
			} else {
				result = urnsResult
			}
		}

		testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"`+result+`"`))
	}))
}

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	err := run(context.Background(), []string{
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad connection config")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected database connection error, got: %v", err)
	}
}

func TestRunIntegration_StartupAndShutdown(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	// Seed a prime so the service has something to resolve.
	_, err := pool.Exec(ctx, `
		INSERT INTO primes (name, vault_address)
		VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	rpcServer := mockVatRPC(t)
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-vat", "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b",
			"-poll-interval", "200ms",
		})
	}()

	// Wait until at least one snapshot has been written.
	deadline := time.After(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM prime_debts`).Scan(&count)
		if err == nil && count >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for debt snapshot to be written")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Verify snapshot fields look sane.
	var primeName, ilkName, debtWad string
	err = pool.QueryRow(ctx, `
		SELECT prime_name, ilk_name, debt_wad FROM prime_debts ORDER BY id LIMIT 1
	`).Scan(&primeName, &ilkName, &debtWad)
	if err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if primeName != "spark" {
		t.Errorf("prime_name = %q, want %q", primeName, "spark")
	}
	if !strings.HasPrefix(ilkName, "ALLOCATOR-SPARK") {
		t.Errorf("ilk_name = %q, expected ALLOCATOR-SPARK prefix", ilkName)
	}
	if debtWad == "" || debtWad == "0.000000000000000000" {
		t.Errorf("debt_wad = %q, expected non-zero value", debtWad)
	}

	// Graceful shutdown.
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
}

func TestRunIntegration_NoPrimesInDB(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	// Ensure primes table is empty.
	if _, err := pool.Exec(ctx, `DELETE FROM primes`); err != nil {
		t.Fatalf("clear primes: %v", err)
	}

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x0"}`)
	}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	err := run(context.Background(), []string{
		"-db", dbURL,
		"-vat", "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b",
	})
	if err == nil {
		t.Fatal("expected error when no primes are registered")
	}
	if !strings.Contains(err.Error(), "no primes") {
		t.Errorf("expected 'no primes' error, got: %v", err)
	}
}
