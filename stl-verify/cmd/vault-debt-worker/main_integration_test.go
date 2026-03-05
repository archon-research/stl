//go:build integration

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// primeFixture holds test data for a single prime vault.
type primeFixture struct {
	name         string
	vaultAddress string // checksummed hex
	ilkName      string // human-readable, e.g. "ALLOCATOR-SPARK-A"
	ilkHex       string // right-padded bytes32 as 64 hex chars (no 0x prefix)
}

// ilkBytes32Hex encodes an ASCII ilk name as a zero-padded 64-char hex string.
func ilkBytes32Hex(name string) string {
	b := make([]byte, 32)
	copy(b, name)
	return fmt.Sprintf("%x", b)
}

// mockVatRPCMulti serves eth_call responses for N prime vaults by inspecting
// calldata length to distinguish the three call types — no address parsing,
// no call-order assumptions:
//
//   - ilk()                  → 4-byte calldata  (just the selector)
//   - ilks(bytes32)          → 36-byte calldata  (selector + 1 arg)
//   - urns(bytes32,address)  → 68-byte calldata  (selector + 2 args)
//
// ilk() calls are answered in the order they arrive (startup resolves primes
// in DB ORDER BY id ASC, which matches the primes slice order in the fixture).
func mockVatRPCMulti(t *testing.T, _ string, primes []primeFixture, rate, art int64) *httptest.Server {
	t.Helper()

	// Pre-build ilk responses in fixture order.
	ilkResults := make([]string, len(primes))
	for i, p := range primes {
		ilkResults[i] = "0x" + p.ilkHex
	}

	// ilks(bytes32) response: Art=0, rate=<rate>, spot=0, line=0, dust=0
	ilksResult := "0x" +
		strings.Repeat("0", 64) + // Art
		fmt.Sprintf("%064x", rate) + // rate
		strings.Repeat("0", 192) // spot, line, dust

	// urns(bytes32,address) response: ink=0, art=<art>
	urnsResult := "0x" +
		strings.Repeat("0", 64) + // ink
		fmt.Sprintf("%064x", art) // art

	var ilkCallIdx atomic.Int64 // tracks which prime's ilk() we're serving next

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string            `json:"method"`
			ID     json.RawMessage   `json:"id"`
			Params []json.RawMessage `json:"params"`
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

		// Extract calldata to determine call type.
		var msg struct {
			Data string `json:"data"`
		}
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params[0], &msg)
		}
		dataHexLen := len(strings.TrimPrefix(msg.Data, "0x"))

		var result string
		switch dataHexLen {
		case 8: // ilk() — 4-byte selector only
			idx := int(ilkCallIdx.Add(1)) - 1
			if idx < len(ilkResults) {
				result = ilkResults[idx]
			} else {
				result = "0x" + strings.Repeat("0", 64)
			}
		case 72: // ilks(bytes32) — selector + 32-byte ilk arg
			result = ilksResult
		case 136: // urns(bytes32,address) — selector + 32-byte ilk + 32-byte address
			result = urnsResult
		default:
			result = "0x" + strings.Repeat("0", 64)
		}

		testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"`+result+`"`))
	}))
}

// mockVatRPC serves eth_call responses for a single-prime (spark) setup,
// dispatching by calldata length rather than call order:
//
//   - 4-byte data  → ilk() call  → returns ALLOCATOR-SPARK-A bytes32
//   - 36-byte data → ilks() call → returns (Art=0, rate, spot=0, line=0, dust=0)
//   - 68-byte data → urns() call → returns (ink=0, art)
func mockVatRPC(t *testing.T) *httptest.Server {
	t.Helper()

	// ALLOCATOR-SPARK-A right-padded to 32 bytes
	ilkResult := "0x" + ilkBytes32Hex("ALLOCATOR-SPARK-A")

	// ilks() tuple: Art=0, rate=1e27 (1 RAY — no accumulated fees), spot=0, line=0, dust=0
	ray := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	ilksResult := "0x" +
		strings.Repeat("0", 64) + // Art = 0
		fmt.Sprintf("%064x", ray) + // rate = 1 RAY
		strings.Repeat("0", 192) // spot, line, dust

	// urns() tuple: ink=0, art=1000 * 1e18 (1000 USDS of normalized debt)
	art1000 := new(big.Int).Mul(big.NewInt(1000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	urnsResult := "0x" +
		strings.Repeat("0", 64) + // ink = 0
		fmt.Sprintf("%064x", art1000) // art = 1000 USDS

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string            `json:"method"`
			ID     json.RawMessage   `json:"id"`
			Params []json.RawMessage `json:"params"`
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

		var msg struct {
			Data string `json:"data"`
		}
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params[0], &msg)
		}
		dataHexLen := len(strings.TrimPrefix(msg.Data, "0x"))

		var result string
		switch dataHexLen {
		case 8: // ilk() — 4-byte selector only
			result = ilkResult
		case 72: // ilks(bytes32)
			result = ilksResult
		case 136: // urns(bytes32,address)
			result = urnsResult
		default:
			result = "0x" + strings.Repeat("0", 64)
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
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000") // 1s in ms — fail fast so defer shutdownOTEL doesn't block tests
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

	// Clear any primes left by previous runs, then seed exactly one.
	// mockVatRPC only handles a single-prime call sequence, so extra primes
	// would receive wrong ABI data and produce empty ilk names.
	if _, err := pool.Exec(ctx, `TRUNCATE primes CASCADE`); err != nil {
		t.Fatalf("truncate primes: %v", err)
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO primes (name, vault_address)
		VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')
	`)
	if err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	rpcServer := mockVatRPC(t)
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000") // 1s in ms — fail fast so defer shutdownOTEL doesn't block tests
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
	case <-time.After(20 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
}

func TestRunIntegration_NoPrimesInDB(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	// Ensure primes table is empty.
	if _, err := pool.Exec(ctx, `TRUNCATE primes CASCADE`); err != nil {
		t.Fatalf("truncate primes: %v", err)
	}

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x0"}`)
	}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000") // 1s in ms — fail fast so defer shutdownOTEL doesn't block tests
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

// ---------------------------------------------------------------------------
// Additional integration tests
// ---------------------------------------------------------------------------

// TestRunIntegration_MultipleVaults verifies that the service correctly resolves
// and polls three distinct prime vaults, writing one snapshot per prime per tick.
func TestRunIntegration_MultipleVaults(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	const vatAddr = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"

	primes := []primeFixture{
		{
			name:         "spark",
			vaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA",
			ilkName:      "ALLOCATOR-SPARK-A",
			ilkHex:       ilkBytes32Hex("ALLOCATOR-SPARK-A"),
		},
		{
			name:         "grove",
			vaultAddress: "0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0",
			ilkName:      "ALLOCATOR-GROVE-A",
			ilkHex:       ilkBytes32Hex("ALLOCATOR-GROVE-A"),
		},
		{
			name:         "obex",
			vaultAddress: "0xC1A83e7C92E4b09fD95B0FB8e5E25E6A6543db4E",
			ilkName:      "ALLOCATOR-OBEX-A",
			ilkHex:       ilkBytes32Hex("ALLOCATOR-OBEX-A"),
		},
	}

	// Truncate first so primes from other tests don't corrupt the mock's call sequence.
	if _, err := pool.Exec(ctx, `TRUNCATE primes CASCADE`); err != nil {
		t.Fatalf("truncate primes: %v", err)
	}

	// Seed all three primes. Pass vault_address as raw 20-byte slice (BYTEA).
	for _, p := range primes {
		addrHex := strings.TrimPrefix(strings.ToLower(p.vaultAddress), "0x")
		addrBytes, err := hex.DecodeString(addrHex)
		if err != nil {
			t.Fatalf("decode vault address for %s: %v", p.name, err)
		}
		_, err = pool.Exec(ctx, `
			INSERT INTO primes (name, vault_address)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, p.name, addrBytes)
		if err != nil {
			t.Fatalf("seed prime %s: %v", p.name, err)
		}
	}

	// rate = 1e9 (non-zero), art = 500e9 (non-zero debt)
	rpcServer := mockVatRPCMulti(t, vatAddr, primes, int64(1e9), int64(500e9))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000") // 1s in ms — fail fast so defer shutdownOTEL doesn't block tests
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-vat", vatAddr,
			"-poll-interval", "200ms",
		})
	}()

	// Wait until we have at least one snapshot per prime (3 total).
	deadline := time.After(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `SELECT COUNT(DISTINCT prime_name) FROM prime_debts`).Scan(&count)
		if err == nil && count >= len(primes) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for snapshots from all primes")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Verify each prime has an ilk_name and non-zero debt_wad.
	rows, err := pool.Query(ctx, `
		SELECT prime_name, ilk_name, debt_wad FROM prime_debts
		ORDER BY prime_name, id
	`)
	if err != nil {
		t.Fatalf("query snapshots: %v", err)
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	for rows.Next() {
		var primeName, ilkName, debtWad string
		if err := rows.Scan(&primeName, &ilkName, &debtWad); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		if ilkName == "" {
			t.Errorf("prime %q: ilk_name is empty", primeName)
		}
		if debtWad == "" || debtWad == "0.000000000000000000" {
			t.Errorf("prime %q: debt_wad = %q, expected non-zero", primeName, debtWad)
		}
		seen[primeName] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate snapshot rows: %v", err)
	}
	for _, p := range primes {
		if _, ok := seen[p.name]; !ok {
			t.Errorf("no snapshot written for prime %q", p.name)
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
}

// TestRunIntegration_SnapshotAccumulation verifies that each poll tick appends a
// new row — the table is append-only and rows are never overwritten.
func TestRunIntegration_SnapshotAccumulation(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	const vatAddr = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"

	if _, err := pool.Exec(ctx, `TRUNCATE primes CASCADE`); err != nil {
		t.Fatalf("truncate primes: %v", err)
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO primes (name, vault_address)
		VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')
	`)
	if err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	primes := []primeFixture{{
		name:         "spark",
		vaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA",
		ilkHex:       ilkBytes32Hex("ALLOCATOR-SPARK-A"),
	}}

	rpcServer := mockVatRPCMulti(t, vatAddr, primes, int64(1e9), int64(1000e9))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000") // 1s in ms — fail fast so defer shutdownOTEL doesn't block tests
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-vat", vatAddr,
			"-poll-interval", "150ms",
		})
	}()

	// Wait for at least 3 rows — meaning the initial sync plus two ticker ticks fired.
	const wantRows = 3
	deadline := time.After(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM prime_debts`).Scan(&count)
		if err == nil && count >= wantRows {
			break
		}
		select {
		case <-deadline:
			var got int
			_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM prime_debts`).Scan(&got)
			t.Fatalf("timed out: want %d rows, have %d", wantRows, got)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// All rows should have distinct synced_at timestamps (append-only).
	var distinctTimes int
	err = pool.QueryRow(ctx, `SELECT COUNT(DISTINCT synced_at) FROM prime_debts`).Scan(&distinctTimes)
	if err != nil {
		t.Fatalf("query distinct synced_at: %v", err)
	}
	if distinctTimes < wantRows {
		t.Errorf("expected %d distinct synced_at values, got %d — rows may have been overwritten", wantRows, distinctTimes)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
}

// TestRunIntegration_InvalidVatFlag verifies that run() returns an error immediately
// when the -vat flag is not a valid Ethereum address.
func TestRunIntegration_InvalidVatFlag(t *testing.T) {
	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()
	_ = pool

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000") // 1s in ms — fail fast so defer shutdownOTEL doesn't block tests
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	err := run(context.Background(), []string{
		"-db", dbURL,
		"-vat", "not-a-valid-address",
	})
	if err == nil {
		t.Fatal("expected error for invalid vat address")
	}
	if !strings.Contains(err.Error(), "vat") && !strings.Contains(err.Error(), "address") {
		t.Errorf("expected vat/address error, got: %v", err)
	}
}

// TestRunIntegration_InvalidPollInterval verifies that run() returns an error
// immediately when -poll-interval cannot be parsed as a duration.
func TestRunIntegration_InvalidPollInterval(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000") // 1s in ms — fail fast so defer shutdownOTEL doesn't block tests
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	err := run(context.Background(), []string{
		"-db", "postgres://localhost/test",
		"-vat", "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b",
		"-poll-interval", "not-a-duration",
	})
	if err == nil {
		t.Fatal("expected error for invalid poll interval")
	}
	if !strings.Contains(err.Error(), "poll") && !strings.Contains(err.Error(), "interval") && !strings.Contains(err.Error(), "duration") {
		t.Errorf("expected poll interval parse error, got: %v", err)
	}
}
