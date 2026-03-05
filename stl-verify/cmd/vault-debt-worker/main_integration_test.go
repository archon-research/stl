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

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

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

// bigIntHex64 formats a *big.Int as a zero-padded 64-char hex string.
func bigIntHex64(v *big.Int) string {
	return fmt.Sprintf("%064x", v)
}

// ---------------------------------------------------------------------------
// Multicall3 mock helpers
// ---------------------------------------------------------------------------

const multicall3ABIJSON = `[{
	"name":"aggregate3",
	"type":"function",
	"inputs":[{"name":"calls","type":"tuple[]","components":[
		{"name":"target","type":"address"},
		{"name":"allowFailure","type":"bool"},
		{"name":"callData","type":"bytes"}
	]}],
	"outputs":[{"name":"returnData","type":"tuple[]","components":[
		{"name":"success","type":"bool"},
		{"name":"returnData","type":"bytes"}
	]}]
}]`

type subcallDispatcher func(target common.Address, callData []byte) ([]byte, bool)

var parsedMulticall3ABI abi.ABI

func init() {
	var err error
	parsedMulticall3ABI, err = abi.JSON(strings.NewReader(multicall3ABIJSON))
	if err != nil {
		panic("parse multicall3 ABI: " + err.Error())
	}
}

// handleMulticall3 decodes an aggregate3 eth_call, dispatches each sub-call,
// and returns the ABI-encoded aggregate3 result.
func handleMulticall3(calldata []byte, dispatch subcallDispatcher) (string, error) {
	if len(calldata) < 4 {
		return "", fmt.Errorf("calldata too short")
	}

	args, err := parsedMulticall3ABI.Methods["aggregate3"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return "", fmt.Errorf("unpack aggregate3 inputs: %w", err)
	}

	rawCalls, ok := args[0].([]struct {
		Target       common.Address `json:"target"`
		AllowFailure bool           `json:"allowFailure"`
		CallData     []byte         `json:"callData"`
	})
	if !ok {
		return "", fmt.Errorf("unexpected type for calls: %T", args[0])
	}

	type abiResult struct {
		Success    bool   `abi:"success"`
		ReturnData []byte `abi:"returnData"`
	}

	encoded := make([]abiResult, len(rawCalls))
	for i, c := range rawCalls {
		returnData, success := dispatch(c.Target, c.CallData)
		encoded[i] = abiResult{Success: success, ReturnData: returnData}
	}

	packed, err := parsedMulticall3ABI.Methods["aggregate3"].Outputs.Pack(encoded)
	if err != nil {
		return "", fmt.Errorf("pack aggregate3 outputs: %w", err)
	}

	return "0x" + hex.EncodeToString(packed), nil
}

// ---------------------------------------------------------------------------
// Mock RPC servers
// ---------------------------------------------------------------------------

const mockBlockNumber = "0x1400000" // 20971520

// mockVatRPCMulti serves eth_blockNumber and multicall3 aggregate3 eth_calls.
func mockVatRPCMulti(t *testing.T, _ string, primes []primeFixture, rate, art *big.Int) *httptest.Server {
	t.Helper()

	ilkResultBytes := make([][]byte, len(primes))
	for i, p := range primes {
		padded := make([]byte, 32)
		raw, _ := hex.DecodeString(p.ilkHex)
		copy(padded, raw)
		ilkResultBytes[i] = padded
	}

	ilksReturnHex := strings.Repeat("0", 64) + bigIntHex64(rate) + strings.Repeat("0", 192)
	ilksReturnBytes, _ := hex.DecodeString(ilksReturnHex)

	urnsReturnHex := strings.Repeat("0", 64) + bigIntHex64(art)
	urnsReturnBytes, _ := hex.DecodeString(urnsReturnHex)

	var ilkCallIdx atomic.Int64

	dispatch := func(target common.Address, callData []byte) ([]byte, bool) {
		switch len(callData) {
		case 4: // ilk()
			idx := int(ilkCallIdx.Add(1)) - 1
			if idx < len(ilkResultBytes) {
				return ilkResultBytes[idx], true
			}
			return make([]byte, 32), true
		case 36: // ilks(bytes32)
			return ilksReturnBytes, true
		case 68: // urns(bytes32,address)
			return urnsReturnBytes, true
		default:
			return make([]byte, 32), true
		}
	}

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

		switch req.Method {
		case "eth_blockNumber":
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"`+mockBlockNumber+`"`))
			return
		case "eth_chainId":
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x1"`))
			return
		case "eth_call":
			// handled below
		default:
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x1"`))
			return
		}

		var msg struct {
			Data  string `json:"data"`
			Input string `json:"input"`
		}
		if len(req.Params) == 0 {
			http.Error(w, "missing params", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(req.Params[0], &msg); err != nil {
			http.Error(w, "bad param unmarshal", http.StatusBadRequest)
			return
		}
		calldata := msg.Input
		if calldata == "" {
			calldata = msg.Data
		}

		calldataBytes, err := hex.DecodeString(strings.TrimPrefix(calldata, "0x"))
		if err != nil {
			http.Error(w, "bad calldata hex", http.StatusBadRequest)
			return
		}

		result, err := handleMulticall3(calldataBytes, dispatch)
		if err != nil {
			t.Logf("handleMulticall3 error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"`+result+`"`))
	}))
}

// mockVatRPC serves a single-prime (spark) setup.
func mockVatRPC(t *testing.T) *httptest.Server {
	t.Helper()

	rayVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	wadVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	art1000 := new(big.Int).Mul(big.NewInt(1000), wadVal)

	primes := []primeFixture{{
		name:         "spark",
		vaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA",
		ilkHex:       ilkBytes32Hex("ALLOCATOR-SPARK-A"),
	}}

	return mockVatRPCMulti(t, "", primes, rayVal, art1000)
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

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

	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

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

	var primeName, ilkName, debtWad string
	var blockNumber int64
	err = pool.QueryRow(ctx, `
		SELECT prime_name, ilk_name, debt_wad::text, block_number FROM prime_debts ORDER BY id LIMIT 1
	`).Scan(&primeName, &ilkName, &debtWad, &blockNumber)
	if err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if primeName != "spark" {
		t.Errorf("prime_name = %q, want %q", primeName, "spark")
	}
	if !strings.HasPrefix(ilkName, "ALLOCATOR-SPARK") {
		t.Errorf("ilk_name = %q, expected ALLOCATOR-SPARK prefix", ilkName)
	}
	if debtWad == "" || debtWad == "0" {
		t.Errorf("debt_wad = %q, expected non-zero value", debtWad)
	}
	if blockNumber <= 0 {
		t.Errorf("block_number = %d, expected positive", blockNumber)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
}

func TestRunIntegration_NoPrimesInDB(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	if _, err := pool.Exec(ctx, `TRUNCATE primes CASCADE`); err != nil {
		t.Fatalf("truncate primes: %v", err)
	}

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x0"}`)
	}))
	defer rpcServer.Close()

	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

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

func TestRunIntegration_MultipleVaults(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	const vatAddr = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"

	primes := []primeFixture{
		{name: "spark", vaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA", ilkHex: ilkBytes32Hex("ALLOCATOR-SPARK-A")},
		{name: "grove", vaultAddress: "0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0", ilkHex: ilkBytes32Hex("ALLOCATOR-GROVE-A")},
		{name: "obex", vaultAddress: "0xC1A83e7C92E4b09fD95B0FB8e5E25E6A6543db4E", ilkHex: ilkBytes32Hex("ALLOCATOR-OBEX-A")},
	}

	if _, err := pool.Exec(ctx, `TRUNCATE primes CASCADE`); err != nil {
		t.Fatalf("truncate primes: %v", err)
	}
	for _, p := range primes {
		addrHex := strings.TrimPrefix(strings.ToLower(p.vaultAddress), "0x")
		addrBytes, _ := hex.DecodeString(addrHex)
		if _, err := pool.Exec(ctx, `INSERT INTO primes (name, vault_address) VALUES ($1, $2) ON CONFLICT DO NOTHING`, p.name, addrBytes); err != nil {
			t.Fatalf("seed prime %s: %v", p.name, err)
		}
	}

	rayVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	wadVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	art500 := new(big.Int).Mul(big.NewInt(500), wadVal)

	rpcServer := mockVatRPCMulti(t, vatAddr, primes, rayVal, art500)
	defer rpcServer.Close()

	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{"-db", dbURL, "-vat", vatAddr, "-poll-interval", "200ms"})
	}()

	deadline := time.After(15 * time.Second)
	for {
		var count int
		_ = pool.QueryRow(ctx, `SELECT COUNT(DISTINCT prime_name) FROM prime_debts`).Scan(&count)
		if count >= len(primes) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for snapshots from all primes")
		case <-time.After(100 * time.Millisecond):
		}
	}

	rows, _ := pool.Query(ctx, `SELECT prime_name, ilk_name, debt_wad::text, block_number FROM prime_debts ORDER BY prime_name, id`)
	defer rows.Close()

	seen := make(map[string]struct{})
	for rows.Next() {
		var primeName, ilkName, debtWad string
		var blockNumber int64
		if err := rows.Scan(&primeName, &ilkName, &debtWad, &blockNumber); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		if ilkName == "" {
			t.Errorf("prime %q: empty ilk_name", primeName)
		}
		if debtWad == "" || debtWad == "0" {
			t.Errorf("prime %q: zero debt_wad", primeName)
		}
		if blockNumber <= 0 {
			t.Errorf("prime %q: non-positive block_number", primeName)
		}
		seen[primeName] = struct{}{}
	}
	for _, p := range primes {
		if _, ok := seen[p.name]; !ok {
			t.Errorf("no snapshot for prime %q", p.name)
		}
	}

	cancel()
	<-errCh
}

func TestRunIntegration_SnapshotAccumulation(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	const vatAddr = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"

	if _, err := pool.Exec(ctx, `TRUNCATE primes CASCADE`); err != nil {
		t.Fatalf("truncate primes: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO primes (name, vault_address) VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')`); err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	primes := []primeFixture{{name: "spark", vaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA", ilkHex: ilkBytes32Hex("ALLOCATOR-SPARK-A")}}

	rayVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	wadVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	art1000 := new(big.Int).Mul(big.NewInt(1000), wadVal)

	rpcServer := mockVatRPCMulti(t, vatAddr, primes, rayVal, art1000)
	defer rpcServer.Close()

	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{"-db", dbURL, "-vat", vatAddr, "-poll-interval", "150ms"})
	}()

	const wantRows = 3
	deadline := time.After(15 * time.Second)
	for {
		var count int
		_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM prime_debts`).Scan(&count)
		if count >= wantRows {
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

	var distinctTimes int
	_ = pool.QueryRow(ctx, `SELECT COUNT(DISTINCT synced_at) FROM prime_debts`).Scan(&distinctTimes)
	if distinctTimes < wantRows {
		t.Errorf("expected %d distinct synced_at values, got %d", wantRows, distinctTimes)
	}

	cancel()
	<-errCh
}

func TestRunIntegration_InvalidVatFlag(t *testing.T) {
	_, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	}))
	defer rpcServer.Close()

	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	err := run(context.Background(), []string{"-db", dbURL, "-vat", "not-a-valid-address"})
	if err == nil {
		t.Fatal("expected error for invalid vat address")
	}
}

func TestRunIntegration_InvalidPollInterval(t *testing.T) {
	_, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	err := run(context.Background(), []string{"-db", dbURL, "-vat", "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b", "-poll-interval", "not-a-duration"})
	if err == nil {
		t.Fatal("expected error for invalid poll interval")
	}
}
