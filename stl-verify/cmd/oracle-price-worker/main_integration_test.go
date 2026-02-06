//go:build integration

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

func setupTestDatabase(t *testing.T) (*pgxpool.Pool, string, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("5432/tcp").
				WithStartupTimeout(60*time.Second),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	for i := 0; i < 30; i++ {
		if pool.Ping(ctx) == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	return pool, dsn, func() {
		pool.Close()
		container.Terminate(ctx)
	}
}

func setupEmptyDatabase(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("5432/tcp").
				WithStartupTimeout(60*time.Second),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())

	return dsn, func() {
		container.Terminate(ctx)
	}
}

// ---------------------------------------------------------------------------
// Mock Ethereum JSON-RPC server
// ---------------------------------------------------------------------------

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id"`
}

func startMockEthRPC(t *testing.T, numTokens int) *httptest.Server {
	t.Helper()

	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load multicall3 ABI: %v", err)
	}
	providerABI, err := abis.GetPoolAddressProviderABI()
	if err != nil {
		t.Fatalf("load provider ABI: %v", err)
	}
	oracleABI, err := abis.GetSparkLendOracleABI()
	if err != nil {
		t.Fatalf("load oracle ABI: %v", err)
	}

	// Use zero address to match the service's initial cached oracle address,
	// avoiding the retry code path (which is tested in unit tests).
	oracleAddr := common.Address{}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")

		var req jsonRPCRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}

		switch req.Method {
		case "eth_call":
			blockNum := parseBlockFromEthCall(req.Params)
			writeMulticallResponse(w, req.ID, blockNum, numTokens, oracleAddr,
				multicallABI, providerABI, oracleABI)

		case "eth_getBlockByNumber":
			blockNum := parseBlockFromGetBlock(req.Params)
			writeBlockHeaderResponse(w, req.ID, blockNum)

		default:
			writeRPCError(w, req.ID, -32601, "method not found: "+req.Method)
		}
	}))
}

func writeMulticallResponse(w http.ResponseWriter, id json.RawMessage,
	blockNum int64, numTokens int, oracleAddr common.Address,
	multicallABI, providerABI, oracleABI *abi.ABI,
) {
	oracleAddrData, _ := providerABI.Methods["getPriceOracle"].Outputs.Pack(oracleAddr)

	prices := make([]*big.Int, numTokens)
	for i := range prices {
		prices[i] = new(big.Int).Mul(
			big.NewInt(1000+blockNum*10+int64(i)),
			new(big.Int).SetInt64(1e8),
		)
	}
	pricesData, _ := oracleABI.Methods["getAssetsPrices"].Outputs.Pack(prices)

	type Result struct {
		Success    bool
		ReturnData []byte
	}
	aggResult, _ := multicallABI.Methods["aggregate3"].Outputs.Pack([]Result{
		{Success: true, ReturnData: oracleAddrData},
		{Success: true, ReturnData: pricesData},
	})

	resultHex := "0x" + hex.EncodeToString(aggResult)
	resultJSON, _ := json.Marshal(resultHex)
	writeRPCResult(w, id, json.RawMessage(resultJSON))
}

func writeBlockHeaderResponse(w http.ResponseWriter, id json.RawMessage, blockNum int64) {
	timestamp := 1700000000 + blockNum*12
	header := map[string]string{
		"parentHash":       fmt.Sprintf("0x%064x", blockNum-1),
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"miner":            "0x0000000000000000000000000000000000000000",
		"stateRoot":        "0x0000000000000000000000000000000000000000000000000000000000000000",
		"transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"receiptsRoot":     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"logsBloom":        "0x" + strings.Repeat("0", 512),
		"difficulty":       "0x0",
		"number":           fmt.Sprintf("0x%x", blockNum),
		"gasLimit":         "0x1c9c380",
		"gasUsed":          "0x0",
		"timestamp":        fmt.Sprintf("0x%x", timestamp),
		"extraData":        "0x",
		"mixHash":          "0x0000000000000000000000000000000000000000000000000000000000000000",
		"nonce":            "0x0000000000000000",
		"baseFeePerGas":    "0x0",
	}
	headerJSON, _ := json.Marshal(header)
	writeRPCResult(w, id, json.RawMessage(headerJSON))
}

func writeRPCResult(w http.ResponseWriter, id, result json.RawMessage) {
	json.NewEncoder(w).Encode(map[string]json.RawMessage{
		"jsonrpc": json.RawMessage(`"2.0"`),
		"id":      id,
		"result":  result,
	})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	errJSON, _ := json.Marshal(map[string]interface{}{"code": code, "message": message})
	json.NewEncoder(w).Encode(map[string]json.RawMessage{
		"jsonrpc": json.RawMessage(`"2.0"`),
		"id":      id,
		"error":   json.RawMessage(errJSON),
	})
}

func parseBlockFromEthCall(params json.RawMessage) int64 {
	var p []json.RawMessage
	json.Unmarshal(params, &p)
	if len(p) < 2 {
		return 100
	}
	var blockHex string
	json.Unmarshal(p[1], &blockHex)
	return parseHexInt64(blockHex)
}

func parseBlockFromGetBlock(params json.RawMessage) int64 {
	var p []json.RawMessage
	json.Unmarshal(params, &p)
	if len(p) < 1 {
		return 100
	}
	var blockHex string
	json.Unmarshal(p[0], &blockHex)
	return parseHexInt64(blockHex)
}

func parseHexInt64(s string) int64 {
	s = strings.TrimPrefix(s, "0x")
	n, _ := strconv.ParseInt(s, 16, 64)
	return n
}

// ---------------------------------------------------------------------------
// Mock SQS server (AWS JSON 1.0 protocol)
// ---------------------------------------------------------------------------

type mockSQSServer struct {
	mu                sync.Mutex
	messageDelivered  bool
	receiveCallCount  int
	deleteCallCount   int
	firstCallReceived chan struct{}
}

func startMockSQS(t *testing.T) (*httptest.Server, *mockSQSServer) {
	t.Helper()

	state := &mockSQSServer{
		firstCallReceived: make(chan struct{}, 1),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		target := r.Header.Get("X-Amz-Target")

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")

		switch {
		case strings.Contains(target, "ReceiveMessage"):
			state.mu.Lock()
			state.receiveCallCount++
			delivered := state.messageDelivered
			state.messageDelivered = true
			state.mu.Unlock()

			// Signal first call received
			select {
			case state.firstCallReceived <- struct{}{}:
			default:
			}

			if !delivered {
				// First call: deliver one block event message
				blockEvent := `{"chainId":1,"blockNumber":18000000,"version":1,"blockHash":"0xabc","blockTimestamp":1700000000}`
				fmt.Fprintf(w, `{"Messages":[{"MessageId":"msg-1","ReceiptHandle":"handle-1","Body":%s}]}`,
					mustMarshal(blockEvent))
			} else {
				// Subsequent calls: no messages
				fmt.Fprint(w, `{"Messages":[]}`)
			}

		case strings.Contains(target, "DeleteMessage"):
			state.mu.Lock()
			state.deleteCallCount++
			state.mu.Unlock()
			fmt.Fprint(w, `{}`)

		default:
			// Handle any unexpected actions gracefully
			_ = body
			fmt.Fprint(w, `{}`)
		}
	}))

	return server, state
}

func mustMarshal(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

func TestRunIntegration_HappyPath(t *testing.T) {
	pool, dbURL, cleanup := setupTestDatabase(t)
	defer cleanup()

	ctx := context.Background()

	var tokenCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM oracle_asset oa
		 JOIN oracle_source os ON os.id = oa.oracle_source_id
		 WHERE os.name = 'sparklend' AND oa.enabled = true`).Scan(&tokenCount); err != nil {
		t.Fatalf("count tokens: %v", err)
	}
	if tokenCount == 0 {
		t.Fatal("no seeded oracle assets found")
	}

	rpcServer := startMockEthRPC(t, tokenCount)
	defer rpcServer.Close()

	sqsServer, sqsState := startMockSQS(t)
	defer sqsServer.Close()

	// Configure environment for run()
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	errCh := make(chan error, 1)
	go func() {
		errCh <- run([]string{
			"-queue", "http://localhost/test-queue",
			"-db", dbURL,
		})
	}()

	// Wait for the service to start (SQS ReceiveMessage call indicates it's polling)
	select {
	case <-sqsState.firstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for service to start")
	}

	// Wait for the block event to be processed (prices stored in DB)
	deadline := time.After(10 * time.Second)
	for {
		var count int
		pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price`).Scan(&count)
		if count >= tokenCount {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for prices (got %d, want %d)", count, tokenCount)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Send SIGINT to trigger graceful shutdown
	p, _ := os.FindProcess(os.Getpid())
	p.Signal(syscall.SIGINT)

	// Wait for run() to return
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after SIGINT")
	}

	// Verify prices
	var priceCount int
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price`).Scan(&priceCount)
	if priceCount < tokenCount {
		t.Errorf("expected at least %d prices, got %d", tokenCount, priceCount)
	}

	// Verify DeleteMessage was called
	sqsState.mu.Lock()
	deletes := sqsState.deleteCallCount
	sqsState.mu.Unlock()
	if deletes < 1 {
		t.Errorf("expected at least 1 DeleteMessage call, got %d", deletes)
	}
}

func TestRunIntegration_MissingAlchemyAPIKey(t *testing.T) {
	t.Setenv("ALCHEMY_API_KEY", "")
	// Ensure ALCHEMY_API_KEY is truly empty
	os.Unsetenv("ALCHEMY_API_KEY")

	err := run([]string{
		"-queue", "http://localhost/test-queue",
		"-db", "postgres://localhost:5432/testdb",
	})
	if err == nil {
		t.Fatal("expected error for missing ALCHEMY_API_KEY")
	}
	if !strings.Contains(err.Error(), "ALCHEMY_API_KEY") {
		t.Errorf("expected ALCHEMY_API_KEY error, got: %v", err)
	}
}

func TestRunIntegration_InvalidFlags(t *testing.T) {
	err := run([]string{"-nonexistent"})
	if err == nil {
		t.Fatal("expected error for invalid flags")
	}
}

func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	err := run([]string{
		"-queue", "http://localhost/test-queue",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected database/connect error, got: %v", err)
	}
}

func TestRunIntegration_VerboseFlag(t *testing.T) {
	// Verbose flag is set before any infrastructure, so even a failing run covers it.
	t.Setenv("ALCHEMY_API_KEY", "")
	os.Unsetenv("ALCHEMY_API_KEY")

	err := run([]string{
		"-queue", "http://localhost/test-queue",
		"-db", "postgres://localhost:5432/testdb",
		"-verbose",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "ALCHEMY_API_KEY") {
		t.Errorf("expected ALCHEMY_API_KEY error, got: %v", err)
	}
}

func TestRunIntegration_OracleSourceNotFound(t *testing.T) {
	// Database without migrations — oracle_source table does not exist
	dsn, cleanup := setupEmptyDatabase(t)
	defer cleanup()

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", "http://localhost:1")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	err := run([]string{
		"-queue", "http://localhost/test-queue",
		"-db", dsn,
	})
	if err == nil {
		t.Fatal("expected error for missing oracle source table")
	}
	if !strings.Contains(err.Error(), "oracle source") {
		t.Errorf("expected oracle source error, got: %v", err)
	}
}
