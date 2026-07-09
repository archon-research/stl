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
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedLocalStackCfg testutil.LocalStackConfig
)

const (
	// archiveBucket receives raw SC call archives when ARCHIVE_SC_CALLS=true.
	archiveBucket = "test-prime-debt-worker-raw-sc-calls"
	// archivePrefix is the chain_id partition rawsckey.Build writes under for chainID=1.
	archivePrefix = "raw-sc-calls/chain_id=1/"
)

func TestMain(m *testing.M) {
	dsn, dbCleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn
	lsCfg, lsCleanup := testutil.StartLocalStackForMain("s3")
	sharedLocalStackCfg = lsCfg

	code := m.Run()

	lsCleanup()
	dbCleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

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
// Mock RPC servers
// ---------------------------------------------------------------------------

const mockBlockNumber = "0x1400000" // 20971520

// mockVatRPCMulti serves eth_blockNumber and multicall3 aggregate3 eth_calls.
func mockVatRPCMulti(t *testing.T, _ string, primes []primeFixture, rate, art *big.Int) *httptest.Server {
	t.Helper()

	ilkResultBytes := make([][]byte, len(primes))
	for i, p := range primes {
		padded := make([]byte, 32)
		raw, err := hex.DecodeString(p.ilkHex)
		if err != nil {
			t.Fatalf("decode ilk hex for prime %q: %v", p.name, err)
		}
		copy(padded, raw)
		ilkResultBytes[i] = padded
	}

	ilksReturnHex := strings.Repeat("0", 64) + bigIntHex64(rate) + strings.Repeat("0", 192)
	ilksReturnBytes, err := hex.DecodeString(ilksReturnHex)
	if err != nil {
		t.Fatalf("decode ilks return hex: %v", err)
	}

	urnsReturnHex := strings.Repeat("0", 64) + bigIntHex64(art)
	urnsReturnBytes, err := hex.DecodeString(urnsReturnHex)
	if err != nil {
		t.Fatalf("decode urns return hex: %v", err)
	}

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

		result, err := testutil.HandleMulticall3(calldataBytes, dispatch)
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
// SQS helpers
// ---------------------------------------------------------------------------

// enqueueBlockEvents sends block events to the mock SQS server.
func enqueueBlockEvents(t *testing.T, sqsState *testutil.MockSQSServer, startBlock int64, count int, chainID int64) {
	t.Helper()
	for i := 0; i < count; i++ {
		event := outbound.BlockEvent{
			ChainID:        chainID,
			BlockNumber:    startBlock + int64(i),
			Version:        0,
			BlockHash:      fmt.Sprintf("0x%064x", startBlock+int64(i)),
			ParentHash:     fmt.Sprintf("0x%064x", startBlock+int64(i)-1),
			BlockTimestamp: time.Now().Unix(),
			ReceivedAt:     time.Now(),
		}
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("marshal block event: %v", err)
		}
		sqsState.AddMessage(string(body))
	}
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	sqsServer, _ := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	err := run(context.Background(), []string{
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
		"-queue", "http://localhost/test-queue",
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

	pool, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	if _, err := pool.Exec(ctx, `TRUNCATE prime CASCADE`); err != nil {
		t.Fatalf("truncate prime: %v", err)
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO prime (name, vault_address)
		VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')
	`)
	if err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	rpcServer := mockVatRPC(t)
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	// Enqueue enough block events to trigger at least one sweep
	enqueueBlockEvents(t, sqsState, 20971520, 5, 1)

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-vat", "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b",
			"-queue", sqsServer.URL + "/queue/test",
			"-sweep-blocks", "1",
		})
	}()

	deadline := time.After(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM prime_debt`).Scan(&count)
		if err == nil && count >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for debt snapshot to be written")
		case <-time.After(100 * time.Millisecond):
		}
	}

	var ilkName, debtWad string
	var primeID, blockNumber int64
	err = pool.QueryRow(ctx, `
		SELECT prime_id, ilk_name, debt_wad::text, block_number FROM prime_debt ORDER BY synced_at LIMIT 1
	`).Scan(&primeID, &ilkName, &debtWad, &blockNumber)
	if err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if primeID <= 0 {
		t.Errorf("prime_id = %d, expected positive", primeID)
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

	pool, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	if _, err := pool.Exec(ctx, `TRUNCATE prime CASCADE`); err != nil {
		t.Fatalf("truncate prime: %v", err)
	}

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x0"}`)
	}))
	defer rpcServer.Close()

	sqsServer, _ := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	err := run(context.Background(), []string{
		"-db", dbURL,
		"-vat", "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b",
		"-queue", sqsServer.URL + "/queue/test",
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

	pool, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	const vatAddr = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"

	primes := []primeFixture{
		{name: "spark", vaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA", ilkHex: ilkBytes32Hex("ALLOCATOR-SPARK-A")},
		{name: "grove", vaultAddress: "0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0", ilkHex: ilkBytes32Hex("ALLOCATOR-GROVE-A")},
		{name: "obex", vaultAddress: "0xC1A83e7C92E4b09fD95B0FB8e5E25E6A6543db4E", ilkHex: ilkBytes32Hex("ALLOCATOR-OBEX-A")},
	}

	if _, err := pool.Exec(ctx, `TRUNCATE prime CASCADE`); err != nil {
		t.Fatalf("truncate prime: %v", err)
	}
	for _, p := range primes {
		addrHex := strings.TrimPrefix(strings.ToLower(p.vaultAddress), "0x")
		addrBytes, err := hex.DecodeString(addrHex)
		if err != nil {
			t.Fatalf("decode vault address for prime %q: %v", p.name, err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO prime (name, vault_address) VALUES ($1, $2) ON CONFLICT DO NOTHING`, p.name, addrBytes); err != nil {
			t.Fatalf("seed prime %s: %v", p.name, err)
		}
	}

	rayVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	wadVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	art500 := new(big.Int).Mul(big.NewInt(500), wadVal)

	rpcServer := mockVatRPCMulti(t, vatAddr, primes, rayVal, art500)
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	enqueueBlockEvents(t, sqsState, 20971520, 5, 1)

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-vat", vatAddr,
			"-queue", sqsServer.URL + "/queue/test",
			"-sweep-blocks", "1",
		})
	}()

	deadline := time.After(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `SELECT COUNT(DISTINCT prime_id) FROM prime_debt`).Scan(&count)
		if err == nil && count >= len(primes) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for snapshots from all primes")
		case <-time.After(100 * time.Millisecond):
		}
	}

	rows, err := pool.Query(ctx, `
		SELECT p.name, pd.ilk_name, pd.debt_wad::text, pd.block_number
		FROM prime_debt pd
		JOIN prime p ON p.id = pd.prime_id
		ORDER BY p.name, pd.synced_at
	`)
	if err != nil {
		t.Fatalf("query snapshots: %v", err)
	}
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
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate snapshot rows: %v", err)
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

	pool, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	const vatAddr = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"

	if _, err := pool.Exec(ctx, `TRUNCATE prime CASCADE`); err != nil {
		t.Fatalf("truncate prime: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO prime (name, vault_address) VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')`); err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	primes := []primeFixture{{name: "spark", vaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA", ilkHex: ilkBytes32Hex("ALLOCATOR-SPARK-A")}}

	rayVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	wadVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	art1000 := new(big.Int).Mul(big.NewInt(1000), wadVal)

	rpcServer := mockVatRPCMulti(t, vatAddr, primes, rayVal, art1000)
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	const wantRows = 3
	enqueueBlockEvents(t, sqsState, 20971520, wantRows+2, 1)

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-vat", vatAddr,
			"-queue", sqsServer.URL + "/queue/test",
			"-sweep-blocks", "1",
		})
	}()

	deadline := time.After(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM prime_debt`).Scan(&count)
		if err == nil && count >= wantRows {
			break
		}
		select {
		case <-deadline:
			var got int
			if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM prime_debt`).Scan(&got); err != nil {
				t.Fatalf("timed out and failed to query count: %v", err)
			}
			t.Fatalf("timed out: want %d rows, have %d", wantRows, got)
		case <-time.After(100 * time.Millisecond):
		}
	}

	var distinctTimes int
	if err := pool.QueryRow(ctx, `SELECT COUNT(DISTINCT synced_at) FROM prime_debt`).Scan(&distinctTimes); err != nil {
		t.Fatalf("query distinct synced_at: %v", err)
	}
	if distinctTimes < wantRows {
		t.Errorf("expected %d distinct synced_at values, got %d", wantRows, distinctTimes)
	}

	cancel()
	<-errCh
}

// TestRunIntegration_ArchivesRawCalls drives one block through the worker with
// ARCHIVE_SC_CALLS=true: it seeds a single prime, enqueues block events, and
// serves the standard multicall mock RPC. Processing the sweep drives real
// Multicall3 calls through the archiving-wrapped multicaller, so the worker must
// write a raw SC call object to S3 keyed under the block's reorg version with
// the "prime-debt" source.
func TestRunIntegration_ArchivesRawCalls(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	const vatAddr = "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b"

	if _, err := pool.Exec(ctx, `TRUNCATE prime CASCADE`); err != nil {
		t.Fatalf("truncate prime: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO prime (name, vault_address) VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')`); err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	primes := []primeFixture{{name: "spark", vaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA", ilkHex: ilkBytes32Hex("ALLOCATOR-SPARK-A")}}

	rayVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	wadVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	art1000 := new(big.Int).Mul(big.NewInt(1000), wadVal)

	rpcServer := mockVatRPCMulti(t, vatAddr, primes, rayVal, art1000)
	defer rpcServer.Close()

	// Create the archive bucket so the fire-and-forget archiver has somewhere to write.
	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(archiveBucket)}); err != nil {
		t.Fatalf("create bucket %s: %v", archiveBucket, err)
	}

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	// The sweep reads debt on blocks that are multiples of -sweep-blocks; enqueue a
	// short run starting at the mock block number so at least one sweep fires.
	const startBlock = int64(20971520)
	enqueueBlockEvents(t, sqsState, startBlock, 5, 1)

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")
	t.Setenv("ARCHIVE_SC_CALLS", "true")
	t.Setenv("RAW_SC_BUCKET", archiveBucket)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-vat", vatAddr,
			"-queue", sqsServer.URL + "/queue/test",
			"-sweep-blocks", "1",
		})
	}()

	// Wait until a debt snapshot is written so the sweep (and its multicalls) has run.
	testutil.WaitForCondition(t, 15*time.Second, func() bool {
		var count int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM prime_debt`).Scan(&count); err != nil {
			return false
		}
		return count >= 1
	}, "a debt snapshot to be persisted")

	// Find the block version the sweep actually wrote so we can assert on the same key.
	// enqueueBlockEvents publishes events with version 0, so the archive key embeds bv=0.
	var sweptBlock int64
	if err := pool.QueryRow(ctx, `SELECT block_number FROM prime_debt ORDER BY synced_at LIMIT 1`).Scan(&sweptBlock); err != nil {
		t.Fatalf("query swept block: %v", err)
	}

	// Archives are fire-and-forget; poll until the object lands. rawsckey.Build formats
	// {block}_{blockVersion}_{source}_{batchHash}, so the worker must archive under the
	// event's version (0) with the "prime-debt" source.
	wantSegment := fmt.Sprintf("%d_%d_prime-debt_", sweptBlock, 0)
	testutil.WaitForCondition(t, 30*time.Second, func() bool {
		out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(archiveBucket),
			Prefix: aws.String(archivePrefix),
		})
		if err != nil {
			return false
		}
		for _, obj := range out.Contents {
			if strings.Contains(aws.ToString(obj.Key), wantSegment) {
				return true
			}
		}
		return false
	}, fmt.Sprintf("a raw SC call archive whose key contains %q", wantSegment))

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	// run() has returned, so every fire-and-forget archive write has drained. The
	// positive check above can be satisfied by an unrelated number-pinned Execute
	// batch at the same block, so assert directly that nothing was archived at
	// block 0: a hash-pinned state read reaching the archiver without
	// WithBlockNumber would key its batch there (VEC-471). A real archive's
	// filename starts with the block number, never "0_". ctx is cancelled above,
	// so list under a fresh context.
	listOut, listErr := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(archiveBucket),
		Prefix: aws.String(archivePrefix),
	})
	if listErr != nil {
		t.Fatalf("listing archive bucket: %v", listErr)
	}
	for _, obj := range listOut.Contents {
		key := aws.ToString(obj.Key)
		if base := key[strings.LastIndex(key, "/")+1:]; strings.HasPrefix(base, "0_") {
			t.Fatalf("raw SC call archive keyed at block 0 (%s): a hash-pinned state read was archived without WithBlockNumber", key)
		}
	}
}

func TestRunIntegration_InvalidVatFlag(t *testing.T) {
	_, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	}))
	defer rpcServer.Close()

	sqsServer, _ := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ETH_RPC_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")

	err := run(context.Background(), []string{
		"-db", dbURL,
		"-vat", "not-a-valid-address",
		"-queue", sqsServer.URL + "/queue/test",
	})
	if err == nil {
		t.Fatal("expected error for invalid vat address")
	}
}
