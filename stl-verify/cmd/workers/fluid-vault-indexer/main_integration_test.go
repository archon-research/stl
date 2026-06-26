//go:build integration

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN           string
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
)

// testBucket / testDeployEnv satisfy the cache reader's bucket-name convention
// (stl-sentinel{env}-{chain}-raw); chainID=1 -> "ethereum".
const (
	testBucket    = "stl-sentineltest-ethereum-raw"
	testDeployEnv = "test"
)

func TestMain(m *testing.M) {
	dsn, dbCleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn
	redisAddr, redisCleanup := testutil.StartRedisForMain()
	sharedRedisAddr = redisAddr
	lsCfg, lsCleanup := testutil.StartLocalStackForMain("s3")
	sharedLocalStackCfg = lsCfg

	code := m.Run()

	lsCleanup()
	redisCleanup()
	dbCleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("S3_BUCKET", testBucket)
	t.Setenv("DEPLOY_ENV", testDeployEnv)

	err := run(context.Background(), []string{
		"-queue", "http://localhost/test-queue",
		"-redis", "localhost:6379",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad connection config")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "Redis") {
		t.Errorf("expected connection error, got: %v", err)
	}
}

// TestRunIntegration_StartupAndShutdown wires run() against real TimescaleDB,
// Redis, and LocalStack S3 with a mock Ethereum RPC. The mock answers the
// startup reconcile (eth_blockNumber + an empty getAllVaultsAddresses multicall)
// so the service reaches its SQS polling loop, then a context cancel triggers a
// clean shutdown.
func TestRunIntegration_StartupAndShutdown(t *testing.T) {
	ctx := context.Background()

	_, dbURL, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()

	rpcServer := buildEmptyResolverMockRPC(t)
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(testBucket)}); err != nil {
		t.Fatalf("create S3 bucket: %v", err)
	}

	t.Setenv("BUILD_GIT_HASH", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("S3_BUCKET", testBucket)
	t.Setenv("DEPLOY_ENV", testDeployEnv)
	t.Setenv("CHAIN_ID", "1")

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-queue", "http://localhost/test-queue",
			"-db", dbURL,
			"-redis", sharedRedisAddr,
		})
	}()

	select {
	case <-sqsState.FirstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for service to start polling SQS")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	if receives := sqsState.Receives(); receives < 1 {
		t.Errorf("expected at least 1 ReceiveMessage call, got %d", receives)
	}
}

// buildEmptyResolverMockRPC serves eth_blockNumber and a single-call Multicall3
// aggregate3 whose only call is getAllVaultsAddresses, returning an empty
// address[]. That is all the startup reconcile needs when there are no vaults.
func buildEmptyResolverMockRPC(t *testing.T) *httptest.Server {
	t.Helper()

	const multicall3Address = "0xca11bde05977b3631167028862be2a173976ca11"

	resolverABI, err := abis.GetFluidVaultResolverABI()
	if err != nil {
		t.Fatalf("load Fluid VaultResolver ABI: %v", err)
	}
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load Multicall3 ABI: %v", err)
	}

	emptyAddrs, err := resolverABI.Methods["getAllVaultsAddresses"].Outputs.Pack([]common.Address{})
	if err != nil {
		t.Fatalf("pack empty getAllVaultsAddresses: %v", err)
	}
	type mcResult struct {
		Success    bool
		ReturnData []byte
	}
	aggOut, err := multicallABI.Methods["aggregate3"].Outputs.Pack([]mcResult{{Success: true, ReturnData: emptyAddrs}})
	if err != nil {
		t.Fatalf("pack aggregate3: %v", err)
	}
	aggHex := "0x" + hex.EncodeToString(aggOut)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req rpcutil.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}
		switch req.Method {
		case "eth_blockNumber":
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x129a3c0"`))
		case "eth_call":
			target := testutil.ExtractCallTarget(req.Params)
			if strings.ToLower(target) != multicall3Address {
				testutil.WriteRPCResult(w, req.ID, json.RawMessage(`"0x"`))
				return
			}
			resultJSON, _ := json.Marshal(aggHex)
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
		default:
			testutil.WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
		}
	}))
}
