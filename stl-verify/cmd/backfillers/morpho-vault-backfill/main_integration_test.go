//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedDSN        string
	sharedLocalStack testutil.LocalStackConfig
)

func TestMain(m *testing.M) {
	dsn, cleanupDB := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	localstack, cleanupLocalStack := testutil.StartLocalStackForMain("s3")
	sharedLocalStack = localstack

	code := m.Run()

	cleanupLocalStack()
	cleanupDB()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

// chainFixtureServer answers the one JSON-RPC call the composition root makes at
// startup: the chain-ID check against CHAIN_ID.
func chainFixtureServer(t *testing.T, chainIDHex string) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding the RPC request: %v", err)
			return
		}
		if req.Method != "eth_chainId" {
			t.Errorf("unexpected RPC method %q; this fixture only serves eth_chainId", req.Method)
		}

		w.Header().Set("Content-Type", "application/json")
		if _, err := fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%q}`, req.ID, chainIDHex); err != nil {
			t.Errorf("writing the RPC response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// seedBucket creates a bucket of this test's own and returns its name. Sibling
// tests share one LocalStack container, so anything a test counts needs its own
// bucket.
func seedBucket(t *testing.T, ctx context.Context) string {
	t.Helper()

	bucket := testutil.S3TestBucketName(t, "morpho-backfill-")
	client := testutil.NewS3Client(t, ctx, sharedLocalStack)
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("creating bucket %s: %v", bucket, err)
	}
	return bucket
}

// putReceipts writes one block's receipt object in the archive's own gzipped
// layout, so the activity reads it through exactly the path production uses.
func putReceipts(t *testing.T, ctx context.Context, bucket string, blockNumber int64, receipts []shared.TransactionReceipt) {
	t.Helper()

	body, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshalling receipts: %v", err)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatalf("gzipping receipts: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing the gzip writer: %v", err)
	}

	client := testutil.NewS3Client(t, ctx, sharedLocalStack)
	key := s3key.Build(blockNumber, 1, s3key.Receipts)
	if _, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(compressed.Bytes()),
	}); err != nil {
		t.Fatalf("putting %s: %v", key, err)
	}
}

// setWorkerEnv installs the environment a deployed pod would have, so the tests
// below exercise the real config loading rather than a hand-built config.
func setWorkerEnv(t *testing.T, bucket, rpcURL string) {
	t.Helper()

	t.Setenv("CHAIN_ID", "1")
	t.Setenv("S3_BUCKET", bucket)
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcURL)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStack.Endpoint)
	t.Setenv("AWS_REGION", sharedLocalStack.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	// The build registry refuses to register a build it cannot identify, and a
	// `go test` binary carries no VCS stamp.
	t.Setenv("BUILD_GIT_HASH", "integration-test")
}

func newDeps(t *testing.T, pool *pgxpool.Pool) temporal.Dependencies {
	t.Helper()
	return temporal.Dependencies{Pool: pool, Logger: testutil.DiscardLogger()}
}

// newActivityEnv runs the real composition root and returns a Temporal activity
// environment with the wired activities registered, so these tests drive the
// same object the worker registers, through the same harness Temporal uses.
//
// The activities must run inside this environment rather than being called
// directly: activity.GetLogger panics without an activity context.
func newActivityEnv(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *testsuite.TestActivityEnvironment {
	t.Helper()

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loading the worker configuration: %v", err)
	}
	activities, err := newBackfillActivities(ctx, newDeps(t, pool), cfg)
	if err != nil {
		t.Fatalf("wiring the backfill activities: %v", err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(activities)
	return env
}

// The Workflow Type is an operator-facing contract: the runbook tells the
// on-call to start `--type MorphoVaultBackfill`, and Temporal resolves it by
// string. So the literal is spelled out here rather than referencing
// workflowTypeName — using the constant would rename both sides together and pin
// nothing.
func TestIntegration_Register_ExposesTheDocumentedWorkflowType(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	server := chainFixtureServer(t, "0x1")
	setWorkerEnv(t, seedBucket(t, ctx), server.URL)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, newDeps(t, pool), env); err != nil {
		t.Fatalf("running the production registration: %v", err)
	}
	// Mocked so the run proves the registered names resolve, not that S3 holds
	// this range: the phases themselves are exercised by the activity tests below.
	env.OnActivity("DiscoverVaults", mock.Anything, mock.Anything).Return(discoveryResult{Vaults: 1}, nil)
	env.OnActivity("ReplayPartition", mock.Anything, mock.Anything).Return(2, nil)

	env.ExecuteWorkflow("MorphoVaultBackfill", BackfillParams{From: 2000, To: 2500})

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected the workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("running the workflow by its documented type name: %v", err)
	}
}

// The chain-ID check is the composition root's fail-fast: a node on another
// chain makes every block number in a run's range meaningless, and the mismatch
// would otherwise surface as missing S3 keys rather than as itself.
func TestIntegration_Register_RefusesAChainIDMismatch(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	server := chainFixtureServer(t, "0xa4b1")
	setWorkerEnv(t, seedBucket(t, ctx), server.URL)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	err := register(ctx, newDeps(t, pool), env)

	if err == nil {
		t.Fatal("expected registration to fail when the node reports another chain")
	}
	if !strings.Contains(err.Error(), "chain ID mismatch") {
		t.Errorf("error = %v, want it to name the chain ID mismatch", err)
	}
}

// The discovery activity must read the archive through the production reader.
// A range whose receipts carry nothing Morpho-related yields no candidates and
// no error — the "quiet range" outcome the workflow treats as ordinary.
func TestIntegration_DiscoverVaults_FindsNoCandidatesInAnUnrelatedRange(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	bucket := seedBucket(t, ctx)
	for block := int64(0); block < partition.BlockRangeSize; block++ {
		if block%500 == 0 {
			putReceipts(t, ctx, bucket, block, []shared.TransactionReceipt{{
				TransactionHash: fmt.Sprintf("0x%064x", block),
				BlockHash:       fmt.Sprintf("0x%064x", block),
				Logs: []shared.Log{{
					Address: "0xdAC17F958D2ee523a2206206994597C13D831ec7",
					Topics:  []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
				}},
			}})
		}
	}

	setWorkerEnv(t, bucket, chainFixtureServer(t, "0x1").URL)
	env := newActivityEnv(t, ctx, pool)

	got := runDiscovery(t, env, blockRange{From: 0, To: 999})

	if got.Candidates != 0 {
		t.Errorf("Candidates = %d, want 0 for a range of unrelated ERC20 transfers", got.Candidates)
	}
	if got.Vaults != 0 {
		t.Errorf("Vaults = %d, want 0", got.Vaults)
	}
}

// The corrupt-log backstop, end to end: a log whose topic0 is a recognised
// Morpho Blue event but whose body cannot be decoded fails the run rather than
// silently thinning the discovered vault set. Reaching that verdict at all means
// the activity listed, fetched, decompressed and walked a real archived object.
func TestIntegration_DiscoverVaults_FailsOnAnUndecodableMorphoBlueLog(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	morphoBlueABI, err := abis.GetMorphoBlueEventsABI()
	if err != nil {
		t.Fatalf("GetMorphoBlueEventsABI: %v", err)
	}

	bucket := seedBucket(t, ctx)
	for block := range int64(partition.BlockRangeSize) {
		var logs []shared.Log
		if block == 42 {
			// Supply declares three indexed params, so a lone topic0 is
			// recognised by IsMorphoBlueEvent and then fails to decode.
			logs = []shared.Log{{
				Address:         morpho_indexer.MorphoBlueAddress.Hex(),
				Topics:          []string{morphoBlueABI.Events["Supply"].ID.Hex()},
				TransactionHash: "0xabc",
			}}
		}
		putReceipts(t, ctx, bucket, block, []shared.TransactionReceipt{{
			TransactionHash: fmt.Sprintf("0x%064x", block),
			BlockHash:       fmt.Sprintf("0x%064x", block),
			Logs:            logs,
		}})
	}

	setWorkerEnv(t, bucket, chainFixtureServer(t, "0x1").URL)
	env := newActivityEnv(t, ctx, pool)

	var activities *backfillActivities
	if _, err := env.ExecuteActivity(activities.DiscoverVaults, blockRange{From: 0, To: 999}); err == nil {
		t.Fatal("expected an undecodable Morpho Blue log to fail the activity")
	}
}

// Replay loads the vault registry from the database, so a database with no
// VaultV2 vault has nothing to replay — and must say so instead of reading S3
// and reporting a complete pass over logs it could never have matched.
func TestIntegration_ReplayPartition_ReplaysNothingWhenNoV2VaultIsKnown(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	// Deliberately an empty bucket: reaching S3 at all here would be the bug.
	setWorkerEnv(t, seedBucket(t, ctx), chainFixtureServer(t, "0x1").URL)
	env := newActivityEnv(t, ctx, pool)

	events := replayOnePartition(t, env, partitionWork{
		Range:     blockRange{From: 0, To: 999},
		Partition: partition.GetPartition(0),
	})

	if events != 0 {
		t.Errorf("replayed %d events with no V2 vault registered, want 0", events)
	}
}

func runDiscovery(t *testing.T, env *testsuite.TestActivityEnvironment, rng blockRange) discoveryResult {
	t.Helper()

	var activities *backfillActivities
	encoded, err := env.ExecuteActivity(activities.DiscoverVaults, rng)
	if err != nil {
		t.Fatalf("DiscoverVaults: %v", err)
	}
	var got discoveryResult
	if err := encoded.Get(&got); err != nil {
		t.Fatalf("decoding the activity result: %v", err)
	}
	return got
}

func replayOnePartition(t *testing.T, env *testsuite.TestActivityEnvironment, work partitionWork) int {
	t.Helper()

	var activities *backfillActivities
	encoded, err := env.ExecuteActivity(activities.ReplayPartition, work)
	if err != nil {
		t.Fatalf("ReplayPartition: %v", err)
	}
	var events int
	if err := encoded.Get(&events); err != nil {
		t.Fatalf("decoding the activity result: %v", err)
	}
	return events
}
