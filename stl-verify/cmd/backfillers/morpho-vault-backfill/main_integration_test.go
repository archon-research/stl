//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
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
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
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
	env.OnActivity("DiscoverVaults", mock.Anything, mock.Anything).Return(discoveryResult{Vaults: 1, KnownV2Vaults: 1}, nil)
	env.OnActivity("ReplayPartition", mock.Anything, mock.Anything).Return(partitionReplay{EventsReplayed: 2}, nil)

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
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
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
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	bucket := seedBucket(t, ctx)
	// Contiguous, and the range below covers exactly these blocks: the scan
	// refuses a partition slice the archive has a hole in, so a sampled seed
	// would fail on the gap rather than on the candidates it is about.
	const lastBlock = int64(4)
	for block := range lastBlock + 1 {
		putReceipts(t, ctx, bucket, block, []shared.TransactionReceipt{{
			TransactionHash: fmt.Sprintf("0x%064x", block),
			BlockHash:       fmt.Sprintf("0x%064x", block),
			Logs: []shared.Log{{
				Address: "0xdAC17F958D2ee523a2206206994597C13D831ec7",
				Topics:  []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
			}},
		}})
	}

	setWorkerEnv(t, bucket, chainFixtureServer(t, "0x1").URL)
	env := newActivityEnv(t, ctx, pool)

	got := runDiscovery(t, env, blockRange{From: 0, To: lastBlock})

	if got.Candidates != 0 {
		t.Errorf("Candidates = %d, want 0 for a range of unrelated ERC20 transfers", got.Candidates)
	}
	if got.Vaults != 0 {
		t.Errorf("Vaults = %d, want 0", got.Vaults)
	}
	if got.KnownV2Vaults != 0 {
		t.Errorf("KnownV2Vaults = %d, want 0 against a database with no vault at all", got.KnownV2Vaults)
	}
}

// The corrupt-log backstop, end to end: a log whose topic0 is a recognised
// Morpho Blue event but whose body cannot be decoded fails the run rather than
// silently thinning the discovered vault set. Reaching that verdict at all means
// the activity listed, fetched, decompressed and walked a real archived object.
func TestIntegration_DiscoverVaults_FailsOnAnUndecodableMorphoBlueLog(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
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

// A run against a database with no VaultV2 vault skips the replay phase, so the
// discovery scan is the only thing that reads S3 at all. It must still prove the
// range is archived: without that, a fresh database over an unarchived window
// would end green having verified nothing.
func TestIntegration_Backfill_FailsOnAnArchiveGapWhenNoV2VaultIsKnown(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	const missingBlock = int64(3)
	bucket := seedQuietBlocks(t, ctx, 1, 6, missingBlock)
	setWorkerEnv(t, bucket, chainFixtureServer(t, "0x1").URL)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, newDeps(t, pool), env); err != nil {
		t.Fatalf("running the production registration: %v", err)
	}
	env.ExecuteWorkflow("MorphoVaultBackfill", BackfillParams{From: 1, To: 6})

	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected the archive gap to fail the run rather than report success over it")
	}
	if !strings.Contains(err.Error(), fmt.Sprint(missingBlock)) {
		t.Errorf("error = %v, want it to name the missing block %d", err, missingBlock)
	}
}

// The other half: over a COMPLETE archive the same zero-vault run is a success,
// and reports the empty replay honestly rather than by having skipped the check.
func TestIntegration_Backfill_SucceedsWithNothingToReplayOverACompleteArchive(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	bucket := seedQuietBlocks(t, ctx, 1, 6, -1)
	setWorkerEnv(t, bucket, chainFixtureServer(t, "0x1").URL)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, newDeps(t, pool), env); err != nil {
		t.Fatalf("running the production registration: %v", err)
	}
	env.ExecuteWorkflow("MorphoVaultBackfill", BackfillParams{From: 1, To: 6})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("running over a complete archive: %v", err)
	}
	var got BackfillResult
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatalf("decoding the workflow result: %v", err)
	}
	if got.PartitionsRun != 0 {
		t.Errorf("PartitionsRun = %d, want 0: no VaultV2 vault is known, so no partition replays", got.PartitionsRun)
	}
	if got.Discovered == nil || got.Discovered.KnownV2Vaults != 0 {
		t.Errorf("Discovered = %+v, want a run that found no VaultV2 vault", got.Discovered)
	}
}

// seedQuietBlocks archives one unrelated-ERC20 receipt per block in [from,to],
// skipping omitBlock (pass -1 to archive them all).
func seedQuietBlocks(t *testing.T, ctx context.Context, from, to, omitBlock int64) string {
	t.Helper()

	bucket := seedBucket(t, ctx)
	for block := from; block <= to; block++ {
		if block == omitBlock {
			continue
		}
		putReceipts(t, ctx, bucket, block, []shared.TransactionReceipt{{
			TransactionHash: fmt.Sprintf("0x%064x", block),
			BlockHash:       fmt.Sprintf("0x%064x", block),
			Logs: []shared.Log{{
				Address: "0xdAC17F958D2ee523a2206206994597C13D831ec7",
				Topics:  []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
			}},
		}})
	}
	return bucket
}

// Replay loads the vault registry from the database, so a database with no
// VaultV2 vault has nothing to replay — and must say so instead of reading S3
// and reporting a complete pass over logs it could never have matched.
func TestIntegration_ReplayPartition_ReplaysNothingWhenNoV2VaultIsKnown(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	// Deliberately an empty bucket: reaching S3 at all here would be the bug.
	setWorkerEnv(t, seedBucket(t, ctx), chainFixtureServer(t, "0x1").URL)
	env := newActivityEnv(t, ctx, pool)

	replayed := replayOnePartition(t, env, partitionWork{
		Range:     blockRange{From: 0, To: 999},
		Partition: partition.GetPartition(0),
	})

	if replayed.EventsReplayed != 0 {
		t.Errorf("replayed %d events with no V2 vault registered, want 0", replayed.EventsReplayed)
	}
	if replayed.RowsAppended != (appendedRows{}) {
		t.Errorf("appended %+v rows with no V2 vault registered, want none", replayed.RowsAppended)
	}
}

// The replay path is metered, end to end. buildReplayService left Config.Telemetry
// nil, so every instrument the morpho-indexer records went to a nil recorder:
// morpho_v2_adapter_registrations_total could never exist for a replayed or
// bootstrap-seeded adapter, and the morpho-v2-bootstrap runbook's first check
// queries exactly that. Driving a real AddAdapter through the service this
// composition root builds is what proves the wiring, rather than reading a field
// back.
func TestIntegration_BuildReplayService_MetersTheReplayPath(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	ctx := context.Background()

	vault := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")
	seedV2VaultRow(t, ctx, pool, vault)

	reader := installTestMeterProvider(t)
	multicaller := testutil.NewMockMulticaller()
	wireAdapterRegistrationReads(t, multicaller, adapter)

	t.Setenv("BUILD_GIT_HASH", "integration-test")
	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("registering the build: %v", err)
	}
	svc, _, err := buildReplayService(testutil.DiscardLogger(), multicaller, pool, buildReg.BuildID(), 1)
	if err != nil {
		t.Fatalf("buildReplayService: %v", err)
	}
	if err := svc.LoadVaultRegistry(ctx); err != nil {
		t.Fatalf("loading the vault registry: %v", err)
	}

	err = svc.ReplayMetaMorphoLog(ctx, addAdapterLog(t, vault, adapter), 23_400_000,
		common.HexToHash("0x11"), 1, time.Unix(1_760_000_000, 0).UTC())
	if err != nil {
		t.Fatalf("ReplayMetaMorphoLog: %v", err)
	}

	// The chain label is asserted too: the counter is per-chain, so a service
	// handed a raw chain id instead of a chain NAME would meter every replay under
	// a series the per-chain alerts never select.
	want := map[string]string{"chain": "mainnet", "observed_via": "add_adapter_event"}
	if got := counterValue(t, reader, "morpho.v2.adapter.registrations", want); got != 1 {
		t.Errorf("morpho.v2.adapter.registrations%v = %d, want 1: a replay service with no Telemetry records nothing", want, got)
	}
}

// A replay's row tally has to come from the service the composition root actually built:
// a counting repository the wiring forgot to pass to NewReplayService would leave every
// per-partition line reporting zero rows for a run that wrote plenty — and reporting zero
// is exactly the symptom of the silent loss the count exists to expose.
func TestIntegration_BuildReplayService_CountsTheRowsItAppends(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	ctx := context.Background()

	vault := common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	adapter := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")
	seedV2VaultRow(t, ctx, pool, vault)

	multicaller := testutil.NewMockMulticaller()
	wireAdapterRegistrationReads(t, multicaller, adapter)

	t.Setenv("BUILD_GIT_HASH", "integration-test")
	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		t.Fatalf("registering the build: %v", err)
	}
	svc, counted, err := buildReplayService(testutil.DiscardLogger(), multicaller, pool, buildReg.BuildID(), 1)
	if err != nil {
		t.Fatalf("buildReplayService: %v", err)
	}
	if err := svc.LoadVaultRegistry(ctx); err != nil {
		t.Fatalf("loading the vault registry: %v", err)
	}

	err = svc.ReplayMetaMorphoLog(ctx, addAdapterLog(t, vault, adapter), 23_400_000,
		common.HexToHash("0x11"), 1, time.Unix(1_760_000_000, 0).UTC())
	if err != nil {
		t.Fatalf("ReplayMetaMorphoLog: %v", err)
	}

	want := appendedRows{AdapterStates: 1, MembershipObservations: 1}
	if counted.counts != want {
		t.Errorf("counts after one AddAdapter replay = %+v, want %+v", counted.counts, want)
	}
}

// installTestMeterProvider points the global meter provider — the one
// morpho_indexer.NewTelemetry reads — at an in-memory reader for one test, and
// restores whatever was there.
func installTestMeterProvider(t *testing.T) sdkmetric.Reader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shutting down the test meter provider: %v", err)
		}
	})
	return reader
}

// counterValue sums the named int64 counter's data points whose attributes
// include every entry of want. Attributes outside want are ignored, so a test
// asserts only the labels it cares about.
func counterValue(t *testing.T, reader sdkmetric.Reader, name string, want map[string]string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if hasAttributes(dp.Attributes, want) {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func hasAttributes(set attribute.Set, want map[string]string) bool {
	for key, value := range want {
		got, ok := set.Value(attribute.Key(key))
		if !ok || got.AsString() != value {
			return false
		}
	}
	return true
}

// seedV2VaultRow inserts the protocol, asset token and VaultV2 row a replay
// expects to already exist — replay never discovers, it only drives logs of
// vaults the database already holds.
func seedV2VaultRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, vault common.Address) {
	t.Helper()
	var protocolID int64
	err := pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (1, $1, 'Morpho Blue', 'lending', 18883124, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`, morpho_indexer.MorphoBlueAddress.Bytes()).Scan(&protocolID)
	if err != nil {
		t.Fatalf("seeding protocol: %v", err)
	}

	var tokenID int64
	err = pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, $1, 'USDC', 6)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
		 RETURNING id`,
		common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").Bytes()).Scan(&tokenID)
	if err != nil {
		t.Fatalf("seeding token: %v", err)
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
		 VALUES (1, $1, $2, 'Test Vault', 'tVAULT', $3, 3, 23400000)
		 ON CONFLICT DO NOTHING`,
		protocolID, vault.Bytes(), tokenID); err != nil {
		t.Fatalf("seeding morpho_vault: %v", err)
	}
}

// addAdapterLog builds the AddAdapter log from the registered ABI, so the
// fixture cannot drift from the real event signature.
func addAdapterLog(t *testing.T, vault, adapter common.Address) shared.Log {
	t.Helper()
	eventsABI, err := abis.GetVaultV2EventsABI()
	if err != nil {
		t.Fatalf("GetVaultV2EventsABI: %v", err)
	}
	return shared.Log{
		Address:         vault.Hex(),
		Topics:          []string{eventsABI.Events["AddAdapter"].ID.Hex(), common.BytesToHash(adapter.Bytes()).Hex()},
		TransactionHash: "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		LogIndex:        "0x0",
	}
}

// wireAdapterRegistrationReads answers the two chain reads registering an adapter
// issues: the number-pinned type probe (morpho() succeeds, morphoVaultV1()
// reverts ⇒ MarketV1) and the hash-pinned realAssets() seed.
func wireAdapterRegistrationReads(t *testing.T, mc *testutil.MockMulticaller, adapter common.Address) {
	t.Helper()
	adapterABI, err := abis.GetVaultV2AdapterReadABI()
	if err != nil {
		t.Fatalf("GetVaultV2AdapterReadABI: %v", err)
	}
	pack := func(args abi.Arguments, values ...any) []byte {
		data, err := args.Pack(values...)
		if err != nil {
			t.Fatalf("packing return data: %v", err)
		}
		return data
	}

	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 || calls[0].Target != adapter {
			return nil, fmt.Errorf("unexpected number-pinned multicall of %d calls", len(calls))
		}
		return []outbound.Result{
			{Success: true, ReturnData: pack(adapterABI.Methods["morpho"].Outputs, common.HexToAddress("0x1"))},
			{Success: false},
		}, nil
	}
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) != 1 || calls[0].Target != adapter {
			return nil, fmt.Errorf("unexpected hash-pinned multicall of %d calls", len(calls))
		}
		return []outbound.Result{{Success: true, ReturnData: pack(adapterABI.Methods["realAssets"].Outputs, big.NewInt(777))}}, nil
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

func replayOnePartition(t *testing.T, env *testsuite.TestActivityEnvironment, work partitionWork) partitionReplay {
	t.Helper()

	var activities *backfillActivities
	encoded, err := env.ExecuteActivity(activities.ReplayPartition, work)
	if err != nil {
		t.Fatalf("ReplayPartition: %v", err)
	}
	var replayed partitionReplay
	if err := encoded.Get(&replayed); err != nil {
		t.Fatalf("decoding the activity result: %v", err)
	}
	return replayed
}
