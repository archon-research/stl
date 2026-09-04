//go:build integration

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedRedisAddr     string
	sharedLocalStackCfg testutil.LocalStackConfig
	integrationChainID  = int64(1)

	// integrationHeadDepth puts both blocks well outside the reorg window the
	// service refuses to repair inside.
	integrationHeadDepth = int64(5000)
)

// blockFixture is one height the mock node serves and the archive holds
// something for.
type blockFixture struct {
	number    int64
	hash      string
	parent    string
	timestamp int64
}

// orphanOnly is the ARCT-379 shape: the archive holds the losing fork's _0_
// objects and nothing else, so the repair belongs at version 1.
var orphanOnly = blockFixture{
	number:    25395651,
	hash:      "0x4d1c1a52b1f5e5a0c6f0b0a0d9e8c7b6a594837261504f3e2d1c0b9a8f7e6d5c",
	parent:    "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	timestamp: 0x68b0c0c0,
}

// The hashes the archive holds in these fixtures: a losing fork at version 0, and
// a second one at version 1 where a correction was attempted before.
const (
	losingFork = "0x1111111111111111111111111111111111111111111111111111111111111111"
	secondFork = "0x2222222222222222222222222222222222222222222222222222222222222222"
)

// alreadyCorrected reorged once before, so version 1 is taken — by a single
// object, which is enough to occupy the slot — and the repair belongs at 2.
var alreadyCorrected = blockFixture{
	number:    25087888,
	hash:      "0x9f8e7d6c5b4a39281706f5e4d3c2b1a09988776655443322110ffeeddccbbaa9",
	parent:    "0x1122334455667788991122334455667788991122334455667788991122334455",
	timestamp: 0x68a0b0b0,
}

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		RedisAddr:          &sharedRedisAddr,
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "s3,sns,sqs",
	}))
}

// TestRepublish_LandsInTheCacheAndOnTheTopic drives the deployed wiring — register,
// loadConfig, the real Redis adapter, the real S3 and SNS adapters and the real
// batched RPC client — against LocalStack and an in-process node. It is what proves
// the three things a consumer depends on and a unit test with fakes cannot: the
// version comes from the objects actually in the archive, the cache keys carry that
// version, and the SNS message that reaches a subscribed FIFO queue is the block
// event for it.
func TestRepublish_LandsInTheCacheAndOnTheTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	deployment := newDeployment(t, ctx)
	deployment.archive(t, ctx, orphanOnly, 0, losingFork, s3key.Block, s3key.Receipts, s3key.Traces)
	deployment.archive(t, ctx, alreadyCorrected, 0, losingFork, s3key.Block, s3key.Receipts, s3key.Traces)
	deployment.archive(t, ctx, alreadyCorrected, 1, secondFork, s3key.Block)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, temporal.Dependencies{Logger: discardLogger()}, env); err != nil {
		t.Fatalf("register: %v", err)
	}

	env.ExecuteWorkflow(workflowTypeName, input(orphanOnly.number, alreadyCorrected.number))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	assertRepublishedVersions(t, env, map[int64]int{orphanOnly.number: 1, alreadyCorrected.number: 2})
	assertCachedUnderVersion(t, ctx, deployment.keyPrefix, orphanOnly, 1)
	assertCachedUnderVersion(t, ctx, deployment.keyPrefix, alreadyCorrected, 2)
	assertPublishedEvents(t, ctx, deployment.sqs, deployment.queueURL,
		map[int64]int{orphanOnly.number: 1, alreadyCorrected.number: 2})
}

// A height whose archive already holds the canonical block needs no repair, and
// republishing it would append an identical correction for good. The worker
// refuses it while deriving the version, so nothing is cached or published.
func TestRepublish_RefusesAHeightTheArchiveAlreadyHoldsCanonically(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	deployment := newDeployment(t, ctx)
	deployment.archive(t, ctx, orphanOnly, 0, orphanOnly.hash, s3key.Block, s3key.Receipts, s3key.Traces)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, temporal.Dependencies{Logger: discardLogger()}, env); err != nil {
		t.Fatalf("register: %v", err)
	}

	env.ExecuteWorkflow(workflowTypeName, input(orphanOnly.number))

	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), "already canonical") {
		t.Fatalf("error = %v, want the run refused for a height that needs no repair", err)
	}
	assertNothingCached(t, ctx, deployment.keyPrefix, orphanOnly)
}

// The same archive state as the test above, and the opposite outcome: a #849
// repair leaves the canonical block in S3 with no indexer told, so
// archiveRepaired republishes AT the version those objects occupy — version 0
// here, where a repair of a height the archive never held writes.
func TestRepublish_PublishesAtTheVersionARepairedArchiveHolds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	deployment := newDeployment(t, ctx)
	deployment.archive(t, ctx, orphanOnly, 0, orphanOnly.hash, s3key.Block, s3key.Receipts, s3key.Traces)

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, temporal.Dependencies{Logger: discardLogger()}, env); err != nil {
		t.Fatalf("register: %v", err)
	}

	env.ExecuteWorkflow(workflowTypeName,
		json.RawMessage(fmt.Sprintf(`{"blocks":[%d],"archiveRepaired":true}`, orphanOnly.number)))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	assertRepublishedVersions(t, env, map[int64]int{orphanOnly.number: 0})
	assertCachedUnderVersion(t, ctx, deployment.keyPrefix, orphanOnly, 0)
	assertEventDescribes(t, receiveBlockEvent(t, ctx, deployment.sqs, deployment.queueURL), orphanOnly, 0)
}

// TestRegister_RefusesAConfigItCannotPublishWith keeps the startup guard on the
// wiring path an operator actually reaches: a topic naming another chain must stop
// the worker, not surface as corrections written into the wrong chain's feed.
func TestRegister_RefusesAConfigItCannotPublishWith(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	deployment := newDeployment(t, ctx)
	t.Setenv("AWS_SNS_TOPIC_ARN", strings.Replace(deployment.topicARN, "-ethereum-", "-base-", 1))

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	err := register(ctx, temporal.Dependencies{Logger: discardLogger()}, env)

	if err == nil || !strings.Contains(err.Error(), "sns topic") {
		t.Fatalf("error = %v, want one naming the topic mismatch", err)
	}
}

// TestRegister_RefusesAnArchiveItCannotList keeps the startup probe on the path a
// deployment reaches: a bucket this pod may not list — a missing Pod Identity
// grant, or a name that is not there — must stop the worker rather than fail
// every height of the first run.
func TestRegister_RefusesAnArchiveItCannotList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	newDeployment(t, ctx)
	t.Setenv("S3_BUCKET", "stl-sentinel"+deployEnv+"-ethereum-raw-never-created")

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	err := register(ctx, temporal.Dependencies{Logger: discardLogger()}, env)

	if err == nil || !strings.Contains(err.Error(), "s3:ListBucket") {
		t.Fatalf("error = %v, want one naming the listing grant it needs", err)
	}
}

// deployment is the environment a deployed pod would find: a chain-named FIFO
// topic with a queue subscribed to it, the chain's raw archive bucket, a node to
// read blocks from, and the env vars the ConfigMap and ExternalSecret supply.
type deployment struct {
	s3        *awss3.Client
	sqs       *awssqs.Client
	topicARN  string
	queueURL  string
	bucket    string
	keyPrefix string
}

// deployEnv is what chainutil's guards check the topic and bucket names against,
// so the environment the test declares and the names it creates have to agree.
const deployEnv = "brtest"

func newDeployment(t *testing.T, ctx context.Context) deployment {
	t.Helper()
	snsc, sqsc := awsClients(t, ctx)

	topicARN := createFifoTopic(t, ctx, snsc, "stl-sentinel"+deployEnv+"-ethereum-blocks.fifo")
	queueURL := createFifoQueue(t, ctx, sqsc, testutil.SQSTestFifoQueueName(t, "block-republisher-"))
	subscribeQueueToTopic(t, ctx, snsc, sqsc, topicARN, queueURL)

	s3c := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	bucket := testutil.S3TestBucketName(t, "stl-sentinel"+deployEnv+"-ethereum-raw-")
	testutil.EnsureBucket(t, ctx, s3c, bucket)

	node := startMockRPCServer(t, orphanOnly, alreadyCorrected)
	t.Cleanup(node.Close)

	keyPrefix := testutil.SanitizeTestName(t.Name())
	t.Setenv("CHAIN_ID", fmt.Sprintf("%d", integrationChainID))
	t.Setenv("DEPLOY_ENV", deployEnv)
	t.Setenv("AWS_SNS_TOPIC_ARN", topicARN)
	t.Setenv("AWS_SNS_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_S3_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("S3_BUCKET", bucket)
	t.Setenv("AWS_REGION", sharedLocalStackCfg.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", node.URL)
	t.Setenv("REDIS_ADDR", sharedRedisAddr)
	t.Setenv("REDIS_KEY_PREFIX", keyPrefix)

	return deployment{s3: s3c, sqs: sqsc, topicARN: topicARN, queueURL: queueURL, bucket: bucket, keyPrefix: keyPrefix}
}

// archive puts the height's objects for one version into the raw bucket, the way
// raw-data-backup would have: gzipped payloads naming the block they hold, which
// is what the worker compares against the canonical chain.
func (d deployment) archive(t *testing.T, ctx context.Context, block blockFixture, version int, hash string, dataTypes ...s3key.DataType) {
	t.Helper()
	bodies := map[s3key.DataType]string{
		s3key.Block:    fmt.Sprintf(`{"hash":%q,"number":"0x%x"}`, hash, block.number),
		s3key.Receipts: fmt.Sprintf(`[{"blockHash":%q}]`, hash),
		s3key.Traces:   `[]`,
	}

	for _, dataType := range dataTypes {
		key := s3key.Build(block.number, version, dataType)
		if _, err := d.s3.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(d.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(gzipped(t, bodies[dataType])),
		}); err != nil {
			t.Fatalf("seeding %s: %v", key, err)
		}
	}
}

func gzipped(t *testing.T, payload string) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(payload)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func assertRepublishedVersions(t *testing.T, env *testsuite.TestWorkflowEnvironment, want map[int64]int) {
	t.Helper()
	var result RepublishResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decoding the workflow result: %v", err)
	}
	if len(result.Republished) != len(want) {
		t.Fatalf("result = %+v, want %d blocks republished", result, len(want))
	}
	for _, republished := range result.Republished {
		if got := republished.Version; got != want[republished.BlockNumber] {
			t.Errorf("block %d republished at version %d, want %d",
				republished.BlockNumber, got, want[republished.BlockNumber])
		}
	}
}

// assertCachedUnderVersion reads the raw keys rather than going back through the
// adapter: the key layout is the contract every worker builds its own read from.
func assertCachedUnderVersion(t *testing.T, ctx context.Context, keyPrefix string, block blockFixture, version int) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: sharedRedisAddr})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("closing the redis client: %v", err)
		}
	})

	exists := func(key string) int64 {
		count, err := client.Exists(ctx, key).Result()
		if err != nil {
			t.Fatalf("EXISTS %s: %v", key, err)
		}
		return count
	}
	key := func(version int, dataType string) string {
		return fmt.Sprintf("%s:%d:%d:%d:%s", keyPrefix, integrationChainID, block.number, version, dataType)
	}

	for _, dataType := range []string{"block", "receipts", "traces"} {
		if exists(key(version, dataType)) != 1 {
			t.Errorf("cache key %s is missing", key(version, dataType))
		}
	}
	// Ethereum's watcher fetches no blobs, so republishing them would hand the
	// backup worker a data type it does not expect at this version.
	if exists(key(version, "blobs")) != 0 {
		t.Errorf("cache key %s was written; this chain's watcher publishes no blobs", key(version, "blobs"))
	}
	// Nothing may land in a slot the archive already holds: version 0 is the
	// losing fork's data, and every version below the chosen one is taken.
	for occupied := range version {
		if exists(key(occupied, "block")) != 0 {
			t.Errorf("cache key %s was written; that slot is already occupied", key(occupied, "block"))
		}
	}
}

func assertNothingCached(t *testing.T, ctx context.Context, keyPrefix string, block blockFixture) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: sharedRedisAddr})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("closing the redis client: %v", err)
		}
	})

	pattern := fmt.Sprintf("%s:%d:%d:*", keyPrefix, integrationChainID, block.number)
	keys, err := client.Keys(ctx, pattern).Result()
	if err != nil {
		t.Fatalf("KEYS %s: %v", pattern, err)
	}
	if len(keys) != 0 {
		t.Errorf("cached %v for a height that needed no repair", keys)
	}
}

func assertPublishedEvents(t *testing.T, ctx context.Context, sqsc *awssqs.Client, queueURL string, want map[int64]int) {
	t.Helper()
	seen := make(map[int64]outbound.BlockEvent, len(want))
	for range want {
		event := receiveBlockEvent(t, ctx, sqsc, queueURL)
		seen[event.BlockNumber] = event
	}

	for _, block := range []blockFixture{orphanOnly, alreadyCorrected} {
		event, delivered := seen[block.number]
		if !delivered {
			t.Fatalf("no event delivered for block %d", block.number)
		}
		assertEventDescribes(t, event, block, want[block.number])
	}
}

func receiveBlockEvent(t *testing.T, ctx context.Context, sqsc *awssqs.Client, queueURL string) outbound.BlockEvent {
	t.Helper()
	body := receiveOneSQSMessage(t, ctx, sqsc, queueURL, 30*time.Second)
	var event outbound.BlockEvent
	if err := json.Unmarshal([]byte(body), &event); err != nil {
		t.Fatalf("decoding the delivered BlockEvent: %v", err)
	}
	return event
}

func assertEventDescribes(t *testing.T, event outbound.BlockEvent, block blockFixture, version int) {
	t.Helper()
	if event.ChainID != integrationChainID {
		t.Errorf("event = %+v, want chain %d", event, integrationChainID)
	}
	if event.Version != version {
		t.Errorf("block %d published at version %d, want the free slot %d", block.number, event.Version, version)
	}
	if event.BlockHash != block.hash || event.ParentHash != block.parent {
		t.Errorf("event hashes = %s / %s, want %s / %s",
			event.BlockHash, event.ParentHash, block.hash, block.parent)
	}
	if event.BlockTimestamp != block.timestamp {
		t.Errorf("event block timestamp = %d, want %d", event.BlockTimestamp, block.timestamp)
	}
	if !event.IsReorg || !event.IsBackfill {
		t.Errorf("event flags = reorg %t / backfill %t, want both set", event.IsReorg, event.IsBackfill)
	}
	if event.ReceivedAt.IsZero() {
		t.Error("event has a zero ReceivedAt")
	}
}

func awsClients(t *testing.T, ctx context.Context) (*awssns.Client, *awssqs.Client) {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(sharedLocalStackCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	snsc := awssns.NewFromConfig(awsCfg, func(o *awssns.Options) {
		o.BaseEndpoint = aws.String(sharedLocalStackCfg.Endpoint)
	})
	sqsc := awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(sharedLocalStackCfg.Endpoint)
	})
	return snsc, sqsc
}

// startMockRPCServer answers the by-number reads Republish issues for every
// fixture, with the same canonical hash on every one so no reorg is detected.
func startMockRPCServer(t *testing.T, fixtures ...blockFixture) *httptest.Server {
	t.Helper()
	headers := make(map[int64]json.RawMessage, len(fixtures))
	fullBlocks := make(map[int64]json.RawMessage, len(fixtures))
	var head int64
	for _, block := range fixtures {
		headers[block.number] = json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":%q,"parentHash":%q,"timestamp":"0x%x"}`,
			block.number, block.hash, block.parent, block.timestamp))
		fullBlocks[block.number] = json.RawMessage(fmt.Sprintf(
			`{"number":"0x%x","hash":%q,"parentHash":%q,"timestamp":"0x%x","transactions":[]}`,
			block.number, block.hash, block.parent, block.timestamp))
		head = max(head, block.number+integrationHeadDepth)
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		answer := func(req rpcReq) rpcResp {
			out := rpcResp{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "eth_blockNumber":
				out.Result = json.RawMessage(fmt.Sprintf(`"0x%x"`, head))
			case "eth_getBlockByNumber":
				if fullTxParam(req) {
					out.Result = fullBlocks[hexParam(t, req)]
					break
				}
				out.Result = headers[hexParam(t, req)]
			case "eth_getBlockReceipts":
				out.Result = json.RawMessage(`[]`)
			case "trace_block":
				out.Result = json.RawMessage(`[]`)
			default:
				out.Result = json.RawMessage(`null`)
			}
			return out
		}

		var batch []rpcReq
		if err := json.Unmarshal(body, &batch); err == nil && len(batch) > 0 {
			replies := make([]rpcResp, len(batch))
			for i := range batch {
				replies[i] = answer(batch[i])
			}
			writeJSON(t, w, replies)
			return
		}
		var single rpcReq
		if err := json.Unmarshal(body, &single); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(t, w, answer(single))
	}))
}

func stringParam(req rpcReq) string {
	if len(req.Params) == 0 {
		return ""
	}
	param, _ := req.Params[0].(string)
	return param
}

// fullTxParam is eth_getBlockByNumber's second argument: the payload read asks
// for full transactions, the header reads do not.
func fullTxParam(req rpcReq) bool {
	if len(req.Params) < 2 {
		return false
	}
	full, _ := req.Params[1].(bool)
	return full
}

func hexParam(t *testing.T, req rpcReq) int64 {
	t.Helper()
	number, err := strconv.ParseInt(strings.TrimPrefix(stringParam(req), "0x"), 16, 64)
	if err != nil {
		t.Errorf("%s asked for %q, which is not a block number", req.Method, stringParam(req))
	}
	return number
}

type rpcReq struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Errorf("encoding the mock RPC response: %v", err)
	}
}

func createFifoTopic(t *testing.T, ctx context.Context, snsc *awssns.Client, name string) string {
	t.Helper()
	out, err := snsc.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String(name),
		Attributes: map[string]string{
			"FifoTopic":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return *out.TopicArn
}

func createFifoQueue(t *testing.T, ctx context.Context, sqsc *awssqs.Client, name string) string {
	t.Helper()
	out, err := sqsc.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			string(sqstypes.QueueAttributeNameFifoQueue):         "true",
			string(sqstypes.QueueAttributeNameVisibilityTimeout): "30",
		},
	})
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	return *out.QueueUrl
}

func subscribeQueueToTopic(t *testing.T, ctx context.Context, snsc *awssns.Client, sqsc *awssqs.Client, topicARN, queueURL string) {
	t.Helper()
	attrs, err := sqsc.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("get queue attrs: %v", err)
	}
	if _, err := snsc.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn:   aws.String(topicARN),
		Protocol:   aws.String("sqs"),
		Endpoint:   aws.String(attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]),
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
}

func receiveOneSQSMessage(t *testing.T, ctx context.Context, sqsc *awssqs.Client, queueURL string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := sqsc.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     2,
		})
		if err != nil {
			t.Fatalf("receive message: %v", err)
		}
		if len(out.Messages) > 0 {
			if out.Messages[0].Body == nil {
				t.Fatalf("received an SQS message with no body")
			}
			if _, err := sqsc.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: out.Messages[0].ReceiptHandle,
			}); err != nil {
				t.Fatalf("delete message: %v", err)
			}
			return *out.Messages[0].Body
		}
	}
	t.Fatalf("timed out waiting for an SQS message on %s", queueURL)
	return ""
}

// Both names are spelled out rather than compared to their constants, which
// would rename together and pin nothing. The alert regex in
// alerts/vector-cronjobs.yaml and the runbook carry the same two strings.
// Every chain's queue name is pinned in config_test.go.
func TestDeployedNames_MatchTheAlertsAndTheRunbook(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")

	queue, err := taskQueueName()
	if err != nil {
		t.Fatalf("taskQueueName() error = %v", err)
	}

	if queue != "block-republisher" {
		t.Errorf("taskQueueName() = %q, want %q", queue, "block-republisher")
	}
	if workflowTypeName != "BlockRepublish" {
		t.Errorf("workflowTypeName = %q, want %q", workflowTypeName, "BlockRepublish")
	}
}

// run() is the whole binary from main()'s point of view. Cancelling before it
// reaches Temporal is the one path a test can drive without a server, and it is
// what proves the signal context actually stops the worker.
// A SIGTERM during startup — a pod rolled while it was still wiring itself up —
// is a shutdown, not a failure: surfacing the cancelled context would exit 1 and
// make an ordinary rollout read like a crash.
func TestRun_StopsCleanlyWhenTheContextIsCancelled(t *testing.T) {
	t.Setenv("CHAIN_ID", "1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := run(ctx)

	if err != nil {
		t.Fatalf("run = %v, want a cancelled startup reported as a clean stop", err)
	}
}
