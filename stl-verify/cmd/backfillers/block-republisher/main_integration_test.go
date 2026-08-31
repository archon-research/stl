//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	sharedRedisAddr      string
	sharedLocalStackCfg  testutil.LocalStackConfig
	integrationChainID   = int64(1)
	integrationBlock     = int64(25395651)
	integrationBlockHash = "0x4d1c1a52b1f5e5a0c6f0b0a0d9e8c7b6a594837261504f3e2d1c0b9a8f7e6d5c"
	integrationParent    = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	integrationTimestamp = int64(0x68b0c0c0)

	// integrationHeadDepth puts the block well outside the reorg window the
	// service refuses to repair inside.
	integrationHeadDepth = int64(5000)
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		RedisAddr:          &sharedRedisAddr,
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "sns,sqs",
	}))
}

// TestRepublish_LandsInTheCacheAndOnTheTopic drives the deployed wiring — register,
// loadConfig, the real Redis adapter, the real SNS adapter and the real batched RPC
// client — against LocalStack and an in-process node. It is what proves the two
// things a consumer depends on and a unit test with fakes cannot: the cache keys
// carry the requested version, and the SNS message that reaches a subscribed FIFO
// queue is the block event for that version.
func TestRepublish_LandsInTheCacheAndOnTheTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	deployment := newDeployment(t, ctx)
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	if err := register(ctx, temporal.Dependencies{Logger: discardLogger()}, env); err != nil {
		t.Fatalf("register: %v", err)
	}

	env.ExecuteWorkflow(workflowTypeName, RepublishParams{Blocks: []int64{integrationBlock}, Version: new(1)})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow: %v", err)
	}
	var result RepublishResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("decoding the workflow result: %v", err)
	}
	if len(result.Republished) != 1 || result.Republished[0].BlockHash != integrationBlockHash {
		t.Fatalf("result = %+v, want one block republished at hash %s", result, integrationBlockHash)
	}
	assertCachedUnderVersion(t, ctx, deployment.keyPrefix)
	assertPublishedEvent(t, ctx, deployment.sqs, deployment.queueURL)
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

// deployment is the environment a deployed pod would find: a chain-named FIFO
// topic with a queue subscribed to it, a node to read blocks from, and the env
// vars the ConfigMap and ExternalSecret supply.
type deployment struct {
	sqs       *awssqs.Client
	topicARN  string
	queueURL  string
	keyPrefix string
}

func newDeployment(t *testing.T, ctx context.Context) deployment {
	t.Helper()
	snsc, sqsc := awsClients(t, ctx)

	// The topic name is what chainutil.ValidateSNSTopicForChain checks against
	// CHAIN_ID and DEPLOY_ENV, so the deploy environment the test declares and the
	// topic it creates have to agree.
	const deployEnv = "brtest"
	topicARN := createFifoTopic(t, ctx, snsc, "stl-sentinel"+deployEnv+"-ethereum-blocks.fifo")
	queueURL := createFifoQueue(t, ctx, sqsc, testutil.SQSTestFifoQueueName(t, "block-republisher-"))
	subscribeQueueToTopic(t, ctx, snsc, sqsc, topicARN, queueURL)

	node := startMockRPCServer(t)
	t.Cleanup(node.Close)

	keyPrefix := testutil.SanitizeTestName(t.Name())
	t.Setenv("CHAIN_ID", fmt.Sprintf("%d", integrationChainID))
	t.Setenv("DEPLOY_ENV", deployEnv)
	t.Setenv("AWS_SNS_TOPIC_ARN", topicARN)
	t.Setenv("AWS_SNS_ENDPOINT", sharedLocalStackCfg.Endpoint)
	t.Setenv("AWS_REGION", sharedLocalStackCfg.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", node.URL)
	t.Setenv("REDIS_ADDR", sharedRedisAddr)
	t.Setenv("REDIS_KEY_PREFIX", keyPrefix)

	return deployment{sqs: sqsc, topicARN: topicARN, queueURL: queueURL, keyPrefix: keyPrefix}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// assertCachedUnderVersion reads the raw keys rather than going back through the
// adapter: the key layout is the contract every worker builds its own read from.
func assertCachedUnderVersion(t *testing.T, ctx context.Context, keyPrefix string) {
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
		return fmt.Sprintf("%s:%d:%d:%d:%s", keyPrefix, integrationChainID, integrationBlock, version, dataType)
	}

	for _, dataType := range []string{"block", "receipts", "traces"} {
		if exists(key(1, dataType)) != 1 {
			t.Errorf("cache key %s is missing", key(1, dataType))
		}
	}
	// Ethereum's watcher fetches no blobs, so republishing them would hand the
	// backup worker a data type it does not expect at this version.
	if exists(key(1, "blobs")) != 0 {
		t.Errorf("cache key %s was written; this chain's watcher publishes no blobs", key(1, "blobs"))
	}
	// Nothing may land in the version-0 slot: that is the losing fork's data.
	if exists(key(0, "block")) != 0 {
		t.Errorf("cache key %s was written; a republish must never touch version 0", key(0, "block"))
	}
}

func assertPublishedEvent(t *testing.T, ctx context.Context, sqsc *awssqs.Client, queueURL string) {
	t.Helper()
	body := receiveOneSQSMessage(t, ctx, sqsc, queueURL, 30*time.Second)

	var event outbound.BlockEvent
	if err := json.Unmarshal([]byte(body), &event); err != nil {
		t.Fatalf("decoding the delivered BlockEvent: %v", err)
	}
	if event.ChainID != integrationChainID || event.BlockNumber != integrationBlock {
		t.Errorf("event = %+v, want chain %d block %d", event, integrationChainID, integrationBlock)
	}
	if event.Version != 1 {
		t.Errorf("event version = %d, want the requested 1", event.Version)
	}
	if event.BlockHash != integrationBlockHash || event.ParentHash != integrationParent {
		t.Errorf("event hashes = %s / %s, want %s / %s",
			event.BlockHash, event.ParentHash, integrationBlockHash, integrationParent)
	}
	if event.BlockTimestamp != integrationTimestamp {
		t.Errorf("event block timestamp = %d, want %d", event.BlockTimestamp, integrationTimestamp)
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

// startMockRPCServer answers the three reads Republish issues, with the same
// canonical hash on both by-number reads so no reorg is detected.
func startMockRPCServer(t *testing.T) *httptest.Server {
	t.Helper()
	header := json.RawMessage(fmt.Sprintf(`{"number":"0x1836b83","hash":%q,"parentHash":%q,"timestamp":"0x%x"}`,
		integrationBlockHash, integrationParent, integrationTimestamp))
	fullBlock := json.RawMessage(fmt.Sprintf(`{"number":"0x1836b83","hash":%q,"parentHash":%q,"timestamp":"0x%x","transactions":[]}`,
		integrationBlockHash, integrationParent, integrationTimestamp))

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
				out.Result = json.RawMessage(fmt.Sprintf(`"0x%x"`, integrationBlock+integrationHeadDepth))
			case "eth_getBlockByNumber":
				out.Result = header
			case "eth_getBlockByHash":
				out.Result = fullBlock
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
			return *out.Messages[0].Body
		}
	}
	t.Fatalf("timed out waiting for an SQS message on %s", queueURL)
	return ""
}

// run() is the whole binary from main()'s point of view. Cancelling before it
// reaches Temporal is the one path a test can drive without a server, and it is
// what proves the signal context actually stops the worker.
func TestRun_StopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want it to surface the cancelled context", err)
	}
}
