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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestRun_EndToEnd exercises the full null-payload-refill flow against
// LocalStack (S3, SNS, SQS) and a small in-process JSON-RPC mock server. The
// tool no longer talks to Postgres — all canonical-block metadata is derived
// from RPC via eth_getBlockByNumber. The subtests cover both key sources:
//
//   - FileKeysFile: --keys-file is set; the tool ignores the bucket scan and
//     reads keys from a pre-built file.
//   - BucketScan:   --keys-file is empty; the tool lists S3 objects with
//     Size <= --max-size and refills each one.
//
// Both variants share the same LocalStack seeding via runRefillScenario.
func TestRun_EndToEnd(t *testing.T) {
	t.Run("FileKeysFile", func(t *testing.T) {
		runRefillScenario(t, true)
	})
	t.Run("BucketScan", func(t *testing.T) {
		runRefillScenario(t, false)
	})
}

// runRefillScenario seeds LocalStack and the mock RPC, then runs Run() once.
// When useKeysFile is true the keys-file path is supplied; when false the
// tool scans the bucket via ListObjectsV2.
func runRefillScenario(t *testing.T, useKeysFile bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// 1. Bring up LocalStack (S3+SNS+SQS).
	_, lsCfg := testutil.StartLocalStack(t, ctx, "s3,sns,sqs")

	const chainID int64 = 43114
	const blockNum int64 = 85149017
	const blockHash = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const parentHash = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	const blockTimestamp int64 = 1730000000

	// 2. Set up AWS clients.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(lsCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	s3c := awsS3.NewFromConfig(awsCfg, func(o *awsS3.Options) {
		o.BaseEndpoint = aws.String(lsCfg.Endpoint)
		o.UsePathStyle = true
	})
	snsc := awssns.NewFromConfig(awsCfg, func(o *awssns.Options) {
		o.BaseEndpoint = aws.String(lsCfg.Endpoint)
	})
	sqsc := awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(lsCfg.Endpoint)
	})

	// 3. Create S3 bucket and seed it with a null gzipped block object plus
	//    a sentinel large object (BucketScan path must skip the large one).
	suffix := strings.ReplaceAll(testutil.SanitizeTestName(t.Name()), "_", "-")
	bucket := "refill-" + suffix
	if _, err := s3c.CreateBucket(ctx, &awsS3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	const key = "85149000-85149999/85149017_0_block.json.gz"
	putGzippedObject(t, ctx, s3c, bucket, key, []byte("null"))
	// Large sentinel object so the scan path must filter it out.
	const sentinelKey = "85149000-85149999/85149018_0_block.json.gz"
	putGzippedObject(t, ctx, s3c, bucket, sentinelKey, bytes.Repeat([]byte("x"), 4096))

	// 4. Create a FIFO SNS topic and a subscribed FIFO SQS queue.
	topicArn := createFifoTopic(t, ctx, snsc, "refill-topic-"+suffix+".fifo")
	queueURL := createFifoQueue(t, ctx, sqsc, "refill-queue-"+suffix+".fifo")
	subscribeQueueToTopic(t, ctx, snsc, sqsc, topicArn, queueURL)

	// 5. Start the in-process JSON-RPC server that mimics Alchemy. The
	//    canonical header response is what loadCanonicalBlock decodes for
	//    hash/parentHash/timestamp; the data response is what fetchData reads
	//    via GetBlockDataByHash.
	canonicalHeader := json.RawMessage(fmt.Sprintf(
		`{"hash":%q,"parentHash":%q,"timestamp":"0x%x"}`,
		blockHash, parentHash, blockTimestamp,
	))
	validBlock := json.RawMessage(`{"number":"0x512f1c9","hash":"` + blockHash + `","parentHash":"` + parentHash + `"}`)
	validReceipts := json.RawMessage(`[]`)
	rpcSrv := startMockRPCServer(t, mockResponses{
		BlockByNumber: canonicalHeader,
		Block:         validBlock,
		Receipts:      validReceipts,
		Traces:        json.RawMessage(`[]`),
		Blobs:         json.RawMessage(`[]`),
	})
	t.Cleanup(rpcSrv.Close)

	// 6. Build Options for the chosen key source.
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.jsonl")

	opts := Options{
		Bucket:      bucket,
		ChainID:     chainID,
		StateFile:   stateFile,
		SNSTopicARN: topicArn,
		RPCURL:      rpcSrv.URL,
		SNSEndpoint: lsCfg.Endpoint,
		S3Endpoint:  lsCfg.Endpoint,
		AWSRegion:   lsCfg.Region,
		Concurrency: 1,
	}
	if useKeysFile {
		keysFile := filepath.Join(tmpDir, "keys.txt")
		if err := os.WriteFile(keysFile, []byte(key+"\n"), 0o644); err != nil {
			t.Fatalf("write keys file: %v", err)
		}
		opts.KeysFile = keysFile
	} else {
		opts.MaxSize = 40
		opts.Prefix = "85149000-85149999/"
	}

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", lsCfg.Region)

	exitCode, err := Run(ctx, opts, logger)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("Run exit code = %d, want 0", exitCode)
	}

	// 7. Verify S3 was overwritten with non-null content.
	got := fetchObject(t, ctx, s3c, bucket, key)
	if rpcutil.IsNullOrEmpty(got) {
		t.Fatalf("S3 object still null after refill: %q", got)
	}
	if !bytes.Contains(got, []byte(blockHash)) {
		t.Errorf("S3 object %q does not contain expected block hash %q", got, blockHash)
	}

	// 8. Verify SNS published an event into the SQS queue.
	msg := receiveOneSQSMessage(t, ctx, sqsc, queueURL, 10*time.Second)
	var raw map[string]any
	if err := json.Unmarshal([]byte(msg), &raw); err != nil {
		t.Fatalf("sqs message body not JSON: %v body=%q", err, msg)
	}
	// SNS subscribed without RawMessageDelivery wraps in {"Message": "..."}.
	var event outbound.BlockEvent
	if m, ok := raw["Message"].(string); ok {
		if err := json.Unmarshal([]byte(m), &event); err != nil {
			t.Fatalf("inner BlockEvent JSON: %v", err)
		}
	} else {
		if err := json.Unmarshal([]byte(msg), &event); err != nil {
			t.Fatalf("plain BlockEvent JSON: %v", err)
		}
	}
	if event.ChainID != chainID || event.BlockNumber != blockNum || event.BlockHash != blockHash {
		t.Errorf("event mismatch: %+v", event)
	}
	if event.ParentHash != parentHash {
		t.Errorf("event parent hash = %q, want %q", event.ParentHash, parentHash)
	}
	if event.BlockTimestamp != blockTimestamp {
		t.Errorf("event block timestamp = %d, want %d", event.BlockTimestamp, blockTimestamp)
	}

	// 9. State file has stage:sns for the key.
	st, err := Load(stateFile)
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	defer st.Close()
	if stage, _ := st.Lookup(key); stage != StageSNS {
		t.Errorf("state stage = %q, want %q", stage, StageSNS)
	}
	// Sentinel must not have been processed (still over the size threshold,
	// and even in file mode we never asked for it).
	if stage, _ := st.Lookup(sentinelKey); stage != "" {
		t.Errorf("sentinel key unexpectedly recorded stage=%q", stage)
	}
}

// ----- helpers ---------------------------------------------------------------

type mockResponses struct {
	BlockByNumber json.RawMessage
	Block         json.RawMessage
	Receipts      json.RawMessage
	Traces        json.RawMessage
	Blobs         json.RawMessage
}

func startMockRPCServer(t *testing.T, resp mockResponses) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		// Try to parse as batch first, falling back to single.
		var batch []rpcReq
		if err := json.Unmarshal(body, &batch); err == nil && len(batch) > 0 {
			out := make([]rpcResp, len(batch))
			for i := range batch {
				out[i] = handleSingle(batch[i], resp)
			}
			writeJSON(w, out)
			return
		}
		var single rpcReq
		if err := json.Unmarshal(body, &single); err != nil {
			http.Error(w, "bad json: "+err.Error(), 400)
			return
		}
		writeJSON(w, handleSingle(single, resp))
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

func handleSingle(req rpcReq, resp mockResponses) rpcResp {
	r := rpcResp{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "eth_getBlockByNumber":
		r.Result = resp.BlockByNumber
	case "eth_getBlockByHash":
		r.Result = resp.Block
	case "eth_getBlockReceipts":
		r.Result = resp.Receipts
	case "trace_block":
		r.Result = resp.Traces
	case "eth_getBlobSidecars":
		r.Result = resp.Blobs
	default:
		r.Result = json.RawMessage("null")
	}
	return r
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func putGzippedObject(t *testing.T, ctx context.Context, s3c *awsS3.Client, bucket, key string, content []byte) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(content); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	_, err := s3c.PutObject(ctx, &awsS3.PutObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		Body:            bytes.NewReader(buf.Bytes()),
		ContentEncoding: aws.String("gzip"),
		ContentType:     aws.String("application/json"),
	})
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
}

func fetchObject(t *testing.T, ctx context.Context, s3c *awsS3.Client, bucket, key string) []byte {
	t.Helper()
	out, err := s3c.GetObject(ctx, &awsS3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	// Decompress only if Content-Encoding is gzip; LocalStack returns
	// gzip-encoded content with that header.
	if out.ContentEncoding != nil && *out.ContentEncoding == "gzip" {
		gz, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatalf("gzip reader: %v", err)
		}
		body, err = io.ReadAll(gz)
		if err != nil {
			t.Fatalf("gzip read: %v", err)
		}
		_ = gz.Close()
	}
	return body
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

func subscribeQueueToTopic(t *testing.T, ctx context.Context, snsc *awssns.Client, sqsc *awssqs.Client, topicArn, queueURL string) {
	t.Helper()
	attrs, err := sqsc.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("get queue attrs: %v", err)
	}
	queueArn := attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	if _, err := snsc.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
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
			return *out.Messages[0].Body
		}
	}
	t.Fatalf("timed out waiting for SQS message on %s", queueURL)
	return ""
}
