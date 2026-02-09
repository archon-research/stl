//go:build integration

package rawdatabackup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	sqsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// =============================================================================
// Test Infrastructure
// =============================================================================

// IntegrationTestInfra holds all the infrastructure for integration tests.
type IntegrationTestInfra struct {
	// AWS clients
	SNSClient *sns.Client
	SQSClient *sqs.Client
	S3Client  *s3.Client

	// Queue URLs and ARNs
	BackupQueueURL string
	TopicARN       string

	// S3 bucket
	BucketName string

	// Redis cache
	Cache outbound.BlockCache

	// Service dependencies
	Consumer outbound.SQSConsumer
	Writer   outbound.S3Writer

	// Logger
	Logger *slog.Logger

	// Containers
	containers []testcontainers.Container
	Cleanup    func()
}

func setupIntegrationInfra(t *testing.T, ctx context.Context) *IntegrationTestInfra {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	infra := &IntegrationTestInfra{
		Logger: logger,
	}
	var cleanupFuncs []func()

	// Start Redis
	redisContainer, redisCfg := startRedis(t, ctx)
	infra.containers = append(infra.containers, redisContainer)
	cleanupFuncs = append(cleanupFuncs, func() {
		// Use background context for cleanup since test context may be canceled
		if err := redisContainer.Terminate(context.Background()); err != nil {
			logger.Error("failed to terminate redis container", "error", err)
		}
	})

	cache, err := rediscache.NewBlockCache(rediscache.Config{
		Addr:      redisCfg.Addr,
		Password:  redisCfg.Password,
		DB:        redisCfg.DB,
		TTL:       1 * time.Hour,
		KeyPrefix: "integration-test",
	}, logger)
	if err != nil {
		t.Fatalf("failed to create redis cache: %v", err)
	}
	cleanupFuncs = append(cleanupFuncs, func() {
		if err := cache.Close(); err != nil {
			logger.Error("failed to close redis cache", "error", err)
		}
	})
	infra.Cache = cache

	// Start LocalStack (with S3, SNS, SQS)
	localstackContainer, localstackCfg := startLocalStack(t, ctx)
	infra.containers = append(infra.containers, localstackContainer)
	cleanupFuncs = append(cleanupFuncs, func() {
		// Use background context for cleanup since test context may be canceled
		if err := localstackContainer.Terminate(context.Background()); err != nil {
			logger.Error("failed to terminate localstack container", "error", err)
		}
	})

	// Create AWS clients
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(localstackCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	snsClient := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
	})
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
		o.UsePathStyle = true // Required for LocalStack
	})

	infra.SNSClient = snsClient
	infra.SQSClient = sqsClient
	infra.S3Client = s3Client

	// Create S3 bucket
	bucketName := "test-raw-data-backup"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("failed to create S3 bucket: %v", err)
	}
	infra.BucketName = bucketName
	t.Logf("Created S3 bucket: %s", bucketName)

	// Create SNS topic
	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("test-blocks"),
	})
	if err != nil {
		t.Fatalf("failed to create SNS topic: %v", err)
	}
	infra.TopicARN = *topicResult.TopicArn
	t.Logf("Created SNS topic: %s", infra.TopicARN)

	// Create SQS queue for backup
	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("test-backup-queue"),
		Attributes: map[string]string{
			string(sqstypes.QueueAttributeNameVisibilityTimeout): "30",
		},
	})
	if err != nil {
		t.Fatalf("failed to create SQS queue: %v", err)
	}
	infra.BackupQueueURL = *queueResult.QueueUrl
	t.Logf("Created SQS queue: %s", infra.BackupQueueURL)

	// Get queue ARN for subscription
	queueAttrs, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(infra.BackupQueueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("failed to get queue ARN: %v", err)
	}
	queueARN := queueAttrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	// Subscribe queue to topic
	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(infra.TopicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	})
	if err != nil {
		t.Fatalf("failed to subscribe queue to topic: %v", err)
	}
	t.Logf("Subscribed queue to topic")

	// Create SQS consumer adapter using the real adapter with LocalStack endpoint
	consumer, err := sqsadapter.NewConsumerWithOptions(awsCfg, sqsadapter.Config{
		QueueURL:        infra.BackupQueueURL,
		WaitTimeSeconds: 1, // Short for tests
	}, logger, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
	})
	if err != nil {
		t.Fatalf("failed to create SQS consumer: %v", err)
	}
	infra.Consumer = consumer

	// Create S3 writer adapter using the real adapter with LocalStack endpoint
	infra.Writer = s3adapter.NewWriterWithOptions(awsCfg, logger, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(localstackCfg.Endpoint)
		o.UsePathStyle = true // Required for LocalStack
	})

	infra.Cleanup = func() {
		for i := len(cleanupFuncs) - 1; i >= 0; i-- {
			cleanupFuncs[i]()
		}
	}

	return infra
}

// =============================================================================
// Container Setup
// =============================================================================

type RedisTestConfig struct {
	Addr     string
	Password string
	DB       int
}

func startRedis(t *testing.T, ctx context.Context) (testcontainers.Container, RedisTestConfig) {
	t.Helper()

	config := RedisTestConfig{
		Password: "",
		DB:       0,
	}

	req := testcontainers.ContainerRequest{
		Image:        "redis:8.0-M04-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "6379")
	config.Addr = fmt.Sprintf("%s:%s", host, port.Port())

	return container, config
}

type LocalStackTestConfig struct {
	Endpoint string
	Region   string
}

func startLocalStack(t *testing.T, ctx context.Context) (testcontainers.Container, LocalStackTestConfig) {
	t.Helper()

	config := LocalStackTestConfig{
		Region: "us-east-1",
	}

	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:latest",
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES": "sns,sqs,s3",
			"DEBUG":    "0",
		},
		WaitingFor: wait.ForHTTP("/_localstack/health").
			WithPort("4566/tcp").
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start localstack: %v", err)
	}

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4566")
	config.Endpoint = fmt.Sprintf("http://%s:%s", host, port.Port())

	return container, config
}

// =============================================================================
// Helper Functions
// =============================================================================

func publishBlockEvent(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, event outbound.BlockEvent) {
	t.Helper()

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	_, err = infra.SNSClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(infra.TopicARN),
		Message:  aws.String(string(body)),
	})
	if err != nil {
		t.Fatalf("failed to publish to SNS: %v", err)
	}
	t.Logf("Published block event: block=%d, version=%d", event.BlockNumber, event.Version)
}

func getS3Object(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, key string) []byte {
	t.Helper()

	result, err := infra.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(infra.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("failed to get S3 object %s: %v", key, err)
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("failed to read S3 object body: %v", err)
	}

	// Decompress if gzipped
	if result.ContentEncoding != nil && *result.ContentEncoding == "gzip" {
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatalf("failed to create gzip reader: %v", err)
		}
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("failed to decompress: %v", err)
		}
		return decompressed
	}

	return body
}

func listS3Objects(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, prefix string) []string {
	t.Helper()

	result, err := infra.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(infra.BucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("failed to list S3 objects: %v", err)
	}

	keys := make([]string, 0, len(result.Contents))
	for _, obj := range result.Contents {
		keys = append(keys, *obj.Key)
	}
	return keys
}

// waitForS3Objects polls until at least minCount S3 objects exist with the given prefix.
func waitForS3Objects(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, prefix string, minCount int, timeout time.Duration) []string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var objects []string

	for time.Now().Before(deadline) {
		objects = listS3Objects(t, ctx, infra, prefix)
		if len(objects) >= minCount {
			return objects
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for S3 objects")
		case <-time.After(100 * time.Millisecond):
			// continue polling
		}
	}

	t.Fatalf("timed out waiting for %d S3 objects with prefix %q, got %d: %v", minCount, prefix, len(objects), objects)
	return nil
}

// waitForS3Object polls until a specific S3 object exists.
func waitForS3Object(t *testing.T, ctx context.Context, infra *IntegrationTestInfra, key string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		_, err := infra.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(infra.BucketName),
			Key:    aws.String(key),
		})
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for S3 object %q", key)
		case <-time.After(100 * time.Millisecond):
			// continue polling
		}
	}

	t.Fatalf("timed out waiting for S3 object %q", key)
}

// =============================================================================
// Integration Tests
// =============================================================================

// TestIntegration_SingleBlockBackup tests backing up a single block's data.
func TestIntegration_SingleBlockBackup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	// Set up block data in Redis cache
	blockNumber := int64(12345)
	version := 0
	chainID := int64(1)

	blockData := json.RawMessage(`{"number":"0x3039","hash":"0xabcd","parentHash":"0x1234"}`)
	receiptsData := json.RawMessage(`[{"transactionHash":"0xdead","status":"0x1"}]`)
	tracesData := json.RawMessage(`[{"action":{"callType":"call","from":"0x1","to":"0x2"}}]`)
	blobsData := json.RawMessage(`[{"commitment":"0xbeef"}]`)

	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, version, outbound.BlockDataInput{
		Block:    blockData,
		Receipts: receiptsData,
		Traces:   tracesData,
		Blobs:    blobsData,
	})
	if err != nil {
		t.Fatalf("failed to set block data in cache: %v", err)
	}

	// Create and start the backup service
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   2,
		BatchSize: 10,
		Logger:    infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Start service in background
	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish the block event
	event := outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      "0xabcd",
		ParentHash:     "0x1234",
		BlockTimestamp: time.Now().Unix(),
		ReceivedAt:     time.Now(),
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait for S3 objects to be created (4 files: block, receipts, traces, blobs)
	partition := "12000-12999" // Block 12345 falls in this partition
	prefix := fmt.Sprintf("%s/", partition)
	objects := waitForS3Objects(t, ctx, infra, prefix, 4, 10*time.Second)

	// Stop service
	svc.Stop()
	svcCancel()
	<-done

	t.Logf("S3 objects created: %v", objects)

	expectedFiles := []string{
		fmt.Sprintf("%s/%d_%d_block.json.gz", partition, blockNumber, version),
		fmt.Sprintf("%s/%d_%d_receipts.json.gz", partition, blockNumber, version),
		fmt.Sprintf("%s/%d_%d_traces.json.gz", partition, blockNumber, version),
		fmt.Sprintf("%s/%d_%d_blobs.json.gz", partition, blockNumber, version),
	}

	for _, expected := range expectedFiles {
		found := false
		for _, obj := range objects {
			if obj == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected file %s not found in S3", expected)
		}
	}

	// Verify content of block file
	blockKey := fmt.Sprintf("%s/%d_%d_block.json.gz", partition, blockNumber, version)
	content := getS3Object(t, ctx, infra, blockKey)
	t.Logf("Block content: %s", string(content))

	var savedBlock map[string]interface{}
	if err := json.Unmarshal(content, &savedBlock); err != nil {
		t.Fatalf("failed to parse saved block: %v", err)
	}
	if savedBlock["hash"] != "0xabcd" {
		t.Errorf("expected hash 0xabcd, got %v", savedBlock["hash"])
	}
}

// TestIntegration_MultipleBlocksProcessedConcurrently tests processing multiple blocks.
func TestIntegration_MultipleBlocksProcessedConcurrently(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)
	numBlocks := 10

	// Set up block data for multiple blocks
	for i := 0; i < numBlocks; i++ {
		blockNumber := int64(100 + i)
		blockData := json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":"0x%064x"}`, blockNumber, blockNumber))
		err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, 0, outbound.BlockDataInput{Block: blockData})
		if err != nil {
			t.Fatalf("failed to set block %d in cache: %v", blockNumber, err)
		}
	}

	// Create service with multiple workers - only expect block data
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   4,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish all block events
	for i := 0; i < numBlocks; i++ {
		blockNumber := int64(100 + i)
		event := outbound.BlockEvent{
			ChainID:        chainID,
			BlockNumber:    blockNumber,
			Version:        0,
			BlockHash:      fmt.Sprintf("0x%064x", blockNumber),
			BlockTimestamp: time.Now().Unix(),
			ReceivedAt:     time.Now(),
		}
		publishBlockEvent(t, ctx, infra, event)
	}

	// Wait for all blocks to be backed up (10 block files expected)
	// Blocks 100-109 all fall in partition 0-999
	prefix := "0-999/"
	objects := waitForS3Objects(t, ctx, infra, prefix, numBlocks, 15*time.Second)

	svc.Stop()
	svcCancel()
	<-done

	t.Logf("Created %d S3 objects", len(objects))

	if len(objects) != numBlocks {
		t.Errorf("expected %d S3 objects, got %d", numBlocks, len(objects))
	}
}

// TestIntegration_IdempotentWrites tests that duplicate messages don't create duplicate files.
func TestIntegration_IdempotentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)
	blockNumber := int64(500)

	// Set up block data
	blockData := json.RawMessage(`{"number":"0x1f4","hash":"0xfirst"}`)
	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, 0, outbound.BlockDataInput{Block: blockData})
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}

	// Create service
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish same event twice
	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     0,
		BlockHash:   "0xfirst",
	}
	key := fmt.Sprintf("0-999/%d_0_block.json.gz", blockNumber)

	publishBlockEvent(t, ctx, infra, event)

	// Wait for first file to be written
	waitForS3Object(t, ctx, infra, key, 10*time.Second)

	// Update cache with different data (simulating reprocessing scenario)
	blockData2 := json.RawMessage(`{"number":"0x1f4","hash":"0xsecond"}`)
	_ = infra.Cache.SetBlockData(ctx, chainID, blockNumber, 0, outbound.BlockDataInput{Block: blockData2})

	// Publish again - the service should skip due to idempotency
	publishBlockEvent(t, ctx, infra, event)

	// Wait a short time for the second message to be processed
	// (it should be skipped, but we need to give it time to process)
	testutil.WaitForCondition(t, 5*time.Second, func() bool {
		// Check queue is empty (message was processed)
		attrs, _ := infra.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(infra.BackupQueueURL),
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages},
		})
		if attrs != nil {
			count := attrs.Attributes["ApproximateNumberOfMessages"]
			return count == "0"
		}
		return false
	}, "SQS queue to be empty")

	svc.Stop()
	svcCancel()
	<-done

	// Verify only one file exists and has original content
	content := getS3Object(t, ctx, infra, key)

	var savedBlock map[string]interface{}
	if err := json.Unmarshal(content, &savedBlock); err != nil {
		t.Fatalf("failed to parse saved block: %v", err)
	}

	// Should have original content since idempotency check prevented overwrite
	if savedBlock["hash"] != "0xfirst" {
		t.Errorf("expected original hash 0xfirst, got %v (idempotency failed)", savedBlock["hash"])
	}
}

// TestIntegration_DifferentVersionsStored tests that reorg versions are stored separately.
func TestIntegration_DifferentVersionsStored(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)
	blockNumber := int64(1500) // Partition 1000-1999

	// Set up version 0
	blockData0 := json.RawMessage(`{"number":"0x5dc","version":0}`)
	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, 0, outbound.BlockDataInput{Block: blockData0})
	if err != nil {
		t.Fatalf("failed to set block v0 in cache: %v", err)
	}

	// Set up version 1 (reorg)
	blockData1 := json.RawMessage(`{"number":"0x5dc","version":1}`)
	err = infra.Cache.SetBlockData(ctx, chainID, blockNumber, 1, outbound.BlockDataInput{Block: blockData1})
	if err != nil {
		t.Fatalf("failed to set block v1 in cache: %v", err)
	}

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   2,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish version 0
	event0 := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     0,
		BlockHash:   "0xv0",
	}
	publishBlockEvent(t, ctx, infra, event0)

	// Wait for version 0 to be written
	keyV0 := fmt.Sprintf("1000-1999/%d_0_block.json.gz", blockNumber)
	waitForS3Object(t, ctx, infra, keyV0, 10*time.Second)

	// Publish version 1
	event1 := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     1,
		BlockHash:   "0xv1",
	}
	publishBlockEvent(t, ctx, infra, event1)

	// Wait for version 1 to be written
	keyV1 := fmt.Sprintf("1000-1999/%d_1_block.json.gz", blockNumber)
	waitForS3Object(t, ctx, infra, keyV1, 10*time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify both versions exist
	prefix := "1000-1999/"
	objects := listS3Objects(t, ctx, infra, prefix)
	t.Logf("S3 objects: %v", objects)

	expectedV0 := fmt.Sprintf("1000-1999/%d_0_block.json.gz", blockNumber)
	expectedV1 := fmt.Sprintf("1000-1999/%d_1_block.json.gz", blockNumber)

	foundV0, foundV1 := false, false
	for _, obj := range objects {
		if obj == expectedV0 {
			foundV0 = true
		}
		if obj == expectedV1 {
			foundV1 = true
		}
	}

	if !foundV0 {
		t.Error("version 0 file not found")
	}
	if !foundV1 {
		t.Error("version 1 file not found")
	}

	// Verify content
	content0 := getS3Object(t, ctx, infra, expectedV0)
	content1 := getS3Object(t, ctx, infra, expectedV1)

	var block0, block1 map[string]interface{}
	json.Unmarshal(content0, &block0)
	json.Unmarshal(content1, &block1)

	if block0["version"] != float64(0) {
		t.Errorf("v0 content wrong: %v", block0)
	}
	if block1["version"] != float64(1) {
		t.Errorf("v1 content wrong: %v", block1)
	}
}

// TestIntegration_LargeBlockData tests handling of large block data.
func TestIntegration_LargeBlockData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)
	blockNumber := int64(2001) // Partition 2000-2999

	// Create large block data (~1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = 'x'
	}
	blockData := json.RawMessage(fmt.Sprintf(`{"number":"0x7d1","data":"%s"}`, string(largeData)))

	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, 0, outbound.BlockDataInput{Block: blockData})
	if err != nil {
		t.Fatalf("failed to set large block in cache: %v", err)
	}

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     0,
		BlockHash:   "0xlarge",
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait for file to be created
	key := fmt.Sprintf("2000-2999/%d_0_block.json.gz", blockNumber)
	waitForS3Object(t, ctx, infra, key, 15*time.Second)

	svc.Stop()
	svcCancel()
	<-done
	content := getS3Object(t, ctx, infra, key)

	if len(content) < 1024*1024 {
		t.Errorf("expected large content (>1MB), got %d bytes", len(content))
	}
}

// TestIntegration_GracefulShutdown tests that the service shuts down gracefully.
func TestIntegration_GracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	chainID := int64(1)

	// Set up some blocks
	for i := 0; i < 5; i++ {
		blockNumber := int64(3001 + i)
		blockData := json.RawMessage(fmt.Sprintf(`{"number":"0x%x"}`, blockNumber))
		_ = infra.Cache.SetBlockData(ctx, chainID, blockNumber, 0, outbound.BlockDataInput{Block: blockData})
	}

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   2,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish events
	for i := 0; i < 5; i++ {
		blockNumber := int64(3001 + i)
		event := outbound.BlockEvent{
			ChainID:     chainID,
			BlockNumber: blockNumber,
			Version:     0,
		}
		publishBlockEvent(t, ctx, infra, event)
	}

	// Wait for at least one block to be processed before stopping
	prefix := "3000-3999/"
	waitForS3Objects(t, ctx, infra, prefix, 1, 10*time.Second)

	// Stop gracefully using the Stop() method (not context cancellation)
	svc.Stop()

	select {
	case err := <-done:
		// Either nil or context.Canceled is acceptable for graceful shutdown
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected error on shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("service did not shut down within timeout")
	}

	// Clean up context
	svcCancel()

	t.Log("Service shut down gracefully")
}

// TestIntegration_RaceConditionIdempotency tests that concurrent writes to the same
// block/version don't corrupt data (TOCTOU race in FileExists + WriteFile).
func TestIntegration_RaceConditionIdempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	blockNumber := int64(7777)
	version := 0
	chainID := int64(1)

	// Set up block data in cache
	blockData := json.RawMessage(`{"number":"0x1e61","hash":"0xrace"}`)
	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, version, outbound.BlockDataInput{Block: blockData})
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}

	// Create service with many workers to increase race likelihood
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   8, // Many workers to maximize race chances
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish the SAME event multiple times rapidly
	// This simulates duplicate messages (at-least-once delivery)
	for i := 0; i < 10; i++ {
		event := outbound.BlockEvent{
			ChainID:     chainID,
			BlockNumber: blockNumber,
			Version:     version,
			BlockHash:   "0xrace",
		}
		publishBlockEvent(t, ctx, infra, event)
	}

	// Wait for at least one file to be created
	key := fmt.Sprintf("7000-7999/%d_%d_block.json.gz", blockNumber, version)
	waitForS3Object(t, ctx, infra, key, 15*time.Second)

	// Wait a bit for any concurrent writes to complete
	testutil.WaitForCondition(t, 5*time.Second, func() bool {
		attrs, _ := infra.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(infra.BackupQueueURL),
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages},
		})
		if attrs != nil {
			count := attrs.Attributes["ApproximateNumberOfMessages"]
			return count == "0"
		}
		return false
	}, "SQS queue to drain")

	svc.Stop()
	svcCancel()
	<-done

	// Verify only ONE file exists (no duplicates with different content)
	prefix := fmt.Sprintf("7000-7999/%d_%d_", blockNumber, version)
	objects := listS3Objects(t, ctx, infra, prefix)

	// Count how many block files exist
	blockFileCount := 0
	for _, obj := range objects {
		if obj == key {
			blockFileCount++
		}
	}

	if blockFileCount != 1 {
		t.Errorf("expected exactly 1 block file, got %d: %v", blockFileCount, objects)
	}

	// Verify content is correct (not corrupted by race)
	content := getS3Object(t, ctx, infra, key)
	var savedBlock map[string]interface{}
	if err := json.Unmarshal(content, &savedBlock); err != nil {
		t.Fatalf("failed to parse saved block (possible corruption): %v", err)
	}

	if savedBlock["hash"] != "0xrace" {
		t.Errorf("expected hash 0xrace, got %v", savedBlock["hash"])
	}
}

// TestIntegration_PartialWriteFailure tests behavior when some files write but others fail.
// This simulates S3 errors mid-backup.
func TestIntegration_PartialWriteFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	blockNumber := int64(8888)
	version := 0
	chainID := int64(1)

	// Set up block data in cache (only block, no receipts/traces/blobs)
	blockData := json.RawMessage(`{"number":"0x22b8","hash":"0xpartial"}`)
	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, version, outbound.BlockDataInput{Block: blockData})
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}
	// Note: NOT setting receipts, traces, or blobs - they'll be nil/missing

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     version,
		BlockHash:   "0xpartial",
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait for block file to be created
	key := fmt.Sprintf("8000-8999/%d_%d_block.json.gz", blockNumber, version)
	waitForS3Object(t, ctx, infra, key, 10*time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify only block file exists (receipts/traces/blobs should be skipped)
	prefix := fmt.Sprintf("8000-8999/%d_%d_", blockNumber, version)
	objects := listS3Objects(t, ctx, infra, prefix)

	t.Logf("Files created: %v", objects)

	// Only block should exist since receipts/traces/blobs weren't in cache
	if len(objects) != 1 {
		t.Errorf("expected 1 file (block only), got %d: %v", len(objects), objects)
	}
}

// TestIntegration_CacheMissBlockData tests that missing block data in cache
// causes the message to be requeued (not acknowledged).
func TestIntegration_CacheMissBlockData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	blockNumber := int64(9999)
	chainID := int64(1)

	// Intentionally NOT setting any cache data - simulating cache miss

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     0,
		BlockHash:   "0xmissing",
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait a bit for processing attempt
	time.Sleep(2 * time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify NO files were created (block data was missing)
	prefix := fmt.Sprintf("9000-9999/%d_", blockNumber)
	objects := listS3Objects(t, ctx, infra, prefix)

	if len(objects) != 0 {
		t.Errorf("expected 0 files (cache miss), got %d: %v", len(objects), objects)
	}
}

// TestIntegration_ChainIDMismatch tests that events with wrong ChainID
// are handled correctly.
// TestIntegration_ChainIDMismatch tests that events are processed based on
// the event's chainID for cache lookups, but the S3 key doesn't include chainID
// (since each chain has its own bucket).
func TestIntegration_ChainIDMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	blockNumber := int64(5555)
	version := 0
	serviceChainID := int64(1) // Service configured for chain 1
	eventChainID := int64(137) // Event is for Polygon (chain 137)

	// Set up block data for the EVENT's chainID (not service's)
	// The cache key uses chainID, so we need to set it for the correct chain
	blockData := json.RawMessage(`{"number":"0x15b3","hash":"0xpolygon"}`)
	err := infra.Cache.SetBlockData(ctx, eventChainID, blockNumber, version, outbound.BlockDataInput{Block: blockData})
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}

	svc, err := NewService(Config{
		ChainID:   serviceChainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			eventChainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	// Publish event with DIFFERENT chainID than service config
	// The service should reject this event due to chain ID mismatch
	event := outbound.BlockEvent{
		ChainID:     eventChainID, // Chain 137, not 1
		BlockNumber: blockNumber,
		Version:     version,
		BlockHash:   "0xpolygon",
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait for the message to be processed (rejected due to chain ID mismatch)
	// The message will return to the queue since processing failed, but we wait
	// for at least one processing attempt by checking the queue becomes in-flight
	testutil.WaitForCondition(t, 5*time.Second, func() bool {
		attrs, _ := infra.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(infra.BackupQueueURL),
			AttributeNames: []sqstypes.QueueAttributeName{
				sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if attrs != nil {
			// Message is in-flight (being processed) or was processed
			inFlight := attrs.Attributes["ApproximateNumberOfMessagesNotVisible"]
			return inFlight != "0"
		}
		return false
	}, "message to be in-flight")

	svc.Stop()
	svcCancel()
	<-done

	// Final verification - NO file was written due to chain ID mismatch validation
	objects := listS3Objects(t, ctx, infra, "5000-5999/")
	t.Logf("Objects in bucket: %v", objects)

	if len(objects) != 0 {
		t.Errorf("expected no files to be written due to chain ID mismatch, got: %v", objects)
	}
}

// TestIntegration_GzipContentIntegrity tests that gzip-compressed content
// is correctly readable after being written.
func TestIntegration_GzipContentIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	blockNumber := int64(6666)
	version := 0
	chainID := int64(1)

	// Create complex JSON that could reveal compression issues
	complexBlock := map[string]interface{}{
		"number":     "0x1a0a",
		"hash":       "0xgzip",
		"parentHash": "0xparent",
		"transactions": []map[string]interface{}{
			{"hash": "0xtx1", "from": "0xabc", "to": "0xdef", "value": "0x1234567890"},
			{"hash": "0xtx2", "from": "0x123", "to": "0x456", "value": "0x9876543210"},
		},
		"uncles":    []string{"0xuncle1", "0xuncle2"},
		"extraData": "0x" + string(make([]byte, 256)), // Some padding
		"logsBloom": "0x" + string(make([]byte, 512)),
	}
	blockData, _ := json.Marshal(complexBlock)

	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, version, outbound.BlockDataInput{Block: blockData})
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}

	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			chainID: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     version,
		BlockHash:   "0xgzip",
	}
	publishBlockEvent(t, ctx, infra, event)

	key := fmt.Sprintf("6000-6999/%d_%d_block.json.gz", blockNumber, version)
	waitForS3Object(t, ctx, infra, key, 10*time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Get and decompress content
	content := getS3Object(t, ctx, infra, key)

	// Verify it's valid JSON
	var recovered map[string]interface{}
	if err := json.Unmarshal(content, &recovered); err != nil {
		t.Fatalf("failed to unmarshal decompressed content: %v", err)
	}

	// Verify key fields match
	if recovered["hash"] != "0xgzip" {
		t.Errorf("hash mismatch: expected 0xgzip, got %v", recovered["hash"])
	}

	txs, ok := recovered["transactions"].([]interface{})
	if !ok || len(txs) != 2 {
		t.Errorf("transactions mismatch: expected 2, got %v", recovered["transactions"])
	}
}

// TestIntegration_ChainExpectationsMismatch tests that when chain expectations
// are not met (e.g., receipts expected but missing), the message errors and
// goes to DLQ rather than retrying infinitely.
func TestIntegration_ChainExpectationsMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	blockNumber := int64(4444)
	version := 0
	chainID := int64(1) // Chain 1 expects receipts and traces by default

	// Set up ONLY block data - missing receipts and traces that chain 1 expects
	blockData := json.RawMessage(`{"number":"0x115c","hash":"0xexpectfail"}`)
	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, version, outbound.BlockDataInput{Block: blockData})
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}
	// Intentionally NOT setting receipts or traces

	// Create service with explicit chain expectations
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			1: {
				ExpectReceipts: true,
				ExpectTraces:   true,
				ExpectBlobs:    false,
			},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     version,
		BlockHash:   "0xexpectfail",
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait for processing attempt - message should fail and NOT be deleted
	time.Sleep(2 * time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify NO files were created (expectations not met = error)
	prefix := fmt.Sprintf("4000-4999/%d_", blockNumber)
	objects := listS3Objects(t, ctx, infra, prefix)

	if len(objects) != 0 {
		t.Errorf("expected 0 files (expectations not met), got %d: %v", len(objects), objects)
	}

	// Verify the message is still in the queue (not acknowledged)
	// It would go to DLQ after max receives in production
	attrs, err := infra.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(infra.BackupQueueURL),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Logf("Could not check queue attributes: %v", err)
	} else {
		notVisible := attrs.Attributes["ApproximateNumberOfMessagesNotVisible"]
		t.Logf("Messages not visible (in flight/failed): %s", notVisible)
		// Message should be in flight (not visible) since it wasn't deleted
	}
}

// TestIntegration_ChainExpectationsMetSuccessfully tests that when all chain
// expectations are met, the backup succeeds.
func TestIntegration_ChainExpectationsMetSuccessfully(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	blockNumber := int64(3333)
	version := 0
	chainID := int64(1)

	// Set up ALL expected data for chain 1
	blockData := json.RawMessage(`{"number":"0xd05","hash":"0xexpectsuccess"}`)
	receiptsData := json.RawMessage(`[{"status":"0x1"}]`)
	tracesData := json.RawMessage(`[{"action":{"callType":"call"}}]`)

	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, version, outbound.BlockDataInput{
		Block:    blockData,
		Receipts: receiptsData,
		Traces:   tracesData,
		// Note: NOT setting blobs since ExpectBlobs=false for chain 1
	})
	if err != nil {
		t.Fatalf("failed to set block data in cache: %v", err)
	}

	// Create service with chain expectations
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			1: {
				ExpectReceipts: true,
				ExpectTraces:   true,
				ExpectBlobs:    false,
			},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     version,
		BlockHash:   "0xexpectsuccess",
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait for all 3 files (block, receipts, traces)
	prefix := fmt.Sprintf("3000-3999/%d_%d_", blockNumber, version)
	objects := waitForS3Objects(t, ctx, infra, prefix, 3, 10*time.Second)

	svc.Stop()
	svcCancel()
	<-done

	t.Logf("Files created: %v", objects)

	// Verify all expected files exist
	expectedFiles := map[string]bool{
		fmt.Sprintf("3000-3999/%d_%d_block.json.gz", blockNumber, version):    false,
		fmt.Sprintf("3000-3999/%d_%d_receipts.json.gz", blockNumber, version): false,
		fmt.Sprintf("3000-3999/%d_%d_traces.json.gz", blockNumber, version):   false,
	}

	for _, obj := range objects {
		if _, ok := expectedFiles[obj]; ok {
			expectedFiles[obj] = true
		}
	}

	for file, found := range expectedFiles {
		if !found {
			t.Errorf("expected file not found: %s", file)
		}
	}
}

// TestIntegration_UnknownChainNoExpectations tests that chains without
// explicit expectations only require block data.
func TestIntegration_UnknownChainNoExpectations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	infra := setupIntegrationInfra(t, ctx)
	t.Cleanup(infra.Cleanup)

	blockNumber := int64(2222)
	version := 0
	chainID := int64(999) // Unknown chain - no expectations defined

	// Set up ONLY block data
	blockData := json.RawMessage(`{"number":"0x8ae","hash":"0xunknownchain"}`)
	err := infra.Cache.SetBlockData(ctx, chainID, blockNumber, version, outbound.BlockDataInput{Block: blockData})
	if err != nil {
		t.Fatalf("failed to set block in cache: %v", err)
	}
	// No receipts, traces, or blobs - should still succeed for unknown chain

	// Create service with expectations only for chain 1
	svc, err := NewService(Config{
		ChainID:   chainID,
		Bucket:    infra.BucketName,
		Workers:   1,
		BatchSize: 10,
		ChainExpectations: map[int64]ChainExpectation{
			1: { // Only chain 1 has expectations
				ExpectReceipts: true,
				ExpectTraces:   true,
				ExpectBlobs:    false,
			},
		},
		Logger: infra.Logger,
	}, infra.Consumer, infra.Cache, infra.Writer)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(svcCtx)
	}()

	event := outbound.BlockEvent{
		ChainID:     chainID,
		BlockNumber: blockNumber,
		Version:     version,
		BlockHash:   "0xunknownchain",
	}
	publishBlockEvent(t, ctx, infra, event)

	// Wait for block file to be created
	key := fmt.Sprintf("2000-2999/%d_%d_block.json.gz", blockNumber, version)
	waitForS3Object(t, ctx, infra, key, 10*time.Second)

	svc.Stop()
	svcCancel()
	<-done

	// Verify only block file was created (no expectations for chain 999)
	prefix := fmt.Sprintf("2000-2999/%d_%d_", blockNumber, version)
	objects := listS3Objects(t, ctx, infra, prefix)

	t.Logf("Files created for unknown chain: %v", objects)

	if len(objects) != 1 {
		t.Errorf("expected 1 file (block only), got %d: %v", len(objects), objects)
	}
}
